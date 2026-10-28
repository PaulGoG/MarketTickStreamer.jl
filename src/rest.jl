# REST requests and vendor-file downloads: the per-request policy (timeouts,
# retries, rate cap, connections), the retry schedule shared by every
# adapter, and the resumable Range downloader the vendor fetchers use.

# Concurrent Range requests per vendor file: a ceiling of courtesy to the
# vendors' hosts, well inside HTTP.jl's connection pool (16 per host).
const MAX_DOWNLOAD_CONNECTIONS = 8

"""
    RestPolicy(request_timeout_s, connect_timeout_s, max_retries;
               rng = Xoshiro(), max_download_rate = Inf, download_connections = 1)
    RestPolicy()                 # 30 s, 10 s, 5 retries, no download cap, one connection
    RestPolicy(cfg::Config)      # the `[rest]` table, jitter seeded from `[rng] seed`

How a provider's REST requests are bounded: the read timeout and the connection
timeout, both in whole seconds, and the number of retries on a transient
failure. Without a read timeout a request against a server that accepts the
connection and then goes silent blocks its task for good, and with it a
backfill. `max_download_rate` caps the transfer rate of vendor files (Binance
dumps, ITCH and IEX day files) in bytes per second, so a multi-gigabyte
download does not take the whole of a shared link; `Inf` leaves it uncapped.
`download_connections` (1 to 8) is the number of
Range requests a vendor file is fetched on at once, for hosts whose path
limits each TCP connection; the cap applies to their sum.
`rng` draws the jitter of the retry backoff; two policies are equal when their
bounds are, the generator being state rather than policy.
"""
struct RestPolicy
    request_timeout_s::Int
    connect_timeout_s::Int
    max_retries::Int
    rng::Xoshiro
    max_download_rate::Float64
    download_connections::Int
    function RestPolicy(
        request_timeout_s::Integer,
        connect_timeout_s::Integer,
        max_retries::Integer;
        rng::Xoshiro = Xoshiro(),
        max_download_rate::Real = Inf,
        download_connections::Integer = 1,
    )
        request_timeout_s >= 1 ||
            throw(ArgumentError("request_timeout_s must be >= 1, got $request_timeout_s"))
        connect_timeout_s >= 1 ||
            throw(ArgumentError("connect_timeout_s must be >= 1, got $connect_timeout_s"))
        max_retries >= 0 ||
            throw(ArgumentError("max_retries must be >= 0, got $max_retries"))
        max_download_rate > 0 || throw(
            ArgumentError(
                "max_download_rate must be positive (Inf for no cap), got $max_download_rate",
            ),
        )
        1 <= download_connections <= MAX_DOWNLOAD_CONNECTIONS || throw(
            ArgumentError(
                "download_connections must be in 1:$MAX_DOWNLOAD_CONNECTIONS, got $download_connections",
            ),
        )
        return new(
            request_timeout_s,
            connect_timeout_s,
            max_retries,
            rng,
            max_download_rate,
            download_connections,
        )
    end
end

Base.:(==)(a::RestPolicy, b::RestPolicy) =
    a.request_timeout_s == b.request_timeout_s &&
    a.connect_timeout_s == b.connect_timeout_s &&
    a.max_retries == b.max_retries &&
    a.max_download_rate == b.max_download_rate &&
    a.download_connections == b.download_connections
Base.hash(p::RestPolicy, h::UInt) = hash(
    p.request_timeout_s,
    hash(
        p.connect_timeout_s,
        hash(p.max_retries, hash(p.max_download_rate, hash(p.download_connections, h))),
    ),
)

RestPolicy() = RestPolicy(30, 10, 5)
# The REST generator takes the master seed's neighbour, so REST retries and
# stream reconnects draw from independent streams of one recorded seed.
# `max_download_mb_s` is in 10⁶ bytes per second, 0 meaning no cap.
RestPolicy(cfg::Config) = RestPolicy(
    cfg.rest_request_timeout_s,
    cfg.rest_connect_timeout_s,
    cfg.rest_max_retries;
    rng = Xoshiro(UInt64(cfg.rng_seed) ⊻ 0x1),
    max_download_rate = cfg.rest_max_download_mb_s > 0 ?
                        1e6 * cfg.rest_max_download_mb_s : Inf,
    download_connections = cfg.rest_download_connections,
)

# Jittered exponential backoff: `base · 2^attempt`, capped at `cap`, scaled by
# a uniform factor in [0.5, 1.5) drawn from `rng`.
_backoff_delay(rng, base::Real, attempt::Integer, cap::Real) =
    min(base * 2.0^attempt, cap) * (0.5 + rand(rng))

# HTTP statuses retried with backoff: rate limiting (429) and transient
# server errors. Other client errors fail at once.
const RETRYABLE_STATUS = (429, 500, 502, 503, 504)

# Transport failures worth retrying: the connection could not be made, the
# server went silent past the read timeout, or the stream broke mid-response.
_transient(e) =
    e isa HTTP.ConnectError ||
    e isa HTTP.TimeoutError ||
    e isa HTTP.RequestError ||
    e isa Base.IOError ||
    e isa EOFError

# Run `f` under the policy's retry schedule: a retryable HTTP status or a
# transport failure is retried with exponential backoff (or the server's
# Retry-After), anything else propagates at once.
function _with_retry(f, policy::RestPolicy)
    for attempt in 0:policy.max_retries
        try
            return f()
        catch e
            status = e isa HTTP.StatusError && e.status in RETRYABLE_STATUS
            ((status || _transient(e)) && attempt < policy.max_retries) || rethrow()
            ra =
                status ? tryparse(Float64, HTTP.header(e.response, "Retry-After", "")) :
                nothing
            delay = ra !== nothing ? ra : _backoff_delay(policy.rng, 1.0, attempt, Inf)
            if status
                @warn "REST status $(e.status); retrying" attempt delay =
                    round(delay; digits = 1)
            else
                @warn "REST transport failure; retrying" attempt delay =
                    round(delay; digits = 1) exception = e
            end
            sleep(min(delay, 60.0))
        end
    end
end

_get_with_retry(url, headers; query = nothing, policy::RestPolicy = RestPolicy()) =
    _with_retry(policy) do
        HTTP.get(
            url,
            headers;
            query,
            retry = false,
            readtimeout = policy.request_timeout_s,
            connect_timeout = policy.connect_timeout_s,
        )
    end

# Fetch `url` into `path` in bounded Range requests of `chunk_bytes`, resuming
# a `<path>.partial` left by an earlier attempt. HTTP.jl's read timeout bounds
# a whole request, so one request per chunk keeps a transfer of hours inside
# the policy's timeouts, and a dropped connection costs one chunk, retried on
# the policy's schedule. A server that ignores the range (200) streams the
# whole body in one request; 404 returns `found = false` with nothing
# written; 416 past the end means the partial file is already complete.
# The body is read at no more than the policy's `max_download_rate`. With
# `download_connections > 1`, once a 206 has named the total length the rest
# is fetched on that many connections (`_download_parallel!`). Progress is
# logged every `progress_s` seconds.
# Returns `(; found, headers)`, the headers of the last response, for the
# caller's integrity check.
function _download_resumable(
    url::AbstractString,
    path::AbstractString,
    policy::RestPolicy;
    chunk_bytes::Integer = 8 << 20,
    progress_s::Real = 300,
)
    chunk_bytes >= 1 || throw(ArgumentError("chunk_bytes must be >= 1, got $chunk_bytes"))
    tmp = path * ".partial"
    found = true
    resp_headers = Pair{String,String}[]
    total = Int64(-1)                   # from Content-Range, once a 206 names it
    mark = Ref((time(), isfile(tmp) ? filesize(tmp) : Int64(0)))
    progress(bytes) = _log_progress!(mark, path, bytes, total, progress_s)
    while true
        before = isfile(tmp) ? filesize(tmp) : Int64(0)
        total >= 0 && before >= total && break
        if total >= 0 && policy.download_connections > 1 && total - before > chunk_bytes
            _download_parallel!(
                url,
                tmp,
                before,
                total,
                policy,
                Int64(chunk_bytes);
                progress,
            )
            continue
        end
        done = false
        # The offset is read inside the retry: a request that failed mid-body
        # has already appended what it received, and the next attempt must
        # continue from there, not from where the failed one started.
        _with_retry(policy) do
            have = isfile(tmp) ? filesize(tmp) : Int64(0)
            bad_start = Int64(-1)       # a 206 that did not start at `have`
            bad_status = 0
            bad_message = Ref{Any}(nothing)
            HTTP.open(
                "GET",
                url,
                ["Range" => "bytes=$(have)-$(have + chunk_bytes - 1)"];
                retry = false,
                status_exception = false,
                readtimeout = _chunk_timeout(policy, chunk_bytes),
                connect_timeout = policy.connect_timeout_s,
            ) do http
                HTTP.startread(http)
                st = http.message.status
                resp_headers = [String(k) => String(v) for (k, v) in http.message.headers]
                if st == 404
                    found = false
                    done = true
                elseif st == 206
                    range = HTTP.header(http.message, "Content-Range", "")
                    start = _content_range_start(range)
                    if start != have
                        bad_start = start           # raised outside the handler: not a transport error
                    else
                        total = _content_range_total(range)
                        open(tmp, "a") do f
                            _paced_copy!(f, http, policy.max_download_rate)
                        end
                    end
                elseif st == 200                    # the range was ignored: the whole body
                    open(tmp, "w") do f
                        _paced_copy!(f, http, policy.max_download_rate)
                    end
                    done = true
                elseif st == 416                    # nothing past `have`: complete
                    done = true
                else
                    bad_status = st             # raised outside the handler, see below
                    bad_message[] = http.message
                end
            end
            # Raised here and not inside the handler: an exception thrown there
            # arrives wrapped as a transport failure and would be retried on
            # every status, where `_with_retry` retries only RETRYABLE_STATUS.
            bad_status != 0 &&
                throw(HTTP.StatusError(bad_status, "GET", String(url), bad_message[]))
            bad_start >= 0 && throw(
                VendorError(
                    String(url),
                    "$url answered the range from byte $bad_start, not the requested $have: " *
                    "refusing to splice",
                ),
            )
        end
        done && break
        filesize(tmp) > before ||
            throw(VendorError(String(url), "no progress downloading $url at byte $before"))
        progress(filesize(tmp))
    end
    if !found
        rm(tmp; force = true)
        return (; found = false, headers = resp_headers)
    end
    mv(tmp, path; force = true)
    return (; found = true, headers = resp_headers)
end

# Bytes `have:total-1` of `url` appended to `tmp` on `download_connections`
# connections. Each connection claims the next chunk, fetches it into memory
# (`_fetch_range`) and hands it over; chunks are appended strictly in order, so
# `tmp` remains a prefix of the remote file and an interrupted transfer
# resumes as a sequential one would, losing at most the chunks in memory. A
# connection waits rather than run more than `2n` chunks ahead of the file,
# which bounds the memory held at about `3n` chunks. The rate cap is shared
# out evenly. The first failure (after the policy's retries) ends the
# transfer and is rethrown; requests in flight finish on their own.
# `progress` is called with the length of `tmp` after each append.
function _download_parallel!(
    url::AbstractString,
    tmp::AbstractString,
    have::Int64,
    total::Int64,
    policy::RestPolicy,
    chunk_bytes::Int64;
    progress = _ -> nothing,
)
    n = policy.download_connections
    rate = policy.max_download_rate / n
    starts = have:chunk_bytes:(total-1)
    window = 2n
    fetched = Dict{Int,Vector{UInt8}}()
    next = Ref(1)                       # the next chunk to claim
    written = Ref(0)                    # chunks appended to `tmp`
    failure = Ref{Any}(nothing)         # the first error, rethrown below
    cond = Condition()
    # Every task runs on the caller's thread (`@async`), so the shared state
    # changes only between yields.
    function connection()
        try
            while failure[] === nothing && next[] <= length(starts)
                k = next[]
                next[] += 1
                while k - written[] > window && failure[] === nothing
                    wait(cond)
                end
                failure[] === nothing || break
                a = starts[k]
                fetched[k] =
                    _fetch_range(url, a, min(a + chunk_bytes, total) - 1, policy, rate)
                notify(cond)
            end
        catch e
            failure[] === nothing && (failure[] = e)
            notify(cond)
        end
        return nothing
    end
    for _ in 1:min(n, length(starts))
        @async connection()
    end
    try
        for k in eachindex(starts)
            while !haskey(fetched, k)
                failure[] === nothing || throw(failure[])
                wait(cond)
            end
            chunk = pop!(fetched, k)
            open(f -> write(f, chunk), tmp, "a")
            written[] = k
            notify(cond)
            progress(starts[k] + length(chunk))
        end
    catch e
        failure[] === nothing && (failure[] = e)
        notify(cond)
        rethrow()
    end
    return nothing
end

# Bytes `a:b` of `url` at no more than `rate` bytes per second, in one Range
# request retried on the policy's schedule from the last byte received. Only a
# 206 that starts where asked is accepted.
function _fetch_range(
    url::AbstractString,
    a::Int64,
    b::Int64,
    policy::RestPolicy,
    rate::Float64,
)
    len = b - a + 1
    buf = IOBuffer(; sizehint = len)
    while position(buf) < len
        got = position(buf)
        _with_retry(policy) do
            from = a + position(buf)
            resp = HTTP.Response(0)
            start = Int64(-1)
            HTTP.open(
                "GET",
                url,
                ["Range" => "bytes=$(from)-$(b)"];
                retry = false,
                status_exception = false,
                readtimeout = _chunk_timeout(policy, b - from + 1; rate),
                connect_timeout = policy.connect_timeout_s,
            ) do http
                HTTP.startread(http)
                resp = http.message
                if resp.status == 206
                    start = _content_range_start(HTTP.header(resp, "Content-Range", ""))
                    start == from && _paced_copy!(buf, http, rate)
                end
            end
            # judged outside the handler, where an error is not a transport failure
            resp.status in RETRYABLE_STATUS &&
                throw(HTTP.StatusError(resp.status, "GET", String(url), resp))
            resp.status == 206 || throw(
                VendorError(
                    String(url),
                    "$url answered bytes $from-$b with status $(resp.status), not 206",
                ),
            )
            start == from || throw(
                VendorError(
                    String(url),
                    "$url answered the range from byte $start, not the requested $from: " *
                    "refusing to splice",
                ),
            )
        end
        position(buf) > got || throw(
            VendorError(String(url), "no progress downloading $url at byte $(a + got)"),
        )
    end
    position(buf) == len ||
        throw(VendorError(String(url), "$url sent more than bytes $a-$b"))
    return take!(buf)
end

# Log a transfer's progress once `every` seconds have passed since the last
# report, whose time and byte count `mark` holds.
function _log_progress!(mark, path, bytes::Integer, total::Integer, every::Real)
    t0, b0 = mark[]
    t = time()
    t - t0 >= every || return nothing
    received_gb = round(bytes / 1e9; digits = 2)
    total_gb = total < 0 ? missing : round(total / 1e9; digits = 2)
    mb_s = round((bytes - b0) / max(t - t0, 1e-3) / 1e6; digits = 2)
    @info "downloading" file = basename(path) received_gb total_gb mb_s
    mark[] = (t, bytes)
    return nothing
end

# The request timeout of one chunk: the policy's, or longer when the chunk
# would not arrive within it at 50 kB/s, the slowest host worth waiting for,
# or at the download cap (`rate`, the policy's by default) when that is lower
# still.
_chunk_timeout(
    policy::RestPolicy,
    chunk_bytes::Integer;
    rate::Float64 = policy.max_download_rate,
) = max(policy.request_timeout_s, ceil(Int, chunk_bytes / min(50_000.0, rate)))

# Copy a response body to `io` at no more than `rate` bytes per second,
# measured from the start of the request: whenever the bytes received run
# ahead of that schedule the reader sleeps, and TCP flow control holds the
# sender back. Pacing per request means a stall or a retry backoff earns no
# burst afterwards.
function _paced_copy!(io::IO, http, rate::Float64)
    t0 = time()
    n = 0
    while !eof(http)
        n += write(io, readavailable(http))
        if isfinite(rate)
            ahead = n / rate - (time() - t0)
            ahead > 0 && sleep(ahead)
        end
    end
    return n
end

# The first byte and the total length named by a `Content-Range: bytes
# a-b/total` header, or -1 for either when the header does not say.
function _content_range_start(value::AbstractString)
    m = match(r"bytes\s+(\d+)-\d+/", value)
    return m === nothing ? Int64(-1) : parse(Int64, m[1])
end
function _content_range_total(value::AbstractString)
    m = match(r"bytes\s+\d+-\d+/(\d+)", value)
    return m === nothing ? Int64(-1) : parse(Int64, m[1])
end
