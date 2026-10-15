# Live WebSocket source: connect → auth → subscribe → stream, with jittered
# exponential-backoff reconnection and a hard session deadline.
#
# The producer feeds a bounded Channel{Trade}; if consumers stall, `put!`
# blocks and TCP backpressure does the rest — no unbounded buffering.

"""
    AbstractProvider

Supertype for market-data provider adapters.

A concrete adapter supplies [`provider_spec`](@ref) and [`make_provider`](@ref)
for the name it answers to, [`exchange_tz`](@ref), [`stream_protocol!`](@ref),
[`market_clock`](@ref), [`historical_trades`](@ref) and its own message
parsing. It overrides [`always_open`](@ref), [`session_days`](@ref) and
[`feed_delay_ns`](@ref) where the venue departs from the defaults.

Two adapters ship: `providers/alpaca.jl` (US equities, New York calendar,
credentialed, closes overnight and at weekends) and `providers/binance.jl`
(crypto spot, UTC calendar, public, never closes). They differ on every one of
those axes, which is what keeps the interface honest.
"""
abstract type AbstractProvider end

"""
    ProviderSpec(feeds, backfill_feeds, bulk_feeds, tz, needs_credentials, max_page_limit,
                 regular_session, price_unit, size_unit, timestamp_unit)

What the configuration layer must know about a provider *before* any provider
object exists: which feed names it accepts, the calendar its tape is filed
under, whether it needs credentials at all, the largest REST page the venue
serves, and the venue's regular session.

Without this the config layer has to hard-code one vendor's vocabulary, which
is how `provider.feed` came to be validated against Alpaca's feed names for
every provider, and how a 24-hour venue would have had its days cut at
midnight in New York.

`bulk_feeds` names the feeds the venue also publishes as daily bulk files,
which [`bulk_trades`](@ref) downloads; it is empty for a venue without such
a service, and the configuration refuses `backfill.source = "bulk"` for it.

`max_page_limit` exists because a venue may clamp an over-limit request
instead of rejecting it, and a clamped page is indistinguishable from the last
page of the tape.

`regular_session` is `(open, close)` in exchange-local hours, `(0.0, 24.0)`
for a venue that never closes. The multi-day figures lay their days out on
it; nothing in acquisition depends on it, since session railings come from
the venue's own clock.

`price_unit` and `size_unit` are what the venue's prices and sizes are
denominated in, as the figure axes should name them: dollars and shares on a
US equity tape, quote asset and base asset on a crypto pair.

`timestamp_unit` is the resolution of the venue's exchange timestamps as the
adapter receives them — `"nanosecond"` on the SIP, `"microsecond"` on
Binance, which answers in milliseconds unless asked. It is written into
every session sidecar, so the clock a capture was quantized to is known
after the fact.
"""
struct ProviderSpec
    feeds::Vector{String}
    backfill_feeds::Vector{String}
    bulk_feeds::Vector{String}
    tz::TimeZone
    needs_credentials::Bool
    max_page_limit::Int
    regular_session::Tuple{Float64,Float64}
    price_unit::String
    size_unit::String
    timestamp_unit::String
end

"""Provider names this build knows how to construct."""
const KNOWN_PROVIDERS = ("alpaca", "binance")

"""
    provider_spec(::Val{name}) -> ProviderSpec

Static description of the provider called `name`. Implemented per adapter.
"""
provider_spec(::Val{P}) where {P} = throw(
    ArgumentError(
        "unknown provider \"$(P)\"; known providers: $(join(KNOWN_PROVIDERS, ", "))",
    ),
)

provider_spec(name::AbstractString) = provider_spec(Val(Symbol(name)))

"""
    make_provider(::Val{name}, cfg, key, secret) -> AbstractProvider

Construct the provider called `name` from configuration and credentials.
`key`/`secret` are empty strings when the spec says none are needed.
Implemented per adapter.

The fallback throws rather than returning anything: a fallback with a value
would widen every caller's inferred provider type to include it, and then
each provider method downstream would carry a branch with no matching method
— which is how JET found this the first time it was written otherwise.
"""
make_provider(::Val{P}, ::Config, ::AbstractString, ::AbstractString) where {P} = throw(
    ArgumentError(
        "unknown provider \"$(P)\"; known providers: $(join(KNOWN_PROVIDERS, ", "))",
    ),
)

"""
    exchange_tz(p::AbstractProvider) -> TimeZone

The calendar the provider's tape is filed under — the clock that decides which
date a print belongs to. Defaults to the provider's [`ProviderSpec`](@ref).
"""
function exchange_tz end

"""
    exchange_day_start_ns(p::AbstractProvider, d::Date) -> Int64

First instant of exchange date `d` on this provider's calendar, in ns since
the epoch.

A request window and the key its rows are bucketed by must come from the same
clock. Building the window on UTC days while filing rows on exchange dates
agrees only while the exchange sits at UTC-4, and silently loses an hour a day
otherwise — the defect fixed in 0.2.0. `ZonedDateTime` resolves the offset per
date, including across the 23- and 25-hour transition days.
"""
function exchange_day_start_ns(p::AbstractProvider, d::Date)
    tz = exchange_tz(p)
    return round(
        Int64,
        datetime2unix(DateTime(astimezone(ZonedDateTime(DateTime(d), tz), tz"UTC"))),
    ) * NS_PER_SEC
end

"""
    RestPolicy(request_timeout_s, connect_timeout_s, max_retries; rng = Xoshiro())
    RestPolicy()                 # 30 s, 10 s, 5 retries
    RestPolicy(cfg::Config)      # the `[rest]` table, jitter seeded from `[rng] seed`

How a provider's REST requests are bounded: the read timeout and the connection
timeout, both in whole seconds, and the number of retries on a transient
failure. Without a read timeout a request against a server that accepts the
connection and then goes silent blocks its task for good, and with it a
backfill. `rng` draws the jitter of the retry backoff; two policies are equal
when their bounds are, the generator being state rather than policy.
"""
struct RestPolicy
    request_timeout_s::Int
    connect_timeout_s::Int
    max_retries::Int
    rng::Xoshiro
    function RestPolicy(
        request_timeout_s::Integer,
        connect_timeout_s::Integer,
        max_retries::Integer;
        rng::Xoshiro = Xoshiro(),
    )
        request_timeout_s >= 1 ||
            throw(ArgumentError("request_timeout_s must be >= 1, got $request_timeout_s"))
        connect_timeout_s >= 1 ||
            throw(ArgumentError("connect_timeout_s must be >= 1, got $connect_timeout_s"))
        max_retries >= 0 ||
            throw(ArgumentError("max_retries must be >= 0, got $max_retries"))
        return new(request_timeout_s, connect_timeout_s, max_retries, rng)
    end
end

Base.:(==)(a::RestPolicy, b::RestPolicy) =
    a.request_timeout_s == b.request_timeout_s &&
    a.connect_timeout_s == b.connect_timeout_s &&
    a.max_retries == b.max_retries
Base.hash(p::RestPolicy, h::UInt) =
    hash(p.request_timeout_s, hash(p.connect_timeout_s, hash(p.max_retries, h)))

RestPolicy() = RestPolicy(30, 10, 5)
# The REST generator takes the master seed's neighbour, so REST retries and
# stream reconnects draw from independent streams of one recorded seed.
RestPolicy(cfg::Config) = RestPolicy(
    cfg.rest_request_timeout_s,
    cfg.rest_connect_timeout_s,
    cfg.rest_max_retries;
    rng = Xoshiro(UInt64(cfg.rng_seed) ⊻ 0x1),
)

# Jittered exponential backoff: `base · 2^attempt`, capped at `cap`, scaled by
# a uniform factor in [0.5, 1.5) drawn from `rng`.
_backoff_delay(rng, base::Real, attempt::Integer, cap::Real) =
    min(base * 2.0^attempt, cap) * (0.5 + rand(rng))

# GET with exponential-backoff retry on transient statuses (rate limiting,
# server hiccups). Client errors other than 429 fail immediately. Shared by
# every REST adapter; `Retry-After` is honored when the server sends it.
# Requests are bounded by the caller's `RestPolicy` timeouts, and a transport
# failure — refused connection, read timeout, broken stream — retries on the
# same schedule.
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
                @warn "REST $(e.status) — backing off" attempt delay =
                    round(delay; digits = 1)
            else
                @warn "REST transport failure — backing off" attempt delay =
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

# Fetch `url` into `path`, resuming a `<path>.partial` left by an earlier
# attempt with a Range request. A server that answers 206 appends to it, one
# that ignores the range (200) restarts the transfer; 404 returns `false`
# with nothing written; a failed transfer keeps the partial file for the next
# attempt. Returns `(; found, headers)`: the response headers of the
# transfer that completed, for the caller's integrity check.
function _download_resumable(url::AbstractString, path::AbstractString, policy::RestPolicy)
    tmp = path * ".partial"
    have = isfile(tmp) ? filesize(tmp) : 0
    headers = have > 0 ? ["Range" => "bytes=$(have)-"] : Pair{String,String}[]
    found = true
    resp_headers = Pair{String,String}[]
    HTTP.open(
        "GET",
        url,
        headers;
        retry = false,
        status_exception = false,
        readtimeout = policy.request_timeout_s,
        connect_timeout = policy.connect_timeout_s,
    ) do http
        HTTP.startread(http)
        st = http.message.status
        resp_headers = [String(k) => String(v) for (k, v) in http.message.headers]
        if st == 404
            found = false
        elseif st == 206 || st == 200
            open(tmp, st == 206 ? "a" : "w") do f
                while !eof(http)
                    write(f, readavailable(http))
                end
            end
        else
            throw(HTTP.StatusError(st, "GET", String(url), http.message))
        end
    end
    if !found
        rm(tmp; force = true)
        return (; found = false, headers = resp_headers)
    end
    mv(tmp, path; force = true)
    return (; found = true, headers = resp_headers)
end

"""
    always_open(p::AbstractProvider) -> Bool

Whether the venue never closes. `true` suppresses the market-hours railings:
there is no open to wait for and no close to stop at, and a 24-hour venue
asked for its `next_close` can only answer with a fiction. The session is then
bounded by `limits.max_session_hours` alone.
"""
always_open(::AbstractProvider) = false

"""
    session_days(p::AbstractProvider, start_date, end_date) -> Vector{Date}

The dates in `[start_date, end_date]` on which the venue trades, in order.

Defaults to every calendar date. A venue that rests must opt out, never the
reverse: requesting a day the venue was shut costs one empty response, while
skipping a day it traded loses that day's tape silently — which is what a
hard-coded weekday filter did to a 24-hour venue before this was dispatched.
"""
session_days(::AbstractProvider, start_date::Date, end_date::Date) =
    collect(start_date:Day(1):end_date)

"""
    bulk_trades(p::AbstractProvider, symbol, date; feed, dest_dir,
                each_page = nothing, page_rows = 100_000) -> Vector{Trade} | Int

The trades of `symbol` on exchange date `date` from the venue's daily bulk
file, for a provider whose [`ProviderSpec`](@ref) lists `feed` in
`bulk_feeds`. The file is downloaded into `dest_dir`, verified against the
venue's checksum and kept there as the vendor's record of the day; a
present, verified file is reused without a request. Rows are parsed in
pages of `page_rows`: with `each_page` every page is passed to it and only
the row count is returned, otherwise the trades are accumulated. A day the
venue has not published yields no trades. Backfilled prints carry
`recv_ns = 0`.

The default method throws: a venue without bulk files cannot be asked for
one.
"""
bulk_trades(p::AbstractProvider, ::AbstractString, ::Date; kwargs...) =
    throw(ArgumentError("provider $(nameof(typeof(p))) publishes no bulk files"))

"""
    feed_delay_ns(p::AbstractProvider) -> Int64

Intrinsic delay of the provider's configured feed (ns): the wall-clock lag
between an exchange event and its earliest possible arrival on the wire.
Zero for real-time feeds. Market-hours railings shift by this amount so a
delayed session waits out the silent post-open window and captures the
delayed tape tail after the close.
"""
feed_delay_ns(::AbstractProvider) = Int64(0)

"""
    FatalStreamError(msg)

A stream error that must NOT trigger reconnection (bad credentials,
connection-limit exceeded, invalid subscription). Aborts the session.
"""
struct FatalStreamError <: Exception
    msg::String
end

# Alpaca error codes that retrying cannot fix.
const FATAL_WS_CODES = (401, 402, 403, 404, 405, 406, 409, 410, 411)

"""
    LiveSession

Handle for a running live stream: the tick channel plus control state.
Obtain via [`live_source`](@ref); request shutdown with [`stop!`](@ref).

`stop` is atomic: it is written by whoever ends the session and read by the
producer, the watchdog and the guard tasks, which run on other threads. The
logging layer reads the same flag to mute transport-teardown noise, so the flag
belongs to one session and not to the process.
"""
struct LiveSession
    channel::Channel{Trade}
    stop::Threads.Atomic{Bool}
    ws::Ref{Any}
    stats::Ref{NamedTuple{(:ticks, :frames, :reconnects),NTuple{3,Int}}}
end

"""
    stop!(s::LiveSession)

Signal graceful shutdown: sets the stop flag and closes the underlying
WebSocket, which unblocks the producer loop; the tick channel then closes,
letting sinks drain and finish.
"""
function stop!(s::LiveSession)
    s.stop[] = true
    ws = s.ws[]
    if ws !== nothing
        try
            close(ws)
        catch
        end
        # close() is a handshake; an unresponsive peer that never acks the
        # CLOSE frame would hang the read loop, so sever the transport after
        # a short grace if it is still open.
        Threads.@spawn begin
            sleep(5.0)
            try
                close(ws.io)
            catch
            end
        end
    end
    return nothing
end

# Guard task: gracefully stop the session once the wall clock passes `stop_ns`
# (ns since epoch). Shared by the market-close railing and the session
# deadline. Returns the task; it exits early if the session stops first.
function _stop_at!(s::LiveSession, stop_ns::Int64, message::AbstractString)
    return Threads.@spawn begin
        while !s.stop[]
            remaining = (stop_ns - now_ns()) / 1e9
            remaining <= 0 && break
            sleep(min(remaining, 5.0))
        end
        if !s.stop[]
            @info message
            stop!(s)
        end
    end
end

"""
    schedule_close_stop!(s::LiveSession, close_ns; grace_s = 5.0) -> Task

Spawn a guard task that gracefully [`stop!`](@ref)s the session once the
wall clock passes `close_ns` (ns since epoch, e.g. the market's
`next_close`) plus `grace_s`. Without this, a streamer left unattended sits
on a silent overnight connection until the session deadline.
"""
function schedule_close_stop!(s::LiveSession, close_ns::Int64; grace_s::Real = 5.0)
    return _stop_at!(
        s,
        close_ns + round(Int64, grace_s * NS_PER_SEC),
        "market close reached — stopping session",
    )
end

"""
    stream_protocol!(ch, p::AbstractProvider, cfg, s::LiveSession;
                     on_quote = nothing, on_bar = nothing)

Provider interface: run one connect→auth→subscribe→stream cycle, pushing
normalized `Trade`s into `ch`. Must return on orderly close, throw
[`FatalStreamError`](@ref) on non-retryable protocol errors, and any other
exception on retryable transport failures. Implemented per provider
(see `providers/alpaca.jl`).

`on_quote` and `on_bar`, when given, receive normalized [`Quote`](@ref) and
[`Bar`](@ref) values for the corresponding frames. Neither channel is
subscribed to by default, so neither callback fires unless
`stream.channels` asks for it.
"""
function stream_protocol! end

# Watchdog: force-close `ws` if `last_frame[]` goes stale — HTTP.jl 1.x
# websockets have no read idle timeout, and a silently dead TCP connection
# would otherwise block the read loop forever. Providers call this inside
# their protocol loop; set `alive[] = false` and `wait` it before returning.
# `last_frame` and `alive` are atomics: the protocol loop writes them, this
# task reads them from another thread.
function spawn_watchdog(
    ws,
    s::LiveSession,
    last_frame::Threads.Atomic{Float64},
    alive::Threads.Atomic{Bool},
    stale_timeout_s::Float64,
)
    return Threads.@spawn begin
        while alive[] && !s.stop[]
            if time() - last_frame[] > stale_timeout_s
                @warn "no frames for $(stale_timeout_s)s — closing stale connection"
                try
                    close(ws)
                catch
                end
                break
            end
            sleep(0.25)   # fine-grained so connection teardown isn't held up
        end
    end
end

# Lock-free single-writer stats bump (only the producer task mutates).
function bump!(s::LiveSession; ticks = 0, frames = 0, reconnects = 0)
    st = s.stats[]
    s.stats[] = (;
        ticks = st.ticks + ticks,
        frames = st.frames + frames,
        reconnects = st.reconnects + reconnects,
    )
    return nothing
end

"""
    live_source(p::AbstractProvider, cfg::Config;
                on_quote = nothing, on_bar = nothing,
                stop = Threads.Atomic{Bool}(false)) -> LiveSession

Start the live producer task. Streams ticks into `session.channel` until the
session deadline (`limits.max_session_hours`), a [`stop!`](@ref) call, a
fatal protocol error, or reconnection exhaustion — whichever comes first.
The channel is closed on exit so downstream consumers terminate cleanly.

`on_quote` and `on_bar` are handed normalized [`Quote`](@ref) and
[`Bar`](@ref) values when those channels are subscribed to
(`stream.channels`); neither is by default, and neither is persisted — a
quote stream carries an order of magnitude more messages than the trade
stream, which is a storage decision taken separately.

`stop` lets the caller own the session's stop flag, which [`run_stream`](@ref)
shares with its logger.

Reconnects with jittered exponential backoff
(`stream.reconnect_base_delay_s * 2^attempt`, capped at
`stream.reconnect_max_delay_s`, the jitter drawn from a generator seeded by
`rng.seed`); the attempt counter resets after any connection that actually
delivered data.
"""
function live_source(
    p::AbstractProvider,
    cfg::Config;
    on_quote = nothing,
    on_bar = nothing,
    stop::Threads.Atomic{Bool} = Threads.Atomic{Bool}(false),
)
    ch = Channel{Trade}(cfg.channel_capacity)
    session = LiveSession(
        ch,
        stop,
        Ref{Any}(nothing),
        Ref((; ticks = 0, frames = 0, reconnects = 0)),
    )
    deadline = time() + cfg.max_session_hours * 3600
    # The reconnect loop below only tests the deadline between connections; a
    # healthy connection never returns to it, so a guard task enforces the
    # limit while the stream is up.
    _stop_at!(
        session,
        now_ns() + round(Int64, cfg.max_session_hours * 3600 * NS_PER_SEC),
        "session deadline reached — stopping session",
    )
    Threads.@spawn begin
        attempt = 0
        rng = Xoshiro(UInt64(cfg.rng_seed))
        try
            while !session.stop[] && time() < deadline
                ticks_before = session.stats[].ticks
                try
                    stream_protocol!(ch, p, cfg, session; on_quote, on_bar)
                    session.stop[] || @warn "stream closed by server"
                catch e
                    (e isa FatalStreamError || e isa InterruptException) && rethrow()
                    session.stop[] && break
                    @warn "stream disconnected" exception = (e, catch_backtrace())
                end
                session.stop[] && break
                session.stats[].ticks > ticks_before && (attempt = 0)
                attempt += 1
                if attempt > cfg.reconnect_max_retries
                    @error "reconnection attempts exhausted" attempt
                    break
                end
                bump!(session; reconnects = 1)
                delay = _backoff_delay(
                    rng,
                    cfg.reconnect_base_delay_s,
                    attempt - 1,
                    cfg.reconnect_max_delay_s,
                )
                @info "reconnecting" attempt delay = round(delay; digits = 1)
                sleep(delay)
            end
        catch e
            e isa FatalStreamError ? (@error "fatal stream error — aborting" e.msg) :
            rethrow()
        finally
            close(ch)
        end
    end
    return session
end
