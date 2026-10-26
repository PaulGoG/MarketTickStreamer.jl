# Nasdaq TotalView-ITCH 5.0 full-day files → Trade records.
#
# Framing: every message is prefixed by its 2-byte big-endian length; the
# file is gzip (as served from emi.nasdaq.com) or raw. Every message starts
# with type (1), stock locate (2), tracking number (2), timestamp (6 bytes,
# nanoseconds since midnight, US Eastern local time of the session date);
# integers are big-endian, prices carry four implied decimals, alphas are
# space-padded. An Order Executed message carries neither price nor symbol,
# so the reader keeps every live displayed order of the selected symbols and
# prices the execution at the resting order's display price.

const ITCH50_TZ = tz"America/New_York"

# A live displayed order: enough to price and file an execution against it.
struct ItchOrder
    locate::UInt16
    side::UInt8        # 'B', 'S', or 0x00 when a replace arrived without its original
    price::UInt32      # Price (4): four implied decimals
    shares::UInt32
end

# Big-endian integers and space-padded alphas at the 1-based index `i` of
# their first byte.
@inline _be16(b::Vector{UInt8}, i::Integer) = @inbounds (UInt16(b[i]) << 8) | UInt16(b[i+1])
@inline _be32(b::Vector{UInt8}, i::Integer) =
    (UInt32(_be16(b, i)) << 16) | UInt32(_be16(b, i + 2))
@inline _be48(b::Vector{UInt8}, i::Integer) =
    (UInt64(_be16(b, i)) << 32) | UInt64(_be32(b, i + 2))
@inline _be64(b::Vector{UInt8}, i::Integer) =
    (UInt64(_be32(b, i)) << 32) | UInt64(_be32(b, i + 4))
# Space-padded ASCII → String (allocates; used only in R, which is rare).
@inline _alpha(b::Vector{UInt8}, i::Integer, n::Integer) =
    @inbounds rstrip(String(b[i:(i+n-1)]))

# The tape of a symbol from the Stock Directory's Market Category byte. Nasdaq
# listings (Q G S) are tape C; under the CTA plan NYSE listings (N) are tape A
# and those of the other exchanges (A NYSE American, P NYSE Arca, Z Cboe BZX,
# V IEX) tape B.
function _itch50_tape(category::UInt8)::String
    category in (UInt8('Q'), UInt8('G'), UInt8('S')) && return "C"
    category == UInt8('N') && return "A"
    category in (UInt8('A'), UInt8('P'), UInt8('Z'), UInt8('V')) && return "B"
    return ""
end

# A known message type shorter than its specification: the file is corrupt.
@noinline _itch50_short(t::UInt8, len::Integer, expected::Integer) =
    error("ITCH $(Char(t)) message of $len bytes, expected $expected")

# Reduce a live order by `n` executed or cancelled shares, removing it once
# exhausted.
function _itch50_reduce!(
    orders::Dict{UInt64,ItchOrder},
    ref::UInt64,
    o::ItchOrder,
    n::UInt32,
)
    left = o.shares - min(o.shares, n)
    if left == 0
        delete!(orders, ref)
    else
        orders[ref] = ItchOrder(o.locate, o.side, o.price, left)
    end
    return nothing
end

# First instant of the session date in New York, ns since the epoch.
function _itch50_day_start_ns(date::Date)
    zdt = astimezone(ZonedDateTime(DateTime(date), ITCH50_TZ), tz"UTC")
    return round(Int64, datetime2unix(DateTime(zdt))) * NS_PER_SEC
end

"""
    itch50_date(name) -> Date

The session date encoded in an emi.nasdaq.com file name:
`MMDDYYYY.NASDAQ_ITCH50.gz` or `SMMDDYY-v50.txt.gz`. Throws `ArgumentError`
for any other name.
"""
function itch50_date(name::AbstractString)
    base = basename(name)
    m = match(r"^(\d{2})(\d{2})(\d{4})\.NASDAQ_ITCH50", base)
    if m !== nothing
        mm, dd, yyyy = (parse(Int, something(m[i])) for i in 1:3)
        return Date(yyyy, mm, dd)
    end
    m = match(r"^S(\d{2})(\d{2})(\d{2})-v50", base)
    if m !== nothing
        mm, dd, yy = (parse(Int, something(m[i])) for i in 1:3)
        return Date(2000 + yy, mm, dd)
    end
    throw(ArgumentError("cannot read a session date from \"$name\""))
end

# The file as a byte stream: gzip is detected by its magic, anything else is read raw.
function _open_day_file(path::AbstractString)
    io = open(path, "r")
    magic = read(io, 2)
    seekstart(io)
    return magic == UInt8[0x1f, 0x8b] ? GzipDecompressorStream(io) : io
end

# Fill `buf[len+1:end]` from `io`. Returns `(len, ended, cut)`: `ended` when
# the stream has no more bytes, `cut` when a gzip member ended early — the
# shape of a partial download — which ends the stream as well. Once `ended`,
# the stream is not read again: a cut gzip stream throws on every read.
function _fill!(io::IO, buf::Vector{UInt8}, len::Int)
    ended = false
    cut = false
    try
        while len < length(buf)
            if eof(io)
                ended = true
                break
            end
            n = min(length(buf) - len, bytesavailable(io))
            n == 0 && continue                      # eof(io) refilled the stream's buffer
            GC.@preserve buf unsafe_read(io, pointer(buf, len + 1), n)
            len += n
        end
    catch e
        e isa ZlibError || rethrow()
        ended = true
        cut = true
    end
    return len, ended, cut
end

# Walk the framed messages of `io`, calling `f(buf, m, len)` with `buf[m]` the
# type byte of a message of `len` bytes (buf[m:m+len-1] is the whole message).
# Returns (messages, truncated): `truncated` is true when the stream ended
# inside a length prefix or a message, or inside a gzip member, as a
# Range-request prefix does. A zero length ends the walk (end-of-session
# marker).
function _itch50_scan(f, io::IO; chunk::Integer = 8 << 20)
    buf = Vector{UInt8}(undef, chunk)
    len = 0           # buf[1:len] holds unparsed bytes
    pos = 1
    messages = 0
    truncated = false
    ended = false
    while true
        # Compact the unparsed tail to the front, then fill the buffer.
        if pos > 1
            n_keep = len - pos + 1
            n_keep > 0 && copyto!(buf, 1, buf, pos, n_keep)
            len = n_keep
            pos = 1
        end
        if !ended
            len, ended, cut = _fill!(io, buf, len)
            cut && (truncated = true)
        end
        # Parse every complete message in buf[pos:len].
        stop = false
        while true
            if pos + 1 > len                        # no complete length prefix
                ended && pos <= len && (truncated = true)
                break
            end
            mlen = Int(_be16(buf, pos))
            if mlen == 0
                stop = true
                break
            end
            if pos + 1 + mlen > len                 # message incomplete
                if ended
                    truncated = true
                elseif pos == 1 && len == length(buf)
                    error(
                        "ITCH message of $mlen bytes exceeds the $(length(buf))-byte read buffer",
                    )
                end
                break
            end
            f(buf, pos + 2, mlen)
            messages += 1
            pos += 2 + mlen
        end
        (stop || ended) && break
    end
    return (; messages, truncated)
end

# The match numbers voided by Broken Trade messages, over the whole file: a
# break may follow its print by hours, so a tape that drops or marks broken
# prints needs this before it emits anything.
function _itch50_voided(path::AbstractString)
    voided = Set{UInt64}()
    io = _open_day_file(path)
    try
        _itch50_scan(io) do buf, m, len
            buf[m] == UInt8('B') && push!(voided, _be64(buf, m + 11))
        end
    finally
        close(io)
    end
    return voided
end

"""
    itch50_trades(path, date; symbols = nothing, broken = :drop, each_page = nothing,
                  page_rows = 100_000)
        -> (; trades, messages, prints, broken, unresolved, symbols, truncated)

The executions of one Nasdaq TotalView-ITCH 5.0 day file as [`Trade`](@ref)
records, in file order. `date` is the session date (see [`itch50_date`](@ref));
timestamps are nanoseconds since that date's midnight in New York and are
filed in ns since the epoch. `path` may be the gzip file as served or the
raw bytes.

Prints are the Order Executed message (`E`, priced at the resting order's
display price), the printable Order Executed With Price (`C`), the hidden
Trade (`P`) and the Cross Trade with shares (`Q`); non-printable `C` and
zero-share `Q` are not prints, by Nasdaq's own rule. Each print carries
`exchange = "Q"`, `tape` from the Stock Directory's market category, `id` =
match number, and `conditions` = a kind token (`displayed`, `improved`,
`hidden`, `cross_open`, `cross_close`, `cross_halt`, `cross_intraday`), the
aggressor side (`buy`/`sell`) for `E` and `C`, and `"I"` when the size is
below the symbol's round lot — the sale condition the SIP gives the same
execution, so `price_forming` treats both alike.

`symbols` restricts the output (and the order map) to those symbols;
`nothing` reads every symbol in the file. `broken` decides what a Broken
Trade message does to the print it voids: `:drop` (default) omits it,
`:mark` appends `"broken"` to its conditions, `:keep` leaves it as it was;
the first two read the file twice, the voided ids being collected first.

`each_page` receives pages of `page_rows` prints as they are decoded and the
returned `trades` is then empty; without it the prints are accumulated in
`trades`. `messages` counts every framed message, `prints` the emitted
prints, `broken` the prints voided by a Broken Trade message (under `:keep`,
the Broken Trade messages seen), `unresolved` the `E` messages
whose order was unknown (a file cut before its Add Order), `symbols` the
selected symbols found in the directory, and `truncated` whether the file
ended inside a message, as a partial download does.
"""
function itch50_trades(
    path::AbstractString,
    date::Date;
    symbols::Union{Nothing,AbstractVector{<:AbstractString}} = nothing,
    broken::Symbol = :drop,
    each_page = nothing,
    page_rows::Integer = 100_000,
)
    page_rows >= 1 || throw(ArgumentError("page_rows must be at least 1, got $page_rows"))
    broken in (:drop, :mark, :keep) ||
        throw(ArgumentError("broken must be :drop, :mark or :keep, got $(repr(broken))"))
    wanted = symbols === nothing ? nothing : Set{String}(uppercase.(String.(symbols)))
    voided = broken === :keep ? Set{UInt64}() : _itch50_voided(path)

    day0 = _itch50_day_start_ns(date)
    # Stock Directory, indexed by locate + 1.
    names = fill("", 65536)
    tapes = fill("", 65536)
    lots = zeros(UInt32, 65536)
    selected = falses(65536)
    orders = Dict{UInt64,ItchOrder}()
    sizehint!(orders, 1 << 20)
    prints = Ref(0)
    n_broken = Ref(0)
    unresolved = Ref(0)
    n_symbols = Ref(0)
    acc = each_page === nothing ? Trade[] : nothing
    page = Ref(Trade[])

    function emit!(t::Trade)
        push!(page[], t)
        if length(page[]) >= page_rows
            acc === nothing ? each_page(page[]) : append!(acc, page[])
            page[] = Trade[]
        end
        return nothing
    end

    function print!(
        locate,
        ts,
        price4::UInt32,
        shares::Integer,
        match_number::UInt64,
        kind::String,
        side::UInt8,
    )
        k = locate + 1
        is_voided = match_number in voided
        if is_voided && broken === :drop
            n_broken[] += 1
            return nothing
        end
        c = [kind]
        side == UInt8('B') && push!(c, "sell")
        side == UInt8('S') && push!(c, "buy")
        lot = lots[k]
        lot > 0 && shares < lot && push!(c, "I")
        if is_voided
            push!(c, "broken")
            n_broken[] += 1
        end
        emit!(
            Trade(
                names[k],
                day0 + Int64(ts),
                0,
                price4 / 1e4,
                Float64(shares),
                "Q",
                c,
                tapes[k],
                Int64(match_number),
            ),
        )
        prints[] += 1
        return nothing
    end

    io = _open_day_file(path)
    scan = try
        _itch50_scan(io) do buf, m, len
            t = buf[m]
            if t == UInt8('R')
                len >= 39 || _itch50_short(t, len, 39)
                k = _be16(buf, m + 1) + 1
                name = _alpha(buf, m + 11, 8)
                names[k] = name
                tapes[k] = _itch50_tape(buf[m+19])
                lots[k] = _be32(buf, m + 21)
                sel = wanted === nothing || name in wanted
                selected[k] = sel
                sel && (n_symbols[] += 1)
            elseif t == UInt8('A') || t == UInt8('F')
                expected = t == UInt8('A') ? 36 : 40
                len >= expected || _itch50_short(t, len, expected)
                locate = _be16(buf, m + 1)
                selected[locate+1] || return nothing
                ref = _be64(buf, m + 11)
                orders[ref] =
                    ItchOrder(locate, buf[m+19], _be32(buf, m + 32), _be32(buf, m + 20))
            elseif t == UInt8('E')
                len >= 31 || _itch50_short(t, len, 31)
                locate = _be16(buf, m + 1)
                selected[locate+1] || return nothing
                ref = _be64(buf, m + 11)
                n = _be32(buf, m + 19)
                o = get(orders, ref, nothing)
                if o === nothing
                    unresolved[] += 1
                    return nothing
                end
                ts = _be48(buf, m + 5)
                print!(locate, ts, o.price, n, _be64(buf, m + 23), "displayed", o.side)
                _itch50_reduce!(orders, ref, o, n)
            elseif t == UInt8('C')
                len >= 36 || _itch50_short(t, len, 36)
                locate = _be16(buf, m + 1)
                selected[locate+1] || return nothing
                ref = _be64(buf, m + 11)
                n = _be32(buf, m + 19)
                o = get(orders, ref, nothing)
                if buf[m+31] == UInt8('Y')
                    side = o === nothing ? 0x00 : o.side
                    ts = _be48(buf, m + 5)
                    price = _be32(buf, m + 32)
                    print!(locate, ts, price, n, _be64(buf, m + 23), "improved", side)
                end
                o === nothing || _itch50_reduce!(orders, ref, o, n)
            elseif t == UInt8('X')
                len >= 23 || _itch50_short(t, len, 23)
                locate = _be16(buf, m + 1)
                selected[locate+1] || return nothing
                ref = _be64(buf, m + 11)
                o = get(orders, ref, nothing)
                o === nothing || _itch50_reduce!(orders, ref, o, _be32(buf, m + 19))
            elseif t == UInt8('D')
                len >= 19 || _itch50_short(t, len, 19)
                selected[_be16(buf, m+1)+1] || return nothing
                delete!(orders, _be64(buf, m + 11))
            elseif t == UInt8('U')
                len >= 35 || _itch50_short(t, len, 35)
                locate = _be16(buf, m + 1)
                selected[locate+1] || return nothing
                ref_orig = _be64(buf, m + 11)
                ref_new = _be64(buf, m + 19)
                n = _be32(buf, m + 27)
                price = _be32(buf, m + 31)
                o = pop!(orders, ref_orig, nothing)
                orders[ref_new] =
                    ItchOrder(locate, o === nothing ? 0x00 : o.side, price, n)
            elseif t == UInt8('P')
                len >= 44 || _itch50_short(t, len, 44)
                locate = _be16(buf, m + 1)
                selected[locate+1] || return nothing
                ts = _be48(buf, m + 5)
                n = _be32(buf, m + 20)
                price = _be32(buf, m + 32)
                print!(locate, ts, price, n, _be64(buf, m + 36), "hidden", 0x00)
            elseif t == UInt8('Q')
                len >= 40 || _itch50_short(t, len, 40)
                locate = _be16(buf, m + 1)
                selected[locate+1] || return nothing
                shares = _be64(buf, m + 11)
                shares == 0 && return nothing
                cross = buf[m+39]
                kind = if cross == UInt8('O')
                    "cross_open"
                elseif cross == UInt8('C')
                    "cross_close"
                elseif cross == UInt8('H')
                    "cross_halt"
                elseif cross == UInt8('I')
                    "cross_intraday"
                else
                    "cross"
                end
                ts = _be48(buf, m + 5)
                price = _be32(buf, m + 27)
                print!(locate, ts, price, shares, _be64(buf, m + 31), kind, 0x00)
            elseif t == UInt8('B')
                len >= 19 || _itch50_short(t, len, 19)
                # In the other modes the voided set accounts for the break,
                # counted at the print.
                broken === :keep && (n_broken[] += 1)
            end
            return nothing
        end
    finally
        close(io)
    end

    if !isempty(page[])
        acc === nothing ? each_page(page[]) : append!(acc, page[])
    end
    return (;
        trades = acc === nothing ? Trade[] : acc,
        messages = scan.messages,
        prints = prints[],
        broken = n_broken[],
        unresolved = unresolved[],
        symbols = n_symbols[],
        truncated = scan.truncated,
    )
end

"""
    fetch_itch50(url, dir; policy = RestPolicy()) -> String | Nothing

Download one full-day ITCH 5.0 file from `emi.nasdaq.com/ITCH/` into
`dir/itch50/`, resuming an interrupted transfer, and return its path; a URL
the server does not know (404) returns `nothing`. The server publishes no
checksum, so the file's size is checked against the `Content-Length` it
announces; a size that does not match is an error and the file is removed.
A file already present with the announced size is reused.

```julia
fetch_itch50("https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/12302019.NASDAQ_ITCH50.gz", cfg.bulk_dir)
```
"""
function fetch_itch50(
    url::AbstractString,
    dir::AbstractString;
    policy::RestPolicy = RestPolicy(),
)
    target = joinpath(dir, "itch50")
    mkpath(target)
    path = joinpath(target, HTTP.unescapeuri(basename(String(url))))
    expected = _announced_length(url, policy)
    expected === nothing && return nothing
    isfile(path) && filesize(path) == expected && return path
    r = _download_resumable(url, path, policy)
    r.found || return nothing
    got = filesize(path)
    if got != expected
        rm(path; force = true)
        error(
            "size mismatch for $(basename(path)): announced $expected bytes, got $got; " *
            "the download was discarded",
        )
    end
    return path
end

# The Content-Length a HEAD request announces, or `nothing` on 404. The
# server answers a HEAD with a Range header 416, so none is sent.
function _announced_length(url::AbstractString, policy::RestPolicy)
    resp = _with_retry(policy) do
        r = HTTP.head(
            url;
            retry = false,
            status_exception = false,
            readtimeout = policy.request_timeout_s,
            connect_timeout = policy.connect_timeout_s,
        )
        r.status in RETRYABLE_STATUS &&
            throw(HTTP.StatusError(r.status, "HEAD", String(url), r))
        r
    end::HTTP.Response
    resp.status == 404 && return nothing
    resp.status == 200 || throw(HTTP.StatusError(resp.status, "HEAD", String(url), resp))
    len = HTTP.header(resp, "Content-Length", "")
    isempty(len) && error("no Content-Length announced for $url")
    return parse(Int64, len)
end
