# Canonical tick schema and nanosecond timestamp handling.
#
# All provider adapters normalize their wire formats into these types, so the
# rest of the pipeline (sinks, replay, analysis) is provider-agnostic.
# Timestamps are Int64 nanoseconds since the UNIX epoch (UTC): Dates.DateTime
# is millisecond-resolution and would silently destroy exchange timestamps.

"""
    Trade

A single normalized trade (tick).

Fields
- `symbol`     : ticker symbol
- `time_ns`    : exchange/participant timestamp, ns since UNIX epoch (UTC)
- `recv_ns`    : local wall-clock receive timestamp, ns since UNIX epoch (UTC).
                 The `time_ns → recv_ns` gap measures end-to-end latency and is
                 `0` for records that never crossed the wire (e.g. backfilled).
- `price`      : trade price
- `size`       : trade size (Float64 to accommodate fractional shares/crypto)
- `exchange`   : exchange code (provider-specific, e.g. "V" = IEX)
- `conditions` : trade condition codes (provider-specific)
- `tape`       : tape identifier ("A"/"B"/"C" for US equities, "SPOT" for Binance; may be empty)
- `id`         : provider trade id (0 when absent)
"""
struct Trade
    symbol::String
    time_ns::Int64
    recv_ns::Int64
    price::Float64
    size::Float64
    exchange::String
    conditions::Vector{String}
    tape::String
    id::Int64
end

"""
    Quote

One normalized top-of-book quote: the best bid and offer as a venue reported
them, with both clocks kept as for [`Trade`](@ref) — `time_ns` from the
exchange, `recv_ns` from local receipt.

Quotes are parsed but not subscribed to by default, and not persisted. A
quote stream carries an order of magnitude more messages than the trade
stream, so persisting it is a storage decision, not a parsing one; reach them
through the `on_quote` callback of [`live_source`](@ref).

Sizes are round lots as the tape reports them, not shares.
"""
struct Quote
    symbol::String
    time_ns::Int64
    recv_ns::Int64
    bid_price::Float64
    bid_size::Float64
    bid_exchange::String
    ask_price::Float64
    ask_size::Float64
    ask_exchange::String
    conditions::Vector{String}
    tape::String
end

"""
    Bar

One normalized aggregate bar: `time_ns` is the opening instant and `close_ns`
the closing one, both in nanoseconds since the Unix epoch on the exchange
clock, so a bar and the prints inside it share a time origin.

For an activity bar ([`tick_bars`](@ref), [`volume_bars`](@ref),
[`dollar_bars`](@ref)) `time_ns` is the timestamp of its first print and
`close_ns` that of the print that completed it. The duration
`close_ns - time_ns` is then a random variable — the time the market needed to
transact a fixed amount of activity — and is the observable a waiting-time
study reads off an activity clock. For a venue-computed bar `close_ns` is the
end of the venue's aggregation window.

Like [`Quote`](@ref), bars are parsed but not subscribed to by default. They
are a convenience the venue computes; anything a bar reports can be derived
from the prints this package records, and the derivation is reproducible
whereas the venue's aggregation rules are not fully observable.
"""
struct Bar
    symbol::String
    time_ns::Int64
    close_ns::Int64
    recv_ns::Int64
    open::Float64
    high::Float64
    low::Float64
    close::Float64
    volume::Float64
    trade_count::Int64
    vwap::Float64
end

# Value semantics for the three record types: the default struct `==` compares
# a `conditions` vector by identity, which breaks round-trip comparisons of
# otherwise equal records, and the default `isequal` would then follow `==`,
# so a `Bar` with a NaN `vwap` would not be equal to itself in a `Set`. All
# three compare, hash and `isequal` field by field in declaration order.
for T in (Trade, Quote, Bar)
    n = fieldcount(T)
    @eval _fields(x::$T) = ntuple(i -> getfield(x, i), Val($n))
    @eval Base.:(==)(a::$T, b::$T) = _fields(a) == _fields(b)
    @eval Base.isequal(a::$T, b::$T) = isequal(_fields(a), _fields(b))
    @eval Base.hash(x::$T, h::UInt) = hash(_fields(x), h)
end

const NS_PER_SEC = 1_000_000_000

const RFC3339_RE =
    r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:([Zz])|([+-])(\d{2}):?(\d{2}))$"

"""
    rfc3339_to_ns(s) -> Int64

Parse an RFC 3339 timestamp with up to nanosecond fractional precision into
Int64 nanoseconds since the UNIX epoch (UTC). Supports `Z` and `±hh:mm`
offsets. Throws `ArgumentError` on malformed input.
"""
function rfc3339_to_ns(s::AbstractString)
    m = match(RFC3339_RE, s)
    m === nothing && throw(ArgumentError("not an RFC 3339 timestamp: $s"))
    # `something` narrows the `Union{Nothing,SubString}` of a capture group:
    # groups 1–6 are mandatory in RFC3339_RE, so a `nothing` here is a bug.
    y, mo, d, h, mi, sec = (parse(Int, something(m[i])) for i in 1:6)
    dt = DateTime(y, mo, d, h, mi, sec)
    secs = round(Int64, datetime2unix(dt))          # exact: integer-second DateTime
    frac = m[7]
    frac_ns = frac === nothing ? 0 : parse(Int64, rpad(frac, 9, '0'))
    ns = secs * NS_PER_SEC + frac_ns
    if m[9] !== nothing                              # numeric offset → convert to UTC
        off =
            (parse(Int, something(m[10])) * 3600 + parse(Int, something(m[11])) * 60) *
            NS_PER_SEC
        ns -= m[9] == "+" ? off : -off
    end
    return ns
end

"""
    ns_to_rfc3339(ns) -> String

Render Int64 nanoseconds since the UNIX epoch as an RFC 3339 UTC timestamp
with full nanosecond precision (lossless round-trip with [`rfc3339_to_ns`](@ref)).
"""
function ns_to_rfc3339(ns::Int64)
    secs, frac = fldmod(ns, NS_PER_SEC)
    dt = unix2datetime(secs)
    return string(
        Dates.format(dt, dateformat"yyyy-mm-dd\THH:MM:SS"),
        ".",
        lpad(frac, 9, '0'),
        "Z",
    )
end

"""
    now_ns() -> Int64

Current wall-clock time as Int64 nanoseconds since the UNIX epoch (UTC),
built from microsecond-resolution system time.
"""
now_ns() = round(Int64, time() * 1e6) * 1_000

"""
    ns_to_datetime(ns) -> DateTime

Round a nanosecond epoch timestamp to a millisecond-resolution UTC
`DateTime` (for display and coarse grouping only, never for storage).
"""
ns_to_datetime(ns::Int64) = unix2datetime(ns / NS_PER_SEC)

"""
    trading_date(ns; tz) -> Date

Exchange-local calendar date of a nanosecond epoch timestamp; used to bucket
ticks into per-day files. `tz` defaults to America/New_York.
"""
function trading_date(ns::Int64; tz::TimeZone = tz"America/New_York")
    # Floor to whole seconds in integer arithmetic rather than going through
    # `ns_to_datetime`: that divides by 1e9 in floating point and rounds to the
    # millisecond, so a timestamp in the last half-millisecond of an exchange
    # day rounds up to midnight and files under the following date. US equity
    # tapes close at 20:00 local and never produce such a print, but a 24-hour
    # venue would, and this function decides which file a print lands in.
    zdt = ZonedDateTime(unix2datetime(fld(ns, NS_PER_SEC)), tz"UTC")
    return Date(astimezone(zdt, tz))
end

# The prints of a reader on their way out. With `each_page` set, every `rows`
# prints go to it as one page and none is kept; without it they accumulate in
# `kept`, which the reader returns as `trades`. `prints` counts both ways, so
# every reader returns the same `(; trades, prints, …)` whichever is used.
mutable struct TradePages{F}
    const each_page::F
    const rows::Int
    page::Vector{Trade}
    const kept::Vector{Trade}
    prints::Int
end

function TradePages(each_page, rows::Integer)
    rows >= 1 || throw(ArgumentError("page size must be at least 1, got $rows"))
    return TradePages(each_page, Int(rows), Trade[], Trade[], 0)
end

function Base.push!(p::TradePages, t::Trade)
    p.prints += 1
    if p.each_page === nothing
        push!(p.kept, t)
    else
        push!(p.page, t)
        length(p.page) >= p.rows && _hand_over!(p)
    end
    return p
end

# A page the source delivered whole, as a REST response does.
function Base.append!(p::TradePages, page::Vector{Trade})
    isempty(page) && return p
    p.prints += length(page)
    p.each_page === nothing ? append!(p.kept, page) : p.each_page(page)
    return p
end

# Hand the page in progress to `each_page`, short or not; a reader calls it
# once more when its source is exhausted. A fresh vector follows, since the
# consumer may keep the one it was given.
function _hand_over!(p::TradePages)
    (p.each_page === nothing || isempty(p.page)) && return p
    p.each_page(p.page)
    p.page = Trade[]
    return p
end
