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
- `tape`       : tape identifier ("A"/"B"/"C" for US equities; may be empty)
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

# Value semantics: the default struct `==` compares the `conditions` vector
# by identity, which breaks round-trip comparisons of otherwise equal ticks.
Base.:(==)(a::Trade, b::Trade) =
    a.symbol == b.symbol &&
    a.time_ns == b.time_ns &&
    a.recv_ns == b.recv_ns &&
    a.price == b.price &&
    a.size == b.size &&
    a.exchange == b.exchange &&
    a.conditions == b.conditions &&
    a.tape == b.tape &&
    a.id == b.id

Base.hash(t::Trade, h::UInt) = hash(
    (
        t.symbol,
        t.time_ns,
        t.recv_ns,
        t.price,
        t.size,
        t.exchange,
        t.conditions,
        t.tape,
        t.id,
    ),
    h,
)

"""
    Quote

One normalized top-of-book quote: the best bid and offer as a venue reported
them, with both clocks kept as for [`Trade`](@ref) — `time_ns` from the
exchange, `recv_ns` from local receipt.

Quotes are parsed but not subscribed to by default, and not persisted. A
quote stream runs an order of magnitude above the trade stream in message
count, which is a storage decision rather than a parsing one; reach them
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

# Value semantics, for the same reason as `Trade`: the default comparison
# would compare `conditions` by identity.
Base.:(==)(a::Quote, b::Quote) =
    a.symbol == b.symbol &&
    a.time_ns == b.time_ns &&
    a.recv_ns == b.recv_ns &&
    a.bid_price == b.bid_price &&
    a.bid_size == b.bid_size &&
    a.bid_exchange == b.bid_exchange &&
    a.ask_price == b.ask_price &&
    a.ask_size == b.ask_size &&
    a.ask_exchange == b.ask_exchange &&
    a.conditions == b.conditions &&
    a.tape == b.tape

Base.hash(q::Quote, h::UInt) = hash(
    (
        q.symbol,
        q.time_ns,
        q.recv_ns,
        q.bid_price,
        q.bid_size,
        q.bid_exchange,
        q.ask_price,
        q.ask_size,
        q.ask_exchange,
        q.conditions,
        q.tape,
    ),
    h,
)

"""
    Bar

One normalized aggregate bar. `time_ns` is the bar's opening instant, not its
close, so a bar and the prints inside it share a time origin.

Like [`Quote`](@ref), bars are parsed but not subscribed to by default. They
are a convenience the venue computes; anything a bar reports can be derived
from the prints this package records, and the derivation is reproducible
whereas the venue's aggregation rules are not fully observable.
"""
struct Bar
    symbol::String
    time_ns::Int64
    recv_ns::Int64
    open::Float64
    high::Float64
    low::Float64
    close::Float64
    volume::Float64
    trade_count::Int64
    vwap::Float64
end

Base.:(==)(a::Bar, b::Bar) =
    a.symbol == b.symbol &&
    a.time_ns == b.time_ns &&
    a.recv_ns == b.recv_ns &&
    a.open == b.open &&
    a.high == b.high &&
    a.low == b.low &&
    a.close == b.close &&
    a.volume == b.volume &&
    a.trade_count == b.trade_count &&
    a.vwap == b.vwap

Base.hash(b::Bar, h::UInt) = hash(
    (
        b.symbol,
        b.time_ns,
        b.recv_ns,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,
        b.trade_count,
        b.vwap,
    ),
    h,
)

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

Truncate a nanosecond epoch timestamp to a millisecond-resolution UTC
`DateTime` (for display and coarse grouping only — not for storage).
"""
ns_to_datetime(ns::Int64) = unix2datetime(ns / NS_PER_SEC)

"""
    trading_date(ns; tz) -> Date

Exchange-local calendar date of a nanosecond epoch timestamp; used to bucket
ticks into per-day files. `tz` defaults to America/New_York.
"""
function trading_date(ns::Int64; tz::TimeZone = tz"America/New_York")
    zdt = ZonedDateTime(ns_to_datetime(ns), tz"UTC")
    return Date(astimezone(zdt, tz))
end
