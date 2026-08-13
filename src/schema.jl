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
    y, mo, d, h, mi, sec = (parse(Int, m[i]) for i in 1:6)
    dt = DateTime(y, mo, d, h, mi, sec)
    secs = round(Int64, datetime2unix(dt))          # exact: integer-second DateTime
    frac = m[7]
    frac_ns = frac === nothing ? 0 : parse(Int64, rpad(frac, 9, '0'))
    ns = secs * NS_PER_SEC + frac_ns
    if m[9] !== nothing                              # numeric offset → convert to UTC
        off = (parse(Int, m[10]) * 3600 + parse(Int, m[11]) * 60) * NS_PER_SEC
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
