# Data-quality safeguards: duplicate removal and session auditing.
#
# Reconnections can double-deliver ticks and backfills can overlap live
# captures; neither may silently contaminate the analysis layer. Likewise a
# "successful" session with hidden gaps, out-of-order delivery, or clock skew
# must be visible before any science is done on it.

# Identity of a tick for deduplication: provider id alone is not unique
# across symbols/tapes, and id may be 0, so use the full print identity.
_tick_key(t::Trade) = (t.symbol, t.time_ns, t.id, t.price, t.size, t.exchange)

"""
    dedup_trades(trades) -> Vector{Trade}

Remove exact duplicate prints (same symbol, exchange timestamp, id, price,
size, exchange), keeping first occurrence and preserving order. Guards
against reconnection double-delivery and overlapping backfill/live captures.
"""
function dedup_trades(trades::AbstractVector{Trade})
    seen = Set{NTuple{6, Any}}()
    out = Trade[]
    sizehint!(out, length(trades))
    for t in trades
        k = _tick_key(t)
        k in seen && continue
        push!(seen, k)
        push!(out, t)
    end
    return out
end

"""
    session_report(paths; gap_threshold_s = 60.0) -> DataFrame

Audit raw session files and return one row per symbol:

- `n_trades`, `n_duplicates` (exact duplicate prints)
- `first_time`, `last_time` (UTC, ms precision, from exchange timestamps)
- `n_out_of_order` (exchange timestamps decreasing in arrival order)
- `max_gap_s` / `n_gaps` (largest / count of exchange-time gaps exceeding
  `gap_threshold_s` — judge against the instrument's typical activity;
  quiet symbols gap naturally)
- `median_latency_ms` / `n_negative_latency` (receive minus exchange time,
  live-captured rows only; negative values indicate clock skew. `NaN` when
  the file is pure backfill)

Inspect this before trusting any captured session.
"""
function session_report(paths::AbstractVector{<:AbstractString}; gap_threshold_s::Real = 60.0)
    trades = read_raw(paths)
    rows = NamedTuple[]
    for sym in sort(unique(t.symbol for t in trades))
        ts = [t for t in trades if t.symbol == sym]
        n = length(ts)
        ndup = n - length(dedup_trades(ts))
        n_ooo = count(i -> ts[i].time_ns < ts[i - 1].time_ns, 2:n)
        sorted_ns = sort!([t.time_ns for t in ts])
        gaps = n < 2 ? Float64[] : diff(sorted_ns) ./ NS_PER_SEC
        big = filter(>(float(gap_threshold_s)), gaps)
        lats = [(t.recv_ns - t.time_ns) / 1e6 for t in ts if t.recv_ns > 0]
        push!(rows, (;
            symbol = sym,
            n_trades = n,
            n_duplicates = ndup,
            first_time = ns_to_datetime(sorted_ns[1]),
            last_time = ns_to_datetime(sorted_ns[end]),
            n_out_of_order = n_ooo,
            max_gap_s = isempty(gaps) ? 0.0 : round(maximum(gaps); digits = 3),
            n_gaps = length(big),
            median_latency_ms = isempty(lats) ? NaN : round(median_(lats); digits = 3),
            n_negative_latency = count(<(0.0), lats),
        ))
    end
    return DataFrame(rows)
end

session_report(path::AbstractString; kwargs...) = session_report([path]; kwargs...)

# Median without a Statistics dependency on the hot path's package.
function median_(xs::Vector{Float64})
    s = sort(xs)
    n = length(s)
    return isodd(n) ? s[(n + 1) ÷ 2] : (s[n ÷ 2] + s[n ÷ 2 + 1]) / 2
end

"""
    free_disk_gb(path) -> Float64

Free disk space (GiB) on the filesystem containing `path`.
"""
free_disk_gb(path::AbstractString) = Base.diskstat(path).available / 2^30
