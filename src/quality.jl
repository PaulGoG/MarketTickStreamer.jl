# Data-quality safeguards: duplicate removal and session auditing.
#
# Reconnections can double-deliver ticks and backfills can overlap live
# captures; neither may silently contaminate the analysis layer. Likewise a
# "successful" session with hidden gaps, out-of-order delivery, or clock skew
# must be visible before any science is done on it.

# Identity of a tick for deduplication: provider id alone is not unique
# across symbols/tapes, and id may be 0, so use the full print identity.
const TickKey = Tuple{String, Int64, Int64, Float64, Float64, String}
_tick_key(t::Trade) = (t.symbol, t.time_ns, t.id, t.price, t.size, t.exchange)::TickKey

"""
    dedup_trades(trades) -> Vector{Trade}

Remove exact duplicate prints (same symbol, exchange timestamp, id, price,
size, exchange), keeping first occurrence and preserving order. Guards
against reconnection double-delivery and overlapping backfill/live captures.
"""
function dedup_trades(trades::AbstractVector{Trade})
    seen = Set{TickKey}()
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
            median_latency_ms = isempty(lats) ? NaN : round(median(lats); digits = 3),
            n_negative_latency = count(<(0.0), lats),
        ))
    end
    return DataFrame(rows)
end

session_report(path::AbstractString; kwargs...) = session_report([path]; kwargs...)

"""
    free_disk_gb(path) -> Float64

Free disk space (GiB) on the filesystem containing `path`.
"""
free_disk_gb(path::AbstractString) = Base.diskstat(path).available / 2^30

"""
    check_resident_memory(limit_mb; context = "") -> Nothing

Config-gated RAM ceiling: if the live heap exceeds `limit_mb`, force a
garbage collection; if it still exceeds the ceiling, fail loudly with a
message naming `context` — a graceful stop beats an OOM kill. Call from
long accumulation loops (backfill pages, compaction groups).
"""
function check_resident_memory(limit_mb::Real; context::AbstractString = "")
    live_mb = Base.gc_live_bytes() / 2^20
    live_mb > limit_mb || return nothing
    GC.gc()
    live_mb = Base.gc_live_bytes() / 2^20
    live_mb > limit_mb && error(
        "live heap $(round(live_mb; digits = 0)) MiB exceeds limits.max_resident_mb = " *
        "$(limit_mb)" * (isempty(context) ? "" : " during $context") *
        " — stopping before the OS kills the process")
    return nothing
end

"""
    coverage_report(paths, provider; feed = "sip", page_limit = 10_000,
                    rate_sleep_s = 0.35) -> DataFrame

Compare a live capture against the historical tape: for every symbol in the
raw NDJSON `paths`, count captured trades (after exact-duplicate removal)
and query the provider's historical trade count over the same inclusive
exchange-time window. `coverage = captured / reference`; values below 1
quantify feed coverage and stream drops, values above 1 indicate duplicate
or spurious prints that dedup did not catch.
"""
function coverage_report(paths::AbstractVector{<:AbstractString}, provider;
                         feed::AbstractString = "sip", page_limit::Integer = 10_000,
                         rate_sleep_s::Real = 0.35)
    trades = dedup_trades(read_raw(paths))
    rows = NamedTuple[]
    for sym in sort(unique(t.symbol for t in trades))
        ts = [t.time_ns for t in trades if t.symbol == sym]
        lo, hi = extrema(ts)
        reference = historical_trade_count(provider, sym,
            ns_to_rfc3339(lo), ns_to_rfc3339(hi); feed, page_limit, rate_sleep_s)
        push!(rows, (; symbol = sym, captured = length(ts), reference,
                       coverage = reference == 0 ? NaN :
                                  round(length(ts) / reference; digits = 4)))
    end
    return DataFrame(rows)
end

coverage_report(path::AbstractString, provider; kwargs...) =
    coverage_report([path], provider; kwargs...)
