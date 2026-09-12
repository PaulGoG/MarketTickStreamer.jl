# Data-quality safeguards: duplicate removal and session auditing.
#
# Reconnections can double-deliver ticks and backfills can overlap live
# captures; neither may silently contaminate the analysis layer. Likewise a
# "successful" session with hidden gaps, out-of-order delivery, or clock skew
# must be visible before any science is done on it.

# Identity of a tick for deduplication: provider id alone is not unique
# across symbols/tapes, and id may be 0, so use the full print identity.
const TickKey = Tuple{String,Int64,Int64,Float64,Float64,String}
_tick_key(t::Trade) = (t.symbol, t.time_ns, t.id, t.price, t.size, t.exchange)::TickKey

"""
    NON_PRICE_CONDITIONS

Sale-condition codes that never update a bar's open or close price, keyed by
tape: `"A"` and `"B"` are CTA-processed, `"C"` is UTP-processed, `"O"` is the
OTC tape. The same character means different things on different tapes, which
is why the lists are not shared.

Taken from Alpaca's published lists for its own normalization of the tapes
(verified 2026-09-13), not transcribed from the CTA and UTP plan
specifications: the plans define raw codes, and every vendor normalizes them.
The authoritative decoder for the codes themselves is the provider's own
[`condition_map`](@ref).

Two things this is deliberately not. It is not the plans' "last-sale
eligible" flag, and it is not a per-field eligibility table — the plans track
high/low, open/close, volume and last-sale eligibility separately, and a
print may update some and not others. It answers one question, the one a
price path needs answered: may this print set a price.

Note `"9"` (corrected consolidated close): excluded here because at tick
resolution it is a correction message rather than an execution, though for a
daily bar it is precisely the official close.
"""
const NON_PRICE_CONDITIONS = Dict{String,Vector{String}}(
    "A" => ["B", "C", "H", "I", "M", "Q", "R", "U", "V", "7", "9"],
    "B" => ["B", "C", "H", "I", "M", "Q", "R", "U", "V", "7", "9"],
    "C" => ["C", "H", "I", "M", "Q", "R", "U", "V", "7", "9"],
    "O" => ["C", "I", "N", "R", "U", "V"],
)

"""
    price_forming(t::Trade; non_price = NON_PRICE_CONDITIONS) -> Bool

Whether `t` may set a price, i.e. whether none of its sale conditions is
listed for its tape in `non_price`. A print carrying several conditions is
disqualified by any one of them, which is the precedence rule both tape plans
state: a single "does not update" overrides every permissive condition beside
it.

A print whose tape has no entry in `non_price` is kept. An unknown tape means
no basis on which to exclude, and silently discarding prints on that ground
would be the worse error; [`session_report`](@ref) counts what survives, so
the effect stays visible.

This is an analysis-time predicate. Capture is never filtered — the raw layer
records every print the venue reported, and which subset constitutes "a
trade" is a decision each analysis makes for itself. It is also not one
decision: a price path wants price-forming prints only, whereas an arrival
process or a waiting-time distribution counts every execution, odd lots and
contingent trades included. Filtering the capture would foreclose the second
question to answer the first.
"""
function price_forming(t::Trade; non_price::AbstractDict = NON_PRICE_CONDITIONS)
    excluded = get(non_price, t.tape, nothing)
    excluded === nothing && return true
    for c in t.conditions
        c in excluded && return false
    end
    return true
end

"""
    filter_price_forming(trades; non_price = NON_PRICE_CONDITIONS) -> Vector{Trade}

Keep only the prints for which [`price_forming`](@ref) holds, preserving
order. Use on a loaded capture, never on the way in.
"""
filter_price_forming(
    trades::AbstractVector{Trade};
    non_price::AbstractDict = NON_PRICE_CONDITIONS,
) = filter(t -> price_forming(t; non_price), trades)

"""
    deduplicate_trades(trades) -> Vector{Trade}

Remove exact duplicate prints (same symbol, exchange timestamp, id, price,
size, exchange), keeping first occurrence and preserving order. Guards
against reconnection double-delivery and overlapping backfill/live captures.
"""
function deduplicate_trades(trades::AbstractVector{Trade})
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
- `n_price_forming` (prints that may set a price, see [`price_forming`](@ref);
  the difference from `n_trades` is odd lots, contingent and derivatively
  priced trades, corrections and the like — present in the tape, and in the
  arrival process, but not in a price path)
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
function session_report(
    paths::AbstractVector{<:AbstractString};
    gap_threshold_s::Real = 60.0,
    non_price::AbstractDict = NON_PRICE_CONDITIONS,
)
    groups = Dict{String,Vector{Trade}}()
    for t in read_raw(paths)
        push!(get!(() -> Trade[], groups, t.symbol), t)
    end
    rows = NamedTuple[]
    for sym in sort!(collect(keys(groups)))
        ts = groups[sym]
        n = length(ts)
        ndup = n - length(deduplicate_trades(ts))
        n_ooo = count(i -> ts[i].time_ns < ts[i-1].time_ns, 2:n)
        sorted_ns = sort!([t.time_ns for t in ts])
        gaps = n < 2 ? Float64[] : diff(sorted_ns) ./ NS_PER_SEC
        big = filter(>(float(gap_threshold_s)), gaps)
        lats = [(t.recv_ns - t.time_ns) / 1e6 for t in ts if t.recv_ns > 0]
        push!(
            rows,
            (;
                symbol = sym,
                n_trades = n,
                n_duplicates = ndup,
                n_price_forming = count(t -> price_forming(t; non_price), ts),
                first_time = ns_to_datetime(sorted_ns[1]),
                last_time = ns_to_datetime(sorted_ns[end]),
                n_out_of_order = n_ooo,
                max_gap_s = isempty(gaps) ? 0.0 : round(maximum(gaps); digits = 3),
                n_gaps = length(big),
                median_latency_ms = isempty(lats) ? NaN : round(median(lats); digits = 3),
                n_negative_latency = count(<(0.0), lats),
            ),
        )
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
    check_live_heap(limit_mb; context = "") -> Nothing

Config-gated RAM ceiling: if the live heap exceeds `limit_mb`, force a
garbage collection; if it still exceeds the ceiling, fail loudly with a
message naming `context` — a graceful stop beats an OOM kill. Call from
long accumulation loops (backfill pages, compaction groups).
"""
function check_live_heap(limit_mb::Real; context::AbstractString = "")
    live_mb = Base.gc_live_bytes() / 2^20
    live_mb > limit_mb || return nothing
    GC.gc()
    live_mb = Base.gc_live_bytes() / 2^20
    live_mb > limit_mb && error(
        "live heap $(round(live_mb; digits = 0)) MiB exceeds limits.max_live_heap_mb = " *
        "$(limit_mb)" *
        (isempty(context) ? "" : " during $context") *
        " — stopping before the OS kills the process",
    )
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
function coverage_report(
    paths::AbstractVector{<:AbstractString},
    provider;
    feed::AbstractString = "sip",
    page_limit::Integer = 10_000,
    rate_sleep_s::Real = 0.35,
)
    groups = Dict{String,Vector{Int64}}()
    for t in deduplicate_trades(read_raw(paths))
        push!(get!(() -> Int64[], groups, t.symbol), t.time_ns)
    end
    rows = NamedTuple[]
    for sym in sort!(collect(keys(groups)))
        ts = groups[sym]
        lo, hi = extrema(ts)
        reference = historical_trade_count(
            provider,
            sym,
            ns_to_rfc3339(lo),
            ns_to_rfc3339(hi);
            feed,
            page_limit,
            rate_sleep_s,
        )
        push!(
            rows,
            (;
                symbol = sym,
                captured = length(ts),
                reference,
                coverage = reference == 0 ? NaN : round(length(ts) / reference; digits = 4),
            ),
        )
    end
    return DataFrame(rows)
end

coverage_report(path::AbstractString, provider; kwargs...) =
    coverage_report([path], provider; kwargs...)
