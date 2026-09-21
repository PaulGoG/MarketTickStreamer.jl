# Numerics behind the diagnostic figures, free of any plotting dependency:
# survival functions, decimation, the Hill tail estimator, number and duration
# formatting, exchange-local clock arithmetic. The figures themselves live in
# the CairoMakie package extension.

# Exchange-local hour-of-day (fractional) of a ns epoch timestamp.
function _local_hour(ns::Int64; tz::TimeZone)
    zdt = astimezone(ZonedDateTime(ns_to_datetime(ns), tz"UTC"), tz)
    return Dates.value(Dates.Time(DateTime(zdt))) / 3.6e12
end

_hhmm(h::Real) = (m = round(Int, 60h); @sprintf("%02d:%02d", m ÷ 60, m % 60))

# HH:MM ticks over an exchange-local hour span (domain time format).
function _hhmm_ticks(lo::Real, hi::Real)
    span = hi - lo
    step =
        span > 16 ? 4.0 :
        span > 8 ? 2.0 : span > 3.5 ? 1.0 : span > 1.5 ? 0.5 : span > 0.7 ? 0.25 : 1 / 12
    first = ceil(lo / step) * step
    vals = collect(first:step:hi)
    return (vals, _hhmm.(vals))
end

# Survival function P(X > x) over positive samples, ready for log-log axes.
function _ccdf(xs::Vector{Float64})
    pos = sort!(filter(>(0.0), xs))
    n = length(pos)
    return pos, collect(n:-1:1) ./ n
end

# Deterministic point thinning that preserves the tail: uniform stride over
# the body plus the last `tail` points, so million-tick CCDFs stay
# vector-light without visible change.
function _thin(
    xs::Vector{Float64},
    ys::Vector{Float64};
    cap::Integer = 3000,
    tail::Integer = 300,
)
    n = length(xs)
    n <= cap && return xs, ys
    stride = cld(n, cap - tail)
    idx = sort!(unique(vcat(1:stride:n, (n-tail+1):n)))
    return xs[idx], ys[idx]
end

# Min-max decimation per x-bin: preserves envelopes (spikes survive) while
# bounding a day's price path to ~2 * nbins points.
function _decimate_minmax(x::Vector{Float64}, y::Vector{Float64}; nbins::Integer = 400)
    length(x) <= 2nbins && return x, y
    lo, hi = extrema(x)
    edges = range(lo, hi; length = nbins + 1)
    xo = Float64[]
    yo = Float64[]
    i = 1
    for b in 1:nbins
        r = searchsortedlast(x, edges[b+1])
        r < i && continue
        seg = i:r
        jmin = seg[argmin(@view y[seg])]
        jmax = seg[argmax(@view y[seg])]
        for j in sort!([jmin, jmax])
            push!(xo, x[j])
            push!(yo, y[j])
        end
        i = r + 1
    end
    return xo, yo
end

"""
    _tail_fit(x; frac = 0.1) -> Union{Nothing,NamedTuple}

Hill estimator of the tail exponent `α` in `P(X > x) ∝ x^(-α)` from the `k`
largest of the ascending-sorted positive samples `x`, with `k` the top `frac`
of the sample and at least 30:

    α̂ = k / Σᵢ ln(x₍ₙ₋ᵢ₊₁₎ / x₍ₙ₋ₖ₎),    σ = α̂ / √k.

This is the maximum-likelihood estimator of a Pareto tail above the threshold
`x₍ₙ₋ₖ₎` ([Hill 1975](https://doi.org/10.1214/aos/1176343247)). A
least-squares line through the log-log survival function is not a substitute:
its points are cumulative and therefore strongly correlated, so the slope is
biased and its regression standard error is too small by a large factor
([Clauset, Shalizi & Newman 2009](https://doi.org/10.1137/070710111)).

The threshold is a fixed fraction rather than a fitted `x_min`, and trade sizes
are discrete with mass at round numbers, so the value is a diagnostic of tail
weight, not a measurement of a power law. Returns `nothing` for fewer than 50
samples, for a tail spanning less than a factor of two, or when the tail is
degenerate.
"""
function _tail_fit(x::Vector{Float64}; frac::Real = 0.1)
    n = length(x)
    n < 50 && return nothing
    k = min(n - 1, max(30, round(Int, frac * n)))
    threshold = x[n-k]
    (threshold > 0 && x[n] >= 2threshold) || return nothing
    s = sum(log(x[i] / threshold) for i in (n-k+1):n)
    s > 0 || return nothing
    α = k / s
    return (α = α, σ = α / sqrt(k))
end

# Value and uncertainty to the same number of decimals, set by the leading
# digit of the uncertainty and capped at three: 1.429 ± 0.004, 1.87 ± 0.07.
function _value_pm(value::Real, σ::Real)
    decimals = clamp(-floor(Int, log10(σ)), 0, 3)
    return _fixed(value, decimals), _fixed(σ, decimals)
end

# Fixed-point rendering with exactly `decimals` digits after the point;
# `round` alone drops trailing zeros ("1.4" for 1.40).
function _fixed(v::Real, decimals::Integer)
    decimals == 0 && return string(round(Int, v))
    str = string(round(Float64(v); digits = decimals))
    i = something(findfirst('.', str), length(str))
    return str * "0"^(decimals - (length(str) - i))
end

# A duration in the SI-prefixed unit that keeps it between 1 and 1000.
function _si_seconds(x::Real)
    for (scale, unit) in ((1.0, "s"), (1e-3, "ms"), (1e-6, "μs"))
        x >= scale && return @sprintf("%.3g %s", x / scale, unit)
    end
    return @sprintf("%.3g ns", x / 1e-9)
end

# Space-grouped thousands for in-axis count annotations.
_count_note(n::Integer) = replace(string(n), r"(?<=\d)(?=(\d{3})+$)" => " ")

# `price_forming` for one row of a processed table, where the condition list
# is stored joined by '|' and an empty list reads back as `missing` from CSV.
function _row_price_forming(tape, conditions, non_price::AbstractDict)
    (ismissing(tape) || ismissing(conditions)) && return true
    # `string`, not `String`: CSV type detection reads an all-numeric column
    # of codes ("4", "7") back as integers.
    excluded = get(non_price, string(tape), nothing)
    excluded === nothing && return true
    return !any(c -> c in excluded, eachsplit(string(conditions), '|'))
end

"""
    _pooled_gaps(days; continuous) -> (gaps, excluded)

Waiting times [s] pooled over per-day processed tables `(date, DataFrame)`,
sorted by date, and the number of between-day gaps left out.

Within a day every gap counts. Between two days the gap spans a closure and
is not a waiting time of the arrival process, so it is dropped. On a venue
that never closes (`continuous = true`) midnight is not a boundary: the wait
across it is kept whenever the next calendar day is present, and only gaps
across missing days are dropped.
"""
function _pooled_gaps(days::AbstractVector{<:Tuple{Date,DataFrame}}; continuous::Bool)
    gaps = Float64[]
    bridged = 0
    for (i, (date, df)) in enumerate(days)
        t = sort(df.time_ns)
        append!(gaps, diff(t) ./ NS_PER_SEC)
        (continuous && i < length(days) && !isempty(t)) || continue
        next_date, next_df = days[i+1]
        (next_date == date + Day(1) && !isempty(next_df.time_ns)) || continue
        push!(gaps, (minimum(next_df.time_ns) - t[end]) / NS_PER_SEC)
        bridged += 1
    end
    return gaps, max(length(days) - 1, 0) - bridged
end
