# Session visualization (CairoMakie), publication-grade per project standards.
#
# Two figure families, identical styling:
#   session_figure   — one symbol, one trading day (price, activity, Δt/size CCDFs)
#   overview_figure  — one symbol, many days (price on a concatenated
#                      trading-time axis, day×minute activity heatmap,
#                      intra-session waiting-time and size CCDFs)
# Inter-arrival and size distributions are drawn as survival functions on
# log-log axes — heavy tails are invisible in linear histograms.

"""
    tick_theme() -> Theme

Publication defaults: Computer Modern fonts, 26 pt labels over 22 pt ticks,
boxed axes with 1.5-wide spines, inward ticks, no minor ticks, faint dashed
grey grid, 3-wide data lines, frameless horizontal legends.
"""
tick_theme() = Theme(
    fonts = (; regular = texfont(:text), bold = texfont(:bold), italic = texfont(:italic)),
    fontsize = 26,
    figure_padding = 10,
    linewidth = 3,
    markersize = 14,
    Axis = (
        spinewidth = 1.5,
        xticklabelsize = 22,
        yticklabelsize = 22,
        xgridstyle = :dash,
        ygridstyle = :dash,
        xgridcolor = (:grey, 0.12),
        ygridcolor = (:grey, 0.12),
        xminorticksvisible = false,
        yminorticksvisible = false,
        xtickalign = 1,
        ytickalign = 1,
        rightspinevisible = true,
        topspinevisible = true,
    ),
    Scatter = (strokewidth = 1.5,),
    Legend = (framevisible = false, orientation = :horizontal, titlefont = :bold),
)

# In-axis annotations sit at 0.8 of the label size.
const ANNOTATION_FONTSIZE = 21

const SESSION_OPEN_H = 9.5      # regular US equity session, exchange-local
const SESSION_CLOSE_H = 16.0
const SESSION_LEN_H = SESSION_CLOSE_H - SESSION_OPEN_H

# Exchange-local hour-of-day (fractional) of a ns epoch timestamp.
function _local_hour(ns::Int64; tz::TimeZone)
    zdt = astimezone(ZonedDateTime(ns_to_datetime(ns), tz"UTC"), tz)
    return Dates.value(Dates.Time(DateTime(zdt))) / 3.6e12
end

_hhmm(h::Real) = (m = round(Int, 60h); @sprintf("%02d:%02d", m ÷ 60, m % 60))

# HH:MM ticks over an exchange-local hour span (domain time format).
function _hhmm_ticks(lo::Real, hi::Real)
    span = hi - lo
    step = span > 8 ? 2.0 : span > 3.5 ? 1.0 : span > 1.5 ? 0.5 : span > 0.7 ? 0.25 : 1 / 12
    first = ceil(lo / step) * step
    vals = collect(first:step:hi)
    return (vals, _hhmm.(vals))
end

# m×10^k as a tick label, with the collapse rules: 10^0 → 1 and 10^1 → 10
# always, and a mantissa folds into a plain decimal next to them
# (5×10^0 → 5, 2×10^1 → 20, 2×10^-1 → 0.2).
function _pow10_label(m::Integer, k::Integer)
    (k == 0 || k == 1 || (k == -1 && m != 1)) && return @sprintf("%g", m * 10.0^k)
    m == 1 && return L"10^{%$k}"
    return L"%$m\times10^{%$k}"
end

# Log-axis ticks: decades, with 2× and 5× intermediates while decades are
# sparse (up to three); plain decimals throughout on short spans near unity,
# exponent labels otherwise.
function _log_ticks(lo::Real, hi::Real)
    lo = max(float(lo), 1e-300)
    hi = max(float(hi), lo * 10)
    klo = floor(Int, log10(lo) + 1e-9)
    khi = ceil(Int, log10(hi) - 1e-9)
    ndec = khi - klo
    mantissas = ndec <= 3 ? (1, 2, 5) : (1,)
    ticks = [(m, k) for k in klo:khi for m in mantissas if 0.999lo <= m * 10.0^k <= 1.001hi]
    plain = klo >= -3 && khi <= 4 && ndec <= 4
    labels = AbstractString[
        plain ? @sprintf("%g", m * 10.0^k) : _pow10_label(m, k) for (m, k) in ticks
    ]
    return ([m * 10.0^k for (m, k) in ticks], labels)
end

# y-axis of a survival function: from the decade below the smallest
# probability up to 1, so the floor of the curve sits on a labelled tick.
function _survival_yaxis!(ax, pmin::Real)
    floor_decade = 10.0^floor(Int, log10(pmin) + 1e-9)
    ax.yticks = _log_ticks(floor_decade, 1.0)
    ylims!(ax, 0.7 * floor_decade, 1.6)
    return nothing
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

# Counts per minute, rescaled by a power of 1000 so that one axis never mixes
# exponents in its tick labels. Returns the divisor and the axis label.
function _rate_scale(peak::Real)
    k = peak >= 1e3 ? 3 * floor(Int, log10(peak) / 3) : 0
    k == 0 && return 1.0, L"Trade rate $[\mathrm{min}^{-1}]$"
    return 10.0^k, L"Trade rate $[10^{%$k}\,\mathrm{min}^{-1}]$"
end

function _annotate_tail!(ax, x, color)
    fit = _tail_fit(x)
    fit === nothing && return nothing
    α, σ = _value_pm(fit.α, fit.σ)
    text!(
        ax,
        0.04,
        0.05;
        text = L"Tail $\alpha = %$α \pm %$σ$",
        space = :relative,
        align = (:left, :bottom),
        color,
        fontsize = ANNOTATION_FONTSIZE,
    )
    return nothing
end

# Space-grouped thousands for in-axis count annotations.
_count_note(n::Integer) = replace(string(n), r"(?<=\d)(?=(\d{3})+$)" => " ")

"""
    session_figure(trades; tz = tz"America/New_York",
                   non_price = NON_PRICE_CONDITIONS) -> Figure

Build the 2×2 diagnostic figure for one symbol's single-day ticks:

- price path over exchange-local time (`HH:MM` axis, min–max decimated),
  drawn from the prints that [`price_forming`](@ref) admits under `non_price`,
  with the admitted share annotated,
- activity (trades per minute, every print),
- inter-arrival time survival function `P(Δt > x)` (log-log, decade ticks)
  with the median annotated,
- trade-size survival function `P(S > s)` with a Hill tail exponent.

`trades` must be non-empty and single-symbol (as produced by the grouping in
[`save_session_figures`](@ref)); they are sorted internally by exchange time.
"""
function session_figure(
    trades::Vector{Trade};
    tz::TimeZone = tz"America/New_York",
    non_price::AbstractDict = NON_PRICE_CONDITIONS,
)
    isempty(trades) && throw(ArgumentError("no trades to plot"))
    allequal(t.symbol for t in trades) || throw(
        ArgumentError(
            "session_figure expects a single symbol; got " *
            join(sort(unique(t.symbol for t in trades)), ", "),
        ),
    )
    ts = sort(trades; by = t -> t.time_ns)
    hours = [_local_hour(t.time_ns; tz) for t in ts]
    # The price path is drawn from price-forming prints only: contingent,
    # late and odd-lot prints execute away from the market and would set the
    # axis limits. Activity and both distributions count every print.
    forming = [price_forming(t; non_price) for t in ts]
    n_forming = count(forming)
    # A day without a single price-forming print (a thin, high-priced name
    # trading only in odd lots) still gets a path, drawn from every print.
    shown = n_forming > 0 ? forming : trues(length(ts))
    prices = [t.price for t in ts[shown]]
    date = trading_date(ts[1].time_ns; tz)
    color = Makie.wong_colors()[1]
    time_ticks = _hhmm_ticks(extrema(hours)...)

    fig = Figure(size = (1600, 1100))

    ax1 = Axis(
        fig[1, 1];
        xlabel = "Exchange time [HH:MM]",
        ylabel = "Price [USD]",
        xticks = time_ticks,
    )
    lines!(ax1, _decimate_minmax(hours[shown], prices)...; color)
    plo, phi = extrema(prices)
    span = max(phi - plo, 1e-9 * max(abs(phi), 1.0))
    ylims!(ax1, plo - 0.05 * span, phi + 0.22 * span)  # annotation headroom
    share = round(Int, 100 * n_forming / length(ts))
    text!(
        ax1,
        0.04,
        0.95;
        text = "$(ts[1].symbol), $date\n$(_count_note(length(ts))) prints, " *
               "$(_count_note(n_forming)) price-forming ($share %)",
        space = :relative,
        align = (:left, :top),
        color,
        fontsize = ANNOTATION_FONTSIZE,
    )

    minute = floor.(Int, hours .* 60)
    lo, hi = extrema(minute)
    counts = zeros(Int, hi - lo + 1)
    for m in minute
        counts[m-lo+1] += 1
    end
    divisor, rate_label = _rate_scale(maximum(counts))
    ax2 = Axis(
        fig[1, 2];
        xlabel = "Exchange time [HH:MM]",
        ylabel = rate_label,
        xticks = time_ticks,
    )
    stairs!(ax2, (lo:hi) ./ 60, counts ./ divisor; step = :center, color)

    dts = diff([t.time_ns for t in ts]) ./ NS_PER_SEC
    x3, y3 = _ccdf(dts)
    ax3 = Axis(
        fig[2, 1];
        xlabel = L"Inter-arrival $\Delta t$ [s]",
        ylabel = L"P(\Delta t > x)",
        xscale = log10,
        yscale = log10,
    )
    if !isempty(x3)
        ax3.xticks = _log_ticks(extrema(x3)...)
        _survival_yaxis!(ax3, y3[end])
        lines!(ax3, _thin(x3, y3)...; color)
        text!(
            ax3,
            0.04,
            0.05;
            text = L"Median $\Delta t$ = %$(_si_seconds(median(x3)))",
            space = :relative,
            align = (:left, :bottom),
            color,
            fontsize = ANNOTATION_FONTSIZE,
        )
    end

    x4, y4 = _ccdf([t.size for t in ts])
    ax4 = Axis(
        fig[2, 2];
        xlabel = "Trade size [shares]",
        ylabel = L"P(S > s)",
        xscale = log10,
        yscale = log10,
    )
    if !isempty(x4)
        ax4.xticks = _log_ticks(extrema(x4)...)
        _survival_yaxis!(ax4, y4[end])
        lines!(ax4, _thin(x4, y4)...; color)
        _annotate_tail!(ax4, x4, color)
    end

    return fig
end

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

# One processed per-day file (CSV or Arrow) → DataFrame.
_read_processed(path::AbstractString) =
    endswith(path, ".arrow") ? DataFrame(Arrow.Table(path)) : CSV.read(path, DataFrame)

"""
    overview_figure(symbol, days; tz = tz"America/New_York",
                    non_price = NON_PRICE_CONDITIONS) -> Figure

Multi-day diagnostic figure for one symbol from per-day processed tables
(`days` is a vector of `(date, DataFrame)` pairs, sorted internally by date):

- price path on a concatenated *trading-time* axis (overnight gaps removed,
  session boundaries dashed, days labeled at their centers), drawn from the
  prints that [`price_forming`](@ref) admits under `non_price`,
- day × session-minute activity heatmap,
- pooled **intra-session** inter-arrival CCDF (overnight gaps excluded by
  construction — they would contaminate the waiting-time tail),
- pooled trade-size CCDF with fitted tail exponent.
"""
function overview_figure(
    symbol::AbstractString,
    days::Vector{<:Tuple{Date,DataFrame}};
    tz::TimeZone = tz"America/New_York",
    non_price::AbstractDict = NON_PRICE_CONDITIONS,
)
    isempty(days) && throw(ArgumentError("no days to plot"))
    days = sort(days; by = first)
    nd = length(days)
    color = Makie.wong_colors()[1]
    slot = SESSION_LEN_H + 0.25              # session length + inter-day spacing

    # One grid, the colorbar in a column of its own, so the panels of both rows
    # share their edges.
    fig = Figure(size = (1600, 1100))

    # 1 — price on concatenated trading time
    ax1 = Axis(
        fig[1, 1];
        xlabel = "Trading day",
        ylabel = "Price [USD]",
        xticks = (
            [(i - 0.5) * slot for i in 1:nd],
            [Dates.format(d, dateformat"mm-dd") for (d, _) in days],
        ),
        xticklabelrotation = nd > 6 ? π / 4 : 0.0,
    )
    ntotal = 0
    nforming = 0
    plo, phi = Inf, -Inf
    for (i, (_, df)) in enumerate(days)
        forming = [
            _row_price_forming(tape, conds, non_price) for
            (tape, conds) in zip(df.tape, df.conditions)
        ]
        nforming += count(forming)
        # a day without a price-forming print is drawn from every print
        shown = any(forming) ? forming : trues(nrow(df))
        h = [_local_hour(ns; tz) for ns in df.time_ns[shown]]
        px = Float64.(df.price[shown])
        x = (i - 1) * slot .+ clamp.(h .- SESSION_OPEN_H, -0.1, SESSION_LEN_H + 0.2)
        lines!(ax1, _decimate_minmax(x, px)...; color)
        plo, phi = min(plo, minimum(px)), max(phi, maximum(px))
        i > 1 && vlines!(
            ax1,
            [(i - 1) * slot - 0.125];
            color = (:grey, 0.4),
            linestyle = :dash,
            linewidth = 1.5,
        )
        ntotal += nrow(df)
    end
    span = max(phi - plo, 1e-9 * max(abs(phi), 1.0))
    ylims!(ax1, plo - 0.05 * span, phi + 0.22 * span)  # annotation headroom
    share = round(Int, 100 * nforming / max(ntotal, 1))
    text!(
        ax1,
        0.04,
        0.95;
        text = "$symbol\n$(_count_note(ntotal)) prints, " *
               "$(_count_note(nforming)) price-forming ($share %)",
        space = :relative,
        align = (:left, :top),
        color,
        fontsize = ANNOTATION_FONTSIZE,
    )

    # 2 — day × minute activity heatmap
    nmin = round(Int, 60 * SESSION_LEN_H)
    act = zeros(Float64, nmin + 1, nd)
    for (i, (_, df)) in enumerate(days)
        for ns in df.time_ns
            m = round(Int, 60 * (_local_hour(ns; tz) - SESSION_OPEN_H))
            0 <= m <= nmin && (act[m+1, i] += 1)
        end
    end
    hm_hours = SESSION_OPEN_H .+ (0:nmin) ./ 60
    ax2 = Axis(
        fig[1, 2];
        xlabel = "Exchange time [HH:MM]",
        ylabel = "Trading day",
        xticks = _hhmm_ticks(SESSION_OPEN_H, SESSION_CLOSE_H),
        yticks = (1:nd, [Dates.format(d, dateformat"mm-dd") for (d, _) in days]),
    )
    # Activity spans decades between the opening auction and midday, so the
    # color scale is logarithmic; empty minutes are left blank.
    act[act .== 0] .= NaN
    busy = filter(!isnan, act)
    crange =
        isempty(busy) ? (1.0, 10.0) : (minimum(busy), max(maximum(busy), 10minimum(busy)))
    hm = heatmap!(
        ax2,
        hm_hours,
        1:nd,
        act;
        colormap = :viridis,
        colorscale = log10,
        colorrange = crange,
        nan_color = :white,
    )
    Colorbar(
        fig[1, 3],
        hm;
        label = L"Trade rate $[\mathrm{min}^{-1}]$",
        ticks = _log_ticks(crange...),
    )

    # 3 — pooled intra-session waiting-time CCDF
    dts = Float64[]
    for (_, df) in days
        append!(dts, diff(sort(df.time_ns)) ./ NS_PER_SEC)
    end
    x3, y3 = _ccdf(dts)
    ax3 = Axis(
        fig[2, 1];
        xlabel = L"Intra-session inter-arrival $\Delta t$ [s]",
        ylabel = L"P(\Delta t > x)",
        xscale = log10,
        yscale = log10,
    )
    if !isempty(x3)
        ax3.xticks = _log_ticks(extrema(x3)...)
        _survival_yaxis!(ax3, y3[end])
        lines!(ax3, _thin(x3, y3)...; color)
        text!(
            ax3,
            0.04,
            0.05;
            text = "$nd sessions pooled; $(nd - 1) overnight gaps excluded",
            space = :relative,
            align = (:left, :bottom),
            color,
            fontsize = ANNOTATION_FONTSIZE,
        )
    end

    # 4 — pooled size CCDF
    sizes = Float64[]
    for (_, df) in days
        append!(sizes, Float64.(df.size))
    end
    x4, y4 = _ccdf(sizes)
    ax4 = Axis(
        fig[2, 2];
        xlabel = "Trade size [shares]",
        ylabel = L"P(S > s)",
        xscale = log10,
        yscale = log10,
    )
    if !isempty(x4)
        ax4.xticks = _log_ticks(extrema(x4)...)
        _survival_yaxis!(ax4, y4[end])
        lines!(ax4, _thin(x4, y4)...; color)
        _annotate_tail!(ax4, x4, color)
    end

    return fig
end

"""
    save_session_figures(raw_paths, out_dir = joinpath(PROJECT_ROOT, "plots");
                         formats = ("pdf", "png"), tz = tz"America/New_York",
                         min_trades = 10) -> Vector{String}

Render one diagnostic figure per (symbol, trading day) found in the raw
NDJSON `raw_paths`, saved as `out_dir/SYMBOL_YYYY-MM-DD.pdf|png` (safesave —
existing files are never overwritten). Groups with fewer than `min_trades`
ticks are skipped with an `@info` (distribution panels are meaningless).
PNG output is written at `px_per_unit = 4`. Returns the files written.
"""
function save_session_figures(
    raw_paths::AbstractVector{<:AbstractString},
    out_dir::AbstractString = joinpath(PROJECT_ROOT, "plots");
    formats = ("pdf", "png"),
    tz::TimeZone = tz"America/New_York",
    min_trades::Integer = 10,
)
    trades = deduplicate_trades(read_raw(raw_paths))
    isempty(trades) && return String[]
    mkpath(out_dir)
    groups = Dict{Tuple{String,Date},Vector{Trade}}()
    for t in trades
        push!(get!(() -> Trade[], groups, (t.symbol, trading_date(t.time_ns; tz))), t)
    end
    written = String[]
    with_theme(tick_theme()) do
        for key in sort!(collect(keys(groups)))
            sym, date = key
            g = groups[key]
            if length(g) < min_trades
                @info "skipping sparse group" symbol = sym date n = length(g)
                continue
            end
            fig = session_figure(g; tz)
            for fmt in formats
                path = _safepath(joinpath(out_dir, "$(sym)_$(date).$(fmt)"))
                fmt == "png" ? save(path, fig; px_per_unit = 4) : save(path, fig)
                push!(written, path)
            end
        end
    end
    return written
end

save_session_figures(path::AbstractString, args...; kwargs...) =
    save_session_figures([path], args...; kwargs...)

"""
    save_overview_figures(processed_dir, out_dir = joinpath(PROJECT_ROOT, "plots");
                          symbols = nothing, from = nothing, to = nothing,
                          formats = ("pdf", "png"), tz = tz"America/New_York",
                          min_days = 2) -> Vector{String}

Render one multi-day [`overview_figure`](@ref) per symbol from the processed
tree (`processed_dir/SYMBOL/YYYY-MM-DD.csv|.arrow`; safesave ` #N` siblings
are ignored — the base file per day is authoritative). `symbols`, `from`,
and `to` restrict the sweep. Output: `out_dir/SYMBOL_<from>_<to>.pdf|png`
(safesave). Days are loaded one symbol at a time, so memory stays bounded
by one symbol's span.
"""
function save_overview_figures(
    processed_dir::AbstractString,
    out_dir::AbstractString = joinpath(PROJECT_ROOT, "plots");
    symbols = nothing,
    from::Union{Nothing,Date} = nothing,
    to::Union{Nothing,Date} = nothing,
    formats = ("pdf", "png"),
    tz::TimeZone = tz"America/New_York",
    min_days::Integer = 2,
)
    isdir(processed_dir) || throw(ArgumentError("no processed directory at $processed_dir"))
    mkpath(out_dir)
    written = String[]
    with_theme(tick_theme()) do
        for sym in
            sort(filter(s -> isdir(joinpath(processed_dir, s)), readdir(processed_dir)))
            symbols === nothing || sym in symbols || continue
            days = Tuple{Date,DataFrame}[]
            for f in sort(readdir(joinpath(processed_dir, sym)))
                m = match(r"^(\d{4}-\d{2}-\d{2})\.(csv|arrow)$", f)   # base files only
                m === nothing && continue
                d = Date(something(m[1]))
                from !== nothing && d < from && continue
                to !== nothing && d > to && continue
                push!(days, (d, _read_processed(joinpath(processed_dir, sym, f))))
            end
            length(days) < min_days && continue
            fig = overview_figure(sym, days; tz)
            span = "$(days[1][1])_$(days[end][1])"
            for fmt in formats
                path = _safepath(joinpath(out_dir, "$(sym)_$(span).$(fmt)"))
                fmt == "png" ? save(path, fig; px_per_unit = 4) : save(path, fig)
                push!(written, path)
            end
        end
    end
    return written
end
