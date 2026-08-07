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

Publication defaults: Computer Modern fonts, boxed axes, inward ticks, no
minor ticks, faint dashed grey grid.
"""
tick_theme() = Theme(
    fonts = (; regular = texfont(:text), bold = texfont(:bold), italic = texfont(:italic)),
    fontsize = 22,
    figure_padding = 16,
    Axis = (
        xgridstyle = :dash, ygridstyle = :dash,
        xgridcolor = (:grey, 0.12), ygridcolor = (:grey, 0.12),
        xminorticksvisible = false, yminorticksvisible = false,
        xtickalign = 1, ytickalign = 1,
        rightspinevisible = true, topspinevisible = true,
    ),
)

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
    step = span > 8 ? 2.0 : span > 3.5 ? 1.0 : span > 1.5 ? 0.5 :
           span > 0.7 ? 0.25 : 1 / 12
    first = ceil(lo / step) * step
    vals = collect(first:step:hi)
    return (vals, _hhmm.(vals))
end

# Log-axis ticks per project standards: decades, with 2x/5x intermediates
# when decades are sparse; plain decimals on short spans, exponent labels
# with 10^0 -> 1 and 10^1 -> 10 collapse otherwise.
function _log_ticks(lo::Real, hi::Real)
    lo = max(float(lo), 1e-300)
    hi = max(float(hi), lo * 10)
    klo = floor(Int, log10(lo) + 1e-9)
    khi = ceil(Int, log10(hi) - 1e-9)
    ndec = khi - klo
    mantissas = ndec <= 4 ? (1.0, 2.0, 5.0) : (1.0,)
    vals = Float64[]
    for k in klo:khi, m in mantissas
        v = m * 10.0^k
        0.999lo <= v <= 1.001hi && push!(vals, v)
    end
    plain = klo >= -3 && khi <= 4 && ndec <= 4
    labels = map(vals) do v
        if plain
            @sprintf("%g", v)
        else
            k = round(Int, log10(v))
            k == 0 ? "1" : k == 1 ? "10" : L"10^{%$k}"
        end
    end
    return (vals, AbstractString[labels...])
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
function _thin(xs::Vector{Float64}, ys::Vector{Float64}; cap::Integer = 3000,
               tail::Integer = 300)
    n = length(xs)
    n <= cap && return xs, ys
    stride = cld(n, cap - tail)
    idx = sort!(unique(vcat(1:stride:n, (n - tail + 1):n)))
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
        r = searchsortedlast(x, edges[b + 1])
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

# Least-squares tail exponent of a CCDF on log-log scale over the top of the
# distribution; returns (α, σ) or nothing when the tail is too short.
function _tail_fit(x::Vector{Float64}, p::Vector{Float64}; frac::Real = 0.1)
    n = length(x)
    n < 50 && return nothing
    i0 = max(1, n - max(30, round(Int, frac * n)) + 1)
    lx, lp = log10.(x[i0:end]), log10.(p[i0:end])
    lx[end] - lx[1] < 0.3 && return nothing
    X = hcat(ones(length(lx)), lx)
    β = X \ lp
    dof = length(lx) - 2
    dof < 1 && return nothing
    se = sqrt(sum(abs2, lp .- X * β) / dof * inv(X'X)[2, 2])
    return (α = -β[2], σ = se)
end

function _annotate_tail!(ax, x, p, color)
    fit = _tail_fit(x, p)
    fit === nothing && return nothing
    α, σ = @sprintf("%.3g", fit.α), @sprintf("%.2g", fit.σ)
    text!(ax, 0.04, 0.05; text = L"tail $\alpha = %$α \pm %$σ$",
          space = :relative, align = (:left, :bottom), color, fontsize = 18)
    return nothing
end

# Thin-space thousands grouping for in-axis count annotations.
_count_note(n::Integer) = replace(string(n), r"(?<=\d)(?=(\d{3})+$)" => " ")

"""
    session_figure(trades; tz = tz"America/New_York") -> Figure

Build the 2×2 diagnostic figure for one symbol's single-day ticks:

- price path over exchange-local time (`HH:MM` axis, min–max decimated),
- activity (trades per minute),
- inter-arrival time survival function `P(Δt > x)` (log-log, decade ticks),
- trade-size survival function `P(S > s)` with a fitted tail exponent.

`trades` must be non-empty and single-symbol (as produced by the grouping in
[`save_session_figures`](@ref)); they are sorted internally by exchange time.
"""
function session_figure(trades::Vector{Trade}; tz::TimeZone = tz"America/New_York")
    isempty(trades) && throw(ArgumentError("no trades to plot"))
    ts = sort(trades; by = t -> t.time_ns)
    hours = [_local_hour(t.time_ns; tz) for t in ts]
    prices = [t.price for t in ts]
    date = trading_date(ts[1].time_ns; tz)
    color = Makie.wong_colors()[1]

    fig = Figure(size = (1280, 960))

    ax1 = Axis(fig[1, 1]; xlabel = "exchange time [HH:MM]", ylabel = "price [USD]",
               xticks = _hhmm_ticks(extrema(hours)...))
    lines!(ax1, _decimate_minmax(hours, prices)...; linewidth = 1.2, color)
    plo, phi = extrema(prices)
    ylims!(ax1, plo - 0.05 * (phi - plo), phi + 0.16 * (phi - plo))  # annotation headroom
    text!(ax1, 0.04, 0.95; text = "$(ts[1].symbol), $date\nn = $(_count_note(length(ts)))",
          space = :relative, align = (:left, :top), color, fontsize = 18)

    ax2 = Axis(fig[1, 2]; xlabel = "exchange time [HH:MM]",
               ylabel = L"trade rate $[\mathrm{min}^{-1}]$",
               xticks = _hhmm_ticks(extrema(hours)...))
    minute = floor.(Int, hours .* 60)
    lo, hi = extrema(minute)
    counts = zeros(Int, hi - lo + 1)
    for m in minute
        counts[m - lo + 1] += 1
    end
    stairs!(ax2, (lo:hi) ./ 60, counts; step = :center, linewidth = 1.2, color)

    dts = diff([t.time_ns for t in ts]) ./ NS_PER_SEC
    x3, y3 = _ccdf(dts)
    ax3 = Axis(fig[2, 1]; xlabel = L"inter-arrival $\Delta t$ [s]",
               ylabel = L"P(\Delta t > x)", xscale = log10, yscale = log10)
    if !isempty(x3)
        ax3.xticks = _log_ticks(extrema(x3)...)
        ax3.yticks = _log_ticks(y3[end], 1.0)
        scatterlines!(ax3, _thin(x3, y3)...; markersize = 4, linewidth = 1.0, color)
    end

    x4, y4 = _ccdf([t.size for t in ts])
    ax4 = Axis(fig[2, 2]; xlabel = "trade size [shares]",
               ylabel = L"P(S > s)", xscale = log10, yscale = log10)
    if !isempty(x4)
        ax4.xticks = _log_ticks(extrema(x4)...)
        ax4.yticks = _log_ticks(y4[end], 1.0)
        scatterlines!(ax4, _thin(x4, y4)...; markersize = 4, linewidth = 1.0, color)
        _annotate_tail!(ax4, x4, y4, color)
    end

    return fig
end

# One processed per-day file (CSV or Arrow) → DataFrame.
_read_processed(path::AbstractString) =
    endswith(path, ".arrow") ? DataFrame(Arrow.Table(path)) : CSV.read(path, DataFrame)

"""
    overview_figure(symbol, days; tz = tz"America/New_York") -> Figure

Multi-day diagnostic figure for one symbol from per-day processed tables
(`days` is a date-sorted vector of `(date, DataFrame)`):

- price path on a concatenated *trading-time* axis (overnight gaps removed,
  session boundaries dashed, days labeled at their centers),
- day × session-minute activity heatmap,
- pooled **intra-session** inter-arrival CCDF (overnight gaps excluded by
  construction — they would contaminate the waiting-time tail),
- pooled trade-size CCDF with fitted tail exponent.
"""
function overview_figure(symbol::AbstractString,
                         days::Vector{<:Tuple{Date, DataFrame}};
                         tz::TimeZone = tz"America/New_York")
    isempty(days) && throw(ArgumentError("no days to plot"))
    nd = length(days)
    color = Makie.wong_colors()[1]
    slot = SESSION_LEN_H + 0.25              # session length + inter-day spacing

    fig = Figure(size = (1280, 960))
    top = GridLayout(fig[1, 1])
    bottom = GridLayout(fig[2, 1])

    # 1 — price on concatenated trading time
    ax1 = Axis(top[1, 1]; xlabel = "trading day", ylabel = "price [USD]",
               xticks = ([(i - 0.5) * slot for i in 1:nd],
                         [Dates.format(d, dateformat"mm-dd") for (d, _) in days]),
               xticklabelrotation = nd > 6 ? π / 4 : 0.0)
    ntotal = 0
    for (i, (_, df)) in enumerate(days)
        h = [_local_hour(ns; tz) for ns in df.time_ns]
        x = (i - 1) * slot .+ clamp.(h .- SESSION_OPEN_H, -0.1, SESSION_LEN_H + 0.2)
        lines!(ax1, _decimate_minmax(x, Float64.(df.price))...; linewidth = 1.0, color)
        i > 1 && vlines!(ax1, [(i - 1) * slot - 0.125]; color = (:grey, 0.4),
                         linestyle = :dash, linewidth = 0.8)
        ntotal += nrow(df)
    end
    plo, phi = extrema(reduce(vcat, [Float64.(df.price) for (_, df) in days]))
    ylims!(ax1, plo - 0.05 * (phi - plo), phi + 0.16 * (phi - plo))  # annotation headroom
    text!(ax1, 0.04, 0.95; text = "$symbol\nn = $(_count_note(ntotal))",
          space = :relative, align = (:left, :top), color, fontsize = 18)

    # 2 — day × minute activity heatmap
    nmin = round(Int, 60 * SESSION_LEN_H)
    act = zeros(Float64, nmin + 1, nd)
    for (i, (_, df)) in enumerate(days)
        for ns in df.time_ns
            m = round(Int, 60 * (_local_hour(ns; tz) - SESSION_OPEN_H))
            0 <= m <= nmin && (act[m + 1, i] += 1)
        end
    end
    hm_hours = SESSION_OPEN_H .+ (0:nmin) ./ 60
    ax2 = Axis(top[1, 2]; xlabel = "exchange time [HH:MM]", ylabel = "trading day",
               xticks = _hhmm_ticks(SESSION_OPEN_H, SESSION_CLOSE_H),
               yticks = (1:nd, [Dates.format(d, dateformat"mm-dd") for (d, _) in days]))
    hm = heatmap!(ax2, hm_hours, 1:nd, act; colormap = :viridis)
    Colorbar(top[1, 3], hm; label = L"trades $[\mathrm{min}^{-1}]$")

    # 3 — pooled intra-session waiting-time CCDF
    dts = Float64[]
    for (_, df) in days
        append!(dts, diff(sort(df.time_ns)) ./ NS_PER_SEC)
    end
    x3, y3 = _ccdf(dts)
    ax3 = Axis(bottom[1, 1]; xlabel = L"intra-session inter-arrival $\Delta t$ [s]",
               ylabel = L"P(\Delta t > x)", xscale = log10, yscale = log10)
    if !isempty(x3)
        ax3.xticks = _log_ticks(extrema(x3)...)
        ax3.yticks = _log_ticks(y3[end], 1.0)
        scatterlines!(ax3, _thin(x3, y3)...; markersize = 4, linewidth = 1.0, color)
        text!(ax3, 0.04, 0.05;
              text = "$nd sessions pooled; $(nd - 1) overnight gaps excluded",
              space = :relative, align = (:left, :bottom), color, fontsize = 18)
    end

    # 4 — pooled size CCDF
    sizes = Float64[]
    for (_, df) in days
        append!(sizes, Float64.(df.size))
    end
    x4, y4 = _ccdf(sizes)
    ax4 = Axis(bottom[1, 2]; xlabel = "trade size [shares]", ylabel = L"P(S > s)",
               xscale = log10, yscale = log10)
    if !isempty(x4)
        ax4.xticks = _log_ticks(extrema(x4)...)
        ax4.yticks = _log_ticks(y4[end], 1.0)
        scatterlines!(ax4, _thin(x4, y4)...; markersize = 4, linewidth = 1.0, color)
        _annotate_tail!(ax4, x4, y4, color)
    end
    colsize!(bottom, 1, Relative(0.58))

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
function save_session_figures(raw_paths::AbstractVector{<:AbstractString},
                              out_dir::AbstractString = joinpath(PROJECT_ROOT, "plots");
                              formats = ("pdf", "png"), tz::TimeZone = tz"America/New_York",
                              min_trades::Integer = 10)
    trades = dedup_trades(read_raw(raw_paths))
    isempty(trades) && return String[]
    mkpath(out_dir)
    groups = Dict{Tuple{String, Date}, Vector{Trade}}()
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
function save_overview_figures(processed_dir::AbstractString,
                               out_dir::AbstractString = joinpath(PROJECT_ROOT, "plots");
                               symbols = nothing, from::Union{Nothing, Date} = nothing,
                               to::Union{Nothing, Date} = nothing,
                               formats = ("pdf", "png"), tz::TimeZone = tz"America/New_York",
                               min_days::Integer = 2)
    isdir(processed_dir) || throw(ArgumentError("no processed directory at $processed_dir"))
    mkpath(out_dir)
    written = String[]
    with_theme(tick_theme()) do
        for sym in sort(filter(s -> isdir(joinpath(processed_dir, s)), readdir(processed_dir)))
            symbols === nothing || sym in symbols || continue
            days = Tuple{Date, DataFrame}[]
            for f in sort(readdir(joinpath(processed_dir, sym)))
                m = match(r"^(\d{4}-\d{2}-\d{2})\.(csv|arrow)$", f)   # base files only
                m === nothing && continue
                d = Date(m[1])
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
