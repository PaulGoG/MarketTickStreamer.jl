# Session visualization (CairoMakie): one 2×2 diagnostic figure per symbol
# per trading day — price path, activity profile, and the two distributions
# that matter first for exotic time series work: inter-arrival times and
# trade sizes, both as survival functions on log-log axes (heavy tails are
# invisible in linear histograms).

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

# Exchange-local hour-of-day (fractional) of a ns epoch timestamp.
function _local_hour(ns::Int64; tz::TimeZone)
    zdt = astimezone(ZonedDateTime(ns_to_datetime(ns), tz"UTC"), tz)
    return Dates.value(Dates.Time(DateTime(zdt))) / 3.6e12
end

# Survival function P(X > x) over positive samples, ready for log-log axes.
function _ccdf(xs::Vector{Float64})
    pos = sort!(filter(>(0.0), xs))
    n = length(pos)
    return pos, collect(n:-1:1) ./ n
end

"""
    session_figure(trades; tz = tz"America/New_York") -> Figure

Build the 2×2 diagnostic figure for one symbol's single-day ticks:

- price path over exchange-local time,
- activity (trades per minute),
- inter-arrival time survival function `P(Δt > x)` (log-log),
- trade-size survival function `P(S > s)` (log-log).

`trades` must be non-empty and single-symbol (as produced by the grouping in
[`save_session_figures`](@ref)); they are sorted internally by exchange time.
"""
function session_figure(trades::Vector{Trade}; tz::TimeZone = tz"America/New_York")
    isempty(trades) && throw(ArgumentError("no trades to plot"))
    ts = sort(trades; by = t -> t.time_ns)
    hours = [_local_hour(t.time_ns; tz) for t in ts]
    prices = [t.price for t in ts]

    fig = Figure(size = (1280, 960))

    ax1 = Axis(fig[1, 1]; xlabel = "exchange time [h]", ylabel = "price [USD]")
    lines!(ax1, hours, prices; linewidth = 1.2)

    ax2 = Axis(fig[1, 2]; xlabel = "exchange time [h]",
               ylabel = L"trade rate $[\mathrm{min}^{-1}]$")
    minute = floor.(Int, hours .* 60)
    lo, hi = extrema(minute)
    counts = zeros(Int, hi - lo + 1)
    for m in minute
        counts[m - lo + 1] += 1
    end
    stairs!(ax2, (lo:hi) ./ 60, counts; step = :center, linewidth = 1.2)

    dts = diff([t.time_ns for t in ts]) ./ NS_PER_SEC
    x3, y3 = _ccdf(dts)
    ax3 = Axis(fig[2, 1]; xlabel = L"inter-arrival $\Delta t$ [s]",
               ylabel = L"P(\Delta t > x)", xscale = log10, yscale = log10)
    isempty(x3) || scatterlines!(ax3, x3, y3; markersize = 4, linewidth = 1.0)

    x4, y4 = _ccdf([t.size for t in ts])
    ax4 = Axis(fig[2, 2]; xlabel = "trade size [shares]",
               ylabel = L"P(S > s)", xscale = log10, yscale = log10)
    isempty(x4) || scatterlines!(ax4, x4, y4; markersize = 4, linewidth = 1.0)

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
