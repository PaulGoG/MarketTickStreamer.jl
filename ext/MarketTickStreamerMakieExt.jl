# Session visualization (CairoMakie package extension), publication-grade per
# project standards.
#
# Two figure families, identical styling:
#   session_figure   — one symbol, one trading day (price, activity, Δt/size CCDFs)
#   overview_figure  — one symbol, many days (price on a concatenated
#                      trading-time axis, day×minute activity heatmap,
#                      intra-session waiting-time and size CCDFs)
# Inter-arrival and size distributions are drawn as survival functions on
# log-log axes — heavy tails are invisible in linear histograms.

module MarketTickStreamerMakieExt

using CairoMakie:
    CairoMakie,
    @L_str,
    Axis,
    Colorbar,
    Figure,
    Makie,
    Theme,
    heatmap!,
    lines!,
    save,
    stairs!,
    text!,
    theme_latexfonts,
    vlines!,
    with_theme,
    ylims!
using DataFrames: DataFrame, nrow
using Dates: Dates, @dateformat_str
using MarketTickStreamer:
    MarketTickStreamer,
    NON_PRICE_CONDITIONS,
    NS_PER_SEC,
    PROJECT_ROOT,
    Trade,
    deduplicate_trades,
    price_forming,
    read_raw,
    trading_date,
    _ccdf,
    _count_note,
    _decimate_minmax,
    _hhmm_ticks,
    _local_hour,
    _read_processed,
    _row_price_forming,
    _safesave,
    _si_seconds,
    _tail_fit,
    _thin,
    _value_pm
using Printf: @sprintf
using Statistics: median
using TimeZones: @tz_str, Date, TimeZone

# In-axis annotations sit at 0.8 of the label size.
const ANNOTATION_FONTSIZE = 21

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

# `theme_latexfonts` carries the Computer Modern regular/bold/italic faces.
MarketTickStreamer.tick_theme() = merge(
    Theme(
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
    ),
    theme_latexfonts(),
)

function MarketTickStreamer.session_figure(
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
        # Evaluated outside the string macro, where the import checker can
        # see `median` and `_si_seconds` being used.
        median_gap = _si_seconds(median(x3))
        text!(
            ax3,
            0.04,
            0.05;
            text = L"Median $\Delta t$ = %$(median_gap)",
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

function MarketTickStreamer.overview_figure(
    symbol::AbstractString,
    days::Vector{<:Tuple{Date,DataFrame}};
    tz::TimeZone = tz"America/New_York",
    non_price::AbstractDict = NON_PRICE_CONDITIONS,
    session::Tuple{<:Real,<:Real} = (9.5, 16.0),
)
    isempty(days) && throw(ArgumentError("no days to plot"))
    session_open, session_close = Float64.(session)
    0 <= session_open < session_close <= 24 ||
        throw(ArgumentError("session must satisfy 0 <= open < close <= 24, got $session"))
    session_len = session_close - session_open
    days = sort(days; by = first)
    nd = length(days)
    color = Makie.wong_colors()[1]
    slot = session_len + 0.25                # session length + inter-day spacing

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
        x = (i - 1) * slot .+ clamp.(h .- session_open, -0.1, session_len + 0.2)
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
    nmin = round(Int, 60 * session_len)
    act = zeros(Float64, nmin + 1, nd)
    for (i, (_, df)) in enumerate(days)
        for ns in df.time_ns
            m = round(Int, 60 * (_local_hour(ns; tz) - session_open))
            0 <= m <= nmin && (act[m+1, i] += 1)
        end
    end
    hm_hours = session_open .+ (0:nmin) ./ 60
    ax2 = Axis(
        fig[1, 2];
        xlabel = "Exchange time [HH:MM]",
        ylabel = "Trading day",
        xticks = _hhmm_ticks(session_open, session_close),
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

function MarketTickStreamer.save_session_figures(
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
    with_theme(MarketTickStreamer.tick_theme()) do
        for key in sort!(collect(keys(groups)))
            sym, date = key
            g = groups[key]
            if length(g) < min_trades
                @info "skipping sparse group" symbol = sym date n = length(g)
                continue
            end
            fig = MarketTickStreamer.session_figure(g; tz)
            for fmt in formats
                path = _safesave(joinpath(out_dir, "$(sym)_$(date).$(fmt)")) do tmp
                    fmt == "png" ? save(tmp, fig; px_per_unit = 4) : save(tmp, fig)
                end
                push!(written, path)
            end
        end
    end
    return written
end

MarketTickStreamer.save_session_figures(path::AbstractString, args...; kwargs...) =
    MarketTickStreamer.save_session_figures([path], args...; kwargs...)

function MarketTickStreamer.save_overview_figures(
    processed_dir::AbstractString,
    out_dir::AbstractString = joinpath(PROJECT_ROOT, "plots");
    symbols = nothing,
    from::Union{Nothing,Date} = nothing,
    to::Union{Nothing,Date} = nothing,
    formats = ("pdf", "png"),
    tz::TimeZone = tz"America/New_York",
    session::Tuple{<:Real,<:Real} = (9.5, 16.0),
    min_days::Integer = 2,
)
    isdir(processed_dir) || throw(ArgumentError("no processed directory at $processed_dir"))
    mkpath(out_dir)
    written = String[]
    with_theme(MarketTickStreamer.tick_theme()) do
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
            fig = MarketTickStreamer.overview_figure(sym, days; tz, session)
            span = "$(days[1][1])_$(days[end][1])"
            for fmt in formats
                path = _safesave(joinpath(out_dir, "$(sym)_$(span).$(fmt)")) do tmp
                    fmt == "png" ? save(tmp, fig; px_per_unit = 4) : save(tmp, fig)
                end
                push!(written, path)
            end
        end
    end
    return written
end

end # module
