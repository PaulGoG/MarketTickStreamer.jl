# Figure interface. The functions are declared and documented here and
# implemented by the package extension MarketTickStreamerMakieExt, which
# loads once CairoMakie is loaded; the package itself carries no plotting
# dependency.

"""
    tick_theme() -> Theme

Publication defaults: Computer Modern fonts, 26 pt labels over 22 pt ticks,
boxed axes with 1.5-wide spines, inward ticks, no minor ticks, faint dashed
grey grid, 3-wide data lines, frameless horizontal legends.

Implemented by the CairoMakie extension: `using CairoMakie` first.
"""
function tick_theme end

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

Implemented by the CairoMakie extension: `using CairoMakie` first.
"""
function session_figure end

"""
    overview_figure(symbol, days; tz = tz"America/New_York",
                    non_price = NON_PRICE_CONDITIONS,
                    session = (9.5, 16.0)) -> Figure

Multi-day diagnostic figure for one symbol from per-day processed tables
(`days` is a vector of `(date, DataFrame)` pairs, sorted internally by date):

- price path on a concatenated *trading-time* axis (overnight gaps removed,
  session boundaries dashed, days labeled at their centers), drawn from the
  prints that [`price_forming`](@ref) admits under `non_price`,
- day × session-minute activity heatmap,
- pooled **intra-session** inter-arrival CCDF (overnight gaps excluded by
  construction — they would contaminate the waiting-time tail),
- pooled trade-size CCDF with fitted tail exponent.

`session` is the venue's regular session as exchange-local hours
`(open, close)`, `0 <= open < close <= 24`; it sets the width of a day on the
concatenated axis and the span of the activity heatmap. The default is the US
equity session; a venue that never closes takes `(0.0, 24.0)`.

Implemented by the CairoMakie extension: `using CairoMakie` first.
"""
function overview_figure end

"""
    save_session_figures(raw_paths, out_dir = joinpath(PROJECT_ROOT, "plots");
                         formats = ("pdf", "png"), tz = tz"America/New_York",
                         min_trades = 10) -> Vector{String}

Render one diagnostic figure per (symbol, trading day) found in the raw
NDJSON `raw_paths`, saved as `out_dir/SYMBOL_YYYY-MM-DD.pdf|png` (safesave —
the new figure takes the canonical name, a displaced one is kept as `_#N`).
Groups with fewer than `min_trades` ticks are skipped with an `@info`
(distribution panels are meaningless). PNG output is written at
`px_per_unit = 4`. Returns the files written.

Implemented by the CairoMakie extension: `using CairoMakie` first.
"""
function save_session_figures end

"""
    save_overview_figures(processed_dir, out_dir = joinpath(PROJECT_ROOT, "plots");
                          symbols = nothing, from = nothing, to = nothing,
                          formats = ("pdf", "png"), tz = tz"America/New_York",
                          session = (9.5, 16.0), min_days = 2) -> Vector{String}

Render one multi-day [`overview_figure`](@ref) per symbol from the processed
tree (`processed_dir/SYMBOL/YYYY-MM-DD.csv|.arrow`; safesave `_#N` backups
and `.partial` files are ignored — the base file per day is authoritative
and always the newest). `symbols`, `from`, and `to` restrict the sweep.
Output: `out_dir/SYMBOL_<from>_<to>.pdf|png` (safesave). Days are loaded one
symbol at a time, so memory stays bounded by one symbol's span. `session` is
passed to [`overview_figure`](@ref).

Implemented by the CairoMakie extension: `using CairoMakie` first.
"""
function save_overview_figures end

const _FIGURE_FUNCTIONS = (
    tick_theme,
    session_figure,
    overview_figure,
    save_session_figures,
    save_overview_figures,
)

# A call to a figure function without the extension would otherwise end in a
# bare MethodError on a documented, exported name.
function __init__()
    Base.Experimental.register_error_hint(MethodError) do io, exc, _, _
        if any(f -> exc.f === f, _FIGURE_FUNCTIONS) &&
           Base.get_extension(@__MODULE__, :MarketTickStreamerMakieExt) === nothing
            print(
                io,
                "\nThe figure functions are implemented by a package extension: ",
                "load CairoMakie first (`using CairoMakie`).",
            )
        end
    end
    return nothing
end
