"""
    MarketTickStreamer

Provider-agnostic tick-by-tick market data acquisition for scientific time
series research: live WebSocket streaming, historical REST backfill,
append-only raw persistence, compaction to analysis-ready files, and paced
replay of recorded sessions.

Everything is driven by `config/config.toml`; credentials come from `.env`.
Entry points: [`run_stream`](@ref), [`run_backfill`](@ref),
[`replay_source`](@ref), [`compact_raw`](@ref).
"""
module MarketTickStreamer

# Every name entering this namespace is listed: the test suite asserts with
# ExplicitImports.jl that nothing arrives implicitly, so an import that is no
# longer used, or a new name used without being imported, fails the suite.
using Dates: Dates, @dateformat_str, Day, datetime2unix, dayofweek, unix2datetime
using FileWatching: Pidfile
using InteractiveUtils: InteractiveUtils
using LinearAlgebra: LinearAlgebra
using Printf: @sprintf
using Statistics: Statistics, median
using TOML: TOML

using Arrow: Arrow
using CairoMakie:
    CairoMakie,
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
    vlines!,
    with_theme,
    ylims!
using CSV: CSV
using DataFrames: DataFrames, DataFrame, nrow
using DotEnv: DotEnv
using HTTP: HTTP
using JSON3: JSON3
using LoggingExtras:
    LoggingExtras,
    ConsoleLogger,
    EarlyFilteredLogger,
    FormatLogger,
    Logging,
    MinLevelLogger,
    TeeLogger,
    global_logger
using MathTeXEngine: MathTeXEngine, @L_str, texfont
using ProgressMeter: ProgressMeter, Progress, next!
using TimeZones:
    TimeZones, @tz_str, Date, DateTime, TimeZone, UTC, ZonedDateTime, astimezone, now
using UnicodePlots: barplot, lineplot

export Config, load_config, load_credentials!
export Trade, Quote, Bar
export rfc3339_to_ns, ns_to_rfc3339, ns_to_datetime, now_ns, trading_date
export RawSink,
    open_raw_sink,
    write_batch!,
    close_sink!,
    run_sink!,
    trade_to_json,
    json_to_trade,
    read_raw,
    compact_raw
export deduplicate_trades, session_report, free_disk_gb, check_live_heap, coverage_report
export NON_PRICE_CONDITIONS, price_forming, filter_price_forming, observed_round_lot
export tick_bars, volume_bars, dollar_bars
export monitor_raw
export replay_source
export AbstractProvider,
    LiveSession, FatalStreamError, live_source, stop!, schedule_close_stop!, tee
export AlpacaProvider, BinanceProvider
export market_clock, historical_trades, historical_trade_count, condition_map
export ProviderSpec, provider_spec, make_provider, exchange_tz, always_open, session_days
export run_stream,
    run_backfill,
    session_id,
    setup_logging,
    write_session_meta,
    start_session_meta,
    finalize_session_meta,
    reconcile_sessions!,
    acquire_session_lock,
    run_entrypoint
export tick_theme,
    session_figure, save_session_figures, overview_figure, save_overview_figures

include("schema.jl")
include("config.jl")
include("sinks.jl")
include("quality.jl")
include("resample.jl")
include("monitor.jl")
include("replay.jl")
include("live.jl")
include("providers/alpaca.jl")
include("providers/binance.jl")
include("pipeline.jl")
include("visualization.jl")

end # module
