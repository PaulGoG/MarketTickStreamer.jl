"""
    MarketTickStreamer

Provider-agnostic tick-by-tick market data acquisition for scientific time
series research: live WebSocket streaming, historical REST backfill,
append-only raw persistence, compaction to analysis-ready files, and paced
replay of recorded sessions.

Everything is driven by a TOML configuration (`config/config.toml` in a clone);
credentials come from the environment file it names.
Entry points: [`run_stream`](@ref), [`run_backfill`](@ref),
[`replay_source`](@ref), [`compact_raw`](@ref), [`read_processed`](@ref).

Plotting is optional. The diagnostic figures ([`session_figure`](@ref),
[`overview_figure`](@ref) and their `save_*` drivers) are implemented by a
package extension that loads with CairoMakie, and the terminal plots of
[`monitor_raw`](@ref) by one that loads with UnicodePlots; neither is a
dependency of the package.
"""
module MarketTickStreamer

# Every name entering this namespace is listed: the test suite asserts with
# ExplicitImports.jl that nothing arrives implicitly, so an import that is no
# longer used, or a new name used without being imported, fails the suite.
using Base64: base64decode
using CRC32c: crc32c
using Dates:
    Dates, @dateformat_str, Day, Millisecond, datetime2unix, dayofweek, unix2datetime
using FileWatching: Pidfile
using InteractiveUtils: InteractiveUtils
using LinearAlgebra: LinearAlgebra
using Mmap: Mmap
using Printf: @sprintf
using Random: RandomDevice, Xoshiro
using SHA: sha256
using Statistics: Statistics, median
using TOML: TOML

using Arrow: Arrow
using CSV: CSV
using CodecZlib: GzipDecompressorStream, ZlibError
using DataFrames: DataFrames, DataFrame
using DotEnv: DotEnv
using HTTP: HTTP
using JSON: JSON
using LoggingExtras:
    LoggingExtras,
    ConsoleLogger,
    EarlyFilteredLogger,
    FormatLogger,
    Logging,
    MinLevelLogger,
    TeeLogger
using ProgressMeter: ProgressMeter, Progress, next!
using TimeZones:
    TimeZones, @tz_str, Date, DateTime, TimeZone, UTC, ZonedDateTime, astimezone, now
using ZipArchives: ZipReader, zip_names, zip_readentry

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
export tick_bars, volume_bars, dollar_bars, event_bars
export monitor_raw
export replay_source, processed_files
export corpus_inventory, eachday, read_processed, trades_table, ProcessedDays
export RestPolicy
export AbstractProvider,
    LiveSession, FatalStreamError, live_source, stop!, schedule_close_stop!, tee, snapshots
export AlpacaProvider, BinanceProvider
export market_clock, historical_trades, historical_trade_count, bulk_trades, condition_map
export itch50_trades,
    itch50_date, iex_hist_trades, fetch_itch50, fetch_iex_hist, iex_hist_index
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
include("corpus.jl")
include("replay.jl")
include("live.jl")
include("providers/alpaca.jl")
include("providers/binance.jl")
include("formats/itch50.jl")
include("formats/iex_hist.jl")
include("pipeline.jl")
include("diagnostics.jl")
include("figures.jl")

end # module
