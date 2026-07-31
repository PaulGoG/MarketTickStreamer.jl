"""
    TickStreamer

Provider-agnostic tick-by-tick market data acquisition for scientific time
series research: live WebSocket streaming, historical REST backfill,
append-only raw persistence, compaction to analysis-ready files, and paced
replay of recorded sessions.

Everything is driven by `config/config.toml`; credentials come from `.env`.
Entry points: [`run_stream`](@ref), [`run_backfill`](@ref),
[`replay_source`](@ref), [`compact_raw`](@ref).
"""
module TickStreamer

using Dates
using Logging
using TOML

using Arrow
using CSV
using DataFrames
using DotEnv
using HTTP
using JSON3
using LoggingExtras
using ProgressMeter
using TimeZones

export Config, load_config, load_credentials!
export Trade, rfc3339_to_ns, ns_to_rfc3339, ns_to_datetime, now_ns, trading_date
export RawSink, open_raw_sink, write_batch!, close_sink!, run_sink!,
       trade_to_json, json_to_trade, read_raw, compact_raw
export replay_source
export AbstractProvider, LiveSession, FatalStreamError, live_source, stop!, tee
export AlpacaProvider, market_clock, historical_trades
export run_stream, run_backfill, session_id, setup_logging

include("schema.jl")
include("config.jl")
include("sinks.jl")
include("replay.jl")
include("live.jl")
include("providers/alpaca.jl")
include("pipeline.jl")

end # module
