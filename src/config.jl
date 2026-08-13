# TOML configuration loading and validation.
#
# Everything tunable comes from config/config.toml; nothing operational is
# hardcoded. Credentials come from `.env` / the process environment only.

"""
    Config

Validated, typed view of `config/config.toml`. Constructed via [`load_config`](@ref).
Field groups mirror the TOML tables; see `config/config.toml` for semantics.
"""
struct Config
    # [provider]
    provider::String
    feed::String
    # [stream]
    symbols::Vector{String}
    channels::Vector{String}
    require_market_open::Bool
    wait_for_open::Bool
    stop_at_market_close::Bool
    reconnect_max_retries::Int
    reconnect_base_delay_s::Float64
    reconnect_max_delay_s::Float64
    stale_timeout_s::Float64
    # [storage]
    data_dir::String
    raw_dir::String
    processed_dir::String
    flush_interval_s::Float64
    flush_max_ticks::Int
    processed_format::String
    # [limits]
    max_session_hours::Float64
    max_raw_file_mb::Int
    channel_capacity::Int
    max_symbols::Int
    min_free_disk_gb::Float64
    max_live_heap_mb::Int
    # [replay]
    replay_pace::String
    replay_speed::Float64
    # [backfill]
    backfill_start::Date
    backfill_end::Date
    backfill_feed::String
    backfill_page_limit::Int
    backfill_rate_sleep_s::Float64
    backfill_resume::Bool
    # [monitor]
    monitor_refresh_s::Float64
    monitor_top_symbols::Int
    monitor_rate_window_s::Float64
    # [logging]
    log_level::String
    log_to_file::Bool
    log_dir::String
    # [<provider>] endpoint roots
    endpoints::Dict{String,String}
end

const PROJECT_ROOT = normpath(joinpath(@__DIR__, ".."))

"""
    load_config(path = joinpath(PROJECT_ROOT, "config", "config.toml")) -> Config

Read and validate the TOML configuration. Relative storage/log paths are
resolved against the project root. Throws `ArgumentError` with a specific
message on any invalid value.
"""
function load_config(path::AbstractString = joinpath(PROJECT_ROOT, "config", "config.toml"))
    isfile(path) || throw(ArgumentError("config file not found: $path"))
    raw = TOML.parsefile(path)

    tbl(name) = get(() -> Dict{String,Any}(), raw, name)
    provider = get(tbl("provider"), "name", "alpaca")
    feed = get(tbl("provider"), "feed", "iex")
    feed in ("iex", "sip", "delayed_sip") || throw(
        ArgumentError(
            "provider.feed must be \"iex\", \"sip\" or \"delayed_sip\", got \"$feed\"",
        ),
    )

    st = tbl("stream")
    symbols = String.(get(st, "symbols", String[]))
    isempty(symbols) && throw(ArgumentError("stream.symbols must not be empty"))
    channels = String.(get(st, "channels", ["trades"]))
    bad = setdiff(channels, ("trades", "quotes", "bars"))
    isempty(bad) || throw(ArgumentError("stream.channels contains unknown entries: $bad"))

    sto = tbl("storage")
    data_dir = _resolve(get(sto, "data_dir", "data"))
    fmt = get(sto, "processed_format", "csv")
    fmt in ("csv", "arrow") ||
        throw(ArgumentError("storage.processed_format must be \"csv\" or \"arrow\""))

    lim = tbl("limits")
    max_symbols = Int(get(lim, "max_symbols", 30))
    length(symbols) <= max_symbols || throw(
        ArgumentError(
            "$(length(symbols)) symbols requested but limits.max_symbols = $max_symbols",
        ),
    )

    rep = tbl("replay")
    pace = get(rep, "pace", "recorded")
    pace in ("recorded", "max") ||
        throw(ArgumentError("replay.pace must be \"recorded\" or \"max\""))
    replay_speed = Float64(get(rep, "speed", 1.0))
    replay_speed > 0 || throw(ArgumentError("replay.speed must be positive"))

    bf = tbl("backfill")
    bf_start = Date(get(bf, "start_date", string(today() - Day(1))))
    bf_end = Date(get(bf, "end_date", string(bf_start)))
    bf_start <= bf_end || throw(ArgumentError("backfill.start_date is after end_date"))
    bf_feed = get(bf, "feed", "sip")
    bf_feed in ("iex", "sip") ||
        throw(ArgumentError("backfill.feed must be \"iex\" or \"sip\""))

    mon = tbl("monitor")
    mon_refresh = Float64(get(mon, "refresh_s", 2.0))
    mon_refresh > 0 || throw(ArgumentError("monitor.refresh_s must be positive"))
    mon_top = Int(get(mon, "top_symbols", 10))
    mon_top >= 1 || throw(ArgumentError("monitor.top_symbols must be >= 1"))
    mon_window = Float64(get(mon, "rate_window_s", 300.0))
    mon_window >= mon_refresh ||
        throw(ArgumentError("monitor.rate_window_s must be >= monitor.refresh_s"))

    lg = tbl("logging")
    level = get(lg, "level", "info")
    level in ("debug", "info", "warn", "error") ||
        throw(ArgumentError("logging.level must be one of debug/info/warn/error"))

    endpoints = Dict{String,String}(k => String(v) for (k, v) in tbl(provider))

    max_resident = Int(get(lim, "max_live_heap_mb", 4096))
    max_resident > 0 || throw(ArgumentError("limits.max_live_heap_mb must be positive"))

    return Config(
        provider,
        feed,
        symbols,
        channels,
        Bool(get(st, "require_market_open", true)),
        Bool(get(st, "wait_for_open", false)),
        Bool(get(st, "stop_at_market_close", true)),
        Int(get(st, "reconnect_max_retries", 10)),
        Float64(get(st, "reconnect_base_delay_s", 1.0)),
        Float64(get(st, "reconnect_max_delay_s", 60.0)),
        Float64(get(st, "stale_timeout_s", 120.0)),
        data_dir,
        joinpath(data_dir, get(sto, "raw_subdir", "raw")),
        joinpath(data_dir, get(sto, "processed_subdir", "processed")),
        Float64(get(sto, "flush_interval_s", 30.0)),
        Int(get(sto, "flush_max_ticks", 5000)),
        fmt,
        Float64(get(lim, "max_session_hours", 8.0)),
        Int(get(lim, "max_raw_file_mb", 1024)),
        Int(get(lim, "channel_capacity", 100_000)),
        max_symbols,
        Float64(get(lim, "min_free_disk_gb", 2.0)),
        max_resident,
        pace,
        replay_speed,
        bf_start,
        bf_end,
        bf_feed,
        Int(get(bf, "page_limit", 10_000)),
        Float64(get(bf, "rate_limit_sleep_s", 0.35)),
        Bool(get(bf, "resume", true)),
        mon_refresh,
        mon_top,
        mon_window,
        level,
        Bool(get(lg, "log_to_file", true)),
        _resolve(get(lg, "log_dir", "logs")),
        endpoints,
    )
end

_resolve(p::AbstractString) = isabspath(p) ? String(p) : normpath(joinpath(PROJECT_ROOT, p))

"""
    load_credentials!(; env_path = joinpath(PROJECT_ROOT, ".env")) -> (key, secret)

Load API credentials from `.env` (if present) into the environment and return
`(key_id, secret_key)`. Looks for `ALPACA_API_KEY_ID` / `ALPACA_SECRET_KEY`.
Throws an error listing what is missing if either is absent.
"""
function load_credentials!(; env_path::AbstractString = joinpath(PROJECT_ROOT, ".env"))
    if isfile(env_path)
        Sys.isunix() &&
            (filemode(env_path) & 0o044) != 0 &&
            @warn "credentials file is group/world-readable — consider `chmod 600`" env_path
        DotEnv.load!(env_path; override = false)
    end
    key = get(ENV, "ALPACA_API_KEY_ID", "")
    secret = get(ENV, "ALPACA_SECRET_KEY", "")
    missing_keys = String[]
    isempty(key) && push!(missing_keys, "ALPACA_API_KEY_ID")
    isempty(secret) && push!(missing_keys, "ALPACA_SECRET_KEY")
    isempty(missing_keys) || error(
        "missing credentials: $(join(missing_keys, ", ")). " *
        "Provide them in $env_path or the environment (see .env.example).",
    )
    return key, secret
end
