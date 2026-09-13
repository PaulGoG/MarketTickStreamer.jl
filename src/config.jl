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
    # [quality]
    non_price_conditions::Dict{String,Vector{String}}
    # [<provider>] endpoint roots
    endpoints::Dict{String,String}
    # The provider's calendar — the clock that decides which date a print
    # belongs to. Derived from the provider, not configurable: it is a
    # property of the venue, not a preference.
    exchange_tz::TimeZone
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
    # The provider decides its own vocabulary and calendar; validating either
    # here would bake one vendor into every other.
    spec = provider_spec(provider)
    feed = get(tbl("provider"), "feed", first(spec.feeds))
    feed in spec.feeds || throw(
        ArgumentError(
            "provider.feed must be one of $(join(map(repr, spec.feeds), ", ")) " *
            "for provider \"$provider\", got \"$feed\"",
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
    exchange_today = Date(now(spec.tz))
    bf_start = _resolve_config_date(
        String(get(bf, "start_date", "today-1d")),
        "backfill.start_date",
        exchange_today,
    )
    bf_end = _resolve_config_date(
        String(get(bf, "end_date", string(bf_start))),
        "backfill.end_date",
        exchange_today,
    )
    bf_start <= bf_end || throw(ArgumentError("backfill.start_date is after end_date"))
    bf_feed = get(bf, "feed", last(spec.backfill_feeds))
    bf_feed in spec.backfill_feeds || throw(
        ArgumentError(
            "backfill.feed must be one of $(join(map(repr, spec.backfill_feeds), ", ")) " *
            "for provider \"$provider\", got \"$bf_feed\"",
        ),
    )

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

    # Sale-condition sets are config-driven because they change which prints
    # count as price-forming, which is a scientific choice; the default lands
    # in every session sidecar with the rest of the configuration.
    qual = get(tbl("quality"), "non_price_conditions", nothing)
    non_price = if qual === nothing
        deepcopy(NON_PRICE_CONDITIONS)
    else
        d = Dict{String,Vector{String}}()
        for (tape, codes) in qual
            codes isa AbstractVector || throw(
                ArgumentError(
                    "quality.non_price_conditions.$tape must be an array of condition codes",
                ),
            )
            for c in codes
                (c isa AbstractString && !isempty(c)) || throw(
                    ArgumentError(
                        "quality.non_price_conditions.$tape must contain non-empty strings, got $(repr(c))",
                    ),
                )
            end
            d[String(tape)] = String[String(c) for c in codes]
        end
        d
    end

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
        non_price,
        endpoints,
        spec.tz,
    )
end

_resolve(p::AbstractString) = isabspath(p) ? String(p) : normpath(joinpath(PROJECT_ROOT, p))

"""
    _resolve_config_date(spec, key, reference) -> Date

Resolve a configured calendar date. Accepts an ISO date (`"2026-08-12"`) or a
sentinel relative to `reference`: `"today"`, or `"today-<N>d"` with `N` a
non-negative integer number of calendar days. Sentinels keep a committed
configuration from going stale; they count calendar days, not trading days, so
a window may resolve onto a weekend or holiday and yield no prints. Throws
`ArgumentError` naming `key` on any other value.
"""
function _resolve_config_date(spec::AbstractString, key::AbstractString, reference::Date)
    m = match(r"^today(?:-(\d+)d)?$", spec)
    m === nothing || return reference - Day(m[1] === nothing ? 0 : parse(Int, m[1]))
    d = tryparse(Date, spec)
    d === nothing && throw(
        ArgumentError(
            "$key must be an ISO date \"YYYY-MM-DD\" or a sentinel " *
            "\"today\" / \"today-<N>d\", got \"$spec\"",
        ),
    )
    return d
end

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
