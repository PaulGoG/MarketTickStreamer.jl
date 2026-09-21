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
    replay_clock::String
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

# Keys accepted in each configuration table. Provider tables (KNOWN_PROVIDERS)
# hold endpoint roots and accept any string-valued key.
const CONFIG_KEYS = Dict{String,Vector{String}}(
    "provider" => ["name", "feed"],
    "stream" => [
        "symbols",
        "channels",
        "require_market_open",
        "wait_for_open",
        "stop_at_market_close",
        "reconnect_max_retries",
        "reconnect_base_delay_s",
        "reconnect_max_delay_s",
        "stale_timeout_s",
    ],
    "storage" => [
        "data_dir",
        "raw_subdir",
        "processed_subdir",
        "flush_interval_s",
        "flush_max_ticks",
        "processed_format",
    ],
    "limits" => [
        "max_session_hours",
        "max_raw_file_mb",
        "channel_capacity",
        "max_symbols",
        "min_free_disk_gb",
        "max_live_heap_mb",
    ],
    "replay" => ["pace", "speed", "clock"],
    "backfill" => [
        "start_date",
        "end_date",
        "feed",
        "page_limit",
        "rate_limit_sleep_s",
        "resume",
    ],
    "monitor" => ["refresh_s", "top_symbols", "rate_window_s"],
    "logging" => ["level", "log_to_file", "log_dir"],
    "quality" => ["non_price_conditions"],
)

"""
    _cfg_value(tbl, table, key, T, default) -> T

Read `tbl[key]`, falling back to `default`, and require it to be of type `T`
(one of `Bool`, `Int`, `Float64`, `String`). Throws `ArgumentError` naming
`table.key` otherwise, so a mistyped value cannot reach the pipeline.
"""
function _cfg_value(
    tbl::AbstractDict,
    table::AbstractString,
    key::AbstractString,
    ::Type{Bool},
    default,
)
    v = get(tbl, key, default)
    v isa Bool || throw(ArgumentError("$table.$key must be a boolean, got $(repr(v))"))
    return v::Bool
end

function _cfg_value(
    tbl::AbstractDict,
    table::AbstractString,
    key::AbstractString,
    ::Type{Int},
    default,
)
    v = get(tbl, key, default)
    (v isa Integer && !(v isa Bool)) ||
        throw(ArgumentError("$table.$key must be an integer, got $(repr(v))"))
    return Int(v)
end

function _cfg_value(
    tbl::AbstractDict,
    table::AbstractString,
    key::AbstractString,
    ::Type{Float64},
    default,
)
    v = get(tbl, key, default)
    (v isa Real && !(v isa Bool)) ||
        throw(ArgumentError("$table.$key must be a number, got $(repr(v))"))
    return Float64(v)
end

function _cfg_value(
    tbl::AbstractDict,
    table::AbstractString,
    key::AbstractString,
    ::Type{String},
    default,
)
    v = get(tbl, key, default)
    v isa AbstractString ||
        throw(ArgumentError("$table.$key must be a string, got $(repr(v))"))
    return String(v)
end

# Array-of-strings counterpart of `_cfg_value`.
function _cfg_strings(
    tbl::AbstractDict,
    table::AbstractString,
    key::AbstractString,
    default::Vector{String},
)
    v = get(tbl, key, default)
    # An explicit loop: `all` over a vector of unknown element type infers as
    # a three-valued result, which JET rejects in a boolean context.
    strings = v isa AbstractVector
    if strings
        for x in v
            strings &= x isa AbstractString
        end
    end
    strings ||
        throw(ArgumentError("$table.$key must be an array of strings, got $(repr(v))"))
    return String[String(x) for x in v]
end

"""
    _reject_unknown_keys(raw)

Check the parsed TOML against `CONFIG_KEYS` and the known provider tables. Throws `ArgumentError` naming the offending table or key, so a misspelt
entry cannot be silently ignored in favour of a default.
"""
function _reject_unknown_keys(raw::AbstractDict)
    for (name, value) in raw
        if haskey(CONFIG_KEYS, name)
            value isa AbstractDict || throw(ArgumentError("[$name] must be a table"))
            known = CONFIG_KEYS[name]
            for key in keys(value)
                key in known || throw(
                    ArgumentError(
                        "unknown config key $name.$key; known keys: $(join(known, ", "))",
                    ),
                )
            end
        elseif name in KNOWN_PROVIDERS
            value isa AbstractDict || throw(ArgumentError("[$name] must be a table"))
            for (key, v) in value
                v isa AbstractString ||
                    throw(ArgumentError("$name.$key must be a string, got $(repr(v))"))
            end
        else
            tables = sort!([collect(keys(CONFIG_KEYS)); collect(KNOWN_PROVIDERS)])
            throw(
                ArgumentError(
                    "unknown config table [$name]; known tables: $(join(tables, ", "))",
                ),
            )
        end
    end
    return nothing
end

"""
    load_config(path = joinpath(PROJECT_ROOT, "config", "config.toml")) -> Config

Read and validate the TOML configuration. Relative storage/log paths are
resolved against the project root. Every key is checked for type and documented
bounds, and unknown tables or keys are rejected, so a misspelt key cannot fall
back to its default unnoticed. Throws `ArgumentError` naming the offending key.
"""
function load_config(path::AbstractString = joinpath(PROJECT_ROOT, "config", "config.toml"))
    isfile(path) || throw(ArgumentError("config file not found: $path"))
    raw = TOML.parsefile(path)
    _reject_unknown_keys(raw)

    tbl(name) = get(() -> Dict{String,Any}(), raw, name)
    provider = _cfg_value(tbl("provider"), "provider", "name", String, "alpaca")
    # The provider decides its own vocabulary and calendar; validating either
    # here would bake one vendor into every other.
    spec = provider_spec(provider)
    feed = _cfg_value(tbl("provider"), "provider", "feed", String, first(spec.feeds))
    feed in spec.feeds || throw(
        ArgumentError(
            "provider.feed must be one of $(join(map(repr, spec.feeds), ", ")) " *
            "for provider \"$provider\", got \"$feed\"",
        ),
    )

    st = tbl("stream")
    symbols = _cfg_strings(st, "stream", "symbols", String[])
    isempty(symbols) && throw(ArgumentError("stream.symbols must not be empty"))
    channels = _cfg_strings(st, "stream", "channels", ["trades"])
    bad = setdiff(channels, ("trades", "quotes", "bars"))
    isempty(bad) || throw(ArgumentError("stream.channels contains unknown entries: $bad"))
    max_retries = _cfg_value(st, "stream", "reconnect_max_retries", Int, 10)
    max_retries >= 0 ||
        throw(ArgumentError("stream.reconnect_max_retries must be >= 0, got $max_retries"))
    base_delay = _cfg_value(st, "stream", "reconnect_base_delay_s", Float64, 1.0)
    base_delay > 0 ||
        throw(ArgumentError("stream.reconnect_base_delay_s must be > 0, got $base_delay"))
    max_delay = _cfg_value(st, "stream", "reconnect_max_delay_s", Float64, 60.0)
    max_delay >= base_delay || throw(
        ArgumentError(
            "stream.reconnect_max_delay_s must be >= stream.reconnect_base_delay_s, " *
            "got $max_delay",
        ),
    )
    stale_timeout = _cfg_value(st, "stream", "stale_timeout_s", Float64, 120.0)
    stale_timeout > 0 ||
        throw(ArgumentError("stream.stale_timeout_s must be > 0, got $stale_timeout"))

    sto = tbl("storage")
    data_dir = _resolve(_cfg_value(sto, "storage", "data_dir", String, "data"))
    fmt = _cfg_value(sto, "storage", "processed_format", String, "csv")
    fmt in ("csv", "arrow") ||
        throw(ArgumentError("storage.processed_format must be \"csv\" or \"arrow\""))
    flush_interval = _cfg_value(sto, "storage", "flush_interval_s", Float64, 30.0)
    flush_interval > 0 ||
        throw(ArgumentError("storage.flush_interval_s must be > 0, got $flush_interval"))
    flush_max_ticks = _cfg_value(sto, "storage", "flush_max_ticks", Int, 5000)
    flush_max_ticks >= 1 ||
        throw(ArgumentError("storage.flush_max_ticks must be >= 1, got $flush_max_ticks"))

    lim = tbl("limits")
    max_symbols = _cfg_value(lim, "limits", "max_symbols", Int, 30)
    max_symbols >= 1 ||
        throw(ArgumentError("limits.max_symbols must be >= 1, got $max_symbols"))
    length(symbols) <= max_symbols || throw(
        ArgumentError(
            "$(length(symbols)) symbols requested but limits.max_symbols = $max_symbols",
        ),
    )
    max_session_hours = _cfg_value(lim, "limits", "max_session_hours", Float64, 8.0)
    max_session_hours > 0 ||
        throw(ArgumentError("limits.max_session_hours must be > 0, got $max_session_hours"))
    max_raw_file_mb = _cfg_value(lim, "limits", "max_raw_file_mb", Int, 1024)
    max_raw_file_mb >= 1 ||
        throw(ArgumentError("limits.max_raw_file_mb must be >= 1, got $max_raw_file_mb"))
    channel_capacity = _cfg_value(lim, "limits", "channel_capacity", Int, 100_000)
    channel_capacity >= 1 ||
        throw(ArgumentError("limits.channel_capacity must be >= 1, got $channel_capacity"))
    min_free_disk_gb = _cfg_value(lim, "limits", "min_free_disk_gb", Float64, 2.0)
    min_free_disk_gb >= 0 ||
        throw(ArgumentError("limits.min_free_disk_gb must be >= 0, got $min_free_disk_gb"))
    max_resident = _cfg_value(lim, "limits", "max_live_heap_mb", Int, 4096)
    max_resident > 0 ||
        throw(ArgumentError("limits.max_live_heap_mb must be > 0, got $max_resident"))

    rep = tbl("replay")
    pace = _cfg_value(rep, "replay", "pace", String, "recorded")
    pace in ("recorded", "max") ||
        throw(ArgumentError("replay.pace must be \"recorded\" or \"max\""))
    replay_speed = _cfg_value(rep, "replay", "speed", Float64, 1.0)
    replay_speed > 0 || throw(ArgumentError("replay.speed must be positive"))
    replay_clock = _cfg_value(rep, "replay", "clock", String, "auto")
    replay_clock in ("auto", "recv", "exchange") || throw(
        ArgumentError(
            "replay.clock must be one of \"auto\", \"recv\", \"exchange\", " *
            "got \"$replay_clock\"",
        ),
    )

    bf = tbl("backfill")
    exchange_today = Date(now(spec.tz))
    config_date(v, key) =
        v isa AbstractString ? _resolve_config_date(String(v), key, exchange_today) :
        v isa Date ? v :
        throw(ArgumentError("$key must be a date string or a TOML date, got $(repr(v))"))
    bf_start = config_date(get(bf, "start_date", "today-1d"), "backfill.start_date")
    bf_end = config_date(get(bf, "end_date", string(bf_start)), "backfill.end_date")
    bf_start <= bf_end || throw(ArgumentError("backfill.start_date is after end_date"))
    bf_feed = _cfg_value(bf, "backfill", "feed", String, last(spec.backfill_feeds))
    bf_feed in spec.backfill_feeds || throw(
        ArgumentError(
            "backfill.feed must be one of $(join(map(repr, spec.backfill_feeds), ", ")) " *
            "for provider \"$provider\", got \"$bf_feed\"",
        ),
    )
    page_limit = _cfg_value(bf, "backfill", "page_limit", Int, spec.max_page_limit)
    1 <= page_limit <= spec.max_page_limit || throw(
        ArgumentError(
            "backfill.page_limit must be in 1:$(spec.max_page_limit) " *
            "for provider \"$provider\", got $page_limit",
        ),
    )
    rate_sleep = _cfg_value(bf, "backfill", "rate_limit_sleep_s", Float64, 0.35)
    rate_sleep >= 0 ||
        throw(ArgumentError("backfill.rate_limit_sleep_s must be >= 0, got $rate_sleep"))

    mon = tbl("monitor")
    mon_refresh = _cfg_value(mon, "monitor", "refresh_s", Float64, 2.0)
    mon_refresh > 0 || throw(ArgumentError("monitor.refresh_s must be positive"))
    mon_top = _cfg_value(mon, "monitor", "top_symbols", Int, 10)
    mon_top >= 1 || throw(ArgumentError("monitor.top_symbols must be >= 1"))
    mon_window = _cfg_value(mon, "monitor", "rate_window_s", Float64, 300.0)
    mon_window >= mon_refresh ||
        throw(ArgumentError("monitor.rate_window_s must be >= monitor.refresh_s"))

    lg = tbl("logging")
    level = _cfg_value(lg, "logging", "level", String, "info")
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

    return Config(
        provider,
        feed,
        symbols,
        channels,
        _cfg_value(st, "stream", "require_market_open", Bool, true),
        _cfg_value(st, "stream", "wait_for_open", Bool, false),
        _cfg_value(st, "stream", "stop_at_market_close", Bool, true),
        max_retries,
        base_delay,
        max_delay,
        stale_timeout,
        data_dir,
        joinpath(data_dir, _cfg_value(sto, "storage", "raw_subdir", String, "raw")),
        joinpath(
            data_dir,
            _cfg_value(sto, "storage", "processed_subdir", String, "processed"),
        ),
        flush_interval,
        flush_max_ticks,
        fmt,
        max_session_hours,
        max_raw_file_mb,
        channel_capacity,
        max_symbols,
        min_free_disk_gb,
        max_resident,
        pace,
        replay_speed,
        replay_clock,
        bf_start,
        bf_end,
        bf_feed,
        page_limit,
        rate_sleep,
        _cfg_value(bf, "backfill", "resume", Bool, true),
        mon_refresh,
        mon_top,
        mon_window,
        level,
        _cfg_value(lg, "logging", "log_to_file", Bool, true),
        _resolve(_cfg_value(lg, "logging", "log_dir", String, "logs")),
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
