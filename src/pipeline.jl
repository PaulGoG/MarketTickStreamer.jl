# Session orchestration: wire a tick source to sinks (and future analysis
# taps), with structured logging and graceful shutdown.

"""
    setup_logging(cfg; session_id) -> AbstractLogger

Console logger at `cfg.log_level`, optionally teed to
`cfg.log_dir/<session_id>.log` (plain formatting, no ANSI). The caller
installs it with `global_logger`.
"""
function setup_logging(cfg::Config; session_id::AbstractString)
    level = Dict("debug" => Logging.Debug, "info" => Logging.Info,
                 "warn" => Logging.Warn, "error" => Logging.Error)[cfg.log_level]
    console = ConsoleLogger(stderr, level)
    cfg.log_to_file || return console
    mkpath(cfg.log_dir)
    file = FormatLogger(open(joinpath(cfg.log_dir, "$(session_id).log"), "a")) do io, args
        println(io, "[", Dates.format(Dates.now(UTC), dateformat"yyyy-mm-dd HH:MM:SS"),
                "Z] ", args.level, " ", args.message,
                isempty(args.kwargs) ? "" : " | " *
                    join(("$k=$v" for (k, v) in args.kwargs), " "))
    end
    return TeeLogger(console, MinLevelLogger(file, level))
end

"""
    tee(src::Channel{Trade}, n; capacity = 10_000) -> Vector{Channel{Trade}}

Fan one tick stream out to `n` independent consumers (e.g. persistence plus a
real-time analysis tap). Every output receives every tick, in order. All
outputs close when `src` closes. Backpressure propagates: one stalled
consumer eventually stalls the whole fan-out — by design, so data is never
silently dropped.
"""
function tee(src::Channel{Trade}, n::Integer; capacity::Integer = 10_000)
    outs = [Channel{Trade}(capacity) for _ in 1:n]
    Threads.@spawn begin
        try
            for t in src
                for o in outs
                    put!(o, t)
                end
            end
        finally
            foreach(close, outs)
        end
    end
    return outs
end

session_id(cfg::Config) =
    "$(cfg.provider)_$(cfg.feed)_$(Dates.format(Dates.now(UTC), dateformat"yyyymmdd-HHMMSS"))"

"""
    run_stream(cfg::Config; provider = nothing) -> NamedTuple

Run one complete live capture session:

1. optional market-clock gate (`stream.require_market_open`),
2. live WebSocket source → bounded channel,
3. batched raw NDJSON persistence,
4. graceful shutdown on Ctrl-C, session deadline, or stream termination.

Returns `(; ticks, raw_files)`. Blocks until the session ends.
"""
function run_stream(cfg::Config; provider::Union{AbstractProvider, Nothing} = nothing)
    sid = session_id(cfg)
    global_logger(setup_logging(cfg; session_id = sid))
    if provider === nothing
        key, secret = load_credentials!()
        provider = AlpacaProvider(cfg, key, secret)
    end

    mkpath(cfg.raw_dir)
    free = free_disk_gb(cfg.raw_dir)
    free < cfg.min_free_disk_gb &&
        error("only $(round(free; digits = 2)) GiB free on $(cfg.raw_dir), " *
              "below limits.min_free_disk_gb = $(cfg.min_free_disk_gb) — not starting")

    clock = nothing
    if cfg.require_market_open || cfg.stop_at_market_close
        clock = market_clock(provider)
        if !clock.is_open && cfg.require_market_open
            if cfg.wait_for_open
                wait_s = (rfc3339_to_ns(clock.next_open) - now_ns()) / 1e9 + 10  # settle past the bell
                @info "market closed — waiting for open" next_open = clock.next_open hours =
                    round(wait_s / 3600; digits = 2)
                sleep(max(wait_s, 0.0))
                clock = market_clock(provider)          # refresh next_close for the new session
            else
                @info "market closed — not streaming (set stream.wait_for_open to wait)" next_open =
                    clock.next_open
                return (; ticks = 0, raw_files = String[])
            end
        end
        clock.is_open && @info "market open" next_close = clock.next_close
    end

    session = live_source(provider, cfg)
    cfg.stop_at_market_close && clock !== nothing &&
        schedule_close_stop!(session, rfc3339_to_ns(String(clock.next_close)))
    sink = open_raw_sink(cfg.raw_dir, sid; max_mb = cfg.max_raw_file_mb)
    @info "session started" id = sid raw = sink.path symbols = cfg.symbols

    on_flush = function (n, tot)
        @info "flushed batch" batch = n total = tot
        occ = Base.n_avail(session.channel)
        occ > 0.8 * cfg.channel_capacity &&
            @warn "tick channel nearly full — sink is lagging the stream" occupancy = occ capacity =
                cfg.channel_capacity
        f = free_disk_gb(cfg.raw_dir)
        if f < cfg.min_free_disk_gb
            @error "free disk below limit — stopping session" free_gb = round(f; digits = 2)
            stop!(session)
        end
    end
    sink_task = Threads.@spawn run_sink!(session.channel, sink;
        flush_interval_s = cfg.flush_interval_s,
        flush_max_ticks = cfg.flush_max_ticks,
        on_flush)

    ticks = 0
    try
        ticks = fetch(sink_task)
    catch e
        if e isa InterruptException
            @info "interrupt — shutting down gracefully"
        else
            rethrow()
        end
    finally
        stop!(session)                       # idempotent; unblocks the producer
        try ticks = fetch(sink_task) catch end   # final drain + flush
        close_sink!(sink)
    end
    files = filter(f -> startswith(basename(f), sid),
                   readdir(cfg.raw_dir; join = true, sort = true))
    @info "session finished" ticks files = length(files)
    return (; ticks, raw_files = files)
end

"""
    run_backfill(cfg::Config; provider = nothing) -> Vector{String}

Download historical trades for every configured symbol over
`[backfill.start_date, backfill.end_date]`, persist them through the same
raw-NDJSON + compaction path as live data (marked by `recv_ns = 0`), and
return the processed file paths.
"""
function run_backfill(cfg::Config; provider::Union{AbstractProvider, Nothing} = nothing)
    sid = "backfill_" * session_id(cfg)
    global_logger(setup_logging(cfg; session_id = sid))
    if provider === nothing
        key, secret = load_credentials!()
        provider = AlpacaProvider(cfg, key, secret)
    end
    mkpath(cfg.raw_dir)
    free = free_disk_gb(cfg.raw_dir)
    free < cfg.min_free_disk_gb &&
        error("only $(round(free; digits = 2)) GiB free on $(cfg.raw_dir), " *
              "below limits.min_free_disk_gb = $(cfg.min_free_disk_gb) — not starting")
    sink = open_raw_sink(cfg.raw_dir, sid; max_mb = cfg.max_raw_file_mb)
    prog = Progress(length(cfg.symbols); desc = "Backfill: ", enabled = isinteractive())
    for sym in cfg.symbols
        trades = historical_trades(provider, sym, cfg.backfill_start, cfg.backfill_end;
            feed = cfg.backfill_feed,
            page_limit = cfg.backfill_page_limit,
            rate_sleep_s = cfg.backfill_rate_sleep_s,
            on_page = (n, tot) -> @debug("page", symbol = sym, rows = n, total = tot))
        write_batch!(sink, trades)
        @info "backfilled" symbol = sym trades = length(trades)
        next!(prog)
    end
    close_sink!(sink)
    files = filter(f -> startswith(basename(f), sid),
                   readdir(cfg.raw_dir; join = true, sort = true))
    processed = compact_raw(files, cfg.processed_dir; format = cfg.processed_format)
    @info "backfill compacted" files = processed
    return processed
end
