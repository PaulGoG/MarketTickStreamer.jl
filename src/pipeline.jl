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
    # During a deliberate shutdown, transport-teardown errors surfacing from
    # HTTP.jl internals (EOFError on a socket we closed) are expected noise.
    quiet(logger) = EarlyFilteredLogger(
        log -> !(SHUTTING_DOWN[] && startswith(string(log._module), "HTTP")), logger)
    console = ConsoleLogger(stderr, level)
    cfg.log_to_file || return quiet(console)
    mkpath(cfg.log_dir)
    # always_flush: an abruptly killed session must not lose its log tail.
    file = FormatLogger(open(joinpath(cfg.log_dir, "$(session_id).log"), "a");
                        always_flush = true) do io, args
        println(io, "[", Dates.format(Dates.now(UTC), dateformat"yyyy-mm-dd HH:MM:SS"),
                "Z] ", args.level, " ", args.message,
                isempty(args.kwargs) ? "" : " | " *
                    join(("$k=$v" for (k, v) in args.kwargs), " "))
    end
    return quiet(TeeLogger(console, MinLevelLogger(file, level)))
end

"""
    tee(src::Channel{Trade}, n; capacity = 10_000, lossy = falses(n),
        on_drop = nothing) -> Vector{Channel{Trade}}

Fan one tick stream out to `n` independent consumers (e.g. persistence plus
real-time analysis taps). Outputs close when `src` closes.

Per-output overflow policy: a non-`lossy` output blocks the fan-out when
full (backpressure — data is never silently dropped; use for persistence,
which must always win). A `lossy` output drops the incoming tick instead
when its buffer is full (use for analysis taps that must never stall the
capture); drops are counted, reported via `on_drop(output_index, n_dropped)`
when given, and warned on first occurrence.
"""
function tee(src::Channel{Trade}, n::Integer; capacity::Integer = 10_000,
             lossy = falses(n), on_drop = nothing)
    outs = [Channel{Trade}(capacity) for _ in 1:n]
    dropped = zeros(Int, n)
    Threads.@spawn begin
        try
            for t in src
                for (i, o) in enumerate(outs)
                    if lossy[i] && Base.n_avail(o) >= capacity
                        dropped[i] += 1
                        dropped[i] == 1 &&
                            @warn "lossy tee output saturated — dropping ticks" output = i
                        on_drop === nothing || on_drop(i, dropped[i])
                    else
                        put!(o, t)
                    end
                end
            end
        finally
            foreach(close, outs)
        end
    end
    return outs
end

session_id(cfg::Config; feed::AbstractString = cfg.feed) =
    "$(cfg.provider)_$(feed)_$(Dates.format(Dates.now(UTC), dateformat"yyyymmdd-HHMMSS"))"

"""
    acquire_session_lock(data_dir) -> lock

One session per data tree: takes a PID-file lock at
`<data_dir>/.session.lock` and throws with a precise message if another
live process already holds it (observed failure mode: two backfills
interleaving on one raw directory). Stale locks from dead processes are
broken automatically; release with `close(lock)`.
"""
function acquire_session_lock(data_dir::AbstractString)
    mkpath(data_dir)
    path = joinpath(data_dir, ".session.lock")
    lock = Pidfile.trymkpidlock(path; stale_age = 600.0)
    lock === false && error(
        "another session already holds $path — one capture/backfill per data " *
        "tree (stale locks from dead processes clear automatically)")
    return lock
end

_git_commit() = try
    strip(read(pipeline(`git -C $(PROJECT_ROOT) rev-parse HEAD`; stderr = devnull), String))
catch
    "unknown"
end

_git_dirty() = try
    !isempty(strip(read(pipeline(`git -C $(PROJECT_ROOT) status --porcelain`; stderr = devnull), String)))
catch
    false
end

_toml_value(v) = v
_toml_value(v::Date) = string(v)
_toml_value(v::AbstractVector) = [_toml_value(x) for x in v]
_toml_value(v::Dict) = Dict{String, Any}(string(k) => _toml_value(x) for (k, x) in v)

"""
    write_session_meta(cfg, sid; status, ticks, raw_files, started_utc,
                       finished_utc = nothing) -> String

Persist a provenance sidecar `<raw_dir>/<sid>.meta.toml` next to the
session's raw files: session summary (id, status, pid, span, tick count,
file list), provenance (git commit + dirty flag, Julia and package
versions, hostname), and the full effective configuration snapshot.
The crash-only lifecycle is [`start_session_meta`](@ref) →
[`finalize_session_meta`](@ref), reconciled at startup by
[`reconcile_sessions!`](@ref).
"""
function write_session_meta(cfg::Config, sid::AbstractString;
                            status::AbstractString, ticks::Integer = 0,
                            raw_files::Vector{String} = String[],
                            started_utc::DateTime,
                            finished_utc::Union{Nothing, DateTime} = nothing)
    session = Dict{String, Any}(
        "id" => String(sid),
        "status" => String(status),
        "pid" => getpid(),
        "started_utc" => string(started_utc),
        "ticks" => Int(ticks),
        "raw_files" => [basename(f) for f in raw_files],
    )
    finished_utc === nothing || (session["finished_utc"] = string(finished_utc))
    meta = Dict{String, Any}(
        "session" => session,
        "provenance" => Dict{String, Any}(
            "git_commit" => _git_commit(),
            "git_dirty" => _git_dirty(),
            "julia_version" => string(VERSION),
            "package_version" => string(something(pkgversion(MarketTickStreamer), "unknown")),
            "hostname" => gethostname(),
        ),
        "config" => Dict{String, Any}(String(f) => _toml_value(getfield(cfg, f))
                                      for f in fieldnames(Config)),
    )
    path = _safepath(joinpath(cfg.raw_dir, "$(sid).meta.toml"))
    open(io -> TOML.print(io, meta), path, "w")
    return path
end

"""
    start_session_meta(cfg, sid; started_utc) -> String

Write the sidecar at session START with `status = "running"` — an abruptly
killed process still leaves its provenance on disk.
"""
start_session_meta(cfg::Config, sid::AbstractString; started_utc::DateTime) =
    write_session_meta(cfg, sid; status = "running", started_utc)

"""
    finalize_session_meta(path, status; ticks, raw_files) -> String

Rewrite the running sidecar with the final status (`"completed"` /
`"interrupted"`), tick count, file list, and finish timestamp. The sidecar
is a mutable status record by design — the safesave rule protects data
products, not status metadata.
"""
function finalize_session_meta(path::AbstractString, status::AbstractString;
                               ticks::Integer, raw_files::Vector{String})
    meta = TOML.parsefile(path)
    s = meta["session"]
    s["status"] = String(status)
    s["ticks"] = Int(ticks)
    s["raw_files"] = [basename(f) for f in raw_files]
    s["finished_utc"] = string(Dates.now(UTC))
    open(io -> TOML.print(io, meta), path, "w")
    return path
end

"""
    reconcile_sessions!(raw_dir) -> Int

Crash-only startup reconciliation: sidecars still marked `running` whose
recorded process no longer exists on this host are relabeled `aborted`
(their raw NDJSON remains valid to the last flushed line); zero-byte raw
stubs are reported. Never deletes anything. Returns the number of sidecars
relabeled.
"""
function reconcile_sessions!(raw_dir::AbstractString)
    isdir(raw_dir) || return 0
    n = 0
    for f in readdir(raw_dir; join = true)
        endswith(f, ".meta.toml") || continue
        meta = try
            TOML.parsefile(f)
        catch
            continue
        end
        s = get(meta, "session", nothing)
        s === nothing && continue
        get(s, "status", "") == "running" || continue
        pid = Int(get(s, "pid", 0))
        samehost = get(get(meta, "provenance", Dict{String, Any}()), "hostname", "") ==
                   gethostname()
        pid == getpid() && continue                 # our own freshly-started sidecar
        samehost && pid > 0 && isdir("/proc/$pid") && continue   # genuinely running
        s["status"] = "aborted"
        s["reconciled_utc"] = string(Dates.now(UTC))
        open(io -> TOML.print(io, meta), f, "w")
        @warn "reconciled abruptly-ended session" sidecar = basename(f)
        n += 1
    end
    for f in readdir(raw_dir; join = true)
        endswith(f, ".jsonl") && filesize(f) == 0 &&
            @warn "zero-byte raw stub from an aborted session (left in place)" file =
                basename(f)
    end
    return n
end

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
    started_utc = Dates.now(UTC)
    global_logger(setup_logging(cfg; session_id = sid))
    if provider === nothing
        key, secret = load_credentials!()
        provider = AlpacaProvider(cfg, key, secret)
    end
    delay = feed_delay_ns(provider)
    delay > 0 && @info "delayed feed — railings shifted" delay_s = delay ÷ NS_PER_SEC

    mkpath(cfg.raw_dir)
    slock = acquire_session_lock(cfg.data_dir)
    meta_path = nothing
    status = "completed"
    try
    reconcile_sessions!(cfg.raw_dir)
    free = free_disk_gb(cfg.raw_dir)
    free < cfg.min_free_disk_gb &&
        error("only $(round(free; digits = 2)) GiB free on $(cfg.raw_dir), " *
              "below limits.min_free_disk_gb = $(cfg.min_free_disk_gb) — not starting")

    clock = nothing
    if cfg.require_market_open || cfg.stop_at_market_close
        clock = market_clock(provider)
        if !clock.is_open && cfg.require_market_open
            if cfg.wait_for_open
                # settle past the bell, plus the feed's intrinsic delay: on a
                # delayed feed the first post-open data cannot arrive earlier.
                wait_s = (rfc3339_to_ns(clock.next_open) + delay - now_ns()) / 1e9 + 10
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
    # Delayed feeds keep transmitting the tape tail past the bell; allow an
    # extra minute for late-reported closing prints on top of the delay.
    cfg.stop_at_market_close && clock !== nothing &&
        schedule_close_stop!(session, rfc3339_to_ns(String(clock.next_close)) + delay +
                                      (delay > 0 ? 60 * NS_PER_SEC : Int64(0)))
    sink = open_raw_sink(cfg.raw_dir, sid; max_mb = cfg.max_raw_file_mb)
    meta_path = start_session_meta(cfg, sid; started_utc)
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
            status = "interrupted"
            @info "interrupt — shutting down gracefully"
        else
            rethrow()
        end
    finally
        stop!(session)                       # idempotent; unblocks the producer
        try ticks = fetch(sink_task) catch end   # final drain + flush
        close_sink!(sink)
    end
    files = filter(f -> startswith(basename(f), sid) && endswith(f, ".jsonl"),
                   readdir(cfg.raw_dir; join = true, sort = true))
    finalize_session_meta(meta_path, status; ticks, raw_files = files)
    @info "session finished" status ticks files = length(files) meta = meta_path
    return (; ticks, raw_files = files)
    finally
        close(slock)
    end
end

_processed_exists(cfg::Config, sym::AbstractString, day::Date) =
    any(isfile(joinpath(cfg.processed_dir, sym, "$day.$ext")) for ext in ("csv", "arrow"))

"""
    run_backfill(cfg::Config; provider = nothing) -> Vector{String}

Download historical trades for every configured symbol over
`[backfill.start_date, backfill.end_date]` (weekends skipped), persist them
through the same raw-NDJSON + compaction path as live data (marked by
`recv_ns = 0`), and return the processed file paths.

Robustness: pages are flushed to disk as they arrive (RAM bounded by one
page and `limits.max_resident_mb`); with `backfill.resume`, `(symbol, day)`
pairs already present under `processed/` are skipped, so a rerun after an
abort resumes instead of re-downloading; Ctrl-C finalizes the provenance
sidecar as `interrupted` and preserves all flushed raw data.
"""
function run_backfill(cfg::Config; provider::Union{AbstractProvider, Nothing} = nothing)
    sid = "backfill_" * session_id(cfg; feed = cfg.backfill_feed)
    started_utc = Dates.now(UTC)
    global_logger(setup_logging(cfg; session_id = sid))
    if provider === nothing
        key, secret = load_credentials!()
        provider = AlpacaProvider(cfg, key, secret)
    end
    mkpath(cfg.raw_dir)
    slock = acquire_session_lock(cfg.data_dir)
    status = "completed"
    total = 0
    meta_path = nothing
    sink = nothing
    try
        reconcile_sessions!(cfg.raw_dir)
        free = free_disk_gb(cfg.raw_dir)
        free < cfg.min_free_disk_gb &&
            error("only $(round(free; digits = 2)) GiB free on $(cfg.raw_dir), " *
                  "below limits.min_free_disk_gb = $(cfg.min_free_disk_gb) — not starting")
        sink = open_raw_sink(cfg.raw_dir, sid; max_mb = cfg.max_raw_file_mb)
        meta_path = start_session_meta(cfg, sid; started_utc)
        days = [d for d in cfg.backfill_start:Day(1):cfg.backfill_end if dayofweek(d) <= 5]
        prog = Progress(length(cfg.symbols) * length(days);
                        desc = "Backfill: ", enabled = isinteractive())
        for sym in cfg.symbols, day in days
            if cfg.backfill_resume && _processed_exists(cfg, sym, day)
                @info "resume — already processed, skipping" symbol = sym day
                next!(prog)
                continue
            end
            n = historical_trades(provider, sym, day, day;
                feed = cfg.backfill_feed,
                page_limit = cfg.backfill_page_limit,
                rate_sleep_s = cfg.backfill_rate_sleep_s,
                each_page = page -> begin
                    write_batch!(sink, page)
                    check_resident_memory(cfg.max_resident_mb;
                                          context = "backfill $sym $day")
                end)
            total += n
            @info "backfilled" symbol = sym day trades = n
            next!(prog)
        end
    catch e
        e isa InterruptException || rethrow()
        status = "interrupted"
        @info "interrupt — flushed raw data preserved; rerun resumes" sid
    finally
        if sink !== nothing
            close_sink!(sink)
            # a fully-skipped resume run leaves an empty part file of our own
            # making — remove it rather than accumulate stubs
            sink.n_written == 0 && filesize(sink.path) == 0 && rm(sink.path; force = true)
        end
        files = filter(f -> startswith(basename(f), sid) && endswith(f, ".jsonl"),
                       readdir(cfg.raw_dir; join = true, sort = true))
        meta_path === nothing ||
            finalize_session_meta(meta_path, status; ticks = total, raw_files = files)
        close(slock)
    end
    files = filter(f -> startswith(basename(f), sid) && endswith(f, ".jsonl"),
                   readdir(cfg.raw_dir; join = true, sort = true))
    status == "interrupted" && return String[]
    processed = isempty(files) ? String[] :
        compact_raw(files, cfg.processed_dir; format = cfg.processed_format,
                    max_resident_mb = cfg.max_resident_mb)
    @info "backfill compacted" files = processed
    return processed
end
