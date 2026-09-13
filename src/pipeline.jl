# Session orchestration: wire a tick source to sinks (and future analysis
# taps), with structured logging and graceful shutdown.

"""
    setup_logging(cfg; session_id) -> AbstractLogger

Console logger at `cfg.log_level`, optionally teed to
`cfg.log_dir/<session_id>.log` (plain formatting, no ANSI). The caller
installs it with `global_logger`.
"""
function setup_logging(cfg::Config; session_id::AbstractString)
    level = Dict(
        "debug" => Logging.Debug,
        "info" => Logging.Info,
        "warn" => Logging.Warn,
        "error" => Logging.Error,
    )[cfg.log_level]
    # During a deliberate shutdown, transport-teardown errors surfacing from
    # HTTP.jl internals (EOFError on a socket we closed) are expected noise.
    quiet(logger) = EarlyFilteredLogger(
        log -> !(SHUTTING_DOWN[] && startswith(string(log._module), "HTTP")),
        logger,
    )
    console = ConsoleLogger(stderr, level)
    cfg.log_to_file || return quiet(console)
    mkpath(cfg.log_dir)
    # always_flush: an abruptly killed session must not lose its log tail.
    file = FormatLogger(
        open(joinpath(cfg.log_dir, "$(session_id).log"), "a");
        always_flush = true,
    ) do io, args
        println(
            io,
            "[",
            Dates.format(Dates.now(UTC), dateformat"yyyy-mm-dd HH:MM:SS"),
            "Z] ",
            args.level,
            " ",
            args.message,
            isempty(args.kwargs) ? "" :
            " | " * join(("$k=$v" for (k, v) in args.kwargs), " "),
        )
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
function tee(
    src::Channel{Trade},
    n::Integer;
    capacity::Integer = 10_000,
    lossy = falses(n),
    on_drop = nothing,
)
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

"""
    session_id(cfg::Config; feed = cfg.feed) -> String

Unique identifier for a capture session, `<provider>_<feed>_<yyyymmdd-HHMMSS>`
(UTC start time). Prefixes every raw part file, provenance sidecar, and log
file the session produces.
"""
session_id(
    cfg::Config;
    feed::AbstractString = cfg.feed,
) = "$(cfg.provider)_$(feed)_$(Dates.format(Dates.now(UTC), dateformat"yyyymmdd-HHMMSS"))"

# Fail-fast free-space gate shared by stream and backfill startup.
function _ensure_free_disk(cfg::Config)
    free = free_disk_gb(cfg.raw_dir)
    free < cfg.min_free_disk_gb && error(
        "only $(round(free; digits = 2)) GiB free on $(cfg.raw_dir), " *
        "below limits.min_free_disk_gb = $(cfg.min_free_disk_gb) — not starting",
    )
    return nothing
end

# Raw part files belonging to one session, sorted.
_raw_files_with_prefix(dir::AbstractString, prefix::AbstractString) =
    isdir(dir) ?
    filter(
        f -> startswith(basename(f), prefix) && endswith(f, ".jsonl"),
        readdir(dir; join = true, sort = true),
    ) : String[]

_session_raw_files(cfg::Config, sid::AbstractString) =
    _raw_files_with_prefix(cfg.raw_dir, sid)

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
        "tree (stale locks from dead processes clear automatically)",
    )
    # `trymkpidlock` returns `Union{LockMonitor,Bool}`; the guard above settles
    # it, but only an assertion narrows the binding for callers of `close`.
    return lock::Pidfile.LockMonitor
end

_git_commit() =
    try
        strip(
            read(
                pipeline(`git -C $(PROJECT_ROOT) rev-parse HEAD`; stderr = devnull),
                String,
            ),
        )
    catch
        "unknown"
    end

_git_dirty() =
    try
        !isempty(
            strip(
                read(
                    pipeline(`git -C $(PROJECT_ROOT) status --porcelain`; stderr = devnull),
                    String,
                ),
            ),
        )
    catch
        false
    end

_toml_value(v) = v
_toml_value(v::Date) = string(v)
_toml_value(v::AbstractVector) = [_toml_value(x) for x in v]
_toml_value(v::Dict) = Dict{String,Any}(string(k) => _toml_value(x) for (k, x) in v)

# Platform fingerprint stored with every session sidecar, so results stay
# attributable to config + commit + hardware. Distributed workers and GPU
# devices are not fingerprinted because neither is used by this package.
function _hardware_provenance()
    return Dict{String,Any}(
        "cpu_model" => Sys.cpu_info()[1].model,
        "cpu_threads" => Sys.CPU_THREADS,
        "total_memory_gib" => round(Sys.total_memory() / 2^30; digits = 2),
        "julia_threads" => Threads.nthreads(),
        "blas_threads" => LinearAlgebra.BLAS.get_num_threads(),
        "versioninfo" => sprint(InteractiveUtils.versioninfo),
    )
end

"""
    write_session_meta(cfg, sid; status, ticks, raw_files, started_utc,
                       finished_utc = nothing) -> String

Persist a provenance sidecar `<raw_dir>/<sid>.meta.toml` next to the
session's raw files: session summary (id, status, pid, span, tick count,
file list), provenance (git commit + dirty flag, Julia and package
versions, hostname), a hardware fingerprint (CPU model and logical core
count, total memory, Julia and BLAS thread counts, full `versioninfo`
output), and the full effective configuration snapshot. The crash-only
lifecycle is [`start_session_meta`](@ref) → [`finalize_session_meta`](@ref),
reconciled at startup by [`reconcile_sessions!`](@ref).
"""
function write_session_meta(
    cfg::Config,
    sid::AbstractString;
    status::AbstractString,
    ticks::Integer = 0,
    raw_files::Vector{String} = String[],
    started_utc::DateTime,
    finished_utc::Union{Nothing,DateTime} = nothing,
)
    session = Dict{String,Any}(
        "id" => String(sid),
        "status" => String(status),
        "pid" => getpid(),
        "started_utc" => string(started_utc),
        "ticks" => Int(ticks),
        "raw_files" => [basename(f) for f in raw_files],
    )
    finished_utc === nothing || (session["finished_utc"] = string(finished_utc))
    meta = Dict{String,Any}(
        "session" => session,
        "provenance" => Dict{String,Any}(
            "git_commit" => _git_commit(),
            "git_dirty" => _git_dirty(),
            "julia_version" => string(VERSION),
            "package_version" =>
                string(something(pkgversion(MarketTickStreamer), "unknown")),
            "hostname" => gethostname(),
        ),
        "hardware" => _hardware_provenance(),
        "config" => Dict{String,Any}(
            String(f) => _toml_value(getfield(cfg, f)) for f in fieldnames(Config)
        ),
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
function finalize_session_meta(
    path::AbstractString,
    status::AbstractString;
    ticks::Integer,
    raw_files::Vector{String},
)
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
        samehost =
            get(get(meta, "provenance", Dict{String,Any}()), "hostname", "") ==
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
        endswith(f, ".jsonl") &&
            filesize(f) == 0 &&
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
function run_stream(cfg::Config; provider::Union{AbstractProvider,Nothing} = nothing)
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
        _ensure_free_disk(cfg)

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
        cfg.stop_at_market_close &&
            clock !== nothing &&
            schedule_close_stop!(
                session,
                rfc3339_to_ns(String(clock.next_close)) +
                delay +
                (delay > 0 ? 60 * NS_PER_SEC : Int64(0)),
            )
        sink = open_raw_sink(cfg.raw_dir, sid; max_mb = cfg.max_raw_file_mb)
        meta_path = start_session_meta(cfg, sid; started_utc)
        @info "session started" id = sid raw = sink.path symbols = cfg.symbols

        on_flush = function (n, tot)
            @info "flushed batch" batch = n total = tot
            occ = Base.n_avail(session.channel)
            occ > 0.8 * cfg.channel_capacity &&
                @warn "tick channel nearly full — sink is lagging the stream" occupancy =
                    occ capacity = cfg.channel_capacity
            f = free_disk_gb(cfg.raw_dir)
            if f < cfg.min_free_disk_gb
                @error "free disk below limit — stopping session" free_gb =
                    round(f; digits = 2)
                stop!(session)
            end
        end
        sink_task = Threads.@spawn run_sink!(
            session.channel,
            sink;
            flush_interval_s = cfg.flush_interval_s,
            flush_max_ticks = cfg.flush_max_ticks,
            on_flush,
        )

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
            try
                ticks = fetch(sink_task)
            catch
            end   # final drain + flush
            close_sink!(sink)
        end
        files = _session_raw_files(cfg, sid)
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

Work is done one `(symbol, day)` at a time: its own raw part set, compacted
as soon as that day finishes. Pages are flushed to disk as they arrive, so
RAM stays bounded by one page and `limits.max_live_heap_mb`.

That granularity is what makes `backfill.resume` mean anything on a long
download. The skip test asks whether a day is already present under
`processed/`, so a day must land there the moment it is complete and not
before: a run killed after eight of twelve hours resumes at hour eight.
Compacting only at the end — as this did until 2026-09-13 — left a killed run
with raw data that resume could not see, and the rerun started from nothing.

An interrupted day is not compacted and is therefore downloaded again; its
partial raw file is left on disk rather than deleted, and compaction
deduplicates if it is ever folded in. Ctrl-C finalizes the provenance sidecar
as `interrupted` and returns the days that did complete.
"""
function run_backfill(cfg::Config; provider::Union{AbstractProvider,Nothing} = nothing)
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
    processed = String[]
    try
        reconcile_sessions!(cfg.raw_dir)
        _ensure_free_disk(cfg)
        meta_path = start_session_meta(cfg, sid; started_utc)
        days = [d for d in cfg.backfill_start:Day(1):cfg.backfill_end if dayofweek(d) <= 5]
        prog = Progress(
            length(cfg.symbols) * length(days);
            desc = "Backfill: ",
            enabled = isinteractive(),
        )
        for sym in cfg.symbols, day in days
            if cfg.backfill_resume && _processed_exists(cfg, sym, day)
                @info "resume — already processed, skipping" symbol = sym day
                next!(prog)
                continue
            end
            # One raw part set per (symbol, day), compacted the moment the day
            # finishes. That is what makes `resume` survive a kill: the skip
            # test reads `processed/`, so it can only be accurate if a day
            # lands there as soon as it is complete and never before.
            day_prefix = "$(sid)_$(sym)_$(day)"
            day_sink = open_raw_sink(cfg.raw_dir, day_prefix; max_mb = cfg.max_raw_file_mb)
            n = 0
            try
                n = historical_trades(
                    provider,
                    sym,
                    day,
                    day;
                    feed = cfg.backfill_feed,
                    page_limit = cfg.backfill_page_limit,
                    rate_sleep_s = cfg.backfill_rate_sleep_s,
                    each_page = page -> begin
                        write_batch!(day_sink, page)
                        check_live_heap(
                            cfg.max_live_heap_mb;
                            context = "backfill $sym $day",
                        )
                    end,
                )
            finally
                close_sink!(day_sink)
                # A day that failed before writing anything leaves a zero-byte
                # stub of our own making, carrying nothing to preserve.
                day_sink.n_written == 0 &&
                    filesize(day_sink.path) == 0 &&
                    rm(day_sink.path; force = true)
            end
            day_files = _raw_files_with_prefix(cfg.raw_dir, day_prefix)
            if n == 0
                # A holiday, a halt, or a symbol not yet listed: drop our own
                # empty stub rather than accumulate them.
                for f in day_files
                    filesize(f) == 0 && rm(f; force = true)
                end
            else
                append!(
                    processed,
                    compact_raw(
                        day_files,
                        cfg.processed_dir;
                        format = cfg.processed_format,
                        max_live_heap_mb = cfg.max_live_heap_mb,
                    ),
                )
            end
            total += n
            @info "backfilled" symbol = sym day trades = n
            next!(prog)
        end
    catch e
        e isa InterruptException || rethrow()
        status = "interrupted"
        @info "interrupt — completed days are compacted; a rerun resumes" sid
    finally
        meta_path === nothing || finalize_session_meta(
            meta_path,
            status;
            ticks = total,
            raw_files = _session_raw_files(cfg, sid),
        )
        close(slock)
    end
    @info "backfill finished" status days = length(processed) ticks = total
    return processed
end

"""
    run_entrypoint(main)

Run a command-line entry point's `main` with clean interrupt handling: SIGINT
arrives as an `InterruptException` and exits without a stack trace.

Delivery is best-effort under threads — the crash-only session lifecycle, not
this wrapper, is what actually guarantees that an interrupted run leaves
recoverable state. Shared by the scripts under `scripts/` and the worked
examples so that every entry point behaves the same way at the terminal.

# Example

```julia
run_entrypoint(() -> main(ARGS))
```
"""
function run_entrypoint(main::Function)
    Base.exit_on_sigint(false)
    try
        main()
    catch e
        e isa InterruptException || rethrow()
        println("\nInterrupted — exiting cleanly.")
    end
    return nothing
end
