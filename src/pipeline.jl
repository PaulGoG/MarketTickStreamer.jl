# Session orchestration: wire a tick source to sinks and analysis taps, with
# structured logging and orderly shutdown.

# A log file holds plain text: CSI escape sequences (color, cursor control) are removed.
_strip_ansi(s) = replace(string(s), r"\e\[[0-9;?]*[ -/]*[@-~]" => "")

# A log value as one line of plain text. An `(exception, backtrace)` pair is
# rendered as the error message; printed as a tuple, the backtrace is a list
# of raw pointers.
_log_value(v) = _strip_ansi(v)
_log_value(v::Tuple{Exception,Any}) =
    replace(_strip_ansi(sprint(showerror, v[1])), r"\s*\n\s*" => " ")

"""
    setup_logging(cfg; session_id, shutting_down = Threads.Atomic{Bool}(false))
        -> (logger, io)

Console logger at `cfg.log_level`, optionally teed to
`cfg.log_dir/<session_id>.log` (plain formatting, no ANSI, flushed per record).
Returns the logger and the open log file, or `nothing` without file logging;
the caller runs the session under `with_logger(logger)` and closes `io` when it
ends. Nothing is installed globally, so a session leaves the caller's logger
as it found it and two sessions in one process keep separate logs.

While `shutting_down[]` is true, records from HTTP.jl internals are dropped:
during a deliberate shutdown an `EOFError` from a socket the session closed
itself is expected.
"""
function setup_logging(
    cfg::Config;
    session_id::AbstractString,
    shutting_down::Threads.Atomic{Bool} = Threads.Atomic{Bool}(false),
)
    level = Dict(
        :debug => Logging.Debug,
        :info => Logging.Info,
        :warn => Logging.Warn,
        :error => Logging.Error,
    )[cfg.log_level]
    quiet(logger) = EarlyFilteredLogger(
        log -> !(shutting_down[] && startswith(string(log._module), "HTTP")),
        logger,
    )
    console = ConsoleLogger(stderr, level)
    cfg.log_to_file || return quiet(console), nothing
    mkpath(cfg.log_dir)
    io = open(joinpath(cfg.log_dir, "$(session_id).log"), "a")
    # always_flush: an abruptly killed session must not lose its log tail.
    file = FormatLogger(io; always_flush = true) do io, args
        println(
            io,
            "[",
            Dates.format(Dates.now(UTC), dateformat"yyyy-mm-dd HH:MM:SS"),
            "Z] ",
            args.level,
            " ",
            _strip_ansi(args.message),
            isempty(args.kwargs) ? "" :
            " | " * join(("$k=$(_log_value(v))" for (k, v) in args.kwargs), " "),
        )
    end
    return quiet(TeeLogger(console, MinLevelLogger(file, level))), io
end

# Items buffered in a channel. `Base.n_avail` is not public API and has no
# public equivalent; it is read here and nowhere else.
_occupancy(ch::Channel) = Base.n_avail(ch)

"""
    tee(src::Channel{Trade}, n; capacity = 10_000, lossy = falses(n),
        on_drop = nothing) -> Vector{Channel{Trade}}

Fan one tick stream out to `n` independent consumers (e.g. persistence plus
real-time analysis taps). Outputs close when `src` closes.

Per-output overflow policy: a non-`lossy` output blocks the fan-out when
full, which applies backpressure and drops nothing (the setting for
persistence). A `lossy` output drops the incoming tick when its buffer is
full (the setting for analysis taps that must never stall the capture). Drops
are counted, reported via `on_drop(output_index, n_dropped)` when given, and
warned on first occurrence.

A consumer that closes its output is dropped from the fan-out with a warning
and the other outputs keep receiving, so an analysis tap that dies cannot end
the persistence branch. If every output is closed the fan-out stops with an
error record. A failure of the fan-out task itself is logged (`errormonitor`).
"""
function tee(
    src::Channel{Trade},
    n::Integer;
    capacity::Integer = 10_000,
    lossy = falses(n),
    on_drop = nothing,
)
    length(lossy) == n ||
        throw(DimensionMismatch("lossy has $(length(lossy)) entries for $n outputs"))
    outs = [Channel{Trade}(capacity) for _ in 1:n]
    dropped = zeros(Int, n)
    live = trues(n)
    task = Threads.@spawn begin
        try
            for t in src
                for (i, o) in enumerate(outs)
                    live[i] || continue
                    if !isopen(o)
                        live[i] = false
                        @warn "tee output closed by its consumer; dropped from the fan-out" output =
                            i
                        continue
                    end
                    if lossy[i] && _occupancy(o) >= capacity
                        dropped[i] += 1
                        dropped[i] == 1 &&
                            @warn "lossy tee output saturated; dropping ticks" output =
                                i
                        on_drop === nothing || on_drop(i, dropped[i])
                    else
                        try
                            put!(o, t)
                        catch e
                            # Closed between the check and the put, or while
                            # the put waited for room. A channel closed with an
                            # exception (one bound to a consumer task that
                            # failed) throws that exception here.
                            isopen(o) && rethrow()
                            live[i] = false
                            @warn "tee output closed by its consumer; dropped from the fan-out" output =
                                i
                        end
                    end
                end
                if !any(live)
                    @error "every tee output is closed; fan-out stopped"
                    break
                end
            end
        finally
            foreach(close, outs)
        end
    end
    errormonitor(task)
    return outs
end

"""
    snapshots(fold!, ch, period_ns; on_snapshot, clock = :exchange) -> Int

Drain `ch`, folding every print into a consumer's state with `fold!(t)`, and
call `on_snapshot(at_ns, n, final)` at every boundary of a period clock and
once when the channel closes. `n` is the number of prints folded before the
call, `at_ns` the boundary time in nanoseconds, `final` whether the stream
has ended. Returns the number of prints folded.

The clock is the exchange timestamp (`clock = :exchange`, `time_ns`) or the
receipt timestamp (`:recv`, `recv_ns`); its boundaries are the multiples of
`period_ns`, so a one-minute period snapshots at whole UTC minutes. The first
boundary is the first multiple after the first print. A print at or beyond
the next boundary triggers one call per boundary crossed, before it is
folded, so a window without prints still yields its snapshot, with `n`
unchanged. A late print, one behind the window already open, folds into that
window: the boundary never retreats. The final call carries the last print's
timestamp and `final = true`; an empty stream makes no call. Under `:recv` a
print with `recv_ns = 0` (backfilled or imported) throws an `ArgumentError`.

Snapshots are a function of the sequence alone, never of wall time, so a
replay at any pace reproduces them exactly, and consumers fed by one
[`tee`](@ref) produce aligned snapshots. This is the snapshot convention of
the analysis interface: periodic state on the exchange clock, final state at
close, each stamped with its boundary and its print count.

# Example
```julia
rows = Tuple{Int64,Int,Float64}[]
n = snapshots(ch, 60 * 10^9; on_snapshot = (at, k, _) -> push!(rows, (at, k, value(acc)))) do t
    fit!(acc, t.size)
end
```
"""
function snapshots(
    fold!,
    ch::Channel{Trade},
    period_ns::Integer;
    on_snapshot,
    clock::Symbol = :exchange,
)
    period_ns >= 1 || throw(ArgumentError("period_ns must be >= 1, got $period_ns"))
    clock in (:exchange, :recv) ||
        throw(ArgumentError("clock must be :exchange or :recv, got $(repr(clock))"))
    n = 0
    boundary = typemin(Int64)
    last_ns = typemin(Int64)
    for t in ch
        τ = clock === :exchange ? t.time_ns : t.recv_ns
        # A print that never crossed the wire has no place on the receive
        # clock, and stepping the boundary from zero to the present would
        # make one call per period since 1970.
        (clock === :recv && τ <= 0) && throw(
            ArgumentError(
                "snapshots on clock = :recv met a print without a receive timestamp " *
                "(recv_ns = 0, a backfilled or imported print); use clock = :exchange",
            ),
        )
        n == 0 && (boundary = fld(τ, period_ns) * period_ns + period_ns)
        while τ >= boundary
            on_snapshot(boundary, n, false)
            boundary += period_ns
        end
        fold!(t)
        n += 1
        last_ns = τ
    end
    n > 0 && on_snapshot(last_ns, n, true)
    return n
end

# UTC start time, as stamped into every session id.
const SESSION_STAMP = dateformat"yyyymmdd-HHMMSS"

"""
    session_id(cfg::Config; feed = cfg.feed) -> String

Identifier for a capture session, `<provider>_<feed>_<yyyymmdd-HHMMSS>`, the
UTC time of launch (with `stream.wait_for_open` the sidecar's `started_utc`,
taken at the open, is later). Prefixes every raw part file, provenance
sidecar, and log file the session produces. The stamp has one-second
resolution; the session runners make it unique within a data tree by waiting
for the next second when a session started in the current one already holds
the identifier.
"""
session_id(
    cfg::Config;
    feed::AbstractString = cfg.feed,
) = "$(cfg.provider)_$(feed)_$(Dates.format(Dates.now(UTC), SESSION_STAMP))"

# Construct the configured provider. The adapter loads the credentials its
# spec lists, so public market data asks for none.
_provider_from_config(cfg::Config) = make_provider(Val(Symbol(cfg.provider)), cfg)

# Fail-fast free-space gate shared by stream and backfill startup.
function _ensure_free_disk(cfg::Config)
    free = free_disk_gb(cfg.raw_dir)
    free < cfg.min_free_disk_gb && throw(
        ResourceError(
            :disk,
            "only $(round(free; digits = 2)) GiB free on $(cfg.raw_dir), " *
            "below limits.min_free_disk_gb = $(cfg.min_free_disk_gb); not starting",
        ),
    )
    return nothing
end

# Raw part files belonging to one session: by the name up to the part number,
# then by the number, so the parts of one prefix come in the order they were
# written. The number is padded to three digits, and the names themselves put
# `_part1000` before `_part101`; up to `_part999` the two orders agree for the
# names a session writes.
function _raw_files_with_prefix(dir::AbstractString, prefix::AbstractString)
    isdir(dir) || return String[]
    files = filter(
        f -> startswith(basename(f), prefix) && endswith(f, ".jsonl"),
        readdir(dir; join = true),
    )
    return sort!(files; by = _part_order)
end

# Sort key of a raw part: the path up to its part number, then the number. A
# name without one sorts by itself.
function _part_order(path::AbstractString)
    m = match(r"^(.*_part)([0-9]+)\.jsonl\z", path)
    m === nothing && return (String(path), 0)
    return (
        String(something(m[1])),
        something(tryparse(Int, something(m[2])), typemax(Int)),
    )
end

_session_raw_files(cfg::Config, sid::AbstractString) =
    _raw_files_with_prefix(cfg.raw_dir, sid)

# Whether an earlier session in this data tree holds `sid`: its sidecar, a raw
# part or its log carries the identifier.
_session_id_taken(cfg::Config, sid::AbstractString) =
    isfile(joinpath(cfg.raw_dir, "$(sid).meta.toml")) ||
    !isempty(_raw_files_with_prefix(cfg.raw_dir, sid)) ||
    (cfg.log_to_file && isfile(joinpath(cfg.log_dir, "$(sid).log")))

# A session identifier no earlier session in the tree holds. Identifiers are
# stamped to the second, so two sessions started within one second would
# share one, the second sidecar displacing the first and listing both
# sessions' parts; `make_id` is called again once the clock has passed into
# the next second.
function _fresh_session_id(make_id, cfg::Config)
    sid = make_id()
    while _session_id_taken(cfg, sid)
        sleep(1.001 - rem(time(), 1.0))
        sid = make_id()
    end
    return sid
end

"""
    acquire_session_lock(data_dir; settle_s = 1.0) -> lock

One session per data tree: takes a PID-file lock at `<data_dir>/.session.lock`
and throws a [`SessionError`](@ref) if another live process already holds it,
since two backfills interleaving on one raw directory would corrupt both. The
holder refreshes the file every minute; a lock whose process is gone is broken
once it is two minutes old, and one whose process still runs only after ten.
Release with `close(lock)`.

A starter that found a lock file and took the lock all the same waits
`settle_s` seconds and gives way if the name then holds another file than its
own. Pidfile breaks a stale lock by unlinking the name and creating it anew,
and of two starters that judged the same lock stale the later one unlinks the
lock the earlier one has just created, so both would hold the tree.

The check settles two starters. With three on one stale lock it has a gap: a
starter that arrives after the first unlink and before the second finds no
lock file, creates its own without waiting, and loses the name to the second
unlink while it believes it holds the tree. All three must reach the lock
within the instant that separates one starter's stale check from its unlink.
"""
function acquire_session_lock(data_dir::AbstractString; settle_s::Real = LOCK_SETTLE_S)
    mkpath(data_dir)
    path = joinpath(data_dir, ".session.lock")
    refuse() = throw(
        SessionError(
            path,
            "another session already holds $path; one capture/backfill per data " *
            "tree (stale locks from dead processes clear automatically)",
        ),
    )
    found = ispath(path)
    # Pidfile warns between judging a lock stale and unlinking it. A process's
    # first warning takes about a second to compile, and for that long a second
    # starter could interleave; muted here, the two steps are milliseconds
    # apart, and the takeover is reported below instead.
    pidlock = Logging.with_logger(Logging.NullLogger()) do
        Pidfile.trymkpidlock(path; stale_age = 120.0)
    end
    pidlock === false && refuse()
    # `trymkpidlock` returns `Union{LockMonitor,Bool}`; the guard above settles
    # it, but only an assertion narrows the binding for callers of `close`.
    held = pidlock::Pidfile.LockMonitor
    if found
        mine = try
            sleep(settle_s)
            _still_holds(held, path)
        catch
            close(held)
            rethrow()
        end
        if !mine
            close(held)             # leaves a file that is not its own in place
            refuse()
        end
        @warn "took the data tree over from a lock that was stale or just released" path
    end
    return held
end

# Seconds between taking a lock that replaced an earlier file and checking
# that the name still holds it: far longer than a concurrent starter takes
# from its stale check to its unlink.
const LOCK_SETTLE_S = 1.0

# Whether `path` is still the file `held` created, the test Pidfile's own
# `close` makes before it removes the name.
function _still_holds(held::Pidfile.LockMonitor, path::AbstractString)
    return try
        samefile(stat(held.fd), stat(path))
    catch e
        e isa InterruptException && rethrow()
        false
    end
end

# State of the package's git checkout: commit, dirty flag, and whether either
# could be read. An installed copy is not a checkout and a host may lack git;
# both yield ("unknown", false, false), and `available` says the first two are
# placeholders.
function _git_state(; installed::Bool = _installed_in_depot())
    # `git -C` searches upward, so for a copy installed in a depot that lies
    # inside some other work tree it would report that repository's commit.
    installed && return (; commit = "unknown", dirty = false, available = false)
    git(args) = String(
        strip(read(pipeline(`git -C $(PROJECT_ROOT) $args`; stderr = devnull), String)),
    )
    try
        commit = git(`rev-parse HEAD`)
        dirty = !isempty(git(`status --porcelain`))
        return (; commit, dirty, available = true)
    catch e
        e isa InterruptException && rethrow()
        return (; commit = "unknown", dirty = false, available = false)
    end
end

_toml_value(v) = v
_toml_value(v::Date) = string(v)
# A mode is a Symbol in `Config` and a string in the file it came from.
_toml_value(v::Symbol) = string(v)
# The sidecar snapshots the whole effective config, and TOML has no timezone
# type; the IANA name is what identifies it anyway.
_toml_value(v::TimeZone) = string(v)
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

# Manifest of the active environment, by the file names Pkg itself looks for,
# most specific first. `nothing` when the process runs without a project or
# the environment was never resolved.
function _active_manifest()
    project = Base.active_project()
    project === nothing && return nothing
    dir = dirname(project)
    v = "v$(VERSION.major).$(VERSION.minor)"
    for name in
        ("JuliaManifest-$v.toml", "Manifest-$v.toml", "JuliaManifest.toml", "Manifest.toml")
        path = joinpath(dir, name)
        isfile(path) && return path
    end
    return nothing
end

# Copy the resolved manifest next to the sidecar as `<sid>.manifest.toml` and
# return its base name ("" when there is none). No manifest is tracked in the
# repository, so this copy is what pins the exact dependency versions a
# session ran with.
function _snapshot_manifest(raw_dir::AbstractString, sid::AbstractString)
    src = _active_manifest()
    src === nothing && return ""
    mkpath(raw_dir)
    dst = _safesave(tmp -> cp(src, tmp), joinpath(raw_dir, "$(sid).manifest.toml"))
    return basename(dst)
end

"""
    write_session_meta(cfg, sid; status, ticks, raw_files, started_utc,
                       finished_utc = nothing) -> String

Persist a provenance sidecar `<raw_dir>/<sid>.meta.toml` next to the
session's raw files. It holds the session summary (id, status, pid, span,
tick count, file list); provenance (git commit, dirty flag and whether the
two could be read, Julia and package versions, hostname, the venue's
timestamp unit, and the base name of a copy of the resolved `Manifest.toml`
written alongside as `<sid>.manifest.toml`); a hardware fingerprint (CPU
model and logical core count, total memory, Julia and BLAS thread counts,
full `versioninfo` output); and the full effective configuration snapshot.
The crash-only lifecycle is [`start_session_meta`](@ref) →
[`finalize_session_meta`](@ref), reconciled at startup by
[`reconcile_sessions!`](@ref).
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
    git = _git_state()
    meta = Dict{String,Any}(
        "session" => session,
        "provenance" => Dict{String,Any}(
            "git_commit" => git.commit,
            "git_dirty" => git.dirty,
            "git_available" => git.available,
            "julia_version" => string(VERSION),
            "package_version" =>
                string(something(pkgversion(MarketTickStreamer), "unknown")),
            "hostname" => gethostname(),
            "timestamp_unit" => provider_spec(cfg.provider).timestamp_unit,
            "manifest" => _snapshot_manifest(cfg.raw_dir, sid),
        ),
        "hardware" => _hardware_provenance(),
        "config" => Dict{String,Any}(
            String(f) => _toml_value(getfield(cfg, f)) for f in fieldnames(Config)
        ),
    )
    return _safesave(joinpath(cfg.raw_dir, "$(sid).meta.toml")) do tmp
        open(io -> TOML.print(io, meta), tmp, "w")
    end
end

"""
    start_session_meta(cfg, sid; started_utc) -> String

Write the sidecar at session start with `status = "running"`, so an abruptly
killed process still leaves its provenance on disk.
"""
start_session_meta(cfg::Config, sid::AbstractString; started_utc::DateTime) =
    write_session_meta(cfg, sid; status = "running", started_utc)

# Rewrite `path` through a sibling temporary file and a rename, so a reader
# never sees a truncated sidecar.
function _rewrite_atomic(write::Function, path::AbstractString)
    tmp = path * ".partial"
    open(write, tmp, "w")
    mv(tmp, path; force = true)
    return path
end

"""
    finalize_session_meta(path, status; ticks, raw_files, ticks_processed = nothing,
                          source = nothing, id_breaks = nothing) -> String

Rewrite the running sidecar with the final status (`"completed"`,
`"interrupted"` or `"failed"`), tick count, file list, and finish timestamp.
The sidecar is a status record and is rewritten in place; data products are
never overwritten.

`ticks` counts the prints received and written to `raw/`. A session that
also compacts passes `ticks_processed`, the prints it wrote to `processed/`;
the two differ by the duplicates compaction dropped.

`source` describes the vendor file an import session read (name, size,
checksum, format and the reader's statistics) and is written as the
sidecar's `session.source` table. `id_breaks`, from a bulk backfill, counts
the prints whose trade id did not follow the previous one
([`bulk_trades`](@ref)).
"""
function finalize_session_meta(
    path::AbstractString,
    status::AbstractString;
    ticks::Integer,
    raw_files::Vector{String},
    ticks_processed::Union{Nothing,Integer} = nothing,
    source::Union{Nothing,AbstractDict} = nothing,
    id_breaks::Union{Nothing,Integer} = nothing,
)
    meta = TOML.parsefile(path)
    s = meta["session"]
    s["status"] = String(status)
    s["ticks"] = Int(ticks)
    ticks_processed === nothing || (s["ticks_processed"] = Int(ticks_processed))
    id_breaks === nothing || (s["id_breaks"] = Int(id_breaks))
    source === nothing ||
        (s["source"] = Dict{String,Any}(String(k) => v for (k, v) in source))
    s["raw_files"] = [basename(f) for f in raw_files]
    s["finished_utc"] = string(Dates.now(UTC))
    _rewrite_atomic(io -> TOML.print(io, meta), path)
    return path
end

"""
    reconcile_sessions!(raw_dir) -> Int

Crash-only startup reconciliation: sidecars still marked `running` whose
recorded process is not running on this host are relabeled `aborted` (their
raw NDJSON remains valid to the last flushed line); zero-byte raw stubs are
reported. A sidecar written on another host is relabeled too: the session
runners call this holding the data tree's lock, which admits no other
producer. Never deletes anything. Returns the number of sidecars relabeled.
"""
function reconcile_sessions!(raw_dir::AbstractString)
    isdir(raw_dir) || return 0
    n = 0
    for f in readdir(raw_dir; join = true)
        endswith(f, ".meta.toml") || continue
        meta = try
            TOML.parsefile(f)
        catch e
            e isa InterruptException && rethrow()
            @warn "unreadable sidecar left as is" sidecar = basename(f)
            continue
        end
        s = get(meta, "session", nothing)
        s === nothing && continue
        get(s, "status", "") == "running" || continue
        pid = Int(get(s, "pid", 0))
        samehost =
            get(get(meta, "provenance", Dict{String,Any}()), "hostname", "") ==
            gethostname()
        pid == getpid() && continue                 # this process's own sidecar
        samehost && pid > 0 && isdir("/proc/$pid") && continue   # still running
        s["status"] = "aborted"
        s["reconciled_utc"] = string(Dates.now(UTC))
        _rewrite_atomic(io -> TOML.print(io, meta), f)
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

# One session's bookkeeping, filled in by the body of `_with_session` as it
# runs and read when the sidecar is finalized. `result` is what the session
# returns when an interrupt ends it early.
mutable struct SessionSummary
    status::String
    ticks::Int
    ticks_processed::Union{Nothing,Int}
    source::Union{Nothing,AbstractDict}
    id_breaks::Union{Nothing,Int}
    meta_path::Union{Nothing,String}
    raw_files::Vector{String}
    result::Any
end
SessionSummary() =
    SessionSummary("completed", 0, nothing, nothing, nothing, nothing, String[], nothing)

# The envelope shared by run_stream, run_backfill and run_import: the lock on
# the data tree, reconciliation of stale sidecars, the free-disk gate, the
# sidecar written as `running` before `body(summary)` runs, and its
# finalization on every exit. The body fills `summary` as it goes.
# Status: `completed` on return, `interrupted` on Ctrl-C, after which
# `summary.result` is returned (`result` until the body replaces it), `failed`
# on any other error, which propagates; the body may set `failed` itself, and
# an interrupt does not overwrite it.
function _with_session(
    body,
    cfg::Config,
    sid::AbstractString;
    on_interrupt::Union{Nothing,AbstractString} = nothing,
    result = nothing,
)
    started_utc = Dates.now(UTC)
    mkpath(cfg.raw_dir)
    slock = acquire_session_lock(cfg.data_dir)
    summary = SessionSummary()
    summary.result = result
    try
        reconcile_sessions!(cfg.raw_dir)
        _ensure_free_disk(cfg)
        summary.meta_path = start_session_meta(cfg, sid; started_utc)
        return body(summary)
    catch e
        # Also an interrupt that ended a task the body waited on, which
        # arrives as that task's failure. A body that recorded a failure on
        # its way out (a sink that failed while draining after the interrupt)
        # keeps it: the sidecar then reads as the returned status does.
        if has_wrapped_exception(e, InterruptException)
            summary.status == "failed" || (summary.status = "interrupted")
            on_interrupt === nothing || @info on_interrupt sid
            return summary.result
        else
            summary.status = "failed"
            rethrow()
        end
    finally
        # The lock is released even when the sidecar cannot be finalized (a
        # full disk, for one), so the next session is not refused for it.
        try
            # Listed once: what the sidecar records is what a runner reports.
            summary.raw_files = _session_raw_files(cfg, sid)
            meta_path = summary.meta_path
            meta_path === nothing || finalize_session_meta(
                meta_path,
                summary.status;
                ticks = summary.ticks,
                ticks_processed = summary.ticks_processed,
                raw_files = summary.raw_files,
                source = summary.source,
                id_breaks = summary.id_breaks,
            )
        finally
            close(slock)
        end
    end
end

"""
    run_stream(cfg::Config; provider = nothing) -> NamedTuple

Run one complete live capture session:

1. optional market-clock gate (`stream.require_market_open`),
2. live WebSocket source → bounded channel,
3. batched raw NDJSON persistence,
4. shutdown on Ctrl-C, market close, low disk, session deadline or stream
   termination.

Returns `(; ticks, raw_files, status)`, `status` being the one written to the
sidecar: `"completed"`, `"interrupted"`, or `"failed"` after a fatal stream
error, exhausted reconnections or a session in which no connection opened
and no print arrived, which end the session without an exception.
A session that finds the market closed and does not wait returns no ticks,
no files and `"completed"`. Blocks until the session ends. Logging is scoped
to the call: the session runs under its own logger and the caller's global
logger is untouched.
"""
function run_stream(cfg::Config; provider::Union{AbstractProvider,Nothing} = nothing)
    sid = _fresh_session_id(() -> session_id(cfg), cfg)
    stop = Threads.Atomic{Bool}(false)
    logger, log_io = setup_logging(cfg; session_id = sid, shutting_down = stop)
    try
        return Logging.with_logger(logger) do
            _run_stream(cfg, sid, provider, stop)
        end
    finally
        log_io === nothing || close(log_io)
    end
end

function _run_stream(
    cfg::Config,
    sid::AbstractString,
    provider::Union{AbstractProvider,Nothing},
    stop::Threads.Atomic{Bool},
)
    provider === nothing && (provider = _provider_from_config(cfg))
    delay = feed_delay_ns(provider)
    delay > 0 &&
        @info "delayed feed; market-hours guards shifted" delay_s = delay ÷ NS_PER_SEC
    # The market-clock gate and the wait for the open come before the session
    # takes the data tree, so a capture started on a Friday evening holds no
    # lock over the weekend; the disk check and reconciliation then run at
    # the open, not at launch.
    clock = nothing
    # A venue that never closes has no open to wait for and no close to stop
    # at; the session deadline is the only bound that applies.
    always_open(provider) &&
        (cfg.require_market_open || cfg.stop_at_market_close) &&
        @info "venue trades continuously; market-hours guards do not apply"
    if !always_open(provider) && (cfg.require_market_open || cfg.stop_at_market_close)
        clock = market_clock(provider)
        if !clock.is_open && cfg.require_market_open
            if cfg.wait_for_open
                # settle past the bell, plus the feed's intrinsic delay: on a
                # delayed feed the first post-open data cannot arrive earlier.
                wait_s = (rfc3339_to_ns(clock.next_open) + delay - now_ns()) / 1e9 + 10
                @info "market closed; waiting for open" next_open = clock.next_open hours =
                    round(wait_s / 3600; digits = 2)
                try
                    sleep(max(wait_s, 0.0))
                catch e
                    e isa InterruptException || rethrow()
                    @info "interrupted while waiting for the open" sid
                    return (; ticks = 0, raw_files = String[], status = "interrupted")
                end
                clock = market_clock(provider)          # refresh next_close for the new session
            else
                @info "market closed; not streaming (set stream.wait_for_open to wait)" next_open =
                    clock.next_open
                return (; ticks = 0, raw_files = String[], status = "completed")
            end
        end
        clock.is_open && @info "market open" next_close = clock.next_close
    end

    # Delayed feeds keep transmitting the tape tail past the bell; allow an
    # extra minute for late-reported closing prints on top of the delay. The
    # close is read here, so a clock answer that does not parse fails before
    # a producer exists.
    close_ns =
        cfg.stop_at_market_close && clock !== nothing ?
        rfc3339_to_ns(String(clock.next_close)) +
        delay +
        (delay > 0 ? 60 * NS_PER_SEC : Int64(0)) : nothing

    # The envelope writes the sidecar first, so a compaction that finds a part
    # also finds its running session; the part comes before the producer, so
    # neither can fail with a connection already open and nothing to stop it.
    seen = Ref{Union{Nothing,SessionSummary}}(nothing)
    _with_session(cfg, sid) do summary
        seen[] = summary
        sink = open_raw_sink(cfg.raw_dir, sid; max_mb = cfg.max_raw_file_mb)
        session = try
            live_source(provider, cfg; stop)
        catch
            close_sink!(sink)
            rethrow()
        end
        close_ns === nothing || schedule_close_stop!(session, close_ns)
        @info "session started" id = sid raw = sink.path symbols = cfg.symbols

        on_flush = function (n, tot)
            @info "flushed batch" batch = n total = tot
            occ = _occupancy(session.channel)
            occ > 0.8 * cfg.channel_capacity &&
                @warn "tick channel nearly full; sink lagging" occupancy = occ capacity =
                    cfg.channel_capacity
            f = free_disk_gb(cfg.raw_dir)
            if f < cfg.min_free_disk_gb
                @error "free disk below limit; stopping session" free_gb =
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

        try
            summary.ticks = fetch(sink_task)
            # A session in which no connection opened and no print arrived never
            # reached the venue, whatever ended it: an upgrade left unanswered is
            # ended by stop!'s bound on the producer, which does not set the
            # outcome.
            (
                session.outcome[] === :failed ||
                (session.opens[] == 0 && summary.ticks == 0)
            ) && (summary.status = "failed")
            # An interrupt can land in the producer task instead of this one.
            session.outcome[] === :interrupted && (summary.status = "interrupted")
        catch e
            if has_wrapped_exception(e, InterruptException)
                summary.status = "interrupted"
                @info "interrupt; shutting down"
            else
                summary.status = "failed"
            end
            rethrow()
        finally
            stop!(session)                       # idempotent; unblocks the producer
            if !istaskfailed(sink_task)
                try
                    summary.ticks = fetch(sink_task)     # final drain + flush
                catch e
                    e isa InterruptException && rethrow()
                    summary.status = "failed"
                    @error "sink failed while draining" exception = (e, catch_backtrace())
                end
            end
            close_sink!(sink)
            files = _session_raw_files(cfg, sid)
            @info "session finished" status = summary.status ticks = summary.ticks files =
                length(files) meta = summary.meta_path frames = session.stats[].frames reconnects =
                session.stats[].reconnects
        end
        return nothing
    end
    return _capture_result(seen[])
end

# What a capture tells its caller, read off the summary once the envelope has
# settled it: the status, the tick count and the listing of raw parts that the
# sidecar records, however the session ended. An interrupt that came before
# the body ran left no summary to read.
_capture_result(::Nothing) = (; ticks = 0, raw_files = String[], status = "interrupted")
_capture_result(final::SessionSummary) =
    (; ticks = final.ticks, raw_files = final.raw_files, status = final.status)

_unfinished_marked(_, _) = "fetched before the day ended"

# Whether `day` has not yet ended on the exchange calendar of `cfg`.
_day_not_over(cfg::Config, day::Date) = day >= trading_date(now_ns(); tz = cfg.exchange_tz)

# Under the symbol as configured or in upper case, which is how the Binance
# adapter and the vendor readers file a symbol given in lower case.
_processed_exists(cfg::Config, sym::AbstractString, day::Date) = any(
    isfile(joinpath(cfg.processed_dir, s, "$day.$ext")) for
    s in unique((sym, uppercase(sym))), ext in ("csv", "arrow")
)

"""
    run_backfill(cfg::Config; provider = nothing) -> Vector{String}

Download historical trades for every configured symbol over
`[backfill.start_date, backfill.end_date]` (the provider's trading days,
[`session_days`](@ref)), persist them through the same raw-NDJSON +
compaction path as live data (marked by `recv_ns = 0`), and return the
processed file paths. The sidecar records both counts: `session.ticks`, the
prints received, and `session.ticks_processed`, the prints stored after
compaction removed duplicates (a vendor file can repeat rows).

With `backfill.keep_raw = false` (bulk source only) a day's raw parts are
deleted once it is compacted: the checksum-verified vendor file under
`bulk/` is the day's record, and a rerun with `resume = false` rebuilds the
day from it without the network. The NDJSON copy is about 21 times the
vendor file on Binance `trades`, so this bounds the raw layer to one day.

Work is done one `(symbol, day)` at a time: its own raw part set, compacted
as soon as that day finishes. Pages are flushed to disk as they arrive, so
RAM stays bounded by one page and `limits.max_live_heap_mb`.

`backfill.resume` depends on that granularity. The skip test asks whether a
day is already present under `processed/`, so a day lands there as soon as
it is complete and not before, and a run killed after eight of twelve hours
resumes at hour eight. A day fetched before it has ended on the exchange
calendar is the exception: it lands with the marker `<file>.incomplete.toml`
beside it (see [`compact_raw`](@ref)), and a later run needs
`resume = false` for that day to fetch it whole.

An interrupted day is not compacted and is therefore downloaded again; its
partial raw file stays on disk, and compaction deduplicates it if it is ever
folded in. Ctrl-C finalizes the provenance sidecar
as `interrupted` and returns the days that did complete. Any other error
finalizes it as `failed` and propagates; the days compacted before the error
remain valid and a rerun resumes after them.

Logging is scoped to the call: the session runs under its own logger and the
caller's global logger is untouched.
"""
function run_backfill(cfg::Config; provider::Union{AbstractProvider,Nothing} = nothing)
    isempty(cfg.backfill_feed) && throw(
        ArgumentError(
            "provider \"$(cfg.provider)\" declares no historical feed: nothing to backfill",
        ),
    )
    prefix = cfg.backfill_source === :bulk ? "backfill_bulk_" : "backfill_"
    sid = _fresh_session_id(() -> prefix * session_id(cfg; feed = cfg.backfill_feed), cfg)
    logger, log_io = setup_logging(cfg; session_id = sid)
    try
        return Logging.with_logger(logger) do
            _run_backfill(cfg, sid, provider)
        end
    finally
        log_io === nothing || close(log_io)
    end
end

function _run_backfill(
    cfg::Config,
    sid::AbstractString,
    provider::Union{AbstractProvider,Nothing},
)
    provider === nothing && (provider = _provider_from_config(cfg))
    processed = String[]
    return _with_session(
        cfg,
        sid;
        on_interrupt = "interrupt; completed days are compacted and a rerun resumes",
        result = processed,
    ) do summary
        summary.ticks_processed = 0
        total = 0
        stored = 0
        days = session_days(provider, cfg.backfill_start, cfg.backfill_end)
        bulk = cfg.backfill_source === :bulk
        # The last trade id of each symbol's latest bulk day in this run, so
        # that the next day's first id can be checked against it.
        last_ids = Dict{String,Tuple{Date,Int64}}()   # symbol => (day, last trade id)
        id_breaks = 0
        bulk && (summary.id_breaks = 0)
        for sym in cfg.symbols, day in days
            if cfg.backfill_resume && _processed_exists(cfg, sym, day)
                @info "resume; already processed, skipping" symbol = sym day
                continue
            end
            # One raw part set per (symbol, day), compacted the moment the day
            # finishes, which is what lets `resume` survive a kill (see the
            # docstring).
            day_prefix = "$(sid)_$(sym)_$(day)"
            # Judged before the fetch: one that begins on an open day holds what
            # the venue had published so far, even if it ends after midnight.
            open_day = _day_not_over(cfg, day)
            day_sink = open_raw_sink(cfg.raw_dir, day_prefix; max_mb = cfg.max_raw_file_mb)
            n = 0
            sink_page =
                page -> begin
                    write_batch!(day_sink, page)
                    check_live_heap(cfg.max_live_heap_mb; context = "backfill $sym $day")
                end
            try
                # A bulk day is one file, a REST day a walk of pages; both
                # stream through the same sink, so everything downstream of
                # this point is one path.
                n = if bulk
                    prev = get(last_ids, sym, nothing)
                    r = bulk_trades(
                        provider,
                        sym,
                        day;
                        feed = cfg.backfill_feed,
                        dest_dir = cfg.bulk_dir,
                        each_page = sink_page,
                        previous_id = prev !== nothing && prev[1] == day - Day(1) ?
                                      prev[2] : nothing,
                    )
                    r.last_id === nothing || (last_ids[sym] = (day, r.last_id))
                    r.id_breaks > 0 &&
                        @warn "trade ids not consecutive: prints missing or repeated" symbol =
                            sym day breaks = r.id_breaks
                    id_breaks += r.id_breaks
                    summary.id_breaks = id_breaks
                    r.prints
                else
                    historical_trades(
                        provider,
                        sym,
                        day,
                        day;
                        feed = cfg.backfill_feed,
                        page_limit = cfg.backfill_page_limit,
                        rate_sleep_s = cfg.backfill_rate_sleep_s,
                        each_page = sink_page,
                    ).prints
                end
            finally
                # Removes the part of a day without prints (a holiday, a halt,
                # a symbol not yet listed, a failure before the first page).
                close_sink!(day_sink)
            end
            day_files = _raw_files_with_prefix(cfg.raw_dir, day_prefix)
            n_stored = 0
            if n > 0
                c = _compact_raw(
                    cfg,
                    day_files;
                    incomplete = open_day ? _unfinished_marked : _no_day,
                )
                append!(processed, c.files)
                n_stored = c.rows
                # A bulk day's record is the verified vendor file; its NDJSON
                # copy goes once the day is under processed/, and not before.
                cfg.backfill_keep_raw || foreach(f -> rm(f; force = true), day_files)
            end
            total += n
            stored += n_stored
            summary.ticks = total
            summary.ticks_processed = stored
            bulk && sleep(cfg.backfill_rate_sleep_s)
            @info "backfilled" symbol = sym day trades = n stored = n_stored
        end
        @info "backfill finished" status = summary.status days = length(processed) ticks =
            total stored
        return processed
    end
end

"""
    run_entrypoint(main)

Run a command-line entry point's `main` with clean interrupt handling: SIGINT
arrives as an `InterruptException` and exits without a stack trace.

Delivery is best-effort under threads; recovery after an interrupt rests on
the crash-only session lifecycle and does not depend on this wrapper. Shared
by the scripts under `scripts/` and the worked examples, so every entry point
behaves the same way at the terminal.

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
        has_wrapped_exception(e, InterruptException) || rethrow()
        println("\nInterrupted.")
    end
    return nothing
end
