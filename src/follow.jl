# File-following source: a third source beside `live_source` and
# `replay_source`. A consumer in another process follows a session's raw
# NDJSON parts as they are flushed, coupled to the capture through the files
# and the sidecar only.

"""
    RawTail

Incremental tail over one session's raw part files: the byte offset read so
far and the partial-line carry of each file. Shared by [`monitor_raw`](@ref)
and [`follow_source`](@ref).
"""
struct RawTail
    dir::String
    prefix::String
    offsets::Dict{String,Int64}
    carries::Dict{String,String}
end
RawTail(dir, prefix) =
    RawTail(String(dir), String(prefix), Dict{String,Int64}(), Dict{String,String}())

_session_parts(dir, prefix) = _raw_files_with_prefix(dir, prefix)
_session_parts(t::RawTail) = _session_parts(t.dir, t.prefix)

"""
    latest_session_prefix(dir) -> String

Session id of the most recently modified raw part file in `dir`: the longest
sidecar id that prefixes the part's name (a backfill names its parts
`<id>_<SYMBOL>_<DATE>`), or the name up to `_partNNN` when no sidecar
matches. Throws if no part exists.
"""
function latest_session_prefix(dir::AbstractString)
    best, best_m = nothing, -Inf
    for f in readdir(dir)
        m = match(r"^(.*)_part\d+\.jsonl$", f)
        m === nothing && continue
        t = mtime(joinpath(dir, f))
        t > best_m && ((best, best_m) = (m[1], t))
    end
    best === nothing && throw(ArgumentError("no raw session files found in $dir"))
    return String(something(_sidecar_id(dir, best), best))
end

# The longest sidecar id in `dir` that prefixes `prefix`, or `nothing`.
function _sidecar_id(dir::AbstractString, prefix::AbstractString)
    best = nothing
    for f in readdir(dir)
        endswith(f, ".meta.toml") || continue
        id = chopsuffix(f, ".meta.toml")
        startswith(prefix, id) || continue
        (best === nothing || length(id) > length(best)) && (best = id)
    end
    return best
end

# Start at the current end of every existing part: only what is appended
# afterwards is read. The start is the end of the last complete line, so a
# line being written at attach is read whole once its newline arrives rather
# than counted as a corrupt fragment.
function _skip_to_end!(t::RawTail)
    for p in _session_parts(t)
        t.offsets[p] = _last_line_end(p)
    end
    return t
end

# The offset just past the last newline of `path`, 0 when the file holds no
# complete line. Read backwards in steps of `window` bytes, so the cost is that
# of the last line.
function _last_line_end(path::AbstractString; window::Integer = 1 << 20)
    return open(path) do io
        hi = Int64(filesize(io))
        while hi > 0
            lo = max(Int64(0), hi - window)
            seek(io, lo)
            i = findlast(==(UInt8('\n')), read(io, hi - lo))
            i === nothing || return lo + i
            hi = lo
        end
        return Int64(0)
    end
end

# Bytes read from a part per step of `_drain!`, which bounds its memory
# however far behind the reader is.
const DRAIN_BLOCK = 64 << 20

# A part opened for reading, or `nothing` when it disappeared before the open
# (`backfill.keep_raw = false` deletes a day's parts once it is compacted).
function _open_part(path::AbstractString)
    try
        return open(path)
    catch e
        (e isa SystemError && !isfile(path)) || rethrow()
        return nothing
    end
end

# Read the bytes appended to every part since the last call, parse the
# complete NDJSON lines in file order and hand each print to `f`. The
# incomplete trailing line of a file is carried until its newline arrives (a
# mid-write attach or a kill leaves torn lines); a part that has disappeared
# (`backfill.keep_raw = false`) is skipped. Returns `(prints, corrupt)`: the
# prints delivered and the complete lines that did not parse.
function _drain!(f, t::RawTail; block::Integer = DRAIN_BLOCK)
    prints = 0
    corrupt = 0
    for path in _session_parts(t)
        isfile(path) || continue
        off = get(t.offsets, path, Int64(0))
        sz = filesize(path)
        off < sz || continue
        # One handle per part and call: a part deleted while it is read stays
        # readable through it.
        io = _open_part(path)
        io === nothing && continue
        seek(io, off)
        try
            prints, corrupt = _drain_part!(f, t, path, io, off, block, prints, corrupt)
        finally
            close(io)
        end
    end
    return (prints, corrupt)
end

# Read `path` from `off` to the end of `io` in steps of `block` bytes,
# carrying a torn line between steps and calls. Bytes appended while it reads
# are read too, through the handle even once the part is deleted.
function _drain_part!(f, t::RawTail, path, io, off, block, prints, corrupt)
    while true
        chunk = String(read(io, block))
        isempty(chunk) && break
        off += sizeof(chunk)
        t.offsets[path] = off
        carry = get(t.carries, path, "")
        lines = split(isempty(carry) ? chunk : carry * chunk, '\n')
        t.carries[path] = String(lines[end])
        for ln in @view lines[1:(end-1)]
            all(isspace, ln) && continue
            trade = try
                json_to_trade(ln)
            catch e
                e isa InterruptException && rethrow()
                corrupt += 1
                continue
            end
            f(trade)
            prints += 1
        end
    end
    return (prints, corrupt)
end

# The sidecar of the session whose raw files start with `prefix`: a session
# id is a prefix of its file names, and a backfill's day files carry the
# symbol and date after it, so the longest sidecar id that prefixes the file
# prefix is the session's.
function _sidecar_path(dir::AbstractString, prefix::AbstractString)
    best = _sidecar_id(dir, prefix)
    best === nothing && throw(
        ArgumentError(
            "no sidecar for session $prefix in $dir: follow_source follows sessions " *
            "run by run_stream, run_backfill or run_import",
        ),
    )
    return joinpath(dir, "$(best).meta.toml")
end

# Status recorded in the sidecar; a file caught mid-rewrite reads as still
# running and is re-read at the next poll.
function _sidecar_status(path::AbstractString)
    return try
        String(
            get(
                get(TOML.parsefile(path), "session", Dict{String,Any}()),
                "status",
                "running",
            ),
        )
    catch e
        e isa InterruptException && rethrow()
        "running"
    end
end

"""
    follow_source(dir; session = nothing, from_start = true, poll_s = 0.5,
                  idle_timeout_s = Inf, capacity = 100_000) -> Channel{Trade}

Create a channel that follows a session's raw NDJSON files under `dir` as
they are written, so that a consumer in another process receives the
prints of a running capture, backfill or import through the files alone.
The session is the most recently active one unless `session` names the
prefix of its raw files (a session id, or a backfill's `<id>_<SYMBOL>_<date>`
for one of its days). Prints arrive in file order, the arrival order of the
capture, as the sink flushes them; a torn final line is carried until its
newline arrives.

The sidecar `<id>.meta.toml` records the session's lifecycle: the channel stays
open while its status is `running` and closes once the status leaves it —
`completed`, `interrupted`, `failed`, or `aborted` after a reconciliation —
after every line flushed until then has been delivered. A finished session
is therefore read whole and the channel closes at its end. The sidecar is
required: a raw directory written by a bare [`RawSink`](@ref) has no
lifecycle to follow.

`from_start = true` delivers the session from its first print; `false`
starts at the current end of the files, for a consumer that wants activity
from attach onwards. `poll_s` is the interval between reads when nothing
new has arrived. `idle_timeout_s` bounds how long the files may stay
unchanged while the sidecar still says `running`; past it the channel
closes with a [`SessionError`](@ref), so a consumer following a session whose process died
unreconciled does not wait forever. Lines that do not parse are skipped
and warned about, as [`read_raw`](@ref) does.

A consumer written against [`live_source`](@ref) or [`replay_source`](@ref)
runs unchanged against this channel; [`tee`](@ref) and [`snapshots`](@ref)
apply as to any source.

```julia
for trade in follow_source(cfg.raw_dir)       # ends when the session ends
    # ... a method under test, in a process separate from the capture
end
```
"""
function follow_source(
    dir::AbstractString;
    session::Union{Nothing,AbstractString} = nothing,
    from_start::Bool = true,
    poll_s::Real = 0.5,
    idle_timeout_s::Real = Inf,
    capacity::Integer = 100_000,
)
    poll_s > 0 || throw(ArgumentError("poll_s must be positive"))
    idle_timeout_s > 0 || throw(ArgumentError("idle_timeout_s must be positive"))
    prefix = session === nothing ? latest_session_prefix(dir) : String(session)
    sidecar = _sidecar_path(dir, prefix)
    return Channel{Trade}(capacity; spawn = true) do ch
        tail = RawTail(dir, prefix)
        from_start || _skip_to_end!(tail)
        last_growth = time()
        while true
            # The status is read before the drain: everything flushed before the
            # producer finalized its sidecar is delivered by the drain that follows.
            status = _sidecar_status(sidecar)
            n, corrupt = _drain!(t -> put!(ch, t), tail)
            corrupt > 0 && @warn "skipped corrupt lines" session = prefix count = corrupt
            n > 0 && (last_growth = time())
            status == "running" || break
            if n == 0
                idle = time() - last_growth
                idle > idle_timeout_s && throw(
                    SessionError(
                        sidecar,
                        "no new data for $(round(Int, idle)) s while " *
                        "$(basename(sidecar)) still says running",
                    ),
                )
                sleep(poll_s)
            end
        end
    end
end
