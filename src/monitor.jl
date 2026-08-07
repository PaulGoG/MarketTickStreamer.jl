# Live in-terminal monitoring (attach mode): tail a session's raw NDJSON
# part files from a separate read-only process and render a UnicodePlots
# dashboard. Never touches the producer, its logging, or its progress
# machinery — the monitor can attach to and detach from a running live
# capture or backfill at any time.

"""
    MonitorState

Incremental tail state over one session's raw part files: per-file byte
offsets and partial-line carries, cumulative per-symbol counters, a tick-rate
history window, and the latest exchange timestamp seen (the tape head).
Counters cover data ingested since attach (see `from_start`).
"""
mutable struct MonitorState
    dir::String
    prefix::String
    offsets::Dict{String, Int64}
    carries::Dict{String, String}
    total::Int64
    per_symbol::Dict{String, Int64}
    window::Vector{Tuple{Float64, Int64}}   # (wall clock, new ticks per refresh)
    last_time_ns::Int64
    last_growth::Float64
end

MonitorState(dir, prefix) = MonitorState(String(dir), String(prefix),
    Dict{String, Int64}(), Dict{String, String}(), 0, Dict{String, Int64}(),
    Tuple{Float64, Int64}[], 0, time())

_session_parts(dir, prefix) = [joinpath(dir, f) for f in sort(readdir(dir))
                               if startswith(f, prefix) && endswith(f, ".jsonl")]

"""
    latest_session_prefix(dir) -> String

Prefix (session id) of the most recently modified raw part file in `dir`.
Throws if none exist.
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
    return String(best)
end

# Read bytes appended since the previous call, parse complete NDJSON lines,
# and update counters. Incomplete trailing lines are carried per file until
# their newline arrives (a mid-write attach or kill leaves torn lines).
function _ingest!(st::MonitorState)
    new_ticks = 0
    for path in _session_parts(st.dir, st.prefix)
        off = get(st.offsets, path, Int64(0))
        sz = filesize(path)
        sz <= off && continue
        chunk = open(path) do io
            seek(io, off)
            String(read(io))
        end
        st.offsets[path] = off + sizeof(chunk)
        lines = split(get(st.carries, path, "") * chunk, '\n')
        st.carries[path] = String(lines[end])
        for ln in @view lines[1:(end - 1)]
            isempty(ln) && continue
            t = try
                json_to_trade(ln)
            catch
                continue
            end
            st.total += 1
            new_ticks += 1
            st.per_symbol[t.symbol] = get(st.per_symbol, t.symbol, 0) + 1
            t.time_ns > st.last_time_ns && (st.last_time_ns = t.time_ns)
        end
    end
    new_ticks > 0 && (st.last_growth = time())
    push!(st.window, (time(), new_ticks))
    return new_ticks
end

function _render(st::MonitorState, refresh_s::Real, top_symbols::Integer,
                 rate_window_s::Real, io::IO)
    cutoff = time() - rate_window_s
    filter!(w -> w[1] >= cutoff, st.window)
    rates = [w[2] / refresh_s for w in st.window]
    buf = IOBuffer()
    println(buf, "Session: ", st.prefix)
    bytes = sum(get(st.offsets, p, 0) for p in _session_parts(st.dir, st.prefix); init = 0)
    println(buf, "Ticks (since attach): ", st.total,
            "   rate: ", isempty(rates) ? "—" : "$(round(rates[end]; digits = 1))/s",
            "   read: ", round(bytes / 2^20; digits = 1), " MiB")
    if st.last_time_ns > 0
        lag = (now_ns() - st.last_time_ns) / 1e9
        println(buf, "Tape head: ", ns_to_rfc3339(st.last_time_ns),
                "   lag: ", round(lag; digits = 1), " s")
    end
    stall = time() - st.last_growth
    stall > 3 * refresh_s &&
        println(buf, "! no new data for ", round(Int, stall), " s")
    if length(rates) >= 2
        println(buf, lineplot(rates; title = "Ticks/s (window $(round(Int, rate_window_s)) s)",
                              height = 6, width = 54))
    end
    if !isempty(st.per_symbol)
        top = sort(collect(st.per_symbol); by = last, rev = true)
        top = top[1:min(top_symbols, length(top))]
        println(buf, barplot(first.(top), last.(top); title = "Ticks by symbol"))
    end
    print(io, String(take!(buf)))
    return nothing
end

"""
    monitor_raw(dir; session = nothing, refresh_s = 2.0, top_symbols = 10,
                rate_window_s = 300.0, from_start = false,
                iterations = nothing, io = stdout) -> MonitorState

Attach a live in-terminal dashboard to the session whose raw NDJSON files
live under `dir` (most recently active session unless `session` gives an id
prefix). Tails the part files read-only — safe to run beside a live capture
or backfill. Renders every `refresh_s`: tick totals and rate, tape-head
timestamp and lag, a rate history sparkline, and per-symbol counts.

`from_start = true` ingests the whole existing file first (session totals;
costs one full read of the raw data); the default starts at the current end
of file (activity since attach). `iterations` bounds the number of refreshes
(`nothing` = run until interrupted). Returns the final `MonitorState`.
"""
function monitor_raw(dir::AbstractString;
                     session::Union{Nothing, AbstractString} = nothing,
                     refresh_s::Real = 2.0, top_symbols::Integer = 10,
                     rate_window_s::Real = 300.0, from_start::Bool = false,
                     iterations::Union{Nothing, Integer} = nothing,
                     io::IO = stdout)
    refresh_s > 0 || throw(ArgumentError("refresh_s must be positive"))
    prefix = session === nothing ? latest_session_prefix(dir) : String(session)
    st = MonitorState(dir, prefix)
    if !from_start
        for p in _session_parts(dir, prefix)
            st.offsets[p] = filesize(p)
        end
    end
    clear = io === stdout && iterations === nothing
    n = 0
    while iterations === nothing || n < iterations
        n += 1
        _ingest!(st)
        clear && print(io, "\e[H\e[2J")
        _render(st, refresh_s, top_symbols, rate_window_s, io)
        (iterations === nothing || n < iterations) && sleep(refresh_s)
    end
    return st
end
