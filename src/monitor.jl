# Live in-terminal monitoring (attach mode): tail a session's raw NDJSON
# part files from a separate read-only process and render a dashboard. Never
# touches the producer, its logging, or its progress machinery — the monitor
# can attach to and detach from a running live capture or backfill at any
# time. The dashboard is text; with UnicodePlots loaded, the package extension
# MarketTickStreamerUnicodePlotsExt adds the rate history and the per-symbol
# bar chart.

"""
    MonitorState

Incremental tail state over one session's raw part files — a
[`RawTail`](@ref) for the per-file offsets and carries — with cumulative
per-symbol counters, a tick-rate history window, the latest exchange
timestamp seen (the tape head), and the count of lines that did not parse.
Counters cover data ingested since attach (see `from_start`).
"""
mutable struct MonitorState
    tail::RawTail
    total::Int64
    per_symbol::Dict{String,Int64}
    window::Vector{Tuple{Float64,Int64}}   # (wall clock, new ticks per refresh)
    last_time_ns::Int64
    last_growth::Float64
    corrupt::Int64
end

MonitorState(dir, prefix) = MonitorState(
    RawTail(dir, prefix),
    0,
    Dict{String,Int64}(),
    Tuple{Float64,Int64}[],
    0,
    time(),
    0,
)

# Read what was appended since the previous call and update the counters.
# Torn lines are carried and unparsable ones counted by `_drain!`.
function _ingest!(st::MonitorState)
    new_ticks, corrupt = _drain!(st.tail) do t
        st.total += 1
        st.per_symbol[t.symbol] = get(st.per_symbol, t.symbol, 0) + 1
        t.time_ns > st.last_time_ns && (st.last_time_ns = t.time_ns)
    end
    st.corrupt += corrupt
    new_ticks > 0 && (st.last_growth = time())
    push!(st.window, (time(), new_ticks))
    return new_ticks
end

function _render(
    st::MonitorState,
    refresh_s::Real,
    top_symbols::Integer,
    rate_window_s::Real,
    io::IO,
)
    cutoff = time() - rate_window_s
    filter!(w -> w[1] >= cutoff, st.window)
    rates = [w[2] / refresh_s for w in st.window]
    buf = IOBuffer()
    println(buf, "Session: ", st.tail.prefix)
    bytes = sum(get(st.tail.offsets, p, 0) for p in _session_parts(st.tail); init = 0)
    println(
        buf,
        "Ticks (since attach): ",
        st.total,
        "   rate: ",
        isempty(rates) ? "—" : "$(round(rates[end]; digits = 1))/s",
        "   read: ",
        round(bytes / 2^20; digits = 1),
        " MiB",
    )
    if st.last_time_ns > 0
        lag = (now_ns() - st.last_time_ns) / 1e9
        println(
            buf,
            "Tape head: ",
            ns_to_rfc3339(st.last_time_ns),
            "   lag: ",
            round(lag; digits = 1),
            " s",
        )
    end
    stall = time() - st.last_growth
    stall > 3 * refresh_s && println(buf, "! no new data for ", round(Int, stall), " s")
    st.corrupt > 0 && println(buf, "! ", st.corrupt, " corrupt lines skipped")
    top = sort(collect(st.per_symbol); by = last, rev = true)
    top = top[1:min(top_symbols, length(top))]
    plots = Base.get_extension(@__MODULE__, :MarketTickStreamerUnicodePlotsExt)
    if plots === nothing
        isempty(top) || println(buf, "Ticks by symbol:")
        for (sym, n) in top
            println(buf, "  ", rpad(sym, 10), n)
        end
    else
        length(rates) >= 2 && plots.draw_rate_history(buf, rates, rate_window_s)
        isempty(top) || plots.draw_symbol_counts(buf, first.(top), last.(top))
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
timestamp and lag, and per-symbol counts. With UnicodePlots loaded
(`using UnicodePlots`) the counts become a bar chart and a rate-history plot
is added; without it the dashboard is plain text.

`from_start = true` ingests the whole existing file first (session totals;
costs one full read of the raw data); the default starts at the current end
of file (activity since attach). `iterations` bounds the number of refreshes
(`nothing` = run until interrupted). Returns the final `MonitorState`.
"""
function monitor_raw(
    dir::AbstractString;
    session::Union{Nothing,AbstractString} = nothing,
    refresh_s::Real = 2.0,
    top_symbols::Integer = 10,
    rate_window_s::Real = 300.0,
    from_start::Bool = false,
    iterations::Union{Nothing,Integer} = nothing,
    io::IO = stdout,
)
    refresh_s > 0 || throw(ArgumentError("refresh_s must be positive"))
    prefix = session === nothing ? latest_session_prefix(dir) : String(session)
    st = MonitorState(dir, prefix)
    from_start || _skip_to_end!(st.tail)
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
