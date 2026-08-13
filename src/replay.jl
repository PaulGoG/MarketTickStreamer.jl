# Recorded-session replay: re-emit persisted ticks as a live-like stream.
#
# This is the scientific workhorse — real-time analysis methods are developed
# and validated against *recorded* flux (reproducible, repeatable, free)
# before ever touching the live WebSocket. Live and replay sources feed the
# identical `Channel{Trade}` interface, so downstream code cannot tell them
# apart.

"""
    replay_source(paths; pace = "recorded", speed = 1.0,
                  capacity = 100_000) -> Channel{Trade}

Create a channel that replays the trades recorded in raw NDJSON `paths`
(sorted by receive timestamp).

- `pace = "recorded"`: honor the original inter-arrival times of `recv_ns`,
  compressed by `speed` (e.g. `speed = 60.0` replays an hour in a minute).
- `pace = "max"`: emit as fast as the consumer takes them (throughput mode).

The channel closes when the recording is exhausted, which cleanly terminates
any consumer written against [`run_sink!`](@ref)-style loops.
"""
function replay_source(
    paths::AbstractVector{<:AbstractString};
    pace::AbstractString = "recorded",
    speed::Real = 1.0,
    capacity::Integer = 100_000,
)
    pace in ("recorded", "max") ||
        throw(ArgumentError("pace must be \"recorded\" or \"max\""))
    speed > 0 || throw(ArgumentError("speed must be positive"))
    trades = sort!(read_raw(paths); by = t -> t.recv_ns)
    return Channel{Trade}(capacity; spawn = true) do ch
        isempty(trades) && return
        t_prev = trades[1].recv_ns
        for t in trades
            if pace == "recorded"
                dt = (t.recv_ns - t_prev) / NS_PER_SEC / speed
                dt > 1e-3 && sleep(dt)      # sub-ms gaps: sleep() can't resolve them
                t_prev = t.recv_ns
            end
            put!(ch, t)
        end
    end
end

replay_source(path::AbstractString; kwargs...) = replay_source([path]; kwargs...)
