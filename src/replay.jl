# Recorded-session replay: re-emit persisted ticks as a live-like stream.
#
# This is the scientific workhorse — real-time analysis methods are developed
# and validated against *recorded* flux (reproducible, repeatable, free)
# before ever touching the live WebSocket. Live and replay sources feed the
# identical `Channel{Trade}` interface, so downstream code cannot tell them
# apart.

# Which timestamp paces and orders the replay. Backfilled records carry
# `recv_ns = 0`, so a recording without a complete receive clock can only be
# replayed on exchange time.
function _replay_clock(trades::Vector{Trade}, clock::AbstractString)
    n_missing = count(t -> t.recv_ns <= 0, trades)
    clock == "exchange" && return "exchange"
    if clock == "recv"
        n_missing > 0 && throw(
            ArgumentError(
                "clock = \"recv\" but $n_missing of $(length(trades)) records " *
                "carry no receive timestamp (backfilled); use \"exchange\" or \"auto\"",
            ),
        )
        return "recv"
    end
    n_missing == 0 && return "recv"
    @info "recording has no complete receive clock — replaying on exchange time" missing =
        n_missing
    return "exchange"
end

_recv_stamp(t::Trade) = t.recv_ns
_exchange_stamp(t::Trade) = t.time_ns

# `sleep` cannot resolve intervals below about a millisecond. Shorter waits
# are skipped, which loses nothing: every wait is measured against the
# absolute schedule, so a skipped or overshot sleep is absorbed by the next
# one and the timing error stays bounded instead of accumulating.
const REPLAY_MIN_SLEEP_S = 1e-3

function _emit_paced!(
    ch::Channel{Trade},
    trades::Vector{Trade},
    stamp,
    paced::Bool,
    speed::Float64,
)
    isempty(trades) && return nothing
    t0_rec = stamp(trades[1])
    t0_wall = time()
    for t in trades
        if paced
            due = t0_wall + (stamp(t) - t0_rec) / NS_PER_SEC / speed
            wait_s = due - time()
            wait_s > REPLAY_MIN_SLEEP_S && sleep(wait_s)
        end
        put!(ch, t)
    end
    return nothing
end

"""
    replay_source(paths; pace = "recorded", speed = 1.0, clock = "auto",
                  capacity = 100_000) -> Channel{Trade}

Create a channel that replays the trades recorded in raw NDJSON `paths`
(ordered by the replay clock).

- `pace = "recorded"`: honor the original inter-arrival times of the replay
  clock against an absolute schedule, compressed by `speed` (e.g.
  `speed = 60.0` replays an hour in a minute), so the timing error does not
  accumulate over a session.
- `pace = "max"`: emit as fast as the consumer takes them (throughput mode).

The replay clock is selected by `clock`:

- `"recv"`: local receive time — what a live consumer experienced, network
  jitter included. Throws if any record lacks it.
- `"exchange"`: the venue's own timestamps — the only clock a backfilled
  recording has.
- `"auto"` (default): `"recv"` when every record carries a receive timestamp,
  otherwise `"exchange"`, logged at `@info`.

The channel closes when the recording is exhausted, which cleanly terminates
any consumer written against [`run_sink!`](@ref)-style loops.
"""
function replay_source(
    paths::AbstractVector{<:AbstractString};
    pace::AbstractString = "recorded",
    speed::Real = 1.0,
    clock::AbstractString = "auto",
    capacity::Integer = 100_000,
)
    pace in ("recorded", "max") ||
        throw(ArgumentError("pace must be \"recorded\" or \"max\""))
    speed > 0 || throw(ArgumentError("speed must be positive"))
    clock in ("auto", "recv", "exchange") ||
        throw(ArgumentError("clock must be \"auto\", \"recv\" or \"exchange\""))
    trades = read_raw(paths)
    resolved = _replay_clock(trades, clock)
    stamp = resolved == "recv" ? _recv_stamp : _exchange_stamp
    sort!(trades; by = stamp, alg = MergeSort)
    return Channel{Trade}(capacity; spawn = true) do ch
        _emit_paced!(ch, trades, stamp, pace == "recorded", Float64(speed))
    end
end

replay_source(path::AbstractString; kwargs...) = replay_source([path]; kwargs...)
