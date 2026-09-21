# Recorded-session replay: re-emit persisted ticks as a live-like stream.
#
# This is the scientific workhorse — real-time analysis methods are developed
# and validated against *recorded* flux (reproducible, repeatable, free)
# before ever touching the live WebSocket. Live and replay sources feed the
# identical `Channel{Trade}` interface, so downstream code cannot tell them
# apart.
#
# Two inputs: the raw files of a session, read whole, and the processed day
# files of a corpus, streamed a day at a time so that a symbol-year replays in
# the memory of its busiest day.

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

# `origin` is the pair (recorded stamp, wall clock) the schedule is anchored
# on. It is fixed at the first print of a replay and outlives the batch, so a
# recording streamed day by day keeps one schedule across its days.
function _emit_paced!(
    ch::Channel{Trade},
    trades::Vector{Trade},
    stamp,
    paced::Bool,
    speed::Float64,
    origin::Tuple{Int64,Float64},
)
    t0_rec, t0_wall = origin
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

_is_processed(path::AbstractString) = endswith(path, ".arrow") || endswith(path, ".csv")

# Processed day files grouped by the date in their name, in date order. One
# group holds every symbol of that day.
function _files_by_date(paths::AbstractVector{<:AbstractString})
    groups = Dict{Date,Vector{String}}()
    for p in paths
        m = match(PROCESSED_FILE, basename(p))
        m === nothing && throw(
            ArgumentError(
                "not a processed day file (YYYY-MM-DD.csv|.arrow): $p — safesave " *
                "backups and partial files are not replayed",
            ),
        )
        push!(get!(() -> String[], groups, Date(something(m[1]))), String(p))
    end
    return [groups[d] for d in sort!(collect(keys(groups)))]
end

# Stream processed day files: one day in memory at a time, every symbol of
# the day merged on the replay clock.
function _replay_processed!(
    ch::Channel{Trade},
    days::Vector{Vector{String}},
    clock::AbstractString,
    paced::Bool,
    speed::Float64,
)
    resolved = ""
    origin = (Int64(0), 0.0)
    for files in days
        trades = reduce(vcat, (_trades_from_processed(f) for f in files); init = Trade[])
        isempty(trades) && continue
        if isempty(resolved)
            # The clock is settled on the first day and held: a replay that
            # changed clocks between days would have no single time axis.
            resolved = _replay_clock(trades, clock)
        elseif resolved == "recv" && any(t -> t.recv_ns <= 0, trades)
            throw(
                ArgumentError(
                    "replaying on the receive clock, but $(join(basename.(files), ", ")) " *
                    "holds records without a receive timestamp; use clock = \"exchange\"",
                ),
            )
        end
        stamp = resolved == "recv" ? _recv_stamp : _exchange_stamp
        issorted(trades; by = stamp) || sort!(trades; by = stamp, alg = MergeSort)
        origin[2] == 0.0 && (origin = (stamp(trades[1]), time()))
        _emit_paced!(ch, trades, stamp, paced, speed, origin)
    end
    return nothing
end

"""
    replay_source(paths; pace = "recorded", speed = 1.0, clock = "auto",
                  capacity = 100_000) -> Channel{Trade}

Create a channel that replays recorded trades, ordered by the replay clock.
`paths` are either raw NDJSON files (`.jsonl`) or processed day files
(`YYYY-MM-DD.csv|.arrow`, e.g. from [`processed_files`](@ref)); the two kinds
cannot be mixed.

- Raw files are read whole and sorted, since arrival order is not
  chronological across venues and parts. This is the mode for a session.
- Processed files are **streamed one trading day at a time**: the files of a
  date — one per symbol — are loaded, merged on the replay clock and emitted
  before the next date is opened, so memory is bounded by the busiest day
  however long the span. This is the mode for a corpus. Recorded pace runs on
  one schedule across the days, closures included: replaying a week at
  `speed = 1` takes a week, so pass `speed`, or `pace = "max"`.

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
  otherwise `"exchange"`, logged at `@info`. For processed files the choice is
  made on the first day and held.

The channel closes when the recording is exhausted, which cleanly terminates
any consumer written against [`run_sink!`](@ref)-style loops. An error while
streaming a later day (an unreadable file, a missing receive clock) closes the
channel with that error, and the consumer's iteration rethrows it.
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
    paced = pace == "recorded"
    n_processed = count(_is_processed, paths)
    if n_processed > 0
        n_processed == length(paths) ||
            throw(ArgumentError("raw and processed files cannot be replayed together"))
        days = _files_by_date(paths)
        foreach(f -> isfile(f) || throw(ArgumentError("no such file: $f")), paths)
        return Channel{Trade}(capacity; spawn = true) do ch
            _replay_processed!(ch, days, clock, paced, Float64(speed))
        end
    end
    trades = read_raw(paths)
    resolved = _replay_clock(trades, clock)
    stamp = resolved == "recv" ? _recv_stamp : _exchange_stamp
    sort!(trades; by = stamp, alg = MergeSort)
    return Channel{Trade}(capacity; spawn = true) do ch
        isempty(trades) && return nothing
        _emit_paced!(ch, trades, stamp, paced, Float64(speed), (stamp(trades[1]), time()))
    end
end

replay_source(path::AbstractString; kwargs...) = replay_source([path]; kwargs...)
