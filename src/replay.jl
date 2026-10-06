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

# Absolute schedule of a paced replay. It is anchored on the first print, as
# the pair (recorded stamp, wall clock), and outlives the batch, so a
# recording streamed day by day keeps one schedule across its days. A gap on
# the replay clock longer than `max_gap_ns` is shortened to it, and
# `skipped_ns` holds the recorded time removed so far: the schedule stays
# absolute, and every shorter gap keeps its recorded length.
mutable struct ReplaySchedule
    const speed::Float64
    const max_gap_ns::Int64
    anchored::Bool
    t0_rec::Int64
    t0_wall::Float64
    last_rec::Int64
    skipped_ns::Int64
end

# 9e18 ns is about 285 years: no recorded gap reaches it, and `Inf` maps on it.
ReplaySchedule(speed::Float64, max_gap_s::Float64) = ReplaySchedule(
    speed,
    round(Int64, min(max_gap_s * NS_PER_SEC, 9.0e18)),
    false,
    0,
    0.0,
    0,
    0,
)

# Wall-clock instant at which the print stamped `s` is due.
function _due!(sch::ReplaySchedule, s::Int64)
    if !sch.anchored
        sch.anchored = true
        sch.t0_rec = s
        sch.t0_wall = time()
        sch.last_rec = s
    end
    gap = s - sch.last_rec
    gap > sch.max_gap_ns && (sch.skipped_ns += gap - sch.max_gap_ns)
    sch.last_rec = s
    return sch.t0_wall + (s - sch.t0_rec - sch.skipped_ns) / NS_PER_SEC / sch.speed
end

# `sch === nothing` emits unpaced.
function _emit_paced!(
    ch::Channel{Trade},
    trades::Vector{Trade},
    stamp,
    sch::Union{Nothing,ReplaySchedule},
)
    for t in trades
        if sch !== nothing
            wait_s = _due!(sch, stamp(t)) - time()
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
    sch::Union{Nothing,ReplaySchedule},
)
    resolved = ""
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
        _emit_paced!(ch, trades, stamp, sch)
    end
    return nothing
end

"""
    replay_source(paths; pace = "recorded", speed = 1.0, max_gap_s = Inf,
                  clock = "auto", capacity = 100_000) -> Channel{Trade}

Create a channel that replays recorded trades, ordered by the replay clock.
`paths` are either raw NDJSON files (`.jsonl`) or processed day files
(`YYYY-MM-DD.csv|.arrow`, e.g. from [`processed_files`](@ref)); the two kinds
cannot be mixed.

- Raw files are read whole and sorted, since arrival order is not
  chronological across venues and parts. This is the mode for a session.
- Processed files are **streamed one trading day at a time**: the files of a
  date — one per symbol — are loaded, merged on the replay clock and emitted
  before the next date is opened. Memory is bounded by the busiest date
  summed over the requested symbols, about 100 B per print: the busiest AAPL
  day of a one-year capture (2.2 M prints) holds 215 MiB. This is the mode
  for a corpus. Recorded pace runs on one schedule across the days, closures
  included: replaying a week at `speed = 1` takes a week unless `max_gap_s`
  shortens the closures.

- `pace = "recorded"`: honor the original inter-arrival times of the replay
  clock against an absolute schedule, compressed by `speed` (e.g.
  `speed = 60.0` replays an hour in a minute), so the timing error does not
  accumulate over a session.
- `pace = "max"`: emit as fast as the consumer takes them (throughput mode).

`max_gap_s` caps every gap on the replay clock, in recorded seconds before
`speed` applies: a longer gap is replayed as `max_gap_s`, and timing within
the session is untouched. The default `Inf` replays every gap. It needs no
notion of a market closure, so it serves a venue that trades around the clock
as well. It has no effect at `pace = "max"`.

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
    max_gap_s::Real = Inf,
    clock::AbstractString = "auto",
    capacity::Integer = 100_000,
)
    pace in ("recorded", "max") ||
        throw(ArgumentError("pace must be \"recorded\" or \"max\""))
    speed > 0 || throw(ArgumentError("speed must be positive"))
    max_gap_s > 0 || throw(ArgumentError("max_gap_s must be positive, got $max_gap_s"))
    clock in ("auto", "recv", "exchange") ||
        throw(ArgumentError("clock must be \"auto\", \"recv\" or \"exchange\""))
    # One fresh schedule per replay, anchored on its first emitted print.
    new_schedule() =
        pace == "recorded" ? ReplaySchedule(Float64(speed), Float64(max_gap_s)) : nothing
    n_processed = count(_is_processed, paths)
    if n_processed > 0
        n_processed == length(paths) ||
            throw(ArgumentError("raw and processed files cannot be replayed together"))
        days = _files_by_date(paths)
        foreach(f -> isfile(f) || throw(ArgumentError("no such file: $f")), paths)
        return Channel{Trade}(capacity; spawn = true) do ch
            _replay_processed!(ch, days, clock, new_schedule())
        end
    end
    trades = read_raw(paths)
    resolved = _replay_clock(trades, clock)
    stamp = resolved == "recv" ? _recv_stamp : _exchange_stamp
    sort!(trades; by = stamp, alg = MergeSort)
    return Channel{Trade}(capacity; spawn = true) do ch
        _emit_paced!(ch, trades, stamp, new_schedule())
    end
end

replay_source(path::AbstractString; kwargs...) = replay_source([path]; kwargs...)
