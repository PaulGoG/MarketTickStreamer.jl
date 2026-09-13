"""
Worked consumer: waiting-time statistics from a live-like tick stream.

    julia examples/waiting_times.jl data/raw/<session>_part001.jsonl

This is the shape an external analysis pipeline takes against this package.
It consumes a `Channel{Trade}` and never learns whether the ticks came from
the wire or from a recording — swap `replay_source` for `live_source` and the
consumer is unchanged, which is the point of the interface.

What it demonstrates, beyond the arithmetic:

  * `tee` splitting one stream into two independent consumers, and why this
    estimator takes the lossless output rather than the lossy one;
  * termination by channel close, with no shutdown protocol of its own;
  * single-pass accumulation, so memory stays bounded by the state of the
    estimator rather than by the length of the stream;
  * the population choice — every execution, or price-forming prints only —
    made explicitly at the point where it changes the answer.

The estimator itself is deliberately minimal: an external pipeline brings its
own. This file is about the plumbing.
"""

# Activate the project environment only when this file is run as a script.
# The test suite includes it to check that the example still works, and there
# the environment is already active — activating again would pull it out from
# under the running suite.
if abspath(PROGRAM_FILE) == @__FILE__
    include(joinpath(@__DIR__, "..", "scripts", "startup.jl"))
end

using MarketTickStreamer
using Printf: @printf
using Statistics: mean, median

"""
    WaitingTimes()

Single-pass accumulator for inter-arrival times of consecutive prints, in
seconds of exchange time. Holds the gaps themselves so quantiles and a tail
fit stay available; swap in a streaming quantile sketch when the stream is
unbounded.
"""
mutable struct WaitingTimes
    previous_ns::Int64
    gaps::Vector{Float64}
    n_prints::Int
    n_nonpositive::Int
end
WaitingTimes() = WaitingTimes(0, Float64[], 0, 0)

function observe!(w::WaitingTimes, t::Trade)
    w.n_prints += 1
    if w.previous_ns != 0
        Δt = (t.time_ns - w.previous_ns) / 1e9
        # A consolidated tape interleaves venues and reports late prints, so
        # non-positive gaps are normal rather than corrupt. They are counted
        # and excluded: a waiting time of zero or less is not a waiting time.
        Δt > 0 ? push!(w.gaps, Δt) : (w.n_nonpositive += 1)
    end
    w.previous_ns = t.time_ns
    return w
end

"""
    consume(paths; price_forming_only = false, speed = 0.0) -> WaitingTimes

Drain a replayed session through a lossless analysis tap and return the
accumulated waiting times. `speed = 0.0` replays at maximum rate; any
positive value honors the recorded inter-arrival times, compressed by that
factor, which is how a method meant for the live feed is rehearsed offline.
"""
function consume(paths::Vector{String}; price_forming_only::Bool = false, speed::Real = 0.0)
    source =
        speed > 0 ? replay_source(paths; pace = "recorded", speed = speed) :
        replay_source(paths; pace = "max")

    # Both outputs are lossless, and that is the whole point for this
    # estimator. A lossy tap drops ticks when it falls behind, which is the
    # right trade for a live dashboard and precisely the wrong one for a
    # waiting-time distribution: dropping prints does not thin the sample
    # evenly, it removes the bursts, which is where the distribution lives.
    # Measured here at maximum replay rate, a lossy tap lost 85 000 of
    # 1 225 831 prints — a 7 % bite out of exactly the wrong tail.
    #
    # Backpressure is free offline: a blocked output just slows the replay. On
    # a live capture the calculus differs, and an estimator that cannot keep
    # up belongs on a lossy output or, better, offline against the raw files.
    persist, analyse = tee(source, 2; capacity = 10_000, lossy = [false, false])

    counted = Ref(0)
    counter = Threads.@spawn for _ in persist
        counted[] += 1
    end

    w = WaitingTimes()
    # `seen` counts what reached the tap, which is not what the estimator
    # observed once a population filter is applied — comparing the filtered
    # count against the lossless output would report every filtered print as a
    # dropped one.
    seen = 0
    for trade in analyse
        seen += 1
        price_forming_only && !price_forming(trade) && continue
        observe!(w, trade)
    end
    wait(counter)

    seen == counted[] || @warn "analysis tap dropped ticks" seen persisted = counted[]
    return w
end

function report(w::WaitingTimes, label::AbstractString)
    isempty(w.gaps) && (@info "no positive waiting times" label; return nothing)
    g = sort(w.gaps)
    @printf(
        "%-24s n=%9d  mean=%8.4f s  median=%8.4f s  p99=%8.4f s  max=%9.3f s\n",
        label,
        length(g),
        mean(g),
        median(g),
        g[max(1, round(Int, 0.99 * length(g)))],
        g[end]
    )
    return nothing
end

function main(paths::Vector{String})
    isempty(paths) && error("usage: julia examples/waiting_times.jl <raw .jsonl> ...")
    all = consume(paths)
    forming = consume(paths; price_forming_only = true)
    report(all, "every execution")
    report(forming, "price-forming only")
    # The two populations answer different questions; see the manual's
    # "Replay & Analysis Interfaces" page. On a high-priced name the second
    # holds roughly a third of the prints, and its waiting times are longer
    # in proportion.
    @printf(
        "non-positive gaps skipped: %d (late or same-instant prints)\n",
        all.n_nonpositive
    )
    return nothing
end

# Parenthesised rather than `cond && run_entrypoint(...)`: `@__FILE__` would
# swallow the `&&` and everything after it as macro arguments.
if abspath(PROGRAM_FILE) == @__FILE__
    run_entrypoint(() -> main(ARGS))
end
