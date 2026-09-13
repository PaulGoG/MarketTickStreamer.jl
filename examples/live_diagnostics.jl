"""
Worked consumer: bounded-memory live diagnostics from a tick stream.

    julia examples/live_diagnostics.jl data/raw/<session>_part001.jsonl

The companion to `waiting_times.jl`, and its opposite on both axes that matter
when a consumer runs beside a live capture rather than after one.

`waiting_times.jl` takes the **lossless** tap and keeps **every** gap, because
a distribution estimate must not be thinned by load. This file takes the
**lossy** tap and keeps **no** samples at all: state is a fixed set of
`OnlineStats` accumulators, so memory is constant in the length of the stream
and a slow consumer sheds prints instead of applying backpressure to
acquisition. That is the right trade for a dashboard and the wrong one for an
estimator, which is the whole point of having both files.

What it demonstrates:

  * single-pass accumulators whose footprint does not grow with the stream —
    `Mean`, `Variance`, `Extrema`, `KHist`;
  * why a diagnostic belongs on a lossy output: acquisition must never wait
    for a consumer, and a dropped print costs a dashboard nothing;
  * a size-weighted quantity (VWAP) accumulated as two running sums rather
    than as a ratio of means, which is not the same number;
  * drops reported rather than hidden, so a diagnostic that is quietly seeing
    a third of the tape cannot be mistaken for one that is seeing all of it;
  * and, below, that the choice of quantile sketch is not a detail.

## Not every constant-memory quantile survives this distribution

The obvious sketch is `P2Quantile`, and on inter-arrival times it is wrong by
more than an order of magnitude. Measured on one AAPL session (1 225 830
positive gaps) against exact order statistics:

| τ     | exact      | P² on Δt     | P² on log₁₀Δt | KHist(200) on log₁₀Δt |
|-------|------------|--------------|---------------|-----------------------|
| 0.5   | 0.000661 s | 0.001742 s   | 0.000677 s    | 0.000647 s            |
| 0.9   | 0.071364 s | 2.181328 s   | 0.760613 s    | 0.073208 s            |
| 0.99  | 0.623098 s | 8.158618 s   | 7.004436 s    | 0.596842 s            |
| 0.999 | 5.511254 s | 8.048441 s   | 5.276499 s    | 5.199836 s            |

P² assumes a smooth, roughly unimodal density and adapts five markers to it.
Waiting times on a consolidated tape span six decades with most of the mass in
the first millisecond, so the markers sit in the wrong place and the estimate
is off by 31x at the ninth decile. Taking logs first fixes the median and
still fails at the shoulder. A histogram binned in log₁₀ tracks every quantile
here to within a few percent, for 100 kB of state rather than 280 bytes —
constant either way, which is the property that matters.

The general lesson: a bounded-memory sketch encodes an assumption about the
distribution's shape, and an arrival process on a market tape violates the
usual one. Check a sketch against exact statistics on a real sample before
trusting it, because it fails quietly and returns a plausible number.

On reservoir sampling: `StreamSampling.jl` attaches to a `tee` branch the same
way, but buys little here. The raw layer already retains every print, so an
offline analysis can simply read it, and uniform reservoir sampling destroys
the arrival ordering that waiting-time, long-memory and criticality methods
consume. Where it does earn its place is a live tail sketch over a multi-hour
session, and there a weighted (exponential-jump) scheme beats a uniform one,
because the tail is exactly what uniform sampling under-represents. The
deterministic sketches below cover the rest of the live diagnostic need.

The estimators are deliberately ordinary: an external pipeline brings its own.
This file is about the plumbing and the memory discipline.
"""

# Activate the examples environment only when this file is run as a script.
# The test suite includes it to check that the example still works, and there
# the environment is already active — activating again would pull it out from
# under the running suite.
if abspath(PROGRAM_FILE) == @__FILE__
    include(joinpath(@__DIR__, "activate.jl"))
end

using MarketTickStreamer
using OnlineStats: Extrema, KHist, Mean, Sum, Variance, fit!, nobs, value
using Printf: @printf

# Concrete field types taken from the constructors rather than spelled out:
# OnlineStats parameterises these on the weighting scheme and element type
# (`Mean{Float64,EqualWeight}`), and writing a bare `Mean` would leave the
# field abstract and the accumulation loop dynamically dispatched.
const MeanStat = typeof(Mean())
const VarianceStat = typeof(Variance())
const ExtremaStat = typeof(Extrema(Float64))
const HistStat = typeof(KHist(3))     # the type is independent of the bin count
const SumStat = typeof(Sum(Float64))

"""
    LiveDiagnostics(; n_bins = 200)

Fixed-footprint accumulator set for a tick stream: inter-arrival moments and
quantiles, a log-binned sketch of the waiting-time distribution, and the two
running sums that make a VWAP.

Every accumulator is of constant size, so the whole struct costs the same
after ten prints as after ten million. `n_bins` fixes the histogram's
resolution, and with it the only tunable part of that cost.
"""
mutable struct LiveDiagnostics
    Δt_mean::MeanStat
    Δt_var::VarianceStat
    Δt_extrema::ExtremaStat
    Δt_log_hist::HistStat        # bins over log10(Δt) — see the header
    notional::SumStat            # Σ price × size
    volume::SumStat              # Σ size
    previous_ns::Int64
    first_ns::Int64
    n_nonpositive::Int
end

function LiveDiagnostics(; n_bins::Int = 200)
    # KHist itself rejects ≤ 2 with a bare `error`; check here so the message
    # names the argument the caller actually passed.
    n_bins > 2 || throw(ArgumentError("n_bins must exceed 2, got $(n_bins)"))
    return LiveDiagnostics(
        Mean(),
        Variance(),
        Extrema(Float64),
        KHist(n_bins),
        Sum(Float64),
        Sum(Float64),
        0,
        0,
        0,
    )
end

"""
    observe!(d::LiveDiagnostics, t::Trade) -> d

Fold one print into the accumulators. Allocates nothing that survives the
call, which is what lets this run beside a live capture.
"""
function observe!(d::LiveDiagnostics, t::Trade)
    fit!(d.notional, t.price * t.size)
    fit!(d.volume, t.size)
    d.first_ns == 0 && (d.first_ns = t.time_ns)
    if d.previous_ns != 0
        Δt = (t.time_ns - d.previous_ns) / 1e9
        # A consolidated tape interleaves venues and reports late prints, so
        # non-positive gaps are normal rather than corrupt, and are counted
        # instead of folded in: a waiting time of zero or less is not one.
        if Δt > 0
            fit!(d.Δt_mean, Δt)
            fit!(d.Δt_var, Δt)
            fit!(d.Δt_extrema, Δt)
            # Binned in log10: the gaps span six decades, and a sketch that
            # bins them linearly puts almost every observation in one bin.
            fit!(d.Δt_log_hist, log10(Δt))
        else
            d.n_nonpositive += 1
        end
    end
    d.previous_ns = t.time_ns
    return d
end

"""
    waiting_quantile(d::LiveDiagnostics, τ) -> Float64

Approximate the `τ`-quantile of the waiting-time distribution from the
log-binned histogram, in seconds. Returns `NaN` before anything is observed.

Accurate to a few percent on a real session (see the header table); the
returned value is a bin centre, so it cannot be more precise than the bin
width, and that is the point — an exact order statistic needs the whole
sample.
"""
function waiting_quantile(d::LiveDiagnostics, τ::Real)
    0 < τ < 1 || throw(ArgumentError("τ must lie in (0, 1), got $(τ)"))
    h = value(d.Δt_log_hist)
    total = sum(h.counts)
    total == 0 && return NaN
    acc = 0
    for i in eachindex(h.centers)
        acc += h.counts[i]
        acc >= τ * total && return 10^h.centers[i]
    end
    return 10^h.centers[end]
end

"""
    vwap(d::LiveDiagnostics) -> Float64

Volume-weighted average price, `Σ(price × size) / Σ size`. Accumulated as two
sums on purpose: the ratio of the running means is a different number, and
the mean of the per-print ratios is a third one.
"""
vwap(d::LiveDiagnostics) = value(d.volume) == 0 ? NaN : value(d.notional) / value(d.volume)

"""
    print_rate(d::LiveDiagnostics) -> Float64

Prints per second of exchange time over the span observed so far, or `NaN`
before the span is positive.
"""
function print_rate(d::LiveDiagnostics)
    span = (d.previous_ns - d.first_ns) / 1e9
    return span > 0 ? (nobs(d.Δt_mean) + 1) / span : NaN
end

"""
    consume(paths; n_bins = 200, capacity = 10_000, speed = 0.0)
        -> (diagnostics, n_persisted, n_seen)

Drain a replayed session through a **lossy** diagnostic tap beside a lossless
persistence tap, and return the accumulators together with the counts needed
to report how much the diagnostic actually saw.

`speed = 0.0` replays at maximum rate, which is the condition under which a
lossy tap does drop. Any positive value honors the recorded inter-arrival
times compressed by that factor.
"""
function consume(
    paths::Vector{String};
    n_bins::Int = 200,
    capacity::Int = 10_000,
    speed::Real = 0.0,
)
    source =
        speed > 0 ? replay_source(paths; pace = "recorded", speed = speed) :
        replay_source(paths; pace = "max")

    # Persistence is lossless and always wins; the diagnostic is lossy and
    # yields. Reversing this would let a slow dashboard throttle acquisition,
    # which is the failure a live capture cannot afford.
    persist, diagnose = tee(source, 2; capacity = capacity, lossy = [false, true])

    persisted = Ref(0)
    counter = Threads.@spawn for _ in persist
        persisted[] += 1
    end

    d = LiveDiagnostics(; n_bins = n_bins)
    seen = 0
    for trade in diagnose
        seen += 1
        observe!(d, trade)
    end
    wait(counter)
    return d, persisted[], seen
end

function report(d::LiveDiagnostics, persisted::Integer, seen::Integer)
    dropped = persisted - seen
    pct = persisted == 0 ? 0.0 : 100 * dropped / persisted
    @printf(
        "prints persisted: %9d   seen by diagnostic: %9d   dropped: %d (%.2f %%)\n",
        persisted,
        seen,
        dropped,
        pct
    )
    if nobs(d.Δt_mean) == 0
        println("no positive waiting times observed")
        return nothing
    end
    ex = value(d.Δt_extrema)
    @printf(
        "Δt  mean=%.6f s  sd=%.6f s  min=%.6f s  max=%.3f s\n",
        value(d.Δt_mean),
        sqrt(value(d.Δt_var)),
        ex.min,
        ex.max
    )
    # The quantiles carry ≈ because they are bin centres of a sketch, not
    # order statistics of a retained sample.
    @printf(
        "Δt  median≈%.6f s  p90≈%.6f s  p99≈%.6f s  p99.9≈%.4f s\n",
        waiting_quantile(d, 0.5),
        waiting_quantile(d, 0.9),
        waiting_quantile(d, 0.99),
        waiting_quantile(d, 0.999)
    )
    @printf(
        "VWAP=%.4f   volume=%.0f   print rate=%.1f s⁻¹\n",
        vwap(d),
        value(d.volume),
        print_rate(d)
    )
    @printf(
        "accumulator footprint: %d bytes, independent of stream length\n",
        Base.summarysize(d)
    )
    return nothing
end

function main(paths::Vector{String})
    isempty(paths) && error("usage: julia examples/live_diagnostics.jl <raw .jsonl> ...")
    d, persisted, seen = consume(paths)
    report(d, persisted, seen)
    return nothing
end

# Parenthesised rather than `cond && run_entrypoint(...)`: `@__FILE__` would
# swallow the `&&` and everything after it as macro arguments.
if abspath(PROGRAM_FILE) == @__FILE__
    run_entrypoint(() -> main(ARGS))
end
