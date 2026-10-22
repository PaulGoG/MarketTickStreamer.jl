"""
Worked consumer: follow a running session from another process.

    julia examples/follow_session.jl [session_prefix] [--config path]

This is the streaming consumer class of the manual's file contract. It is
coupled to the capture through the session's raw files and sidecar only, so
the capture never waits on it, never learns it exists, and keeps running if
it falls behind or dies.

It shows `follow_source` as a drop-in for `live_source` or `replay_source`,
the `snapshots` convention applied to a followed stream (one row per minute of
exchange time and one when the stream ends), and termination by channel close
once the sidecar leaves `running`, with no shutdown protocol of its own.

The same loop run on a finished session reads it whole and stops at its end.
"""

# Activate the examples environment only when this file is run as a script.
# The test suite includes it to check that the example still works, and there
# the environment is already active — activating again would pull it out from
# under the running suite.
if abspath(PROGRAM_FILE) == @__FILE__
    include(joinpath(@__DIR__, "activate.jl"))
end

using MarketTickStreamer
using Printf: @printf

"""
    Vwap()

Single-pass accumulator of print count and volume-weighted average price.
"""
mutable struct Vwap
    n::Int
    value::Float64     # Σ price·size
    volume::Float64    # Σ size
end
Vwap() = Vwap(0, 0.0, 0.0)
observe!(a::Vwap, t::Trade) = (a.n += 1; a.value += t.price * t.size; a.volume += t.size; a)
vwap(a::Vwap) = a.volume > 0 ? a.value / a.volume : NaN

"""
    follow(dir; session = nothing, period_ns = 60 * 10^9, poll_s = 0.5, io = stdout) -> Int

Follow the session's raw files under `dir` and print one row per period of
exchange time — the boundary, the prints folded so far, the running VWAP —
and a final row when the session ends. Returns the number of prints folded.
"""
function follow(
    dir::AbstractString;
    session = nothing,
    period_ns::Integer = 60 * 10^9,
    poll_s::Real = 0.5,
    io::IO = stdout,
)
    acc = Vwap()
    ch = follow_source(dir; session, poll_s)
    on_snapshot(at, n, final) = @printf(
        io,
        "%s  prints=%d  vwap=%.4f%s\n",
        ns_to_rfc3339(at),
        n,
        vwap(acc),
        final ? "  final" : ""
    )
    return snapshots(ch, period_ns; on_snapshot) do t
        observe!(acc, t)
    end
end

function main(args::Vector{String})
    cfg_path = joinpath(@__DIR__, "..", "config", "config.toml")
    session = nothing
    i = 1
    while i <= length(args)
        if args[i] == "--config"
            cfg_path = args[i+1]
            i += 2
        else
            session = args[i]
            i += 1
        end
    end
    cfg = load_config(cfg_path)
    follow(cfg.raw_dir; session)
    return nothing
end

# An `if` block, not `cond && run_entrypoint(...)`: `@__FILE__` would swallow
# the `&&` and everything after it as macro arguments.
if abspath(PROGRAM_FILE) == @__FILE__
    run_entrypoint(() -> main(ARGS))
end
