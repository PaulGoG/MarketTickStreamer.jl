"""
Activate and instantiate the examples environment, silently.

    julia -i examples/activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
package itself is consumed by path, so an example always exercises the working
tree rather than a registered snapshot.

The examples carry their own environment because a consumer's dependencies are
not the package's: `live_diagnostics.jl` needs `OnlineStats.jl`, which
MarketTickStreamer deliberately does not depend on — it supplies the stream,
not the estimators.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
