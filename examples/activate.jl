"""
Activate and instantiate the examples environment, silently.

    julia -i examples/activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
package itself is consumed by path, so an example always exercises the working
tree.

The examples carry their own environment because a consumer's dependencies are
not the package's: `live_diagnostics.jl` needs `OnlineStats.jl`, on which
MarketTickStreamer does not depend.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
