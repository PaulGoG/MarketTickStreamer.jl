"""
Activate and instantiate the benchmark environment, silently.

    julia -i bench/activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
package itself is consumed by path, so the benchmarks always measure the
working tree rather than a registered snapshot.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
