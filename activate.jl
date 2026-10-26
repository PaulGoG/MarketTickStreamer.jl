"""
Activate and instantiate the package environment, silently.

    julia -i activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
first instantiation resolves and precompiles and is therefore slow;
afterwards it is a no-op.

Each side environment (`scripts/`, `test/`, `docs/`, `bench/`,
`examples/`) has its own `activate.jl`, which its scripts include first.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
