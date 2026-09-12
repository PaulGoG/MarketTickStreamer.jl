"""
Activate and instantiate the package environment, silently.

    julia -i activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
first instantiation resolves and precompiles and is therefore slow;
afterwards it is a no-op.

Every script under `scripts/` performs these same two calls on start-up via
`scripts/startup.jl`, so running an entry point directly needs no
preparation. This file exists for interactive work.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
