"""
Activate and instantiate the test environment, silently.

    julia -i test/activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
package is consumed by path, so the suite always runs against the working
tree. `Pkg.test()` builds the same environment on its own; this file exists
for interactive work on individual test sets.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
