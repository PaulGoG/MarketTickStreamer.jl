"""
Activate and instantiate the documentation environment, silently.

    julia -i docs/activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
package is consumed by path, so the manual always documents the working tree.
`docs/make.jl` includes this file first; on its own it serves interactive
work. Preview with `LiveServer.servedocs()` from an
environment that has LiveServer.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
