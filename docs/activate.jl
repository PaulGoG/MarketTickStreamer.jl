"""
Activate and instantiate the documentation environment, silently.

    julia -i docs/activate.jl

Run this way it leaves a REPL with the environment active and its manifest
instantiated; from an existing REPL, `include` it for the same effect. The
package is consumed by path, so the manual always documents the working tree.
`docs/make.jl` performs these same two calls on start-up; this file exists
for interactive work, such as `LiveServer.servedocs()`.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
