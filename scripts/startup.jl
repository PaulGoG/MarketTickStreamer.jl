# Silent environment activation — `include` this from any script.
using Pkg
Pkg.activate(joinpath(@__DIR__, ".."); io = devnull)
Pkg.instantiate(; io = devnull)
