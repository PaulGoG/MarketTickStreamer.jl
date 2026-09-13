# Shared script prelude: silent environment activation — `include` this from
# any script, then `using MarketTickStreamer` for `run_entrypoint`.
using Pkg
Pkg.activate(joinpath(@__DIR__, ".."); io = devnull)
Pkg.instantiate(; io = devnull)
