"""
Activate and instantiate the scripts environment, silently.

    julia -i scripts/activate.jl

Every script in this directory includes this file as its first statement, so
`julia scripts/stream.jl` needs no preparation. The package is consumed by
path, so a script always runs the working tree.

The scripts carry their own environment because the package has no plotting
dependency: `visualize.jl` needs CairoMakie and `monitor.jl` draws with
UnicodePlots, and loading either here is what activates the corresponding
package extension.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)
