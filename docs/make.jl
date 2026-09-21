using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)

using Documenter
using MarketTickStreamer

makedocs(
    doctest = true,
    sitename = "MarketTickStreamer",
    authors = "Paul-Adrian Gogîță",
    repo = Documenter.Remotes.GitHub("PaulGoG", "MarketTickStreamer.jl"),
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
        canonical = "https://PaulGoG.github.io/MarketTickStreamer.jl/stable/",
    ),
    modules = [MarketTickStreamer],
    pages = [
        "Home" => "index.md",
        "Architecture" => "architecture.md",
        "Usage & Configuration" => "usage.md",
        "Replay & Analysis Interfaces" => "replay.md",
        "API Reference" => "api.md",
    ],
)

# The docs job pushes the build to the `gh-pages` branch: `dev` from `main`,
# `stable` and `vX.Y.Z` from version tags. Outside CI, or without a token,
# Documenter skips the deployment and the local build stays in `docs/build/`.
deploydocs(
    repo = "github.com/PaulGoG/MarketTickStreamer.jl",
    devbranch = "main",
    push_preview = false,
)
