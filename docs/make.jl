include(joinpath(@__DIR__, "activate.jl"))

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
        "Files & Provenance" => "files.md",
        "Unattended Operation" => "deployment.md",
        "API Reference" => [
            "Acquisition and storage" => "api.md",
            "Providers" => "api_providers.md",
            "Analysis interfaces" => "api_analysis.md",
        ],
    ],
)

# The docs job pushes the build to the `gh-pages` branch: `dev` from `main`,
# `stable` and `vX.Y.Z` from version tags. Outside CI, or without a token,
# Documenter skips the deployment and the local build stays in `docs/build/`.
#
# A release pushes a commit and its tag together, so two docs jobs deploy at
# once and the second push to `gh-pages` is rejected as non-fast-forward.
# `deploydocs` fetches `gh-pages` afresh on every call, so a retry lands on top
# of the other job's deployment.
const DEPLOY_ATTEMPTS = 3
for attempt in 1:DEPLOY_ATTEMPTS
    try
        deploydocs(
            repo = "github.com/PaulGoG/MarketTickStreamer.jl",
            devbranch = "main",
            push_preview = false,
        )
        break
    catch err
        attempt == DEPLOY_ATTEMPTS && rethrow()
        @warn "Documentation deployment failed (attempt $attempt of $DEPLOY_ATTEMPTS); retrying" exception =
            err
        sleep(30 * attempt)
    end
end
