# Live capture session: config-driven, Ctrl-C safe.
#
#   julia --threads=auto scripts/stream.jl [path/to/config.toml]

include(joinpath(@__DIR__, "startup.jl"))
using MarketTickStreamer

function main()
    cfg = load_config(isempty(ARGS) ? joinpath(@__DIR__, "..", "config", "config.toml") : ARGS[1])
    result = run_stream(cfg)
    println("Captured $(result.ticks) ticks into $(length(result.raw_files)) raw file(s).")
    for f in result.raw_files
        println("  ", f)
    end
end

# Ctrl-C arrives as InterruptException (best-effort under threads; the
# crash-only session lifecycle is the real guarantee) and exits cleanly.
Base.exit_on_sigint(false)
try
    main()
catch e
    e isa InterruptException || rethrow()
    println("\nInterrupted — exiting cleanly.")
end
