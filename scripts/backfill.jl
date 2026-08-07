# Historical trade backfill over [backfill.start_date, backfill.end_date].
#
#   julia scripts/backfill.jl [path/to/config.toml]

include(joinpath(@__DIR__, "startup.jl"))
using MarketTickStreamer

function main()
    cfg = load_config(isempty(ARGS) ? joinpath(@__DIR__, "..", "config", "config.toml") : ARGS[1])
    files = run_backfill(cfg)
    println("Backfill complete — $(length(files)) processed file(s):")
    for f in files
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
