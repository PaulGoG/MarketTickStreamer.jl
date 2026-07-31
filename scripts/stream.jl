# Live capture session: config-driven, Ctrl-C safe.
#
#   julia --threads=auto scripts/stream.jl [path/to/config.toml]

include(joinpath(@__DIR__, "startup.jl"))
using TickStreamer

function main()
    cfg = load_config(isempty(ARGS) ? joinpath(@__DIR__, "..", "config", "config.toml") : ARGS[1])
    Base.exit_on_sigint(false)      # deliver Ctrl-C as InterruptException for graceful drain
    result = run_stream(cfg)
    println("Captured $(result.ticks) ticks into $(length(result.raw_files)) raw file(s).")
    for f in result.raw_files
        println("  ", f)
    end
end

main()
