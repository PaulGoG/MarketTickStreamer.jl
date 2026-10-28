# Live capture session: config-driven, Ctrl-C safe.
#
#   julia --threads=auto scripts/stream.jl [--config path/to/config.toml]
#   julia --threads=auto scripts/stream.jl [path/to/config.toml]

include(joinpath(@__DIR__, "activate.jl"))
using MarketTickStreamer
include(joinpath(@__DIR__, "cli.jl"))

function main()
    (; opts, positional) = parse_cli(ARGS; valued = ("config",))
    cfg = load_config(config_path(opts, positional))
    result = run_stream(cfg)
    println("Captured $(result.ticks) ticks into $(length(result.raw_files)) raw file(s).")
    for f in result.raw_files
        println("  ", f)
    end
end

run_entrypoint(main)
