# Historical trade backfill over [backfill.start_date, backfill.end_date].
#
#   julia scripts/backfill.jl [--config path/to/config.toml]
#   julia scripts/backfill.jl [path/to/config.toml]

include(joinpath(@__DIR__, "activate.jl"))
using MarketTickStreamer
include(joinpath(@__DIR__, "cli.jl"))

function main()
    (; opts, positional) = parse_cli(ARGS; valued = ("config",))
    cfg = load_config(config_path(opts, positional))
    files = run_backfill(cfg)
    println("Backfill complete; $(length(files)) processed file(s):")
    for f in files
        println("  ", f)
    end
end

run_entrypoint(main)
