# Inventory of the processed tree: per symbol, the days held, their span, the
# print count and the bytes on disk.
#
#   julia scripts/inventory.jl [--config path/to/config.toml]

include(joinpath(@__DIR__, "activate.jl"))
using MarketTickStreamer
include(joinpath(@__DIR__, "cli.jl"))

function main()
    (; opts, positional) = parse_cli(ARGS; valued = ("config",))
    isempty(positional) || throw(ArgumentError("unexpected argument: $(first(positional))"))
    cfg = load_config(config_path(opts))
    inventory = corpus_inventory(cfg)
    if size(inventory, 1) == 0
        println("No processed data under $(cfg.processed_dir).")
        return
    end
    show(
        stdout,
        MIME("text/plain"),
        inventory;
        allrows = true,
        allcols = true,
        summary = false,
    )
    println()
    println(
        "$(size(inventory, 1)) symbol(s), $(sum(inventory.n_days)) symbol-days, " *
        "$(sum(inventory.n_prints)) prints, " *
        "$(round(sum(inventory.bytes) / 2^30; digits = 2)) GiB",
    )
end

run_entrypoint(main)
