# Inventory of the processed tree: per symbol, the days held, their span, the
# print count and the bytes on disk.
#
#   julia scripts/inventory.jl [--config path/to/config.toml]

include(joinpath(@__DIR__, "activate.jl"))
using MarketTickStreamer

function main()
    cfg_path = joinpath(@__DIR__, "..", "config", "config.toml")
    i = 1
    while i <= length(ARGS)
        if ARGS[i] == "--config"
            cfg_path = ARGS[i+1]
            i += 2
        else
            error("unknown argument: $(ARGS[i])")
        end
    end
    cfg = load_config(cfg_path)
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
