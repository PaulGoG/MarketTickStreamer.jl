# Compact raw NDJSON session files into per-symbol per-day analysis files.
#
#   julia scripts/compact.jl <raw_file.jsonl>... [--config path/to/config.toml]

include(joinpath(@__DIR__, "..", "activate.jl"))
using MarketTickStreamer

function main()
    cfg_path = joinpath(@__DIR__, "..", "config", "config.toml")
    files = String[]
    i = 1
    while i <= length(ARGS)
        if ARGS[i] == "--config"
            cfg_path = ARGS[i+1]
            i += 2
        else
            push!(files, ARGS[i])
            i += 1
        end
    end
    cfg = load_config(cfg_path)
    if isempty(files)   # default: every raw file of the configured provider
        files = filter(
            f -> endswith(f, ".jsonl"),
            readdir(cfg.raw_dir; join = true, sort = true),
        )
    end
    isempty(files) && (println("No raw files to compact."); return)
    written = compact_raw(files, cfg.processed_dir; format = cfg.processed_format)
    println("Wrote $(length(written)) processed file(s):")
    for f in written
        println("  ", f)
    end
end

run_entrypoint(main)
