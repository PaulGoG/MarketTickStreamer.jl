# Compact raw NDJSON session files into per-symbol per-day analysis files.
#
#   julia scripts/compact.jl <raw_file.jsonl>... [--config path/to/config.toml]

include(joinpath(@__DIR__, "activate.jl"))
using MarketTickStreamer
include(joinpath(@__DIR__, "cli.jl"))

function main()
    (; opts, positional) = parse_cli(ARGS; valued = ("config",))
    cfg = load_config(config_path(opts))
    files = positional
    # Default: every .jsonl under the configured raw directory, whatever the provider.
    if isempty(files)
        files = filter(
            f -> endswith(f, ".jsonl"),
            readdir(cfg.raw_dir; join = true, sort = true),
        )
    end
    isempty(files) && (println("No raw files to compact."); return)
    written = compact_raw(cfg, files)
    println("Wrote $(length(written)) processed file(s):")
    for f in written
        println("  ", f)
    end
end

run_entrypoint(main)
