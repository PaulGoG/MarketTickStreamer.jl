# Render diagnostic figures + print a session QA report.
#
#   julia scripts/visualize.jl <raw_file.jsonl>... [--out plots] [--format pdf,png]
#   julia scripts/visualize.jl --overview [--symbols A,B] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
#
# Default mode: per-(symbol, day) session figures from raw NDJSON files (all
# raw session files under the configured data dir when none are given), plus
# the session QA table. --overview mode: multi-day figures per symbol from
# the processed tree (price on trading time, activity heatmap,
# intra-session waiting-time and size CCDFs).

include(joinpath(@__DIR__, "..", "activate.jl"))
using MarketTickStreamer

function main()
    cfg_path = joinpath(@__DIR__, "..", "config", "config.toml")
    out_dir = joinpath(@__DIR__, "..", "plots")
    formats = ("pdf", "png")
    files = String[]
    overview = false
    symbols = nothing
    from = nothing
    to = nothing
    i = 1
    while i <= length(ARGS)
        if ARGS[i] == "--config"
            cfg_path = ARGS[i+1]
            i += 2
        elseif ARGS[i] == "--out"
            out_dir = ARGS[i+1]
            i += 2
        elseif ARGS[i] == "--format"
            formats = Tuple(split(ARGS[i+1], ","))
            i += 2
        elseif ARGS[i] == "--overview"
            overview = true
            i += 1
        elseif ARGS[i] == "--symbols"
            symbols = String.(split(ARGS[i+1], ","))
            i += 2
        elseif ARGS[i] == "--from"
            from = Date(ARGS[i+1])
            i += 2
        elseif ARGS[i] == "--to"
            to = Date(ARGS[i+1])
            i += 2
        else
            push!(files, ARGS[i])
            i += 1
        end
    end
    if overview
        cfg = load_config(cfg_path)
        written =
            save_overview_figures(cfg.processed_dir, out_dir; symbols, from, to, formats)
        println("Wrote $(length(written)) overview figure file(s):")
        foreach(f -> println("  ", f), written)
        return
    end
    if isempty(files)
        cfg = load_config(cfg_path)
        isdir(cfg.raw_dir) && (
            files = filter(
                f -> endswith(f, ".jsonl"),
                readdir(cfg.raw_dir; join = true, sort = true),
            )
        )
    end
    isempty(files) && (println("No raw files found."); return)

    println("Session QA report:")
    show(session_report(files); allrows = true, allcols = true)
    println()

    written = save_session_figures(files, out_dir; formats)
    println("Wrote $(length(written)) figure file(s):")
    for f in written
        println("  ", f)
    end
end

run_entrypoint(main)
