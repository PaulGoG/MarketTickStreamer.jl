# Render per-symbol per-day diagnostic figures + print a session QA report.
#
#   julia scripts/visualize.jl <raw_file.jsonl>... [--out plots] [--format pdf,png]
#
# With no files given, all raw session files under the configured data dir
# are used. Figures: price path, trades/min, inter-arrival CCDF, size CCDF.

include(joinpath(@__DIR__, "startup.jl"))
using MarketTickStreamer

function main()
    cfg_path = joinpath(@__DIR__, "..", "config", "config.toml")
    out_dir = joinpath(@__DIR__, "..", "plots")
    formats = ("pdf", "png")
    files = String[]
    i = 1
    while i <= length(ARGS)
        if ARGS[i] == "--config"
            cfg_path = ARGS[i + 1]; i += 2
        elseif ARGS[i] == "--out"
            out_dir = ARGS[i + 1]; i += 2
        elseif ARGS[i] == "--format"
            formats = Tuple(split(ARGS[i + 1], ",")); i += 2
        else
            push!(files, ARGS[i]); i += 1
        end
    end
    if isempty(files)
        cfg = load_config(cfg_path)
        isdir(cfg.raw_dir) &&
            (files = filter(f -> endswith(f, ".jsonl"), readdir(cfg.raw_dir; join = true, sort = true)))
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

# Ctrl-C arrives as InterruptException (best-effort under threads; the
# crash-only session lifecycle is the real guarantee) and exits cleanly.
Base.exit_on_sigint(false)
try
    main()
catch e
    e isa InterruptException || rethrow()
    println("\nInterrupted — exiting cleanly.")
end
