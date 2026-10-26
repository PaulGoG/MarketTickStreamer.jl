# Render diagnostic figures + print a session QA report.
#
#   julia scripts/visualize.jl <raw_file.jsonl>... [--out plots] [--format pdf,png]
#   julia scripts/visualize.jl --overview [--symbols A,B] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
#
# Default mode: per-(symbol, day) session figures from raw NDJSON files (all
# raw session files under the configured raw directory when none are given), plus
# the session QA table. --overview mode: multi-day figures per symbol from
# the processed tree (price on trading time, activity heatmap,
# intra-session waiting-time and size CCDFs).

include(joinpath(@__DIR__, "activate.jl"))
using CairoMakie: CairoMakie   # activates the figure extension
using MarketTickStreamer
using Dates: Date
include(joinpath(@__DIR__, "cli.jl"))

function main()
    out_dir = joinpath(@__DIR__, "..", "plots")
    formats = ("pdf", "png")
    symbols = nothing
    from = nothing
    to = nothing
    (; opts, flags, positional) = parse_cli(
        ARGS;
        valued = ("config", "out", "format", "symbols", "from", "to"),
        flags = ("overview",),
    )
    files = positional
    overview = "overview" in flags
    haskey(opts, "out") && (out_dir = opts["out"])
    haskey(opts, "format") && (formats = Tuple(split(opts["format"], ",")))
    haskey(opts, "symbols") && (symbols = String.(split(opts["symbols"], ",")))
    haskey(opts, "from") && (from = Date(opts["from"]))
    haskey(opts, "to") && (to = Date(opts["to"]))
    # The exchange calendar, the regular session and the units belong to the
    # provider.
    cfg = load_config(config_path(opts))
    tz = cfg.exchange_tz
    spec = provider_spec(cfg.provider)
    session = spec.regular_session
    (; price_unit, size_unit) = spec
    if overview
        written = save_overview_figures(
            cfg.processed_dir,
            out_dir;
            symbols,
            from,
            to,
            formats,
            tz,
            session,
            price_unit,
            size_unit,
            non_price = cfg.non_price_conditions,
        )
        println("Wrote $(length(written)) overview figure file(s):")
        foreach(f -> println("  ", f), written)
        return
    end
    if isempty(files)
        isdir(cfg.raw_dir) && (
            files = filter(
                f -> endswith(f, ".jsonl"),
                readdir(cfg.raw_dir; join = true, sort = true),
            )
        )
    end
    isempty(files) && (println("No raw files found."); return)

    println("Session QA report:")
    show(
        session_report(files; non_price = cfg.non_price_conditions);
        allrows = true,
        allcols = true,
    )
    println()

    written = save_session_figures(
        files,
        out_dir;
        formats,
        tz,
        price_unit,
        size_unit,
        non_price = cfg.non_price_conditions,
    )
    println("Wrote $(length(written)) figure file(s):")
    for f in written
        println("  ", f)
    end
end

run_entrypoint(main)
