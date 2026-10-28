# Import one vendor day file — a Nasdaq TotalView-ITCH 5.0 day or an IEX HIST
# capture — into the processed tree.
#
#   julia scripts/import.jl <itch50|iex> <file> [--config=path/to/config.toml]
#         [--date=YYYY-MM-DD] [--symbols=A,B,C | --all] [--broken=drop|mark|keep]
#         [--tape=X] [--drop-raw]

include(joinpath(@__DIR__, "activate.jl"))
using Dates: Date
using MarketTickStreamer
include(joinpath(@__DIR__, "cli.jl"))

function main()
    (; opts, flags, positional) = parse_cli(
        ARGS;
        valued = ("config", "date", "symbols", "broken", "tape"),
        flags = ("all", "drop-raw"),
    )
    length(positional) == 2 ||
        throw(ArgumentError("usage: julia scripts/import.jl <itch50|iex> <file> [options]"))
    format, file = Symbol(positional[1]), positional[2]
    cfg = load_config(config_path(opts))
    symbols =
        "all" in flags ? nothing :
        haskey(opts, "symbols") ? String.(split(opts["symbols"], ',')) : cfg.symbols
    files = run_import(
        cfg,
        file;
        format,
        date = haskey(opts, "date") ? Date(opts["date"]) : nothing,
        symbols,
        broken = Symbol(get(opts, "broken", "drop")),
        tape = get(opts, "tape", ""),
        keep_raw = !("drop-raw" in flags),
    )
    println("Import complete; $(length(files)) processed file(s):")
    for f in files
        println("  ", f)
    end
end

run_entrypoint(main)
