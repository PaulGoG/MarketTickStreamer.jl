# Import one vendor day file — a Nasdaq TotalView-ITCH 5.0 day or an IEX HIST
# capture — into the processed tree.
#
#   julia scripts/import.jl <itch50|iex> <file> [--config=path/to/config.toml]
#         [--date=YYYY-MM-DD] [--symbols=A,B,C | --all] [--broken=drop|mark|keep]
#         [--tape=X] [--drop-raw]

include(joinpath(@__DIR__, "activate.jl"))
using Dates: Date
using MarketTickStreamer

function parse_args(args)
    length(args) >= 2 ||
        error("usage: julia scripts/import.jl <itch50|iex> <file> [options]")
    opts = Dict{String,String}()
    for a in args[3:end]
        startswith(a, "--") || error("unexpected argument: $a")
        k, v = occursin('=', a) ? split(a[3:end], '='; limit = 2) : (a[3:end], "")
        opts[String(k)] = String(v)
    end
    return args[1], args[2], opts
end

function main()
    format, file, opts = parse_args(ARGS)
    cfg =
        load_config(get(opts, "config", joinpath(@__DIR__, "..", "config", "config.toml")))
    symbols =
        haskey(opts, "all") ? nothing :
        haskey(opts, "symbols") ? String.(split(opts["symbols"], ',')) : cfg.symbols
    files = run_import(
        cfg,
        file;
        format,
        date = haskey(opts, "date") ? Date(opts["date"]) : nothing,
        symbols,
        broken = Symbol(get(opts, "broken", "drop")),
        tape = get(opts, "tape", ""),
        keep_raw = !haskey(opts, "drop-raw"),
    )
    println("Import complete; $(length(files)) processed file(s):")
    for f in files
        println("  ", f)
    end
end

run_entrypoint(main)
