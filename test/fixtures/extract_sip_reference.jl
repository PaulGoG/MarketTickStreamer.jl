# Write the consolidated tape's record of the golden fixtures' prints to a local
# reference file: for every print the readers decode from the fixtures, the SIP
# print on the same venue (IEX `V`, Nasdaq `Q`) with equal price and size and
# the nearest time. The SIP prints come from your own processed tree (an
# Alpaca backfill with `feed = sip`); Alpaca's terms forbid redistributing
# them, so the output is gitignored and the comparison runs only where it
# exists. The ITCH section is written when the ITCH excerpt is present.
#
#   julia test/fixtures/extract_sip_reference.jl <processed AAPL 2025-12-09 file> <output>
#
# Not run by the suite. See PROVENANCE.md.

include(joinpath(@__DIR__, "..", "activate.jl"))
using MarketTickStreamer
using Dates: Date
using TOML: TOML

function matched(prints, sip, venue)
    pool = filter(s -> s.exchange == venue, sip)
    rows = Dict{String,Any}(
        "time_ns" => Int64[],
        "price" => Float64[],
        "size" => Float64[],
        "flags" => String[],
    )
    for t in prints
        c = filter(s -> s.price == t.price && s.size == t.size, pool)
        isempty(c) && error("no SIP print on $venue matches $(t)")
        s = c[argmin(abs.(getfield.(c, :time_ns) .- t.time_ns))]
        push!(rows["time_ns"], s.time_ns)
        push!(rows["price"], s.price)
        push!(rows["size"], s.size)
        push!(rows["flags"], join(filter(x -> x in ("I", "T", "O", "6"), s.conditions)))
    end
    return rows
end

function main()
    length(ARGS) == 2 ||
        error("usage: extract_sip_reference.jl <processed AAPL 2025-12-09 file> <output>")
    sip = read_processed([ARGS[1]])
    out = Dict{String,Any}()
    iex =
        iex_hist_trades(joinpath(@__DIR__, "iex_AAPL_20251209.pcapng"); symbols = ["AAPL"])
    out["iex"] = matched(iex.trades, sip, "V")
    itch = joinpath(@__DIR__, "itch50_AAPL_20251209.bin")
    if isfile(itch)
        r = itch50_trades(itch, Date(2025, 12, 9); symbols = ["AAPL"])
        out["itch"] = matched(r.trades, sip, "Q")
    end
    open(io -> TOML.print(io, out), ARGS[2], "w")
    println("wrote ", join(sort!(collect(keys(out))), ", "), " to ", ARGS[2])
end

abspath(PROGRAM_FILE) == (@__FILE__) && main()
