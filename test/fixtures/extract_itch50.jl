# Cut the golden ITCH 5.0 fixture from a downloaded Nasdaq day file: every
# System Event, the symbol's Stock Directory message and its crosses with
# shares, its first ten displayed executions (E) and five printable
# executions with price (C), each with the Add Order it executes, and its
# first five hidden trades (P). The messages keep their BinaryFILE length
# prefix and their order in the day file; nothing in them is altered.
#
#   julia test/fixtures/extract_itch50.jl <day file> <symbol> <output>
#
# Not run by the suite. See PROVENANCE.md for the file the fixture came from.

include(joinpath(@__DIR__, "..", "activate.jl"))
using MarketTickStreamer
const M = MarketTickStreamer

function extract(path::AbstractString, symbol::AbstractString; n_e = 10, n_c = 5, n_p = 5)
    kept = Tuple{Int,Vector{UInt8}}[]
    adds = Dict{UInt64,Tuple{Int,Vector{UInt8}}}()      # order ref → (index, bytes)
    used = Set{UInt64}()
    locate = Ref{Int}(-1)
    k = Ref(0)
    counts = Dict('E' => 0, 'C' => 0, 'P' => 0)
    quota = Dict('E' => n_e, 'C' => n_c, 'P' => n_p)
    full() = all(counts[c] >= quota[c] for c in keys(quota))
    io = M._open_day_file(path)
    try
        M._itch50_scan(io; name = basename(path)) do buf, m, len
            k[] += 1
            bytes() = vcat(UInt8[len>>8, len&0xff], buf[m:(m+len-1)])
            t = Char(buf[m])
            if t == 'S'
                push!(kept, (k[], bytes()))
            elseif t == 'R' && len >= 39 && M._alpha(buf, m + 11, 8) == symbol
                locate[] = M._be16(buf, m + 1)
                push!(kept, (k[], bytes()))
            elseif locate[] >= 0 && M._be16(buf, m + 1) == locate[]
                if t in ('A', 'F') && !full()
                    adds[M._be64(buf, m + 11)] = (k[], bytes())
                elseif t == 'Q' && M._be64(buf, m + 11) > 0
                    push!(kept, (k[], bytes()))
                elseif t == 'P' && counts['P'] < quota['P']
                    counts['P'] += 1
                    push!(kept, (k[], bytes()))
                elseif (t == 'E' || (t == 'C' && Char(buf[m+31]) == 'Y')) &&
                       counts[t] < quota[t]
                    ref = M._be64(buf, m + 11)
                    if haskey(adds, ref)
                        counts[t] += 1
                        ref in used || (push!(kept, adds[ref]); push!(used, ref))
                        push!(kept, (k[], bytes()))
                    end
                end
                full() && !isempty(adds) && empty!(adds)
            end
            return nothing
        end
    finally
        close(io)
    end
    sort!(kept; by = first)
    return reduce(vcat, last.(kept)), length(kept), counts
end

function main()
    length(ARGS) == 3 || error("usage: extract_itch50.jl <day file> <symbol> <output>")
    bytes, n, counts = extract(ARGS[1], ARGS[2])
    write(ARGS[3], bytes)
    println("wrote ", n, " messages, ", length(bytes), " bytes; executions ", counts)
end

abspath(PROGRAM_FILE) == (@__FILE__) && main()
