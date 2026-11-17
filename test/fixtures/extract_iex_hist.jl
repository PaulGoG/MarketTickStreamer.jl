# Cut the golden IEX HIST fixture from a downloaded TOPS 1.6 capture: the
# first ten captured frames whose IEX-TP segment holds a Trade Report for the
# symbol, each frame as captured (Ethernet, IPv4, UDP, segment, every message
# in it, other symbols' included), in capture order. The pcapng blocks around
# them are written anew, with zero timestamps; the frames are not altered.
#
#   julia test/fixtures/extract_iex_hist.jl <capture> <symbol> <output>
#
# Not run by the suite. See PROVENANCE.md for the capture the fixture came from.

include(joinpath(@__DIR__, "..", "activate.jl"))
include(joinpath(@__DIR__, "..", "vendor_bytes.jl"))
using MarketTickStreamer
const M = MarketTickStreamer

struct Enough <: Exception end

function extract(path::AbstractString, symbol::AbstractString; n = 10)
    frames = Vector{UInt8}[]
    sym = codeunits(rpad(symbol, 8))
    io = M._open_day_file(path)
    try
        M._iex_packets(io; name = basename(path)) do buf, i, len
            s, seglen = M._iex_segment(buf, i, len)
            (s == 0 || seglen < 40) && return nothing
            M._le16(buf, s + 2) == 0x8003 || return nothing          # TOPS 1.6
            hit = Ref(false)
            M._iex_blocks(buf, s, seglen, Int(M._le16(buf, s + 14))) do b, m, mlen
                mlen >= 38 &&
                    b[m] == UInt8('T') &&
                    view(b, (m+10):(m+17)) == sym &&
                    (hit[] = true)
            end
            hit[] && push!(frames, buf[i:(i+len-1)])
            length(frames) >= n && throw(Enough())
            return nothing
        end
    catch e
        e isa Enough || rethrow()
    finally
        close(io)
    end
    I = IEXBytes
    return vcat(I.shb(), I.idb(), (I.epb(f) for f in frames)...), length(frames)
end

function main()
    length(ARGS) == 3 || error("usage: extract_iex_hist.jl <capture> <symbol> <output>")
    bytes, n = extract(ARGS[1], ARGS[2])
    write(ARGS[3], bytes)
    println("wrote ", n, " frames, ", length(bytes), " bytes")
end

abspath(PROGRAM_FILE) == (@__FILE__) && main()
