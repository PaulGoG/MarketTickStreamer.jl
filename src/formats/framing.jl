# The day-file opener and the buffer refill shared by the ITCH and IEX readers,
# with the big-endian integers both of them decode.

# Big-endian integers at the 1-based index `i` of their first byte.
@inline _be16(b::Vector{UInt8}, i::Integer) = @inbounds (UInt16(b[i]) << 8) | UInt16(b[i+1])
@inline _be32(b::Vector{UInt8}, i::Integer) =
    (UInt32(_be16(b, i)) << 16) | UInt32(_be16(b, i + 2))

# The file as a byte stream: gzip is detected by its magic, anything else is read raw.
function _open_day_file(path::AbstractString)
    io = open(path, "r")
    magic = read(io, 2)
    seekstart(io)
    return magic == UInt8[0x1f, 0x8b] ? GzipDecompressorStream(io) : io
end

# Fill `buf[len+1:end]` from `io`. Returns `(len, ended, cut)`: `ended` when
# the stream has no more bytes, `cut` when a gzip member ended early (the
# shape of a partial download), which ends the stream as well. Once `ended`,
# the stream is not read again: a cut gzip stream throws on every read.
function _fill!(io::IO, buf::Vector{UInt8}, len::Int)
    ended = false
    cut = false
    try
        while len < length(buf)
            if eof(io)
                ended = true
                break
            end
            n = min(length(buf) - len, bytesavailable(io))
            n == 0 && continue                      # eof(io) refilled the stream's buffer
            GC.@preserve buf unsafe_read(io, pointer(buf, len + 1), n)
            len += n
        end
    catch e
        e isa ZlibError || rethrow()
        ended = true
        cut = true
    end
    return len, ended, cut
end
