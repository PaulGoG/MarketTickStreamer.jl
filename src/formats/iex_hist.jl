# IEX HIST capture files → Trade records.
#
# A HIST file is a packet capture of IEX's own multicast feed, one trading
# day: classic pcap until August 2017, pcapng since. Each UDP payload is one
# IEX-TP segment — a 40-byte little-endian header and a run of length-prefixed
# messages — and a Trade Report message (`T`) is one fill with the matching
# engine's nanosecond timestamp, so no capture clock is ever read: the pcap
# timestamps run on TAI since November 2024. TOPS 1.5 (protocol 0x8002),
# TOPS 1.6 (0x8003) and DEEP 1.0 (0x8004) carry the same `T` layout; the 1.5
# message trails four reserved bytes, which the framed length absorbs.

const IEX_PROTOCOLS =
    Dict{UInt16,String}(0x8002 => "TOPS1.5", 0x8003 => "TOPS1.6", 0x8004 => "DEEP1.0")
# Sale-condition flag bits of a Trade Report, named as IEX names them — the
# same letters the consolidated tape uses for the same conditions.
const IEX_FLAGS = ((0x80, "F"), (0x40, "T"), (0x20, "I"), (0x10, "8"), (0x08, "X"))
# The HIST download index; the files themselves live in a Google Cloud Storage bucket.
const IEX_HIST_API = "https://iextrading.com/api/1.0"

# Little-endian integers at the 1-based index `i` of their first byte.
@inline _le16(b::Vector{UInt8}, i::Integer) = @inbounds UInt16(b[i]) | (UInt16(b[i+1]) << 8)
@inline _le32(b::Vector{UInt8}, i::Integer) =
    @inbounds UInt32(_le16(b, i)) | (UInt32(_le16(b, i + 2)) << 16)
@inline _le64(b::Vector{UInt8}, i::Integer) =
    @inbounds UInt64(_le32(b, i)) | (UInt64(_le32(b, i + 4)) << 32)

# The 8-byte space-padded symbol at `i`, right-trimmed; all spaces give "".
function _iex_symbol(b::Vector{UInt8}, i::Integer)
    j = i + 7
    @inbounds while j >= i && b[j] == UInt8(' ')
        j -= 1
    end
    return String(b[i:j])
end

# The tape of a print: one string for every symbol, or looked up per symbol.
_iex_tape(tape::AbstractString, ::AbstractString) = String(tape)
_iex_tape(tape::AbstractDict, sym::AbstractString) = String(get(tape, sym, ""))

@noinline _iex_short(t::UInt8, len::Integer) =
    error("IEX $(Char(t)) message of $len bytes, expected at least 38")

@noinline _iex_malformed(blen::Integer) = error("pcapng block of $blen bytes is malformed")

# Walk the captured packets of a pcap or pcapng stream, calling `f(buf, i, n)`
# with `buf[i:i+n-1]` the link-layer frame. Returns `(; packets, truncated)`:
# `truncated` is true when the stream ended inside a record or inside a gzip
# member. `name` labels the stream in the error raised for a non-capture.
function _iex_packets(f, io::IO; chunk::Integer = 8 << 20, name::AbstractString = "stream")
    buf = Vector{UInt8}(undef, chunk)
    len = 0           # buf[1:len] holds unparsed bytes
    pos = 1
    packets = 0
    truncated = false
    ended = false
    container = :unknown      # :pcap or :pcapng once the magic is read
    swap = false              # classic pcap written big-endian
    while true
        # Compact the unparsed tail to the front, then fill the buffer.
        if pos > 1
            n_keep = len - pos + 1
            n_keep > 0 && copyto!(buf, 1, buf, pos, n_keep)
            len = n_keep
            pos = 1
        end
        if !ended
            len, ended, cut = _fill!(io, buf, len)
            cut && (truncated = true)
        end
        if container === :unknown
            len >= 4 || error("not a pcap or pcapng capture: $name")
            magic = (buf[1], buf[2], buf[3], buf[4])
            if magic == (0x0a, 0x0d, 0x0d, 0x0a)
                container = :pcapng
            elseif magic == (0xd4, 0xc3, 0xb2, 0xa1) || magic == (0xa1, 0xb2, 0xc3, 0xd4)
                if len < 24                         # global header incomplete
                    truncated = true
                    break
                end
                swap = magic[1] == 0xa1
                lt = swap ? _be32(buf, 21) : _le32(buf, 21)
                lt == 1 || error("pcap link type $lt is not Ethernet")
                container = :pcap
                pos = 25
            else
                error("not a pcap or pcapng capture: $name")
            end
        end
        if container === :pcap
            while true
                if pos + 15 > len                   # no complete record header
                    ended && pos <= len && (truncated = true)
                    break
                end
                incl = Int(swap ? _be32(buf, pos + 8) : _le32(buf, pos + 8))
                if pos + 15 + incl > len            # frame incomplete
                    if ended
                        truncated = true
                    elseif pos == 1 && len == length(buf)
                        error(
                            "pcap record of $incl bytes exceeds the $(length(buf))-byte read buffer",
                        )
                    end
                    break
                end
                f(buf, pos + 16, incl)
                packets += 1
                pos += 16 + incl
            end
        else
            while true
                if pos + 11 > len                   # no complete block header
                    ended && pos <= len && (truncated = true)
                    break
                end
                btype = _le32(buf, pos)
                if btype == 0x0a0d0d0a              # Section Header Block
                    order = _le32(buf, pos + 8)
                    order == 0x4d3c2b1a &&
                        error("big-endian pcapng sections are not supported")
                    order == 0x1a2b3c4d ||
                        error("pcapng section header without a byte-order magic")
                end
                blen = Int(_le32(buf, pos + 4))
                (blen >= 12 && blen % 4 == 0) || _iex_malformed(blen)
                if pos + blen - 1 > len             # block incomplete
                    if ended
                        truncated = true
                    elseif pos == 1 && len == length(buf)
                        error(
                            "pcapng block of $blen bytes exceeds the $(length(buf))-byte read buffer",
                        )
                    end
                    break
                end
                if btype == 0x00000001              # Interface Description Block
                    blen >= 20 || _iex_malformed(blen)
                    lt = _le16(buf, pos + 8)
                    lt == 1 || error("pcapng link type $lt is not Ethernet")
                elseif btype == 0x00000006          # Enhanced Packet Block
                    blen >= 32 || _iex_malformed(blen)
                    caplen = Int(_le32(buf, pos + 20))
                    caplen <= blen - 32 || _iex_malformed(blen)
                    f(buf, pos + 28, caplen)
                    packets += 1
                elseif btype == 0x00000003          # Simple Packet Block
                    blen >= 16 || _iex_malformed(blen)
                    f(buf, pos + 12, blen - 16)
                    packets += 1
                end
                pos += blen
            end
        end
        ended && break
    end
    return (; packets, truncated)
end

# The IEX-TP segment carried by the Ethernet frame `buf[i:i+n-1]`: the index
# of its header and the number of bytes to its end, or `0, 0` when the frame
# is not an IPv4/UDP packet.
function _iex_segment(buf::Vector{UInt8}, i::Int, n::Int)
    stop = i + n                                    # one past the frame end
    i + 14 <= stop || return 0, 0
    et = _be16(buf, i + 12)
    ip = i + 14
    if et == 0x8100                                 # 802.1Q tag
        i + 18 <= stop || return 0, 0
        et = _be16(buf, i + 16)
        ip = i + 18
    end
    et == 0x0800 || return 0, 0
    ip + 20 <= stop || return 0, 0
    buf[ip] >> 4 == 4 || return 0, 0
    ihl = Int(buf[ip] & 0x0f) * 4
    ihl >= 20 || return 0, 0
    buf[ip+9] == 17 || return 0, 0
    udp = ip + ihl
    udp + 8 <= stop || return 0, 0
    s = udp + 8
    seglen = min(Int(_be16(buf, udp + 4)) - 8, stop - s)
    seglen > 0 || return 0, 0
    return s, seglen
end

# Call `g(buf, m, len)` for each of the `count` message blocks of the segment
# at `buf[s:s+seglen-1]`, `buf[m:m+len-1]` being the message. Stops early when
# the payload ends; returns the number of blocks seen.
function _iex_blocks(g, buf::Vector{UInt8}, s::Int, seglen::Int, count::Int)
    stop = s + min(seglen, 40 + Int(_le16(buf, s + 12)))
    m = s + 40
    k = 0
    while k < count && m + 1 < stop
        mlen = Int(_le16(buf, m))
        m + 2 + mlen <= stop || break
        g(buf, m + 2, mlen)
        k += 1
        m += 2 + mlen
    end
    return k
end

# The trade ids voided by Trade Break messages, over the whole file: a break
# may follow its print by hours, so a tape that drops or marks broken prints
# needs this before it emits anything.
function _iex_voided(path::AbstractString)
    voided = Set{Int64}()
    io = _itch50_open(path)
    try
        _iex_packets(io; name = basename(path)) do buf, i, n
            s, seglen = _iex_segment(buf, i, n)
            (seglen >= 40 && buf[s] == 0x01) || return nothing
            haskey(IEX_PROTOCOLS, _le16(buf, s + 2)) || return nothing
            _iex_blocks(buf, s, seglen, Int(_le16(buf, s + 14))) do buf, m, len
                (len >= 1 && buf[m] == UInt8('B')) || return nothing
                len >= 38 || _iex_short(buf[m], len)
                push!(voided, reinterpret(Int64, _le64(buf, m + 30)))
                return nothing
            end
            return nothing
        end
    finally
        close(io)
    end
    return voided
end

"""
    iex_hist_trades(path; symbols = nothing, broken = :drop, tape = "", each_page = nothing,
                    page_rows = 100_000)
        -> (; trades, feed, packets, segments, messages, prints, broken, gaps, duplicates,
              truncated)

The Trade Reports of one IEX HIST capture (`.pcap.gz` as published, or the
decompressed pcap/pcapng) as [`Trade`](@ref) records, in capture order. The
feed is read off the IEX-TP protocol id — TOPS 1.5, TOPS 1.6 or DEEP 1.0,
whose Trade Report is one and the same message — so the file name is never
trusted. Timestamps are the matching engine's, nanoseconds since the epoch;
the capture clock is not read.

Each print carries `exchange = "V"` (IEX's market-centre code on the
consolidated tape), `id` = IEX trade id (unique within the day), `tape` as
given (a string for every print, or a `Dict` from symbol to tape; IEX does
not publish the listing venue), and `conditions` = the set sale-condition
flags as the letters IEX itself uses, which are the tape's: `F` intermarket
sweep, `T` extended hours, `I` odd lot, `8` trade-through exempt, `X`
single-price cross. A print with no flag has no condition.

`symbols` restricts the output; `nothing` keeps every symbol. `broken`
decides what a Trade Break does to the print it voids — `:drop` (default)
omits it, `:mark` appends `"broken"`, `:keep` leaves it — the first two
reading the file twice. `each_page` and `page_rows` stream the prints as in
[`itch50_trades`](@ref). The statistics count captured packets, IEX-TP
segments with messages, message blocks, emitted prints, voided prints
(Trade Breaks seen, under `:keep`), sequence gaps and duplicate segments,
and whether the capture ended inside a record.
"""
function iex_hist_trades(
    path::AbstractString;
    symbols = nothing,
    broken::Symbol = :drop,
    tape = "",
    each_page = nothing,
    page_rows::Integer = 100_000,
)
    page_rows >= 1 || throw(ArgumentError("page_rows must be at least 1, got $page_rows"))
    broken in (:drop, :mark, :keep) ||
        throw(ArgumentError("broken must be :drop, :mark or :keep, got $(repr(broken))"))
    tape isa Union{AbstractString,AbstractDict} || throw(
        ArgumentError(
            "tape must be a string or a Dict from symbol to tape, got $(typeof(tape))",
        ),
    )
    wanted = symbols === nothing ? nothing : Set{String}(uppercase.(String.(symbols)))
    voided = broken === :keep ? Set{Int64}() : _iex_voided(path)

    next_seq = Dict{UInt32,UInt64}()    # next expected sequence number per session
    gapped = Set{UInt32}()              # sessions whose gap has been logged
    feed_id = Ref(0x0000)               # protocol of the first processed segment
    segments = Ref(0)
    messages = Ref(0)
    prints = Ref(0)
    n_broken = Ref(0)
    gaps = Ref(0)
    duplicates = Ref(0)
    other_protocol = Ref(0)
    acc = each_page === nothing ? Trade[] : nothing
    page = Ref(Trade[])

    function emit!(t::Trade)
        push!(page[], t)
        if length(page[]) >= page_rows
            acc === nothing ? each_page(page[]) : append!(acc, page[])
            page[] = Trade[]
        end
        return nothing
    end

    function on_message(buf::Vector{UInt8}, m::Int, len::Int)
        len >= 1 || return nothing
        t = buf[m]
        if t == UInt8('T')
            len >= 38 || _iex_short(t, len)
            sym = _iex_symbol(buf, m + 10)
            wanted === nothing || sym in wanted || return nothing
            id = reinterpret(Int64, _le64(buf, m + 30))
            is_voided = id in voided
            if is_voided && broken === :drop
                n_broken[] += 1
                return nothing
            end
            flags = buf[m+1]
            c = String[name for (bit, name) in IEX_FLAGS if flags & bit != 0]
            if is_voided
                push!(c, "broken")
                n_broken[] += 1
            end
            ts = reinterpret(Int64, _le64(buf, m + 2))
            shares = _le32(buf, m + 18)
            price = reinterpret(Int64, _le64(buf, m + 22))
            emit!(
                Trade(
                    sym,
                    ts,
                    0,
                    price / 1e4,
                    Float64(shares),
                    "V",
                    c,
                    _iex_tape(tape, sym),
                    id,
                ),
            )
            prints[] += 1
        elseif t == UInt8('B')
            len >= 38 || _iex_short(t, len)
            # In the other modes the voided set accounts for the break,
            # counted at the print.
            broken === :keep && (n_broken[] += 1)
        end
        return nothing
    end

    io = _itch50_open(path)
    scan = try
        _iex_packets(io; name = basename(path)) do buf, i, n
            s, seglen = _iex_segment(buf, i, n)
            seglen >= 40 || return nothing
            buf[s] == 0x01 || return nothing
            protocol = _le16(buf, s + 2)
            haskey(IEX_PROTOCOLS, protocol) || return nothing
            session = _le32(buf, s + 8)
            count = Int(_le16(buf, s + 14))
            (session == 0 || count == 0) && return nothing
            if feed_id[] == 0x0000
                feed_id[] = protocol
            elseif protocol != feed_id[]
                other_protocol[] += 1
                return nothing
            end
            got = _le64(buf, s + 24)
            expected = get(next_seq, session, got)
            if got < expected
                duplicates[] += 1
                return nothing
            elseif got > expected
                gaps[] += 1
                if !(session in gapped)
                    push!(gapped, session)
                    @warn "IEX sequence gap" session expected got
                end
            end
            next_seq[session] = got + count
            segments[] += 1
            messages[] += _iex_blocks(on_message, buf, s, seglen, count)
            return nothing
        end
    finally
        close(io)
    end

    if !isempty(page[])
        acc === nothing ? each_page(page[]) : append!(acc, page[])
    end
    feed = feed_id[] == 0x0000 ? "" : IEX_PROTOCOLS[feed_id[]]
    skipped = other_protocol[]
    skipped > 0 && @warn "IEX segments of another protocol skipped" feed skipped
    return (;
        trades = acc === nothing ? Trade[] : acc,
        feed,
        packets = scan.packets,
        segments = segments[],
        messages = messages[],
        prints = prints[],
        broken = n_broken[],
        gaps = gaps[],
        duplicates = duplicates[],
        truncated = scan.truncated,
    )
end

"""
    iex_hist_index(date; api_base = IEX_HIST_API, policy = RestPolicy()) -> Vector{Dict} | Nothing

The files IEX publishes for `date` — one entry per feed with `feed`,
`version`, `protocol`, `link` and `size` (compressed bytes) — from the HIST
API, or `nothing` on 404: a weekend, a holiday, a day not yet published
(files appear on the following morning).
"""
function iex_hist_index(
    date::Date;
    api_base::AbstractString = IEX_HIST_API,
    policy::RestPolicy = RestPolicy(),
)
    url = "$(api_base)/hist"
    resp = _with_retry(policy) do
        HTTP.get(
            url;
            query = ["date" => Dates.format(date, dateformat"yyyymmdd")],
            retry = false,
            status_exception = false,
            readtimeout = policy.request_timeout_s,
            connect_timeout = policy.connect_timeout_s,
        )
    end::HTTP.Response
    resp.status == 404 && return nothing
    resp.status == 200 || throw(HTTP.StatusError(resp.status, "GET", url, resp))
    entries = JSON.parse(resp.body)
    entries isa AbstractVector || error("unexpected HIST index for $date")
    return [Dict{String,Any}(String(k) => v for (k, v) in pairs(e)) for e in entries]
end

"""
    fetch_iex_hist(date, dir; feed = "TOPS", api_base = IEX_HIST_API,
                   policy = RestPolicy()) -> String | Nothing

Download the HIST capture of `date` for `feed` (`"TOPS"` or `"DEEP"`,
case-insensitive; the version is whatever IEX lists for that day) into
`dir/iex/`, resuming an interrupted transfer, verify it against the CRC-32C
the file host announces (`x-goog-hash`), and return its path. `nothing`
when IEX lists no files for the date, an error naming the feeds listed when
the feed is not among them. A file already present whose size matches the
index entry and whose CRC-32C matches the host's is reused; a verified
mismatch deletes the file and fails.
"""
function fetch_iex_hist(
    date::Date,
    dir::AbstractString;
    feed::AbstractString = "TOPS",
    api_base::AbstractString = IEX_HIST_API,
    policy::RestPolicy = RestPolicy(),
)
    entries = iex_hist_index(date; api_base, policy)
    entries === nothing && return nothing
    want = uppercase(String(feed))
    e = findfirst(x -> uppercase(String(x["feed"])) == want, entries)
    e === nothing && error(
        "no $want file listed for $date; listed: " *
        join(unique(String(x["feed"]) for x in entries), ", "),
    )
    entry = entries[e]
    day = Dates.format(date, dateformat"yyyymmdd")
    name = "$(day)_$(entry["protocol"])_$(entry["feed"])$(entry["version"]).pcap.gz"
    target = joinpath(dir, "iex")
    mkpath(target)
    path = joinpath(target, name)
    size = parse(Int64, String(entry["size"]))
    link = String(entry["link"])
    if isfile(path) && filesize(path) == size
        announced = _announced_crc32c(link, policy)
        (announced === nothing || _crc32c_hex(path) == announced) && return path
    end
    r = _with_retry(policy) do
        _download_resumable(link, path, policy)
    end
    r.found || error("file listed but missing: $link")
    want_crc = _crc32c_from_headers(r.headers)
    if want_crc !== nothing
        got_crc = _crc32c_hex(path)
        if got_crc != want_crc
            rm(path; force = true)
            error(
                "CRC-32C mismatch for $name: expected $want_crc, got $got_crc — " *
                "the download was discarded",
            )
        end
    end
    got = filesize(path)
    if got != size
        rm(path; force = true)
        error("size mismatch for $name: listed $size bytes, got $got")
    end
    return path
end

# CRC-32C of a file as 8 lowercase hex digits, streamed.
_crc32c_hex(path::AbstractString) = string(open(io -> crc32c(io), path); base = 16, pad = 8)

# The `crc32c=<base64>` member of an `x-goog-hash` header as 8 hex digits, or
# `nothing` when the header or the member is absent. The host base64-encodes
# the four big-endian bytes of the checksum.
function _crc32c_from_headers(headers)
    for (k, v) in headers
        lowercase(k) == "x-goog-hash" || continue
        for part in split(v, ',')
            kv = split(strip(part), '='; limit = 2)
            length(kv) == 2 && lowercase(kv[1]) == "crc32c" || continue
            bytes = base64decode(kv[2])
            length(bytes) == 4 || continue
            return join(string(b; base = 16, pad = 2) for b in bytes)
        end
    end
    return nothing
end

# HEAD the file host for its announced CRC-32C.
function _announced_crc32c(url::AbstractString, policy::RestPolicy)
    resp = _with_retry(policy) do
        HTTP.head(
            url;
            retry = false,
            status_exception = false,
            readtimeout = policy.request_timeout_s,
            connect_timeout = policy.connect_timeout_s,
        )
    end::HTTP.Response
    resp.status == 200 || return nothing
    return _crc32c_from_headers([String(k) => String(v) for (k, v) in resp.headers])
end
