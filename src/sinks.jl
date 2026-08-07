# Persistence: append-only raw NDJSON sink + compaction to analysis files.
#
# Layer 1 (raw)      : one NDJSON line per normalized Trade, appended in timed
#                      batches. Session-stamped filenames, never overwritten,
#                      rolled at limits.max_raw_file_mb. Line-by-line recovery.
# Layer 2 (processed): per-symbol, per-trading-day CSV or Arrow produced by
#                      [`compact_raw`](@ref) for the analysis pipeline.

"""
    trade_to_json(t::Trade) -> String

Serialize a `Trade` to a single NDJSON line (no trailing newline). Timestamps
stay Int64 nanoseconds — the round-trip through [`json_to_trade`](@ref) is
lossless.
"""
trade_to_json(t::Trade) = JSON3.write((
    symbol = t.symbol, time_ns = t.time_ns, recv_ns = t.recv_ns,
    price = t.price, size = t.size, exchange = t.exchange,
    conditions = t.conditions, tape = t.tape, id = t.id,
))

"""
    json_to_trade(line) -> Trade

Parse one NDJSON line written by [`trade_to_json`](@ref) back into a `Trade`.
"""
function json_to_trade(line::AbstractString)
    # String() is identity for String input; for SubStrings of a large parent
    # (e.g. a bulk-read file split into lines) it avoids a pathological JSON3
    # slow path measured at ~2000x the per-line cost.
    o = JSON3.read(String(line))
    return Trade(String(o.symbol), Int64(o.time_ns), Int64(o.recv_ns),
                 Float64(o.price), Float64(o.size), String(o.exchange),
                 String.(o.conditions), String(o.tape), Int64(o.id))
end

"""
    RawSink

Append-only NDJSON writer with size-based file rolling. Create with
[`open_raw_sink`](@ref); feed via [`write_batch!`](@ref); always
[`close_sink!`](@ref) (final flush) on shutdown.
"""
mutable struct RawSink
    dir::String
    prefix::String
    max_bytes::Int64
    part::Int
    path::String
    io::IOStream
    bytes::Int64
    n_written::Int64
end

"""
    open_raw_sink(dir, prefix; max_mb = 1024) -> RawSink

Open a fresh raw NDJSON file `dir/prefix_partNNN.jsonl`. Existing files are
never reopened or overwritten — a new part number is chosen past any that
already exist.
"""
function open_raw_sink(dir::AbstractString, prefix::AbstractString; max_mb::Integer = 1024)
    mkpath(dir)
    part = 1
    path = joinpath(dir, "$(prefix)_part$(lpad(part, 3, '0')).jsonl")
    while isfile(path)
        part += 1
        path = joinpath(dir, "$(prefix)_part$(lpad(part, 3, '0')).jsonl")
    end
    return RawSink(String(dir), String(prefix), Int64(max_mb) * 1024 * 1024,
                   part, path, open(path, "a"), 0, 0)
end

function _roll!(s::RawSink)
    close(s.io)
    s.part += 1
    s.path = joinpath(s.dir, "$(s.prefix)_part$(lpad(s.part, 3, '0')).jsonl")
    while isfile(s.path)
        s.part += 1
        s.path = joinpath(s.dir, "$(s.prefix)_part$(lpad(s.part, 3, '0')).jsonl")
    end
    s.io = open(s.path, "a")
    s.bytes = 0
    return s
end

"""
    write_batch!(s::RawSink, trades) -> RawSink

Append a batch of trades as NDJSON lines and fsync-flush once. Rolls to a new
part file when the size limit is exceeded.
"""
function write_batch!(s::RawSink, trades::AbstractVector{Trade})
    isempty(trades) && return s
    for t in trades
        line = trade_to_json(t)
        println(s.io, line)
        s.bytes += sizeof(line) + 1
        s.n_written += 1
    end
    flush(s.io)
    s.bytes >= s.max_bytes && _roll!(s)
    return s
end

"""
    close_sink!(s::RawSink)

Flush and close the sink's file handle.
"""
close_sink!(s::RawSink) = (flush(s.io); close(s.io); nothing)

"""
    run_sink!(ch::Channel{Trade}, sink::RawSink;
              flush_interval_s = 30.0, flush_max_ticks = 5000,
              on_flush = nothing) -> Int

Consumer loop: drain `ch` into an in-memory batch and persist whenever the
batch reaches `flush_max_ticks` or `flush_interval_s` elapses with pending
data. Returns the total number of trades written. Terminates (after a final
flush) once `ch` is closed and fully drained — closing the channel is the
shutdown signal, so no locks or flags are needed.

`on_flush(n_batch, n_total)` is called after each disk write (for logging).
"""
function run_sink!(ch::Channel{Trade}, sink::RawSink;
                   flush_interval_s::Real = 30.0, flush_max_ticks::Integer = 5000,
                   on_flush = nothing)
    buf = Trade[]
    last_flush = time()
    total = 0
    dowrite = () -> begin
        write_batch!(sink, buf)
        total += length(buf)
        on_flush === nothing || on_flush(length(buf), total)
        empty!(buf)
        last_flush = time()
    end
    while true
        while isready(ch) && length(buf) < flush_max_ticks
            push!(buf, take!(ch))
        end
        if length(buf) >= flush_max_ticks ||
           (!isempty(buf) && time() - last_flush >= flush_interval_s)
            dowrite()
        end
        if !isready(ch)
            isopen(ch) || break
            sleep(0.05)          # idle poll; granularity ≪ flush interval
        end
    end
    isempty(buf) || dowrite()
    return total
end

"""
    read_raw(paths) -> Vector{Trade}

Load one or more raw NDJSON files into memory, skipping (and counting via a
`@warn`) corrupt lines — e.g. a partial last line after a hard kill.
"""
function read_raw(paths::AbstractVector{<:AbstractString})
    trades = Trade[]
    for p in paths
        nbad = 0
        for line in eachline(p)
            isempty(strip(line)) && continue
            try
                push!(trades, json_to_trade(line))
            catch
                nbad += 1
            end
        end
        nbad > 0 && @warn "skipped corrupt lines" file = p count = nbad
    end
    return trades
end

read_raw(path::AbstractString) = read_raw([path])

# Sort one (symbol, day) group by exchange time and write it to the
# processed tree with safesave semantics. Returns the path written.
function _write_group(trades::Vector{Trade}, out_dir::AbstractString,
                      sym::AbstractString, date::Date, format::AbstractString)
    ts = sort(trades; by = t -> t.time_ns)
    out = DataFrame(
        symbol = [t.symbol for t in ts],
        time_ns = [t.time_ns for t in ts],
        recv_ns = [t.recv_ns for t in ts],
        price = [t.price for t in ts],
        size = [t.size for t in ts],
        exchange = [t.exchange for t in ts],
        conditions = [join(t.conditions, "|") for t in ts],
        tape = [t.tape for t in ts],
        id = [t.id for t in ts],
    )
    dir = joinpath(out_dir, sym)
    mkpath(dir)
    path = _safepath(joinpath(dir, "$(date).$(format == "csv" ? "csv" : "arrow")"))
    format == "csv" ? CSV.write(path, out) : Arrow.write(path, out)
    return path
end

"""
    compact_raw(raw_paths, out_dir; format = "csv", dedup = true,
                mem_fraction = 0.5, tz = tz"America/New_York",
                max_resident_mb = nothing) -> Vector{String}

Compact raw NDJSON files into per-symbol, per-trading-day analysis files
(`out_dir/SYMBOL/YYYY-MM-DD.csv|.arrow`), sorted by `time_ns`. Existing
outputs are never overwritten — a ` #N` suffixed sibling is written instead
(safesave semantics). Returns the list of files written.

`dedup = true` drops exact duplicate prints (reconnection double-delivery,
overlapping backfill/live captures) via [`dedup_trades`](@ref), logging the
count. Small inputs are compacted in memory; when the estimated footprint
exceeds `mem_fraction` of currently free RAM the input is instead streamed
line-by-line into per-(symbol, day) spill files and each group is compacted
independently — memory stays bounded by the largest single group.
`max_resident_mb` additionally enforces the configured heap ceiling per
group ([`check_resident_memory`](@ref)).
"""
function compact_raw(raw_paths::AbstractVector{<:AbstractString}, out_dir::AbstractString;
                     format::AbstractString = "csv", dedup::Bool = true,
                     mem_fraction::Real = 0.5, tz::TimeZone = tz"America/New_York",
                     max_resident_mb::Union{Nothing, Real} = nothing)
    format in ("csv", "arrow") || throw(ArgumentError("format must be \"csv\" or \"arrow\""))
    bytes = sum(filesize, raw_paths; init = 0)
    est = 4 * bytes          # parsed structs + DataFrame + sort scratch
    est > mem_fraction * Sys.free_memory() &&
        return _compact_spill(raw_paths, out_dir; format, dedup, tz, max_resident_mb)
    trades = read_raw(raw_paths)
    if dedup
        n0 = length(trades)
        trades = dedup_trades(trades)
        n0 > length(trades) && @info "dropped duplicate prints" count = n0 - length(trades)
    end
    isempty(trades) && return String[]
    groups = Dict{Tuple{String, Date}, Vector{Trade}}()
    for t in trades
        push!(get!(() -> Trade[], groups, (t.symbol, trading_date(t.time_ns; tz))), t)
    end
    return [_write_group(groups[k], out_dir, k[1], k[2], format)
            for k in sort!(collect(keys(groups)))]
end

# Bounded-memory compaction: route each raw line to a per-(symbol, day)
# spill file in one streaming pass, then compact every group independently.
function _compact_spill(raw_paths::AbstractVector{<:AbstractString}, out_dir::AbstractString;
                        format::AbstractString, dedup::Bool, tz::TimeZone,
                        max_resident_mb::Union{Nothing, Real})
    @info "large input — using spill compaction" mib =
        round(sum(filesize, raw_paths; init = 0) / 2^20; digits = 1)
    written = String[]
    mktempdir() do spill
        handles = Dict{Tuple{String, Date}, IOStream}()
        nbad = 0
        for p in raw_paths, line in eachline(p)
            isempty(strip(line)) && continue
            t = try
                json_to_trade(line)
            catch
                nbad += 1
                continue
            end
            key = (t.symbol, trading_date(t.time_ns; tz))
            io = get!(handles, key) do
                open(joinpath(spill, "$(key[1])_$(key[2]).jsonl"), "w")
            end
            println(io, line)
        end
        nbad > 0 && @warn "skipped corrupt lines" count = nbad
        foreach(close, values(handles))
        ndup = 0
        for key in sort!(collect(keys(handles)))
            sym, date = key
            trades = read_raw(joinpath(spill, "$(sym)_$(date).jsonl"))
            if dedup
                n0 = length(trades)
                trades = dedup_trades(trades)
                ndup += n0 - length(trades)
            end
            push!(written, _write_group(trades, out_dir, sym, date, format))
            max_resident_mb === nothing ||
                check_resident_memory(max_resident_mb; context = "compaction $sym $date")
        end
        ndup > 0 && @info "dropped duplicate prints" count = ndup
    end
    return written
end

# First non-existing variant of `path`: path, then "name #2.ext", "name #3.ext", …
function _safepath(path::AbstractString)
    isfile(path) || return String(path)
    base, ext = splitext(path)
    n = 2
    while isfile("$base #$n$ext")
        n += 1
    end
    return "$base #$n$ext"
end
