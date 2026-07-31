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
    o = JSON3.read(line)
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

"""
    compact_raw(raw_paths, out_dir; format = "csv", dedup = true,
                mem_fraction = 0.5, tz = tz"America/New_York") -> Vector{String}

Compact raw NDJSON files into per-symbol, per-trading-day analysis files
(`out_dir/SYMBOL/YYYY-MM-DD.csv|.arrow`), sorted by `time_ns`. Existing
outputs are never overwritten — a ` #N` suffixed sibling is written instead
(safesave semantics). Returns the list of files written.

`dedup = true` drops exact duplicate prints (reconnection double-delivery,
overlapping backfill/live captures) via [`dedup_trades`](@ref), logging the
count. Compaction materializes everything in memory; it refuses to start if
the estimated footprint exceeds `mem_fraction` of currently free RAM —
compact in smaller batches of part files instead.
"""
function compact_raw(raw_paths::AbstractVector{<:AbstractString}, out_dir::AbstractString;
                     format::AbstractString = "csv", dedup::Bool = true,
                     mem_fraction::Real = 0.5, tz::TimeZone = tz"America/New_York")
    format in ("csv", "arrow") || throw(ArgumentError("format must be \"csv\" or \"arrow\""))
    bytes = sum(filesize, raw_paths; init = 0)
    est = 4 * bytes          # parsed structs + DataFrame + sort scratch
    est > mem_fraction * Sys.free_memory() && error(
        "compaction of $(round(bytes / 2^20; digits = 1)) MiB raw would need ≈" *
        "$(round(est / 2^30; digits = 2)) GiB, over $(mem_fraction) of free RAM — " *
        "compact fewer part files per call")
    trades = read_raw(raw_paths)
    if dedup
        n0 = length(trades)
        trades = dedup_trades(trades)
        n0 > length(trades) && @info "dropped duplicate prints" count = n0 - length(trades)
    end
    isempty(trades) && return String[]
    df = DataFrame(
        symbol = [t.symbol for t in trades],
        time_ns = [t.time_ns for t in trades],
        recv_ns = [t.recv_ns for t in trades],
        price = [t.price for t in trades],
        size = [t.size for t in trades],
        exchange = [t.exchange for t in trades],
        conditions = [join(t.conditions, "|") for t in trades],
        tape = [t.tape for t in trades],
        id = [t.id for t in trades],
    )
    df.date = [trading_date(ns; tz) for ns in df.time_ns]
    written = String[]
    for g in groupby(df, [:symbol, :date])
        sym, date = g.symbol[1], g.date[1]
        dir = joinpath(out_dir, sym)
        mkpath(dir)
        out = select(sort(DataFrame(g), :time_ns), Not(:date))
        path = _safepath(joinpath(dir, "$(date).$(format == "csv" ? "csv" : "arrow")"))
        format == "csv" ? CSV.write(path, out) : Arrow.write(path, out)
        push!(written, path)
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
