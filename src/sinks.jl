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
    symbol = t.symbol,
    time_ns = t.time_ns,
    recv_ns = t.recv_ns,
    price = t.price,
    size = t.size,
    exchange = t.exchange,
    conditions = t.conditions,
    tape = t.tape,
    id = t.id,
))

"""
    json_to_trade(line) -> Trade

Parse one NDJSON line written by [`trade_to_json`](@ref) back into a `Trade`.
"""
function json_to_trade(line::AbstractString)
    # String() is identity for String input; for SubStrings of a large parent
    # (e.g. a bulk-read file split into lines) it avoids a pathological JSON3
    # slow path measured at ~2000x the per-line cost.
    # The assertions collapse JSON3's value union to what `trade_to_json`
    # writes, so each conversion below is a static call rather than a dynamic
    # dispatch per field per line; a malformed line fails here, loudly.
    o = JSON3.read(String(line))::JSON3.Object
    return Trade(
        String(o.symbol::AbstractString),
        Int64(o.time_ns::Integer),
        Int64(o.recv_ns::Integer),
        Float64(o.price::Real),
        Float64(o.size::Real),
        String(o.exchange::AbstractString),
        String.(o.conditions::JSON3.Array),
        String(o.tape::AbstractString),
        Int64(o.id::Integer),
    )
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

# First part number >= `part` whose file does not exist yet, with its path.
function _fresh_part(dir::AbstractString, prefix::AbstractString, part::Integer)
    path = joinpath(dir, "$(prefix)_part$(lpad(part, 3, '0')).jsonl")
    while isfile(path)
        part += 1
        path = joinpath(dir, "$(prefix)_part$(lpad(part, 3, '0')).jsonl")
    end
    return part, path
end

"""
    open_raw_sink(dir, prefix; max_mb = 1024) -> RawSink

Open a fresh raw NDJSON file `dir/prefix_partNNN.jsonl`. Existing files are
never reopened or overwritten — a new part number is chosen past any that
already exist.
"""
function open_raw_sink(dir::AbstractString, prefix::AbstractString; max_mb::Integer = 1024)
    mkpath(dir)
    part, path = _fresh_part(dir, prefix, 1)
    return RawSink(
        String(dir),
        String(prefix),
        Int64(max_mb) * 1024 * 1024,
        part,
        path,
        open(path, "a"),
        0,
        0,
    )
end

function _roll!(s::RawSink)
    close(s.io)
    s.part, s.path = _fresh_part(s.dir, s.prefix, s.part + 1)
    s.io = open(s.path, "a")
    s.bytes = 0
    return s
end

"""
    write_batch!(s::RawSink, trades) -> RawSink

Append a batch of trades as NDJSON lines and flush the stream once (buffered
writes reach the OS per batch, not per line). Rolls to a new part file when
the size limit is exceeded.
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
function run_sink!(
    ch::Channel{Trade},
    sink::RawSink;
    flush_interval_s::Real = 30.0,
    flush_max_ticks::Integer = 5000,
    on_flush = nothing,
)
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
            catch e
                # An interrupt landing mid-parse is not a corrupt line.
                e isa InterruptException && rethrow()
                nbad += 1
            end
        end
        nbad > 0 && @warn "skipped corrupt lines" file = p count = nbad
    end
    return trades
end

read_raw(path::AbstractString) = read_raw([path])

# Columns of a processed file that repeat a handful of values a million times
# over. Arrow stores them dictionary-encoded: 44 B per print against 65 B on
# a liquid US equity day, at unchanged read cost, and they read back as
# ordinary string vectors.
const DICTIONARY_COLUMNS = (:symbol, :exchange, :conditions, :tape)

# Sort one (symbol, day) group by exchange time and write it to the
# processed tree with safesave semantics. Returns the path written.
function _write_group(
    trades::Vector{Trade},
    out_dir::AbstractString,
    sym::AbstractString,
    date::Date,
    format::AbstractString,
    compression::Union{Nothing,Symbol} = nothing,
)
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
    path = joinpath(dir, "$(date).$(format == "csv" ? "csv" : "arrow")")
    return _safesave(path) do tmp
        if format == "csv"
            CSV.write(tmp, out)
        else
            for c in DICTIONARY_COLUMNS
                out[!, c] = Arrow.DictEncode(out[!, c])
            end
            Arrow.write(tmp, out; compress = compression)
        end
    end
end

# One processed per-day file (CSV or Arrow) → DataFrame; the inverse of
# `_write_group`.
_read_processed(path::AbstractString) =
    endswith(path, ".arrow") ? DataFrame(Arrow.Table(path)) : CSV.read(path, DataFrame)

"""
    SPILL_HEADROOM

Default multiple of the input size that the spill filesystem must have free
before [`compact_raw`](@ref) will stream to it (`spill_headroom`).

The spill pass trades memory for scratch space, writing every input line back
out once. `tempdir()` is the wrong home for that: on systemd distributions
`/tmp` is a tmpfs sized at half of RAM, so spilling there writes the copy into
RAM and, on an input larger than that, takes the machine down — the failure
this guard exists to prevent. The default scratch location is therefore the
directory holding `out_dir`, and the space is verified before the first line
is read.
"""
const SPILL_HEADROOM = 1.1

"""
    compact_raw(raw_paths, out_dir; format = "csv", dedup = true,
                mem_fraction = 0.5, tz = tz"America/New_York",
                max_live_heap_mb = nothing, scratch_dir = nothing,
                footprint_factor = 4.0, spill_headroom = SPILL_HEADROOM,
                max_open_spill_files = 256, compression = nothing) -> Vector{String}

Compact raw NDJSON files into per-symbol, per-trading-day analysis files
(`out_dir/SYMBOL/YYYY-MM-DD.csv|.arrow`), sorted by `time_ns`. An existing
output is never destroyed: the new file takes the canonical name and the one
it displaces is kept as a numbered backup `YYYY-MM-DD_#N.csv|.arrow`
(safesave semantics, see `_safesave`). Returns the list of files written.

`dedup = true` drops exact duplicate prints (reconnection double-delivery,
overlapping backfill/live captures) via [`deduplicate_trades`](@ref), logging the
count. Small inputs are compacted in memory; when the estimated footprint
exceeds `mem_fraction` of currently free RAM the input is instead streamed
line-by-line into per-(symbol, day) spill files and each group is compacted
independently — memory stays bounded by the largest single group.
`max_live_heap_mb` additionally enforces the configured heap ceiling per
group ([`check_live_heap`](@ref)).

The spill pass writes a verbatim copy of the input, so it needs scratch space
of the input's own size. `scratch_dir` places it; the default is the directory
containing `out_dir`, **not** `tempdir()` — see [`SPILL_HEADROOM`](@ref). The
free space is checked up front and compaction refuses to start without it.

`footprint_factor` is the estimated in-memory size of the parsed input per
input byte, `spill_headroom` the scratch space required as a multiple of the
input size, and `max_open_spill_files` the number of spill files held open at
once: further groups are served by closing the least recently written file and
reopening it in append mode, so the pass works within the process's
file-descriptor limit however many symbol-days the input spans. The pipeline
takes all of them from `[limits]`.

Arrow output stores the `symbol`, `exchange`, `conditions` and `tape` columns
dictionary-encoded, which costs nothing to read and takes a liquid US equity
day from 65 to 44 bytes per print. `compression` (`:zstd` or `:lz4`, Arrow
only) compresses the record batches as well — 8 bytes per print with `:zstd`
on the same day — at the price of memory-mapped reads: an uncompressed file is
mapped and touched lazily, a compressed one is decompressed on load (64 ms
against under 1 ms for that day). Uncompressed is the default.
"""
function compact_raw(
    raw_paths::AbstractVector{<:AbstractString},
    out_dir::AbstractString;
    format::AbstractString = "csv",
    dedup::Bool = true,
    mem_fraction::Real = 0.5,
    tz::TimeZone = tz"America/New_York",
    max_live_heap_mb::Union{Nothing,Real} = nothing,
    scratch_dir::Union{Nothing,AbstractString} = nothing,
    footprint_factor::Real = 4.0,
    spill_headroom::Real = SPILL_HEADROOM,
    max_open_spill_files::Integer = 256,
    compression::Union{Nothing,Symbol} = nothing,
)
    format in ("csv", "arrow") ||
        throw(ArgumentError("format must be \"csv\" or \"arrow\""))
    compression in (nothing, :zstd, :lz4) || throw(
        ArgumentError(
            "compression must be nothing, :zstd or :lz4, got $(repr(compression))",
        ),
    )
    (compression === nothing || format == "arrow") ||
        throw(ArgumentError("compression applies to format = \"arrow\" only"))
    0 < mem_fraction <= 1 ||
        throw(ArgumentError("mem_fraction must be in (0, 1], got $(mem_fraction)"))
    footprint_factor > 0 ||
        throw(ArgumentError("footprint_factor must be positive, got $(footprint_factor)"))
    spill_headroom >= 1 ||
        throw(ArgumentError("spill_headroom must be at least 1, got $(spill_headroom)"))
    max_open_spill_files >= 1 || throw(
        ArgumentError(
            "max_open_spill_files must be at least 1, got $(max_open_spill_files)",
        ),
    )
    bytes = sum(filesize, raw_paths; init = 0)
    est = footprint_factor * bytes          # parsed structs + DataFrame + sort scratch
    est > mem_fraction * Sys.free_memory() && return _compact_spill(
        raw_paths,
        out_dir;
        format,
        dedup,
        tz,
        max_live_heap_mb,
        scratch_dir,
        spill_headroom,
        max_open_spill_files,
        compression,
    )
    trades = read_raw(raw_paths)
    if dedup
        n0 = length(trades)
        trades = deduplicate_trades(trades)
        n0 > length(trades) && @info "dropped duplicate prints" count = n0 - length(trades)
    end
    isempty(trades) && return String[]
    groups = Dict{Tuple{String,Date},Vector{Trade}}()
    for t in trades
        push!(get!(() -> Trade[], groups, (t.symbol, trading_date(t.time_ns; tz))), t)
    end
    return [
        _write_group(groups[k], out_dir, k[1], k[2], format, compression) for
        k in sort!(collect(keys(groups)))
    ]
end

"""
    compact_raw(cfg::Config, raw_paths; dedup = true, scratch_dir = nothing)
        -> Vector{String}

Compact into `cfg.processed_dir` with everything else taken from the
configuration: format, the provider's calendar, the heap ceiling and the spill
thresholds of `[limits]`. This is the form the pipeline and the scripts use, so
a capture is never filed under a calendar other than its provider's.
"""
compact_raw(
    cfg::Config,
    raw_paths::AbstractVector{<:AbstractString};
    dedup::Bool = true,
    scratch_dir::Union{Nothing,AbstractString} = nothing,
) = compact_raw(
    raw_paths,
    cfg.processed_dir;
    format = cfg.processed_format,
    compression = cfg.processed_compression == "none" ? nothing :
                  Symbol(cfg.processed_compression),
    dedup,
    mem_fraction = cfg.compact_mem_fraction,
    tz = cfg.exchange_tz,
    max_live_heap_mb = cfg.max_live_heap_mb,
    scratch_dir,
    footprint_factor = cfg.compact_footprint_factor,
    spill_headroom = cfg.spill_headroom,
    max_open_spill_files = cfg.max_open_spill_files,
)

# Where the spill copy goes. abspath(_, "..") rather than dirname: it
# normalizes a trailing separator, which dirname would otherwise read as
# "spill inside out_dir".
_spill_parent(out_dir::AbstractString, scratch_dir::Nothing) = abspath(out_dir, "..")
_spill_parent(::AbstractString, scratch_dir::AbstractString) = abspath(scratch_dir)

# Refuse to start a spill the scratch filesystem cannot hold. `free` is passed
# in rather than queried here so the refusal is testable without a full disk.
function _ensure_spill_space(
    parent::AbstractString,
    bytes::Real,
    free::Real;
    headroom::Real = SPILL_HEADROOM,
)
    gib(x) = round(x / 2^30; digits = 2)
    free < headroom * bytes && error(
        "spill compaction of $(gib(bytes)) GiB needs $(gib(headroom * bytes)) " *
        "GiB of scratch space, but $(parent) has only $(gib(free)) GiB free — " *
        "pass scratch_dir to place it on another filesystem",
    )
    return nothing
end

# Open spill files, bounded. A (symbol, day) group's file is opened in append
# mode on demand; when the pool is full the least recently written handle is
# closed, and reopened later if its group recurs. `seen` remembers every group
# ever written so the second pass can find its file.
mutable struct SpillPool
    dir::String
    capacity::Int
    handles::Dict{Tuple{String,Date},IOStream}
    last_use::Dict{Tuple{String,Date},Int}
    seen::Set{Tuple{String,Date}}
    clock::Int
end

SpillPool(dir::AbstractString, capacity::Integer) = SpillPool(
    String(dir),
    Int(capacity),
    Dict{Tuple{String,Date},IOStream}(),
    Dict{Tuple{String,Date},Int}(),
    Set{Tuple{String,Date}}(),
    0,
)

_spill_path(pool::SpillPool, key::Tuple{String,Date}) =
    joinpath(pool.dir, "$(key[1])_$(key[2]).jsonl")

function _spill_handle!(pool::SpillPool, key::Tuple{String,Date})
    pool.clock += 1
    pool.last_use[key] = pool.clock
    io = get(pool.handles, key, nothing)
    io === nothing || return io
    if length(pool.handles) >= pool.capacity
        victim = argmin(k -> pool.last_use[k], keys(pool.handles))
        close(pop!(pool.handles, victim))
    end
    push!(pool.seen, key)
    return pool.handles[key] = open(_spill_path(pool, key), "a")
end

function _close_all!(pool::SpillPool)
    foreach(close, values(pool.handles))
    empty!(pool.handles)
    return nothing
end

# Bounded-memory compaction: route each raw line to a per-(symbol, day)
# spill file in one streaming pass, then compact every group independently.
function _compact_spill(
    raw_paths::AbstractVector{<:AbstractString},
    out_dir::AbstractString;
    format::AbstractString,
    dedup::Bool,
    tz::TimeZone,
    max_live_heap_mb::Union{Nothing,Real},
    scratch_dir::Union{Nothing,AbstractString} = nothing,
    spill_headroom::Real = SPILL_HEADROOM,
    max_open_spill_files::Integer = 256,
    compression::Union{Nothing,Symbol} = nothing,
)
    bytes = sum(filesize, raw_paths; init = 0)
    mkpath(out_dir)
    parent = _spill_parent(out_dir, scratch_dir)
    mkpath(parent)
    _ensure_spill_space(
        parent,
        bytes,
        Base.diskstat(parent).available;
        headroom = spill_headroom,
    )
    @info "large input — using spill compaction" mib = round(bytes / 2^20; digits = 1) scratch =
        parent
    written = String[]
    mktempdir(parent; prefix = "compact_spill_") do spill
        pool = SpillPool(spill, max_open_spill_files)
        nbad = 0
        try
            for p in raw_paths, line in eachline(p)
                isempty(strip(line)) && continue
                t = try
                    json_to_trade(line)
                catch e
                    e isa InterruptException && rethrow()
                    nbad += 1
                    continue
                end
                key = (t.symbol, trading_date(t.time_ns; tz))
                println(_spill_handle!(pool, key), line)
            end
        finally
            _close_all!(pool)
        end
        nbad > 0 && @warn "skipped corrupt lines" count = nbad
        ndup = 0
        for key in sort!(collect(pool.seen))
            sym, date = key
            trades = read_raw(_spill_path(pool, key))
            if dedup
                n0 = length(trades)
                trades = deduplicate_trades(trades)
                ndup += n0 - length(trades)
            end
            push!(written, _write_group(trades, out_dir, sym, date, format, compression))
            max_live_heap_mb === nothing ||
                check_live_heap(max_live_heap_mb; context = "compaction $sym $date")
        end
        ndup > 0 && @info "dropped duplicate prints" count = ndup
    end
    return written
end

# First free backup name for `path`: "<base>_#1<ext>", "<base>_#2<ext>", …
function _backup_path(path::AbstractString)
    base, ext = splitext(path)
    n = 1
    while ispath("$(base)_#$(n)$(ext)")
        n += 1
    end
    return "$(base)_#$(n)$(ext)"
end

"""
    _safesave(write_fn, path) -> String

Write a file through `write_fn(tmp_path)` so that `path` always holds the
newest complete result and nothing already there is lost.

The content goes to `<base>.partial<ext>` in the same directory first (the
extension is kept last because writers such as `FileIO.save` infer the format
from it). If `path` already exists it is then preserved as the first free
`<base>_#N<ext>`, `N` from 1 — a hard link where the filesystem allows one, a
copy otherwise — and finally the partial file is renamed onto `path`. The
rename replaces atomically on one filesystem, so at every instant `path` is
either the previous complete file or the new complete one, never absent and
never half-written. Backups are numbered in order of displacement: `_#1` is
the oldest.

If `write_fn` throws, the partial file is removed, `path` is left untouched
and the exception propagates.

These are the semantics of DrWatson's `safesave` — newest data canonical,
prior results kept as numbered backups — without the dependency.
"""
function _safesave(write_fn, path::AbstractString)
    base, ext = splitext(path)
    partial = base * ".partial" * ext
    try
        write_fn(partial)
    catch
        rm(partial; force = true)
        rethrow()
    end
    ispath(path) && _link_or_copy(path, _backup_path(path))
    Base.Filesystem.rename(partial, path)
    return String(path)
end

# Hard links are refused by some filesystems (FAT, several network mounts);
# the backup is then a plain copy.
function _link_or_copy(src::AbstractString, dst::AbstractString)
    try
        hardlink(src, dst)
    catch e
        e isa Base.IOError || rethrow()
        cp(src, dst)
    end
    return dst
end
