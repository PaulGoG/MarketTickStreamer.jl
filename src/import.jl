# Vendor day files → the processed tree. One session per file, through the
# raw sink, compaction and the provenance sidecar a backfill uses, so an
# imported day is indistinguishable downstream from a captured one, except
# that its sidecar names the file it came from.

"""Formats `run_import` reads: Nasdaq TotalView-ITCH 5.0 day files and IEX HIST captures."""
const IMPORT_FORMATS = (:itch50, :iex)

"""
    run_import(cfg, path; format, date = nothing, symbols = cfg.symbols, broken = :drop,
               tape = "", keep_raw = true, replace = false) -> Vector{String}

Import one vendor day file into the processed tree, as one session: the
file's prints stream through a raw NDJSON part set (prefix
`import_<format>_<timestamp>`), the day is compacted per symbol on the
configured provider's calendar, and the sidecar records the file (name,
size, SHA-256, format) and the reader's statistics under `session.source`,
beside the usual provenance. Returns the processed files written.

`format` is `:itch50` ([`itch50_trades`](@ref)) or `:iex`
([`iex_hist_trades`](@ref)). For either format `date` is the session date,
read from the file name ([`itch50_date`](@ref), [`iex_hist_date`](@ref)) when
not given; ITCH timestamps count from midnight and are dated by it.
`symbols` defaults to the configured basket; `nothing` imports every symbol
in the file. `broken` and `tape` pass to the reader. With `keep_raw = false`
the day's raw parts are deleted once the day is under `processed/`, the
checksummed vendor file being the record, as `backfill.keep_raw` does for a
bulk day.

A file that ends inside a message, as a partial download does, is imported
to that point and logged as incomplete; the sidecar's `source.truncated`
says so. The configured provider contributes its calendar and timestamp
unit only; `"alpaca"` is the host for both US-equity formats.

The processed tree is keyed by symbol and day with no source dimension, so
an imported day is filed where a capture of that day would be. A source
therefore gets a data tree of its own, and the import refuses to write a
(symbol, day) that already exists in `processed/` unless `replace = true`,
which displaces the existing files into safesave backups.
"""
function run_import(
    cfg::Config,
    path::AbstractString;
    format::Symbol,
    date::Union{Nothing,Date} = nothing,
    symbols::Union{Nothing,AbstractVector{<:AbstractString}} = cfg.symbols,
    broken::Symbol = :drop,
    tape = "",
    keep_raw::Bool = true,
    replace::Bool = false,
)
    format in IMPORT_FORMATS || throw(
        ArgumentError(
            "format must be one of $(join(map(repr, IMPORT_FORMATS), ", ")), got $(repr(format))",
        ),
    )
    isfile(path) || throw(ArgumentError("no such file: $path"))
    sid = "import_$(format)_$(Dates.format(Dates.now(UTC), SESSION_STAMP))"
    logger, log_io = setup_logging(cfg; session_id = sid)
    try
        return Logging.with_logger(logger) do
            _run_import(
                cfg,
                sid,
                path,
                format,
                date,
                symbols,
                broken,
                tape,
                keep_raw,
                replace,
            )
        end
    finally
        log_io === nothing || close(log_io)
    end
end

function _run_import(
    cfg::Config,
    sid::AbstractString,
    path::AbstractString,
    format::Symbol,
    date::Union{Nothing,Date},
    symbols::Union{Nothing,AbstractVector{<:AbstractString}},
    broken::Symbol,
    tape,
    keep_raw::Bool,
    replace::Bool,
)
    day = _import_day(path, format, date)
    clash = _import_collisions(cfg, day, symbols)
    isempty(clash) ||
        replace ||
        throw(
            ArgumentError(
                "processed files of $day exist for $(join(clash, ", ")) in " *
                "$(cfg.processed_dir): an imported day belongs in a data tree of its own, " *
                "or pass replace = true to displace them into safesave backups",
            ),
        )
    return _with_session(
        cfg,
        sid;
        on_interrupt = "interrupt; compacted output kept, sidecar marked interrupted",
    ) do summary, start_meta
        processed = String[]
        summary.result = processed
        summary.ticks_processed = 0
        summary.source = _import_source(path, format, nothing)
        stats = nothing
        start_meta()
        sink = open_raw_sink(cfg.raw_dir, sid; max_mb = cfg.max_raw_file_mb)
        sink_page =
            page -> begin
                write_batch!(sink, page)
                check_live_heap(
                    cfg.max_live_heap_mb;
                    context = "import $format $(basename(path))",
                )
            end
        try
            stats = if format === :itch50
                itch50_trades(path, day; symbols, broken, each_page = sink_page)
            else
                iex_hist_trades(path; symbols, broken, tape, each_page = sink_page)
            end
        finally
            close_sink!(sink)
            sink.n_written == 0 && filesize(sink.path) == 0 && rm(sink.path; force = true)
        end
        _import_stats!(summary.source, stats)
        summary.ticks = stats.prints
        files = _raw_files_with_prefix(cfg.raw_dir, sid)
        if summary.ticks == 0
            for f in files
                filesize(f) == 0 && rm(f; force = true)
            end
            @warn "no prints read from the file" file = basename(path)
        else
            c = _compact_raw(cfg, files)
            append!(processed, c.files)
            summary.ticks_processed = c.rows
            keep_raw || foreach(f -> rm(f; force = true), files)
        end
        stats.truncated &&
            @warn "the file ended inside a message; the day is incomplete" file =
                basename(path)
        @info "imported" format file = basename(path) messages = stats.messages prints =
            summary.ticks stored = summary.ticks_processed broken = stats.broken
        @info "import finished" status = summary.status files = length(processed) prints =
            summary.ticks stored = summary.ticks_processed
        return processed
    end
end

# The vendor file as the sidecar records it: identity and checksum, computed
# once before the reader runs, and what the reader counted, merged in by
# `_import_stats!` when it returns. `stats` is `nothing` when the reader never ran.
function _import_source(path::AbstractString, format::Symbol, stats)
    src = Dict{String,Any}(
        "file" => basename(path),
        "bytes" => filesize(path),
        "sha256" => _sha256_hex(path),
        "format" => string(format),
    )
    return stats === nothing ? src : _import_stats!(src, stats)
end

function _import_stats!(src::AbstractDict, stats)
    for (k, v) in pairs(stats)
        k === :trades && continue
        src[String(k)] = v
    end
    return src
end

# The session date of a vendor file: as given, or read from its name.
_import_day(path::AbstractString, format::Symbol, date::Union{Nothing,Date}) =
    date !== nothing ? date :
    format === :itch50 ? itch50_date(basename(path)) : iex_hist_date(basename(path))

# The symbols whose processed day already exists in the tree: the import
# would displace them. Every symbol directory is checked when the import is
# unrestricted.
function _import_collisions(cfg::Config, day::Date, symbols)
    syms = if symbols === nothing
        isdir(cfg.processed_dir) ?
        filter(s -> isdir(joinpath(cfg.processed_dir, s)), readdir(cfg.processed_dir)) :
        String[]
    else
        String.(symbols)
    end
    return [s for s in syms if _processed_exists(cfg, s, day)]
end
