# Corpus query path over the processed tree: what it holds, iteration over a
# selection one trading day at a time, and a materializing read behind a
# memory guard. Every day loads in full before its filters apply, so the
# memory bound is the busiest selected date, the same bound replay has.

"""
    BYTES_PER_PRINT

In-memory size of one `Trade` loaded from a processed day file, strings and
shared condition list included: 102 B, measured on the busiest day of a
one-year AAPL capture (2.21 M prints in 215 MiB). The memory guard of
[`read_processed`](@ref) multiplies it by the row count of a selection.
"""
const BYTES_PER_PRINT = 102

# Processed day files grouped by the date in their name, in date order, each
# group in the order the paths were given. One group holds every symbol of a
# day. Safesave backups and partial files are rejected by name, missing files
# by existence.
function _days_from_files(paths::AbstractVector{<:AbstractString})
    groups = Dict{Date,Vector{String}}()
    for p in paths
        m = match(PROCESSED_FILE, basename(p))
        m === nothing && throw(
            ArgumentError(
                "not a processed day file (YYYY-MM-DD.csv|.arrow): $p — safesave " *
                "backups and partial files are not read",
            ),
        )
        isfile(p) || throw(ArgumentError("no such file: $p"))
        push!(get!(() -> String[], groups, Date(something(m[1]))), String(p))
    end
    return [(d, groups[d]) for d in sort!(collect(keys(groups)))]
end

# Row count of one processed day file without parsing its rows: Arrow from
# the file's own metadata (the table is memory-mapped, not loaded), CSV from
# its line count less the header.
_processed_rows(path::AbstractString) =
    endswith(path, ".arrow") ? Int(length(Arrow.Table(path).time_ns)) :
    max(countlines(path) - 1, 0)

# UTC nanosecond bounds [lo, hi) of a window of exchange-local hours on
# `date`: local midnight plus the hours, resolved through the zone for that
# date, so a transition day places 09:30 where its clock said 09:30 and a
# full day spans 23 or 25 hours on the days the clock moves. Integer
# millisecond arithmetic throughout; a Float64 of seconds would carry a
# 256 ns resolution at this epoch.
function _window_ns(date::Date, window::Tuple{Float64,Float64}, tz::TimeZone)
    at(h) = begin
        local_dt = DateTime(date) + Millisecond(round(Int64, h * 3_600_000))
        utc_dt = DateTime(astimezone(ZonedDateTime(local_dt, tz), tz"UTC"))
        Int64(Dates.value(utc_dt - DateTime(1970, 1, 1))) * 1_000_000
    end
    return (at(window[1]), at(window[2]))
end

# `window` as the iterator stores it; `nothing` is the whole day.
function _check_window(window::Union{Nothing,Tuple{<:Real,<:Real}})
    window === nothing && return nothing
    lo, hi = Float64(window[1]), Float64(window[2])
    (0 <= lo < hi <= 24) || throw(
        ArgumentError(
            "window must satisfy 0 <= open < close <= 24 in exchange-local hours, got $(window)",
        ),
    )
    return (lo, hi)
end

"""
    ProcessedDays

The trading days of a selection of processed files, as [`eachday`](@ref)
returns them: an iterator whose elements are `(date, trades)` pairs,
`Tuple{Date,Vector{Trade}}`, in date order. A day is loaded when the
iteration reaches it and released when the consumer drops it, so iterating
a symbol-year costs the memory of its busiest day. `length` is the number of
days in the selection, including days that are empty after filtering.
"""
struct ProcessedDays{Z<:TimeZone}
    days::Vector{Tuple{Date,Vector{String}}}
    window::Union{Nothing,Tuple{Float64,Float64}}
    tz::Z
    price_forming::Bool
    non_price::Dict{String,Vector{String}}
end

Base.length(it::ProcessedDays) = length(it.days)
Base.eltype(::Type{<:ProcessedDays}) = Tuple{Date,Vector{Trade}}
Base.IteratorSize(::Type{<:ProcessedDays}) = Base.HasLength()
Base.IteratorEltype(::Type{<:ProcessedDays}) = Base.HasEltype()

function Base.iterate(it::ProcessedDays, i::Int = 1)
    i > length(it.days) && return nothing
    date, files = it.days[i]
    return ((date, _load_day(it, date, files)), i + 1)
end

Base.show(io::IO, it::ProcessedDays) = print(
    io,
    "ProcessedDays(",
    length(it.days),
    " days, ",
    sum(length(last(d)) for d in it.days; init = 0),
    " files)",
)

# The prints of one day: every file of the day concatenated, merged on
# exchange time when there is more than one, then the window and the
# population applied in that order (an integer comparison before a
# dictionary lookup).
function _load_day(it::ProcessedDays, date::Date, files::Vector{String})
    trades = reduce(vcat, (_trades_from_processed(f) for f in files); init = Trade[])
    issorted(trades; by = t -> t.time_ns) ||
        sort!(trades; by = t -> t.time_ns, alg = MergeSort)
    if it.window !== nothing
        lo, hi = _window_ns(date, it.window, it.tz)
        filter!(t -> lo <= t.time_ns < hi, trades)
    end
    it.price_forming && filter!(t -> price_forming(t; non_price = it.non_price), trades)
    return trades
end

# The processed files of `symbols` over [from, to], symbol by symbol in the
# order given; a String is one symbol.
function _selected_files(
    processed_dir::AbstractString,
    symbols::Union{AbstractString,AbstractVector{<:AbstractString}},
    from::Union{Nothing,Date},
    to::Union{Nothing,Date},
)
    syms = symbols isa AbstractString ? [String(symbols)] : collect(String, symbols)
    isempty(syms) && throw(ArgumentError("symbols is empty"))
    return reduce(
        vcat,
        (processed_files(processed_dir, s; from, to) for s in syms);
        init = String[],
    )
end

"""
    eachday(paths; window = nothing, tz = tz"America/New_York",
            price_forming = false, non_price = NON_PRICE_CONDITIONS) -> ProcessedDays
    eachday(processed_dir, symbols; from = nothing, to = nothing, kwargs...)
    eachday(cfg::Config, symbols = cfg.symbols; from = nothing, to = nothing,
            window = nothing, price_forming = false)

Iterate a selection of the processed tree one trading day at a time, as
`(date, trades)` pairs in date order ([`ProcessedDays`](@ref)). `paths` are
processed day files (`YYYY-MM-DD.csv|.arrow`, e.g. from
[`processed_files`](@ref)); the second form selects them for one symbol or a
vector of symbols over `[from, to]`; the third takes the tree, the calendar
and the condition lists from a configuration. The files of one date — one
per symbol — are loaded together and merged on `time_ns`, so a day holds
every requested symbol in exchange-time order. Memory is bounded by the
busiest selected date at about [`BYTES_PER_PRINT`](@ref) per print.

- `window = (open, close)`: keep the prints whose exchange time falls in
  `[open, close)` on the exchange-local clock of each day, in fractional
  hours — `(9.5, 16.0)` is the US regular session, and a provider's
  `regular_session` plugs in as it is. The bounds are resolved through `tz`
  for every date, so they stay correct across daylight-saving transitions.
  `nothing` keeps the whole day. The `Config` form also accepts
  `window = :regular`, the provider's regular session.
- `tz`: the calendar the window is read on; the `Config` form uses the
  provider's.
- `price_forming = true`: keep the price-forming population only
  ([`price_forming`](@ref)), judged against `non_price`; the `Config` form
  uses `[quality.non_price_conditions]`.

A day that is empty after filtering is still yielded, with an empty vector.
Throws `ArgumentError` for a path that is not a processed day file, a
missing file, a symbol without a directory, an empty symbol list or a
window outside `0 <= open < close <= 24`.

    for (date, trades) in eachday(cfg, "AAPL"; from = Date(2026, 5, 1), window = :regular)
        # ... one regular session at a time
    end
"""
function eachday(
    paths::AbstractVector{<:AbstractString};
    window::Union{Nothing,Tuple{<:Real,<:Real}} = nothing,
    tz::TimeZone = tz"America/New_York",
    price_forming::Bool = false,
    non_price::Dict{String,Vector{String}} = NON_PRICE_CONDITIONS,
)
    return ProcessedDays(
        _days_from_files(paths),
        _check_window(window),
        tz,
        price_forming,
        non_price,
    )
end

function eachday(
    processed_dir::AbstractString,
    symbols::Union{AbstractString,AbstractVector{<:AbstractString}};
    from::Union{Nothing,Date} = nothing,
    to::Union{Nothing,Date} = nothing,
    kwargs...,
)
    return eachday(_selected_files(processed_dir, symbols, from, to); kwargs...)
end

function eachday(
    cfg::Config,
    symbols::Union{AbstractString,AbstractVector{<:AbstractString}} = cfg.symbols;
    from::Union{Nothing,Date} = nothing,
    to::Union{Nothing,Date} = nothing,
    window::Union{Nothing,Symbol,Tuple{<:Real,<:Real}} = nothing,
    price_forming::Bool = false,
)
    return eachday(
        cfg.processed_dir,
        symbols;
        from,
        to,
        window = _config_window(cfg, window),
        tz = cfg.exchange_tz,
        price_forming,
        non_price = cfg.non_price_conditions,
    )
end

# `:regular` is the provider's regular session; any other symbol is refused.
_config_window(::Config, window::Union{Nothing,Tuple{<:Real,<:Real}}) = window
function _config_window(cfg::Config, window::Symbol)
    window === :regular && return provider_spec(cfg.provider).regular_session
    throw(
        ArgumentError(
            "window must be :regular, a tuple of hours or nothing, got $(repr(window))",
        ),
    )
end

"""
    read_processed(paths; mem_fraction = 0.5, kwargs...) -> Vector{Trade}
    read_processed(processed_dir, symbols; from = nothing, to = nothing,
                   mem_fraction = 0.5, kwargs...)
    read_processed(cfg::Config, symbols = cfg.symbols;
                   mem_fraction = cfg.read_mem_fraction, kwargs...)

The prints of a selection of the processed tree in one vector, in date order
and in exchange-time order within each day: [`eachday`](@ref) concatenated,
with the same selection keywords (`from`, `to`, `window`, `tz`,
`price_forming`, `non_price`). This is the form for methods that need the
whole sample in memory.

The read is guarded: it refuses a selection whose prints would occupy more
than `mem_fraction` of the free memory at [`BYTES_PER_PRINT`](@ref) each,
with an error that names [`eachday`](@ref). Without a window or population
filter the refusal comes before any row is read, from the files' row counts;
with a filter the vector is checked as it grows, day by day. The `Config`
form takes `mem_fraction` from `limits.read_mem_fraction`.

    trades = read_processed(cfg, "AAPL"; from = Date(2026, 5, 1), to = Date(2026, 5, 31),
                            window = :regular, price_forming = true)
"""
function read_processed(
    paths::AbstractVector{<:AbstractString};
    mem_fraction::Real = 0.5,
    kwargs...,
)
    0 < mem_fraction <= 1 ||
        throw(ArgumentError("mem_fraction must be in (0, 1], got $(mem_fraction)"))
    it = eachday(paths; kwargs...)
    budget = mem_fraction * Sys.free_memory()
    refuse(n) = error(
        "read_processed: $n prints ≈ $(round(Int, n * BYTES_PER_PRINT / 2^20)) MiB in " *
        "memory exceed mem_fraction = $(mem_fraction) of free memory " *
        "($(round(Int, Sys.free_memory() / 2^20)) MiB); iterate eachday instead or " *
        "narrow the selection",
    )
    out = Trade[]
    if it.window === nothing && !it.price_forming
        n = sum(_processed_rows(f) for (_, files) in it.days for f in files; init = 0)
        n * BYTES_PER_PRINT > budget && refuse(n)
        sizehint!(out, n)
    end
    for (_, trades) in it
        append!(out, trades)
        length(out) * BYTES_PER_PRINT > budget && refuse(length(out))
    end
    return out
end

function read_processed(
    processed_dir::AbstractString,
    symbols::Union{AbstractString,AbstractVector{<:AbstractString}};
    from::Union{Nothing,Date} = nothing,
    to::Union{Nothing,Date} = nothing,
    kwargs...,
)
    return read_processed(_selected_files(processed_dir, symbols, from, to); kwargs...)
end

function read_processed(
    cfg::Config,
    symbols::Union{AbstractString,AbstractVector{<:AbstractString}} = cfg.symbols;
    from::Union{Nothing,Date} = nothing,
    to::Union{Nothing,Date} = nothing,
    window::Union{Nothing,Symbol,Tuple{<:Real,<:Real}} = nothing,
    price_forming::Bool = false,
    mem_fraction::Real = cfg.read_mem_fraction,
)
    return read_processed(
        cfg.processed_dir,
        symbols;
        from,
        to,
        window = _config_window(cfg, window),
        tz = cfg.exchange_tz,
        price_forming,
        non_price = cfg.non_price_conditions,
        mem_fraction,
    )
end

"""
    corpus_inventory(processed_dir; symbols = nothing) -> DataFrame
    corpus_inventory(cfg::Config; symbols = nothing)

What a processed tree holds, one row per symbol, sorted by symbol:
`symbol`, `format` (`"csv"`, `"arrow"` or `"mixed"`), `n_days`,
`first_date`, `last_date`, `n_prints`, `bytes`. Row counts come from the
files' own metadata (Arrow) or line counts (CSV), so no print is parsed;
safesave backups and partial files are not counted. `symbols` restricts the
rows to those symbols and throws `ArgumentError` for one without a
directory; `nothing` lists every symbol directory holding at least one day
file. A symbol with no day file gets no row. Throws `ArgumentError` when
`processed_dir` does not exist.
"""
function corpus_inventory(
    processed_dir::AbstractString;
    symbols::Union{Nothing,AbstractString,AbstractVector{<:AbstractString}} = nothing,
)
    isdir(processed_dir) || throw(ArgumentError("no processed tree at $processed_dir"))
    syms =
        symbols === nothing ?
        filter(s -> isdir(joinpath(processed_dir, s)), readdir(processed_dir)) :
        symbols isa AbstractString ? [String(symbols)] : collect(String, symbols)
    inv = DataFrame(
        symbol = String[],
        format = String[],
        n_days = Int[],
        first_date = Date[],
        last_date = Date[],
        n_prints = Int[],
        bytes = Int[],
    )
    for s in sort!(unique(syms))
        files = processed_files(processed_dir, s)
        isempty(files) && continue
        exts = unique(endswith(f, ".arrow") ? "arrow" : "csv" for f in files)
        # processed_files lists only names that match, so neither `something`
        # can throw; they narrow the types for inference.
        dates = [
            Date(something(something(match(PROCESSED_FILE, basename(f)))[1])) for f in files
        ]
        push!(
            inv,
            (
                s,
                length(exts) == 1 ? only(exts) : "mixed",
                length(files),
                minimum(dates),
                maximum(dates),
                sum(_processed_rows, files; init = 0),
                sum(filesize, files; init = 0),
            ),
        )
    end
    return inv
end

corpus_inventory(cfg::Config; kwargs...) = corpus_inventory(cfg.processed_dir; kwargs...)
