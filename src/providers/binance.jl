# Binance spot adapter: public REST (aggregate trades) and the spot market
# data WebSocket streams. No credentials — spot market data is public.
#
# Wire references:
#   https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
#   https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
# Live trade frame:    {"e":"trade","E":ms,"s":SYM,"t":id,"p":"px","q":"qty",
#                       "T":ms,"m":bool,"M":bool}
# Live aggTrade frame: {"e":"aggTrade","E":ms,"s":SYM,"a":id,"p":"px","q":"qty",
#                       "f":first,"l":last,"T":ms,"m":bool,"M":bool}
# Daily bulk files:    {bulk_base}/data/spot/daily/{aggTrades|trades}/SYM/
#                      SYM-{kind}-YYYY-MM-DD.zip, with SYM-....zip.CHECKSUM
#                      (sha256). One CSV entry, no header. aggTrades columns:
#                      agg_trade_id, price, quantity, first_trade_id,
#                      last_trade_id, transact_time, is_buyer_maker,
#                      is_best_match; trades columns: trade_id, price, qty,
#                      quote_qty, time, is_buyer_maker, is_best_match.
#                      Booleans "True"/"False"; timestamps in milliseconds
#                      before 2025-01-01 and microseconds from it.
#
# The adapter differs from the equity one in three respects:
#   * the venue never closes, so there are no market-hours guards and the
#     calendar is UTC;
#   * prices and quantities arrive as STRINGS, and timestamps in
#     MILLISECONDS by default — this adapter asks for MICROSECONDS (REST
#     header X-MBX-TIME-UNIT, stream parameter timeUnit) and reads either
#     unit by magnitude, against this package's Int64 nanoseconds;
#   * there is no consolidated tape and no sale conditions, but there is an
#     aggressor side, which is the informative flag a crypto print carries.

"""
    BinanceProvider

Connection descriptor for Binance spot market data. Endpoint roots
(`rest_base`, `ws_base`, `bulk_base`) are overridable through the `[binance]`
config table, which is how the test suite points the client at a local mock
server.

`feed` selects the trade granularity: `"trade"` is every execution,
`"aggTrade"` aggregates executions that filled at one price from a single
taker order. They are different populations — see [`historical_trades`](@ref).
`rest` bounds its REST requests ([`RestPolicy`](@ref)).
"""
struct BinanceProvider <: AbstractProvider
    feed::String
    rest_base::String
    ws_base::String
    bulk_base::String
    rest::RestPolicy
end

"""Root of the venue's daily bulk files (`[binance] bulk_base`)."""
const BINANCE_BULK_BASE = "https://data.binance.vision"

BinanceProvider(feed::AbstractString, rest_base::AbstractString, ws_base::AbstractString) =
    BinanceProvider(feed, rest_base, ws_base, BINANCE_BULK_BASE, RestPolicy())

BinanceProvider(
    feed::AbstractString,
    rest_base::AbstractString,
    ws_base::AbstractString,
    bulk_base::AbstractString,
) = BinanceProvider(feed, rest_base, ws_base, bulk_base, RestPolicy())

function BinanceProvider(cfg::Config)
    ep = cfg.endpoints
    return BinanceProvider(
        cfg.feed,
        get(ep, "rest_base", "https://api.binance.com"),
        get(ep, "ws_base", "wss://stream.binance.com:9443"),
        get(ep, "bulk_base", BINANCE_BULK_BASE),
        RestPolicy(cfg),
    )
end

# Spot trades around the clock, every day, on the UTC calendar. A crypto venue
# has no local trading day to speak of, and UTC is the convention its own
# timestamps and daily data dumps use.
provider_spec(::Val{:binance}) = ProviderSpec(
    ["trade", "aggTrade"],
    ["aggTrade"],
    ["aggTrade", "trade"],
    tz"UTC",
    false,
    1_000,
    (0.0, 24.0),
    "quote asset",
    "base asset",
    "microsecond",
)

make_provider(::Val{:binance}, cfg::Config, ::AbstractString, ::AbstractString) =
    BinanceProvider(cfg)

exchange_tz(::BinanceProvider) = provider_spec(Val(:binance)).tz
always_open(::BinanceProvider) = true

# Stream names are lower-case; REST symbols are upper-case.
_stream_name(p::BinanceProvider, sym::AbstractString) = "$(lowercase(sym))@$(p.feed)"

"""
    ws_url(p::BinanceProvider, symbols) -> String

Combined-stream URL for `symbols`. Binance subscribes through the URL rather
than through a post-connection message, so there is no auth or subscribe
handshake: the connection *is* the subscription. `timeUnit=MICROSECOND` asks
for microsecond timestamps; frames in either unit are read.
"""
ws_url(p::BinanceProvider, symbols::Vector{String}) =
    "$(p.ws_base)/stream?streams=" *
    join((_stream_name(p, s) for s in symbols), "/") *
    "&timeUnit=MICROSECOND"

"""
    market_clock(p::BinanceProvider) -> (; is_open, next_open, next_close)

Always open. `next_open` and `next_close` are empty because the venue has
neither; [`always_open`](@ref) is `true` for this provider, so the session
orchestration never asks for them.
"""
market_clock(::BinanceProvider) = (; is_open = true, next_open = "", next_close = "")

# Binance sends numbers as strings ("p":"64000.01"). Accepting a bare number
# as well keeps the parser working against mocks and any future shape change,
# and fails loudly rather than silently yielding NaN.
_num(x::AbstractString) = parse(Float64, x)
_num(x::Real) = Float64(x)

# The venue's timestamps are milliseconds by default and microseconds on
# request, and its bulk files carry microseconds since 2025-01-01. The
# magnitude tells the units apart: 10^14 ms is the year 5138, 10^14 µs was
# 1973.
const BINANCE_MICROSECOND_FLOOR = 100_000_000_000_000
_binance_ns(raw::Integer) =
    raw < BINANCE_MICROSECOND_FLOOR ? Int64(raw) * 1_000_000 : Int64(raw) * 1_000

# The aggressor side, which is what `m` encodes: `m = true` means the BUYER
# sat on the book as maker, so the seller crossed the spread and the print is
# seller-initiated. Carried in `conditions` because it is the one piece of
# per-print classification a crypto venue reports, and losing it would discard
# the trade sign that order-flow work is built on. Spelled out rather than
# abbreviated so it cannot be mistaken for a CTA/UTP condition code.
_aggressor(m::Bool) = m ? ["sell"] : ["buy"]

# Trade and aggTrade frames differ only in the id field (`t` vs `a`), and the
# REST rows differ again (`id`/`price`/`qty`/`time`/`isBuyerMaker`).
function parse_binance_trade(msg, recv_ns::Int64; symbol::AbstractString = "")
    sym = isempty(symbol) ? String(msg.s::AbstractString) : String(symbol)
    id = haskey(msg, :t) ? msg.t : get(msg, :a, 0)
    return Trade(
        sym,
        _binance_ns(msg.T::Integer),
        recv_ns,
        _num(msg.p),
        _num(msg.q),
        "BINANCE",
        _aggressor(Bool(msg.m::Bool)),
        "SPOT",
        Int64(id::Integer),
    )
end

# Microsecond timestamps on request; the venue's default is milliseconds.
const BINANCE_REST_HEADERS = ["X-MBX-TIME-UNIT" => "MICROSECOND"]

"""
    historical_trades(p::BinanceProvider, symbol, start_date, end_date;
                      feed = "aggTrade", page_limit = 1000, rate_sleep_s = 0.1,
                      on_page = nothing, each_page = nothing)

Download aggregate trades for `symbol` over `[start_date, end_date]`
(inclusive UTC dates) from `/api/v3/aggTrades`. Signature and semantics match
the Alpaca adapter's, so the backfill pipeline is provider-agnostic:
`each_page` streams pages and returns only the row count, otherwise every
trade is accumulated.

**Pagination is by trade id, not by time window.** Binance rejects a
`startTime`/`endTime` pair spanning an hour or more, so the range is seeded
with a single `startTime` request and then walked forward with
`fromId = last id + 1` until a row's timestamp passes the end of the range.
Walking hour-wide windows instead would issue 24 requests a day and still
truncate any hour holding more than `page_limit` trades, silently.
`page_limit` above the venue maximum of 1000 is rejected here, because the
server would clamp it silently and the short-page termination rule would then
end the walk after the first page.

Rows are requested at microsecond resolution (`X-MBX-TIME-UNIT:
MICROSECOND`), the clock of the bulk files, so a REST day and a bulk day
agree print for print; a server answering in milliseconds is read all the
same. `startTime` is sent in milliseconds, which the venue accepts in either
unit.

**`aggTrade` is not the tape.** Binance aggregates executions that filled at
one price from one taker order into a single row, so an aggregate trade is a
taker order's fill, not an execution. Counts and inter-arrival times are
therefore not comparable with a raw `trade` stream, and the choice belongs to
the analysis — as with odd lots on an equity tape. Only `aggTrade` has a
time-seekable public endpoint, which is why it is the backfill feed. The
venue's daily bulk files carry both feeds, every execution included: see
[`bulk_trades`](@ref).
"""
function historical_trades(
    p::BinanceProvider,
    symbol::AbstractString,
    start_date::Date,
    end_date::Date;
    feed::AbstractString = "aggTrade",
    page_limit::Integer = 1000,
    rate_sleep_s::Real = 0.1,
    on_page = nothing,
    each_page = nothing,
)
    feed == "aggTrade" || throw(
        ArgumentError(
            "Binance backfill supports only the \"aggTrade\" feed, got \"$feed\": " *
            "/api/v3/trades and /api/v3/historicalTrades cannot seek by time",
        ),
    )
    max_limit = provider_spec(Val(:binance)).max_page_limit
    # The server clamps an over-limit request to its maximum and answers 200,
    # so the short-page test below would read the clamped page as the end of
    # the tape and truncate the day after one request.
    1 <= page_limit <= max_limit || throw(
        ArgumentError("page_limit must be in 1:$max_limit for Binance, got $page_limit"),
    )
    sym = uppercase(String(symbol))
    url = "$(p.rest_base)/api/v3/aggTrades"
    start_ns = exchange_day_start_ns(p, start_date)
    stop_ns = exchange_day_start_ns(p, end_date + Day(1)) - 1

    acc = each_page === nothing ? Trade[] : nothing
    total = 0
    # Seed on time; continue on id. Mixing the two in one request is what the
    # documentation warns against, and what the hour limit applies to.
    query = Dict{String,String}(
        "symbol" => sym,
        "startTime" => string(fld(start_ns, 1_000_000)),
        "limit" => string(page_limit),
    )
    while true
        resp = _get_with_retry(url, BINANCE_REST_HEADERS; query, policy = p.rest)
        rows = JSON.parse(resp.body)::AbstractVector
        isempty(rows) && break
        page = Trade[]
        last_id = 0
        done = false
        for row in rows
            t = parse_binance_trade(row, Int64(0); symbol = sym)
            last_id = t.id
            if t.time_ns > stop_ns
                done = true
                break
            end
            push!(page, t)
        end
        if !isempty(page)
            total += length(page)
            acc === nothing || append!(acc, page)
            each_page === nothing || each_page(page)
            on_page === nothing || on_page(length(page), total)
        end
        # A short page means the tape is exhausted, not that the range is.
        (done || length(rows) < page_limit) && break
        delete!(query, "startTime")
        query["fromId"] = string(last_id + 1)
        sleep(rate_sleep_s)
    end
    return acc === nothing ? total : acc
end

# Vendor path segment of a feed's daily dump.
_bulk_kind(feed::AbstractString) =
    feed == "aggTrade" ? "aggTrades" :
    feed == "trade" ? "trades" :
    throw(
        ArgumentError(
            "Binance publishes bulk files for \"aggTrade\" and \"trade\", got \"$feed\"",
        ),
    )

"""
    bulk_url(p::BinanceProvider, symbol, date; feed = "aggTrade") -> String

URL of the daily spot dump of `symbol` on UTC `date`:
`{bulk_base}/data/spot/daily/{aggTrades|trades}/{SYM}/{SYM}-{kind}-{date}.zip`.
The venue's checksum of the file is at the same URL with `.CHECKSUM`
appended.
"""
function bulk_url(
    p::BinanceProvider,
    symbol::AbstractString,
    date::Date;
    feed::AbstractString = "aggTrade",
)
    kind = _bulk_kind(feed)
    sym = uppercase(String(symbol))
    return "$(p.bulk_base)/data/spot/daily/$kind/$sym/$sym-$kind-$date.zip"
end

# The venue's `.CHECKSUM` body is `<sha256 hex>  <file name>`.
function _expected_sha256(checksum_path::AbstractString)
    tokens = split(strip(read(checksum_path, String)))
    (isempty(tokens) || length(first(tokens)) != 64) &&
        error("malformed checksum file: $checksum_path")
    return lowercase(String(first(tokens)))
end

_sha256_hex(path::AbstractString) = bytes2hex(open(sha256, path))

# The day's dump and its checksum, fetched into `dir` unless a verified copy
# is already there. The checksum is fetched first: it is small, and its
# absence (404) is the venue saying the day is not published. Both go through
# the chunked downloader, whose per-chunk timeout lets a large dump arrive
# over a slow link (a single request is cut off at the policy's timeout,
# whatever its progress). Returns the zip path, or `nothing` for an
# unpublished day.
function _fetch_bulk(p::BinanceProvider, url::AbstractString, dir::AbstractString)
    mkpath(dir)
    path = joinpath(dir, basename(url))
    sums = path * ".CHECKSUM"
    isfile(path) &&
        isfile(sums) &&
        _sha256_hex(path) == _expected_sha256(sums) &&
        return path
    _download_resumable(url * ".CHECKSUM", sums, p.rest).found || return nothing
    _download_resumable(url, path, p.rest).found ||
        error("bulk file listed with a checksum but missing: $url")
    want = _expected_sha256(sums)
    got = _sha256_hex(path)
    if got != want
        rm(path; force = true)
        error(
            "checksum mismatch for $(basename(path)): expected $want, got $got — " *
            "the download was discarded",
        )
    end
    return path
end

# One row of a dump. `cols` is 8 for aggTrades (id, price, quantity, first
# id, last id, time, buyer-is-maker, best match) and 7 for trades (id, price,
# quantity, quote quantity, time, buyer-is-maker, best match). Timestamps in
# either unit, see `_binance_ns`.
function _bulk_trade(fields::AbstractVector{<:AbstractString}, sym::String, cols::Int)
    ts_col, maker_col = cols == 8 ? (6, 7) : (5, 6)
    ns = _binance_ns(parse(Int64, fields[ts_col]))
    maker = lowercase(fields[maker_col]) in ("true", "1")
    return Trade(
        sym,
        ns,
        0,
        parse(Float64, fields[2]),
        parse(Float64, fields[3]),
        "BINANCE",
        _aggressor(maker),
        "SPOT",
        parse(Int64, fields[1]),
    )
end

# The rows of a dump's CSV as pages of trades. A header line, should the
# venue add one, is skipped. A wrong column count or a row outside `date` is
# an error: a print filed under the wrong day would corrupt that day without
# a trace.
function _parse_bulk_csv(
    csv::AbstractVector{UInt8},
    sym::String,
    date::Date,
    feed::AbstractString;
    each_page = nothing,
    page_rows::Integer = 100_000,
    name::AbstractString = "",
)
    cols = feed == "aggTrade" ? 8 : 7
    day_lo = round(Int64, datetime2unix(DateTime(date))) * NS_PER_SEC
    day_hi = day_lo + 86_400 * NS_PER_SEC
    acc = each_page === nothing ? Trade[] : nothing
    page = Trade[]
    total = 0
    first_line = true
    for line in eachline(IOBuffer(csv))
        isempty(line) && continue
        fields = split(line, ',')
        if first_line
            first_line = false
            all(isdigit, fields[1]) || continue
        end
        length(fields) == cols || error(
            "$name: expected $cols columns for feed \"$feed\", got $(length(fields)): $line",
        )
        t = _bulk_trade(fields, sym, cols)
        day_lo <= t.time_ns < day_hi || error(
            "$name: a row dated $(trading_date(t.time_ns; tz = tz"UTC")) in the file of $date",
        )
        push!(page, t)
        if length(page) >= page_rows
            total += length(page)
            acc === nothing ? each_page(page) : append!(acc, page)
            page = Trade[]
        end
    end
    if !isempty(page)
        total += length(page)
        acc === nothing ? each_page(page) : append!(acc, page)
    end
    return acc === nothing ? total : acc
end

"""
    bulk_trades(p::BinanceProvider, symbol, date; feed = "aggTrade", dest_dir,
                each_page = nothing, page_rows = 100_000) -> Vector{Trade} | Int

The trades of `symbol` on UTC `date` from the venue's daily dump
([`bulk_url`](@ref)), the route to a large corpus: one file per day, both
feeds, no pagination. `feed = "aggTrade"` is the population the REST
backfill delivers; `feed = "trade"` is every execution, which no public
REST endpoint can backfill. Prices, quantities, ids and the aggressor side
map onto `Trade` as the REST rows do, and both sources are read at
microsecond resolution — the dumps carry it since 2025-01-01, the REST
rows are requested with it — so a day obtained either way is the same
prints on the same clock.

The zip and its `.CHECKSUM` are stored under `dest_dir/binance/SYMBOL/` and
kept: they are the vendor's own record of the day. A stored file whose
checksum verifies is reused; a download whose checksum does not match is
deleted and the day fails. A day the venue has not published (HTTP 404)
returns nothing, logged at `@info` — the dump of a day appears on the
following UTC day.

Rows are parsed in pages of `page_rows`; `each_page` streams them and
returns only the count, otherwise the trades are accumulated. The vendor's
timestamps are milliseconds before 2025-01-01 and microseconds from then
on; both are read.
"""
function bulk_trades(
    p::BinanceProvider,
    symbol::AbstractString,
    date::Date;
    feed::AbstractString = "aggTrade",
    dest_dir::AbstractString,
    each_page = nothing,
    page_rows::Integer = 100_000,
)
    page_rows >= 1 || throw(ArgumentError("page_rows must be at least 1, got $page_rows"))
    sym = uppercase(String(symbol))
    url = bulk_url(p, sym, date; feed)
    path = _fetch_bulk(p, url, joinpath(dest_dir, "binance", sym))
    if path === nothing
        @info "no bulk file published for this day" symbol = sym date feed
        return each_page === nothing ? Trade[] : 0
    end
    archive = ZipReader(Mmap.mmap(path))
    entries = zip_names(archive)
    length(entries) == 1 || error(
        "$(basename(path)): expected one entry, found $(length(entries)): " *
        join(entries, ", "),
    )
    csv = zip_readentry(archive, only(entries))
    return _parse_bulk_csv(
        csv,
        sym,
        date,
        feed;
        each_page,
        page_rows,
        name = basename(path),
    )
end

# Binance spot streaming. There is no auth and no subscribe handshake — the
# stream set is named in the URL — so the protocol loop is only a read loop.
# Combined-stream frames wrap the payload as {"stream":name,"data":{...}}.
function stream_protocol!(
    ch::Channel{Trade},
    p::BinanceProvider,
    cfg::Config,
    s::LiveSession;
    on_quote = nothing,
    on_bar = nothing,
)
    (on_quote === nothing && on_bar === nothing) ||
        @debug "Binance adapter streams trades only; quote and bar callbacks are idle"
    fatal = Ref{Union{Nothing,FatalStreamError}}(nothing)
    HTTP.WebSockets.open(ws_url(p, cfg.symbols)) do ws
        s.ws[] = ws
        last_frame = Threads.Atomic{Float64}(time())
        alive = Threads.Atomic{Bool}(true)
        watchdog = spawn_watchdog(ws, s, last_frame, alive, cfg.stale_timeout_s)
        try
            for raw in ws
                s.stop[] && break
                last_frame[] = time()
                bump!(s; frames = 1)
                frame = JSON.parse(raw)::AbstractDict
                msg = get(frame, :data, frame)          # combined or raw stream
                e = String(get(msg, :e, ""))
                if e == "trade" || e == "aggTrade"
                    put!(ch, parse_binance_trade(msg, now_ns()))
                    bump!(s; ticks = 1)
                elseif haskey(frame, :error)
                    # A malformed stream name is permanent: reconnecting with
                    # the same configuration would loop forever. The error
                    # leaves the handler by value, as in the Alpaca adapter: an
                    # exception thrown inside it arrives wrapped and would be
                    # retried as a disconnect.
                    err = frame.error
                    fatal[] = FatalStreamError(
                        "Binance stream error $(get(err, :code, 0)): " *
                        "$(get(err, :msg, "unknown"))",
                    )
                    break
                end
            end
        finally
            alive[] = false
            wait(watchdog)
        end
    end
    fatal[] === nothing || throw(fatal[])
    return nothing
end
