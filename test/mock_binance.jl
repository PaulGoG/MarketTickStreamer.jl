# In-process mock of the Binance spot REST + WebSocket APIs, so the Binance
# adapter is exercised end-to-end without network access.
#
# REST : /api/v3/aggTrades — id-seeded pagination, the shape the real endpoint
#        forces (a startTime/endTime pair spanning an hour or more is
#        rejected, so the client seeds on time and walks on `fromId`).
# WS   : /stream?streams=a@aggTrade/b@aggTrade — combined-stream frames
#        wrapped as {"stream":name,"data":{...}}. No auth, no subscribe
#        handshake: the URL is the subscription.
#
# The mock tape is deterministic: `trades_per_day` trades on each UTC date
# from `MOCK_BINANCE_EPOCH`, evenly spaced, with globally increasing ids from
# 1. That makes every count in the tests exact rather than approximate.

using Dates
using HTTP
using JSON
using SHA: sha256
using ZipArchives: ZipWriter, zip_newfile

const MOCK_BINANCE_EPOCH = Date(2026, 1, 1)
const MOCK_BINANCE_DAYS = 5

_mb_day_start_ms(d::Date) = round(Int64, datetime2unix(DateTime(d))) * 1000

# Trade `k` (1-based): day (k-1) ÷ n, slot (k-1) % n, spaced across the day.
function mock_binance_row(k::Integer, trades_per_day::Integer)
    d = MOCK_BINANCE_EPOCH + Day((k - 1) ÷ trades_per_day)
    slot = (k - 1) % trades_per_day
    spacing_ms = 86_400_000 ÷ trades_per_day
    return (;
        a = k,
        p = string(64_000.0 + k),          # strings, as the real API sends
        q = string(0.001 * k),
        f = k,
        l = k,
        T = _mb_day_start_ms(d) + slot * spacing_ms,
        m = isodd(k),                       # alternating aggressor side
        M = true,
    )
end

mock_binance_total(trades_per_day::Integer) = MOCK_BINANCE_DAYS * trades_per_day

# The day's dump as the venue serves it: one CSV entry, no header, booleans
# capitalised, timestamps in microseconds (the unit since 2025-01-01).
function mock_binance_bulk_csv(kind::AbstractString, d::Date, trades_per_day::Integer)
    k0 = Dates.value(d - MOCK_BINANCE_EPOCH) * trades_per_day
    io = IOBuffer()
    for k in (k0+1):(k0+trades_per_day)
        r = mock_binance_row(k, trades_per_day)
        us = r.T * 1000
        m = r.m ? "True" : "False"
        if kind == "aggTrades"
            println(io, "$(r.a),$(r.p),$(r.q),$(r.f),$(r.l),$us,$m,True")
        else
            quote_qty = string(parse(Float64, r.p) * parse(Float64, r.q))
            println(io, "$(r.a),$(r.p),$(r.q),$quote_qty,$us,$m,True")
        end
    end
    return take!(io)
end

# A zip holding one entry, as the venue packs a day.
function mock_binance_bulk_zip(entry::AbstractString, csv::Vector{UInt8})
    return mktempdir() do dir
        path = joinpath(dir, "day.zip")
        ZipWriter(path) do w
            zip_newfile(w, entry)
            write(w, csv)
        end
        read(path)
    end
end

"""
    start_mock_binance_rest(port; trades_per_day = 7, fail_after = typemax(Int),
                            missing_dates = Date[], corrupt_checksum = false,
                            seen_units = nothing)
        -> HTTP server

Serve `/api/v3/aggTrades`. Honors `symbol`, `startTime`, `fromId` and `limit`
exactly as the real endpoint does for the seed-then-walk pattern; returns a
short page once the mock tape is exhausted, which is the client's stop signal.
`fail_after` scripts a mid-run failure to exercise backfill resume.
`seen_units` collects the `X-MBX-TIME-UNIT` header of every request; rows
carry microseconds when it asks for them, as the venue does.

Also serves the daily bulk files under `/data/spot/daily/` from the same tape.
`missing_dates` answers 404 for those days' bulk files, `corrupt_checksum`
serves a checksum that cannot match.
"""
function start_mock_binance_rest(
    port::Integer;
    trades_per_day::Integer = 7,
    fail_after::Integer = typemax(Int),
    missing_dates::Vector{Date} = Date[],
    corrupt_checksum::Bool = false,
    seen_units::Union{Nothing,Vector{String}} = nothing,
)
    router = HTTP.Router()
    hits = Ref(0)
    total = mock_binance_total(trades_per_day)
    HTTP.register!(
        router,
        "GET",
        "/api/v3/aggTrades",
        function (req)
            hits[] += 1
            unit = uppercase(HTTP.header(req, "X-MBX-TIME-UNIT", "MILLISECOND"))
            seen_units === nothing || push!(seen_units, unit)
            hits[] > fail_after && return HTTP.Response(418, "scripted backfill failure")
            q = HTTP.queryparams(HTTP.URI(req.target))
            limit = parse(Int, get(q, "limit", "500"))
            first_k = if haskey(q, "fromId")
                parse(Int, q["fromId"])
            elseif haskey(q, "startTime")
                ms = parse(Int64, q["startTime"])
                k = findfirst(k -> mock_binance_row(k, trades_per_day).T >= ms, 1:total)
                k === nothing ? total + 1 : k
            else
                1
            end
            rows = [
                mock_binance_row(k, trades_per_day) for
                k in first_k:min(first_k+limit-1, total)
            ]
            unit == "MICROSECOND" && (rows = [merge(r, (; T = r.T * 1000)) for r in rows])
            return HTTP.Response(200, JSON.json(rows))
        end,
    )
    # Daily bulk files, built from the same tape as the REST endpoint, so the
    # two routes must agree print for print.
    HTTP.register!(
        router,
        "GET",
        "/data/spot/daily/**",
        function (req)
            m = match(
                r"^/data/spot/daily/(aggTrades|trades)/([A-Z]+)/\2-\1-(\d{4}-\d{2}-\d{2})\.zip(\.CHECKSUM)?$",
                HTTP.URI(req.target).path,
            )
            m === nothing && return HTTP.Response(404, "no such file")
            kind, sym, d = String(m[1]), String(m[2]), Date(m[3])
            last_day = MOCK_BINANCE_EPOCH + Day(MOCK_BINANCE_DAYS - 1)
            (MOCK_BINANCE_EPOCH <= d <= last_day && !(d in missing_dates)) ||
                return HTTP.Response(404, "no such file")
            name = "$sym-$kind-$d"
            zip = mock_binance_bulk_zip(
                "$name.csv",
                mock_binance_bulk_csv(kind, d, trades_per_day),
            )
            if m[4] !== nothing
                sha = corrupt_checksum ? "0"^64 : bytes2hex(sha256(zip))
                return HTTP.Response(200, "$sha  $name.zip\n")
            end
            return HTTP.Response(200, zip)
        end,
    )
    return HTTP.serve!(router, "127.0.0.1", port)
end

"""
    start_mock_binance_ws(port; frames, linger_s = 0.0, fatal_after = 1)
        -> HTTP server

Serve the combined-stream endpoint, sending each of `frames` as its own
`{"stream":…,"data":…}` message, then idling `linger_s` before closing.

Connections past `fatal_after` receive an `{"error":…}` frame instead. A
closed connection is a *retryable* condition — `live_source` reconnects, and
the attempt counter resets after any connection that delivered data — so
without a terminating condition the client would replay these frames until
the session deadline. That is correct client behavior and it has to be
scripted against, exactly as `MockPlan` does for Alpaca.
"""
function start_mock_binance_ws(
    port::Integer;
    frames::Vector,
    linger_s::Real = 0.0,
    fatal_after::Integer = 1,
)
    conns = Ref(0)
    return HTTP.WebSockets.listen!("127.0.0.1", port) do ws
        conns[] += 1
        if conns[] > fatal_after
            HTTP.WebSockets.send(
                ws,
                JSON.json((; error = (; code = 2, msg = "Invalid request"))),
            )
            return nothing
        end
        for f in frames
            HTTP.WebSockets.send(
                ws,
                JSON.json((; stream = "$(lowercase(f.s))@aggTrade", data = f)),
            )
        end
        linger_s > 0 && sleep(linger_s)
        return nothing
    end
end

mock_binance_frame(sym, k; trades_per_day = 7, unit::Symbol = :ms) = begin
    r = mock_binance_row(k, trades_per_day)
    scale = unit === :us ? 1000 : 1
    (;
        e = "aggTrade",
        E = r.T * scale,
        s = sym,
        a = r.a,
        p = r.p,
        q = r.q,
        f = r.f,
        l = r.l,
        T = r.T * scale,
        m = r.m,
        M = r.M,
    )
end

"""
    mock_binance_config_toml(dir; ws_port, rest_port, kwargs...) -> path

Config TOML pointing the Binance adapter at the mock servers, with all
storage under `dir`. `source` and `backfill_feed` set `backfill.source` and
`backfill.feed`.
"""
function mock_binance_config_toml(
    dir::AbstractString;
    ws_port::Integer,
    rest_port::Integer,
    symbols = ["BTCUSDT"],
    feed = "aggTrade",
    source = "rest",
    backfill_feed = "aggTrade",
    start_date = string(MOCK_BINANCE_EPOCH),
    end_date = string(MOCK_BINANCE_EPOCH),
    page_limit = 3,
    require_market_open = true,
    stop_at_market_close = true,
)
    path = joinpath(dir, "binance.toml")
    write(
        path,
        """
[provider]
name = "binance"
feed = "$feed"

[stream]
symbols = $(JSON.json(symbols))
channels = ["trades"]
require_market_open = $require_market_open
stop_at_market_close = $stop_at_market_close
reconnect_max_retries = 1
reconnect_base_delay_s = 0.05
reconnect_max_delay_s = 0.2
stale_timeout_s = 30.0

[storage]
data_dir = "$(joinpath(dir, "data"))"
flush_interval_s = 0.2
flush_max_ticks = 100
processed_format = "csv"

[limits]
max_session_hours = 0.02
max_raw_file_mb = 64
channel_capacity = 10000

[backfill]
feed = "$backfill_feed"
source = "$source"
start_date = "$start_date"
end_date = "$end_date"
page_limit = $page_limit
rate_limit_sleep_s = 0.01

[logging]
level = "warn"
log_to_file = false
log_dir = "$(joinpath(dir, "logs"))"

[binance]
rest_base = "http://127.0.0.1:$rest_port"
ws_base = "ws://127.0.0.1:$ws_port"
bulk_base = "http://127.0.0.1:$rest_port"
""",
    )
    return path
end
