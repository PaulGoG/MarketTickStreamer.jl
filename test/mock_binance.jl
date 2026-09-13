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
using JSON3

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

"""
    start_mock_binance_rest(port; trades_per_day = 7, fail_after = typemax(Int))
        -> HTTP server

Serve `/api/v3/aggTrades`. Honors `symbol`, `startTime`, `fromId` and `limit`
exactly as the real endpoint does for the seed-then-walk pattern; returns a
short page once the mock tape is exhausted, which is the client's stop signal.
`fail_after` scripts a mid-run failure to exercise backfill resume.
"""
function start_mock_binance_rest(
    port::Integer;
    trades_per_day::Integer = 7,
    fail_after::Integer = typemax(Int),
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
            return HTTP.Response(200, JSON3.write(rows))
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
                JSON3.write((; error = (; code = 2, msg = "Invalid request"))),
            )
            return nothing
        end
        for f in frames
            HTTP.WebSockets.send(
                ws,
                JSON3.write((; stream = "$(lowercase(f.s))@aggTrade", data = f)),
            )
        end
        linger_s > 0 && sleep(linger_s)
        return nothing
    end
end

mock_binance_frame(sym, k; trades_per_day = 7) = begin
    r = mock_binance_row(k, trades_per_day)
    (;
        e = "aggTrade",
        E = r.T,
        s = sym,
        a = r.a,
        p = r.p,
        q = r.q,
        f = r.f,
        l = r.l,
        T = r.T,
        m = r.m,
        M = r.M,
    )
end

"""
    mock_binance_config_toml(dir; ws_port, rest_port, kwargs...) -> path

Config TOML pointing the Binance adapter at the mock servers, with all
storage under `dir`.
"""
function mock_binance_config_toml(
    dir::AbstractString;
    ws_port::Integer,
    rest_port::Integer,
    symbols = ["BTCUSDT"],
    feed = "aggTrade",
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
symbols = $(JSON3.write(symbols))
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
feed = "aggTrade"
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
""",
    )
    return path
end
