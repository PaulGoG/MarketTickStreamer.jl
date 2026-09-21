# In-process mock of the Alpaca REST + WebSocket APIs, so the full pipeline
# is exercised end-to-end without credentials or network access.
#
# REST  : /v2/clock, /v2/stocks/{symbol}/trades (two pages, token-paginated)
# WS    : full v2 protocol — connected → auth → authenticated → subscribe →
#         subscription → trade batch → close. Per-connection behavior is
#         scripted via `MockPlan` to test reconnection and fatal errors.

using Dates
using HTTP
using JSON3

const MOCK_KEY = "testkey"
const MOCK_SECRET = "testsecret"

"""
    MockPlan(batches; fatal_after = length(batches), fatal_code = 406, linger_s = 0.0)

Scripted WS behavior: connection `k` streams `batches[k]`, then idles for
`linger_s` before closing (to exercise client-side stops on a live
connection); connections past `fatal_after` receive a fatal error message
instead.
"""
struct MockPlan
    batches::Vector{Vector{Any}}
    fatal_after::Int
    fatal_code::Int
    linger_s::Float64
end
MockPlan(batches; fatal_after = length(batches), fatal_code = 406, linger_s = 0.0) =
    MockPlan(batches, fatal_after, fatal_code, linger_s)

mock_trade(
    sym,
    i;
    price = 100.0 + i,
    size = 10 * i,
    t = "2026-07-30T14:30:$(lpad(i % 60, 2, '0')).00000000$(i % 10)Z",
) = (; T = "t", S = sym, i = i, x = "V", p = price, s = size, t = t, c = ["@"], z = "C")

mock_quote(sym, i; t = "2026-07-30T14:30:$(lpad(i % 60, 2, '0')).000000001Z") = (;
    T = "q",
    S = sym,
    bx = "V",
    bp = 100.0 + i,
    bs = 1,
    ax = "W",
    ap = 100.5 + i,
    as = 2,
    t = t,
    c = ["R"],
    z = "C",
)

# Note `c` here is the CLOSING PRICE, not a condition list: the v2 protocol
# reuses the key across frame types.
mock_bar(sym, i; t = "2026-07-30T14:3$(i % 10):00Z") = (;
    T = "b",
    S = sym,
    o = 100.0,
    h = 101.0,
    l = 99.0,
    c = 100.5,
    v = 1000,
    n = 42,
    vw = 100.25,
    t = t,
)

_send(ws, msgs...) = HTTP.WebSockets.send(ws, JSON3.write(collect(msgs)))

"""
    start_mock_ws(plan; port) -> (server, nconn)

Start the scripted WebSocket server on `port`. `nconn[]` counts accepted
connections. `close(server)` to stop.
"""
function start_mock_ws(plan::MockPlan; port::Integer)
    nconn = Ref(0)
    server = HTTP.WebSockets.listen!("127.0.0.1", port) do ws
        nconn[] += 1
        me = nconn[]
        _send(ws, (; T = "success", msg = "connected"))
        auth = JSON3.read(HTTP.WebSockets.receive(ws))
        if get(auth, :key, "") != MOCK_KEY
            _send(ws, (; T = "error", code = 402, msg = "auth failed"))
            return
        end
        _send(ws, (; T = "success", msg = "authenticated"))
        sub = JSON3.read(HTTP.WebSockets.receive(ws))
        _send(ws, (; T = "subscription", trades = get(sub, :trades, [])))
        if me > plan.fatal_after
            _send(ws, (; T = "error", code = plan.fatal_code, msg = "scripted fatal"))
            return
        end
        for msg in plan.batches[min(me, length(plan.batches))]
            _send(ws, msg)
            sleep(0.005)
        end
        # Linger reading (not sleeping): a well-behaved server must keep
        # servicing the socket so a client CLOSE handshake completes promptly.
        t0 = time()
        while time() - t0 < plan.linger_s
            try
                HTTP.WebSockets.receive(ws)
            catch
                break                     # client closed the connection
            end
        end
    end
    return server, nconn
end

"""
    start_mock_rest(; port, trades_per_page = 3, close_in_s = 3600.0,
                    is_open = true, open_in_s = 64800.0) -> server

REST mock: `/v2/clock` and `/v2/stocks/{symbol}/trades` with two pages linked
by `next_page_token = "page2"`. The clock reports `is_open`, a close
`close_in_s` from each query (shrink it to simulate a half-day's early close)
and an open `open_in_s` from each query — negative to place the opening bell
in the past, which is how the `wait_for_open` railing is exercised without
waiting.
"""
function start_mock_rest(;
    port::Integer,
    trades_per_page::Integer = 3,
    close_in_s::Real = 3600.0,
    is_open::Bool = true,
    open_in_s::Real = 64800.0,
    hits::Ref{Int} = Ref(0),
    fail_after::Integer = typemax(Int),
)
    router = HTTP.Router()
    # next_close must lie in the future or the client's close-guard would
    # immediately stop every test session.
    HTTP.register!(
        router,
        "GET",
        "/v2/clock",
        _ -> begin
            fmt = t -> Dates.format(t, dateformat"yyyy-mm-dd\THH:MM:SS") * "Z"
            now = Dates.now(Dates.UTC)
            HTTP.Response(
                200,
                JSON3.write((;
                    is_open = is_open,
                    next_open = fmt(now + Dates.Second(round(Int, open_in_s))),
                    next_close = fmt(now + Dates.Second(round(Int, close_in_s))),
                )),
            )
        end,
    )
    # Condition decoder: the real endpoint returns a different map per tape,
    # which is the whole reason the package fetches rather than hardcodes it.
    HTTP.register!(
        router,
        "GET",
        "/v2/stocks/meta/conditions/{ticktype}",
        function (req)
            tape = get(HTTP.queryparams(HTTP.URI(req.target)), "tape", "A")
            body =
                tape == "C" ? Dict("@" => "Regular Sale", "I" => "Odd Lot Trade") :
                Dict("@" => "Regular Sale", "B" => "Average Price Trade")
            return HTTP.Response(200, JSON3.write(body))
        end,
    )
    HTTP.register!(
        router,
        "GET",
        "/v2/stocks/{symbol}/trades",
        function (req)
            hits[] += 1
            # `fail_after` scripts a mid-run failure, to exercise resume.
            hits[] > fail_after && return HTTP.Response(404, "scripted backfill failure")
            sym = HTTP.getparams(req)["symbol"]
            q = HTTP.queryparams(HTTP.URI(req.target))
            # Echo the requested day back in the timestamps, as the real
            # endpoint does — compaction files rows by their exchange date, so
            # a mock that ignored the range would put every day in one file.
            day = first(get(q, "start", "2026-07-29T00:00:00Z"), 10)
            page2 = get(q, "page_token", "") == "page2"
            offset = page2 ? trades_per_page : 0
            rows = [
                (;
                    t = "$(day)T15:0$(i % 10):0$(i % 6).123456789Z",
                    x = "V",
                    p = 200.0 + offset + i,
                    s = 5 * i,
                    c = ["@"],
                    i = offset + i,
                    z = "C",
                ) for i in 1:trades_per_page
            ]
            body = (;
                trades = rows,
                symbol = sym,
                next_page_token = page2 ? nothing : "page2",
            )
            return HTTP.Response(200, JSON3.write(body))
        end,
    )
    return HTTP.serve!(router, "127.0.0.1", port)
end

"""
    mock_config_toml(dir; ws_port, rest_port, kwargs...) -> path

Write a minimal config TOML pointing every endpoint at the mock servers and
all storage under `dir`; returns the config path.
"""
function mock_config_toml(
    dir::AbstractString;
    ws_port::Integer,
    rest_port::Integer,
    symbols = ["AAPL", "MSFT"],
    max_retries = 3,
    require_market_open = true,
    wait_for_open = false,
    stale_timeout_s = 30.0,
    feed = "iex",
    start_date = "2026-07-29",
    end_date = "2026-07-29",
    max_session_hours = 0.05,
)
    path = joinpath(dir, "config.toml")
    write(
        path,
        """
[provider]
name = "alpaca"
feed = "$feed"

[stream]
symbols = $(JSON3.write(symbols))
channels = ["trades"]
require_market_open = $require_market_open
wait_for_open = $wait_for_open
reconnect_max_retries = $max_retries
reconnect_base_delay_s = 0.05
reconnect_max_delay_s = 0.2
stale_timeout_s = $stale_timeout_s

[storage]
data_dir = "$(joinpath(dir, "data"))"
flush_interval_s = 0.2
flush_max_ticks = 100
processed_format = "csv"

[limits]
max_session_hours = $max_session_hours
max_raw_file_mb = 64
channel_capacity = 10000

[backfill]
start_date = "$start_date"
end_date = "$end_date"
page_limit = 3
rate_limit_sleep_s = 0.01

[logging]
level = "warn"
log_to_file = false
log_dir = "$(joinpath(dir, "logs"))"

[alpaca]
trading_base = "http://127.0.0.1:$rest_port"
data_base = "http://127.0.0.1:$rest_port"
ws_base = "ws://127.0.0.1:$ws_port/v2"
""",
    )
    return path
end
