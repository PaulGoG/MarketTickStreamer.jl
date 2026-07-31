# Alpaca Markets adapter: REST (market clock, historical trades) and the
# v2 WebSocket streaming protocol (connect → auth → subscribe → data).
#
# Wire references:
#   https://docs.alpaca.markets/docs/real-time-stock-pricing-data
#   https://docs.alpaca.markets/reference/stocktrades
# Live trade message: {"T":"t","S":sym,"i":id,"x":exch,"p":price,"s":size,
#                      "t":RFC3339ns,"c":[conds],"z":tape}

"""
    AlpacaProvider

Connection descriptor for Alpaca Markets: credentials, feed selection and
endpoint roots (overridable via the `[alpaca]` config table, which lets the
test suite point the client at a local mock server).
"""
struct AlpacaProvider <: AbstractProvider
    key::String
    secret::String
    feed::String
    trading_base::String
    data_base::String
    ws_base::String
end

function AlpacaProvider(cfg::Config, key::AbstractString, secret::AbstractString)
    ep = cfg.endpoints
    return AlpacaProvider(key, secret, cfg.feed,
        get(ep, "trading_base", "https://api.alpaca.markets"),
        get(ep, "data_base", "https://data.alpaca.markets"),
        get(ep, "ws_base", "wss://stream.data.alpaca.markets/v2"))
end

ws_url(p::AlpacaProvider) = "$(p.ws_base)/$(p.feed)"

rest_headers(p::AlpacaProvider) =
    ["APCA-API-KEY-ID" => p.key, "APCA-API-SECRET-KEY" => p.secret]

auth_payload(p::AlpacaProvider) =
    JSON3.write((action = "auth", key = p.key, secret = p.secret))

"""
    subscribe_payload(p, symbols, channels) -> String

Build the JSON subscription message for the requested channels
(`"trades"`/`"quotes"`/`"bars"`), e.g.
`{"action":"subscribe","trades":["AAPL","MSFT"]}`.
"""
function subscribe_payload(::AlpacaProvider, symbols::Vector{String}, channels::Vector{String})
    d = Dict{String, Any}("action" => "subscribe")
    for c in channels
        d[c] = symbols
    end
    return JSON3.write(d)
end

# GET with exponential-backoff retry on transient statuses (rate limiting,
# server hiccups). Client errors other than 429 fail immediately.
const RETRYABLE_STATUS = (429, 500, 502, 503, 504)

function _get_with_retry(url, headers; query = nothing, max_retries::Integer = 5)
    for attempt in 0:max_retries
        try
            return HTTP.get(url, headers; query, retry = false)
        catch e
            (e isa HTTP.StatusError && e.status in RETRYABLE_STATUS && attempt < max_retries) ||
                rethrow()
            ra = tryparse(Float64, HTTP.header(e.response, "Retry-After", ""))
            delay = ra !== nothing ? ra : 2.0^attempt * (0.5 + rand())
            @warn "REST $(e.status) — backing off" attempt delay = round(delay; digits = 1)
            sleep(min(delay, 60.0))
        end
    end
end

"""
    market_clock(p) -> (; is_open, next_open, next_close)

Query `/v2/clock` on the trading API. Timestamps are returned as the raw
RFC 3339 strings Alpaca sends (display only — nothing downstream computes
with them).
"""
function market_clock(p::AlpacaProvider)
    resp = _get_with_retry("$(p.trading_base)/v2/clock", rest_headers(p))
    o = JSON3.read(resp.body)
    return (; is_open = Bool(o.is_open),
              next_open = String(o.next_open), next_close = String(o.next_close))
end

# Shared by live-stream ("S" carries the symbol) and historical REST
# (symbol comes from the request path) message shapes.
function parse_alpaca_trade(msg, recv_ns::Int64; symbol::AbstractString = "")
    sym = isempty(symbol) ? String(msg.S) : String(symbol)
    conds = haskey(msg, :c) && msg.c !== nothing ? String.(msg.c) : String[]
    return Trade(sym, rfc3339_to_ns(String(msg.t)), recv_ns,
                 Float64(msg.p), Float64(msg.s),
                 haskey(msg, :x) ? String(msg.x) : "",
                 conds,
                 haskey(msg, :z) ? String(msg.z) : "",
                 haskey(msg, :i) ? Int64(msg.i) : 0)
end

"""
    historical_trades(p, symbol, start_date, end_date;
                      page_limit = 10_000, rate_sleep_s = 0.35,
                      on_page = nothing) -> Vector{Trade}

Download all trades for `symbol` in `[start_date, end_date]` (inclusive,
exchange dates) from `/v2/stocks/{symbol}/trades`, following pagination
tokens until exhausted. `rate_sleep_s` throttles between pages (free tier:
200 requests/min). Backfilled records get `recv_ns = 0` — they never crossed
the wire. `on_page(n_page, n_total)` is called per page for progress.
"""
function historical_trades(p::AlpacaProvider, symbol::AbstractString,
                           start_date::Date, end_date::Date;
                           feed::AbstractString = "sip",
                           page_limit::Integer = 10_000, rate_sleep_s::Real = 0.35,
                           on_page = nothing)
    trades = Trade[]
    url = "$(p.data_base)/v2/stocks/$(symbol)/trades"
    query = Dict{String, String}(
        "start" => "$(start_date)T00:00:00Z",
        "end" => "$(end_date)T23:59:59Z",
        "limit" => string(page_limit),
        "feed" => feed,
    )
    while true
        resp = _get_with_retry(url, rest_headers(p); query)
        o = JSON3.read(resp.body)
        page = get(o, :trades, nothing)   # null/absent when the range has no data
        if page !== nothing
            for msg in page
                push!(trades, parse_alpaca_trade(msg, 0; symbol))
            end
            on_page === nothing || on_page(length(page), length(trades))
        end
        token = get(o, :next_page_token, nothing)
        (token === nothing || token == "") && break
        query["page_token"] = String(token)
        sleep(rate_sleep_s)
    end
    return trades
end

# Alpaca v2 streaming protocol. Frames are JSON arrays of messages; the
# server opens with {"T":"success","msg":"connected"}, we reply with auth,
# then subscribe on {"T":"success","msg":"authenticated"}.
#
# Fatal protocol errors are carried out via a Ref and thrown AFTER
# HTTP.WebSockets.open returns: exceptions thrown inside the handler cross
# HTTP.jl's internal task boundary and may arrive wrapped (TaskFailedException
# etc.), which would defeat the caller's `isa FatalStreamError` dispatch.
function stream_protocol!(ch::Channel{Trade}, p::AlpacaProvider, cfg::Config, s::LiveSession)
    fatal = Ref{Union{Nothing, FatalStreamError}}(nothing)
    HTTP.WebSockets.open(ws_url(p)) do ws
        s.ws[] = ws
        last_frame = Ref(time())
        alive = Ref(true)
        watchdog = spawn_watchdog(ws, s, last_frame, alive, cfg.stale_timeout_s)
        try
            for raw in ws
                s.stop[] && break
                last_frame[] = time()
                bump!(s; frames = 1)
                for msg in JSON3.read(raw)
                    T = String(get(msg, :T, ""))
                    if T == "t"
                        put!(ch, parse_alpaca_trade(msg, now_ns()))
                        bump!(s; ticks = 1)
                    elseif T == "success"
                        m = String(get(msg, :msg, ""))
                        if m == "connected"
                            HTTP.WebSockets.send(ws, auth_payload(p))
                        elseif m == "authenticated"
                            @info "authenticated; subscribing" cfg.symbols cfg.channels
                            HTTP.WebSockets.send(ws, subscribe_payload(p, cfg.symbols, cfg.channels))
                        end
                    elseif T == "subscription"
                        @info "subscription confirmed" trades = get(msg, :trades, [])
                    elseif T == "error"
                        code = Int(get(msg, :code, 0))
                        m = String(get(msg, :msg, "unknown"))
                        code in FATAL_WS_CODES && (fatal[] = FatalStreamError("Alpaca error $code: $m"))
                        fatal[] === nothing && error("Alpaca stream error $code: $m")   # retryable
                    elseif T == "q" || T == "b"
                        # quotes/bars: accepted but not yet normalized — future work
                    end
                end
                fatal[] === nothing || break
            end
        finally
            alive[] = false
            wait(watchdog)
        end
    end
    fatal[] === nothing || throw(fatal[])
end
