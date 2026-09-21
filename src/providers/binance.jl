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
#
# Three things differ from an equity tape and shape everything below:
#   * the venue never closes, so there are no market-hours railings and the
#     calendar is UTC;
#   * prices and quantities arrive as STRINGS, and timestamps in
#     MILLISECONDS, against this package's Int64 nanoseconds;
#   * there is no consolidated tape and no sale conditions, but there is an
#     aggressor side, which is the informative flag a crypto print carries.

"""
    BinanceProvider

Connection descriptor for Binance spot market data. Endpoint roots are
overridable through the `[binance]` config table, which is how the test suite
points the client at a local mock server.

`feed` selects the trade granularity: `"trade"` is every execution,
`"aggTrade"` aggregates executions that filled at one price from a single
taker order. They are different populations — see [`historical_trades`](@ref).
`rest` bounds its REST requests ([`RestPolicy`](@ref)).
"""
struct BinanceProvider <: AbstractProvider
    feed::String
    rest_base::String
    ws_base::String
    rest::RestPolicy
end

BinanceProvider(feed::AbstractString, rest_base::AbstractString, ws_base::AbstractString) =
    BinanceProvider(feed, rest_base, ws_base, RestPolicy())

function BinanceProvider(cfg::Config)
    ep = cfg.endpoints
    return BinanceProvider(
        cfg.feed,
        get(ep, "rest_base", "https://api.binance.com"),
        get(ep, "ws_base", "wss://stream.binance.com:9443"),
        RestPolicy(cfg),
    )
end

# Spot trades around the clock, every day, on the UTC calendar. A crypto venue
# has no local trading day to speak of, and UTC is the convention its own
# timestamps and daily data dumps use.
provider_spec(::Val{:binance}) = ProviderSpec(
    ["trade", "aggTrade"],
    ["aggTrade"],
    tz"UTC",
    false,
    1_000,
    (0.0, 24.0),
    "quote asset",
    "base asset",
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
handshake: the connection *is* the subscription.
"""
ws_url(p::BinanceProvider, symbols::Vector{String}) =
    "$(p.ws_base)/stream?streams=" * join((_stream_name(p, s) for s in symbols), "/")

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

_ms_to_ns(ms::Integer) = Int64(ms) * 1_000_000

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
        _ms_to_ns(msg.T::Integer),
        recv_ns,
        _num(msg.p),
        _num(msg.q),
        "BINANCE",
        _aggressor(Bool(msg.m::Bool)),
        "SPOT",
        Int64(id::Integer),
    )
end

# REST /api/v3/aggTrades rows carry the same fields as the stream frame but
# without `s`, since the symbol is in the request path.
parse_binance_agg_rest(row, symbol::AbstractString) = Trade(
    String(symbol),
    _ms_to_ns(row.T::Integer),
    0,
    _num(row.p),
    _num(row.q),
    "BINANCE",
    _aggressor(Bool(row.m::Bool)),
    "SPOT",
    Int64(row.a::Integer),
)

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

**`aggTrade` is not the tape.** Binance aggregates executions that filled at
one price from one taker order into a single row, so an aggregate trade is a
taker order's fill, not an execution. Counts and inter-arrival times are
therefore not comparable with a raw `trade` stream, and the choice belongs to
the analysis — as with odd lots on an equity tape. Only `aggTrade` has a
time-seekable public endpoint, which is why it is the backfill feed.
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
        resp = _get_with_retry(url, Pair{String,String}[]; query, policy = p.rest)
        rows = JSON3.read(resp.body)::JSON3.Array
        isempty(rows) && break
        page = Trade[]
        last_id = 0
        done = false
        for row in rows
            t = parse_binance_agg_rest(row, sym)
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
                frame = JSON3.read(raw)::JSON3.Object
                msg = get(frame, :data, frame)          # combined or raw stream
                e = String(get(msg, :e, ""))
                if e == "trade" || e == "aggTrade"
                    put!(ch, parse_binance_trade(msg, now_ns()))
                    bump!(s; ticks = 1)
                elseif haskey(frame, :error)
                    # A malformed stream name is permanent: reconnecting with
                    # the same configuration would loop forever.
                    err = frame.error
                    throw(
                        FatalStreamError(
                            "Binance stream error $(get(err, :code, 0)): " *
                            "$(get(err, :msg, "unknown"))",
                        ),
                    )
                end
            end
        finally
            alive[] = false
            wait(watchdog)
        end
    end
end
