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
    return AlpacaProvider(
        key,
        secret,
        cfg.feed,
        get(ep, "trading_base", "https://api.alpaca.markets"),
        get(ep, "data_base", "https://data.alpaca.markets"),
        get(ep, "ws_base", "wss://stream.data.alpaca.markets/v2"),
    )
end

ws_url(p::AlpacaProvider) = "$(p.ws_base)/$(p.feed)"

# US equities: the consolidated tape files a print under the New York calendar
# date, and `sip` is the only backfill feed with the full tape.
provider_spec(::Val{:alpaca}) = ProviderSpec(
    ["iex", "sip", "delayed_sip"],
    ["iex", "sip"],
    tz"America/New_York",
    true,
    10_000,
)

make_provider(::Val{:alpaca}, cfg::Config, key::AbstractString, secret::AbstractString) =
    AlpacaProvider(cfg, key, secret)

exchange_tz(::AlpacaProvider) = provider_spec(Val(:alpaca)).tz

# US equities trade Monday to Friday. Market holidays are not filtered here:
# a holiday returns an empty day, which costs one request and is visible in
# the log, whereas a wrong holiday calendar would skip a day that did trade.
session_days(::AlpacaProvider, start_date::Date, end_date::Date) =
    [d for d in start_date:Day(1):end_date if dayofweek(d) <= 5]

# The delayed_sip feed replays the consolidated tape 15 minutes behind.
feed_delay_ns(p::AlpacaProvider) = p.feed == "delayed_sip" ? 900 * NS_PER_SEC : Int64(0)

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
function subscribe_payload(
    ::AlpacaProvider,
    symbols::Vector{String},
    channels::Vector{String},
)
    d = Dict{String,Any}("action" => "subscribe")
    for c in channels
        d[c] = symbols
    end
    return JSON3.write(d)
end

"""
    market_clock(p) -> (; is_open, next_open, next_close)

Query `/v2/clock` on the trading API. Timestamps are returned as the raw
RFC 3339 strings Alpaca sends (display only — nothing downstream computes
with them).
"""
function market_clock(p::AlpacaProvider)
    resp = _get_with_retry("$(p.trading_base)/v2/clock", rest_headers(p))
    o = JSON3.read(resp.body)::JSON3.Object
    return (;
        is_open = Bool(o.is_open::Bool),
        next_open = String(o.next_open::AbstractString),
        next_close = String(o.next_close::AbstractString),
    )
end

"""
    condition_map(p; ticktype = "trade", tape = "A") -> Dict{String,String}

Fetch the provider's own sale-condition decoder from
`/v2/stocks/meta/conditions/{ticktype}`: a map of condition code to
description for one `tape` (`"A"`, `"B"`, `"C"`). `ticktype` is `"trade"` or
`"quote"`.

The same character carries different meanings on different tapes, and every
vendor normalizes the raw CTA and UTP codes differently, so the glossary is
fetched from the provider that produced the data rather than transcribed into
this package. What *is* held here is the much smaller
[`NON_PRICE_CONDITIONS`](@ref) judgement about which codes disqualify a print
from a price path.

Requires network access; call it once and cache the result.
"""
function condition_map(
    p::AlpacaProvider;
    ticktype::AbstractString = "trade",
    tape::AbstractString = "A",
)
    ticktype in ("trade", "quote") ||
        throw(ArgumentError("ticktype must be \"trade\" or \"quote\", got \"$ticktype\""))
    resp = _get_with_retry(
        "$(p.data_base)/v2/stocks/meta/conditions/$(ticktype)",
        rest_headers(p);
        query = Dict("tape" => String(tape)),
    )
    o = JSON3.read(resp.body)::JSON3.Object
    return Dict{String,String}(String(k) => String(v::AbstractString) for (k, v) in o)
end

# Shared by live-stream ("S" carries the symbol) and historical REST
# (symbol comes from the request path) message shapes.
function parse_alpaca_trade(msg, recv_ns::Int64; symbol::AbstractString = "")
    sym = isempty(symbol) ? String(msg.S) : String(symbol)
    conds = haskey(msg, :c) && msg.c !== nothing ? String.(msg.c) : String[]
    return Trade(
        sym,
        rfc3339_to_ns(String(msg.t)),
        recv_ns,
        Float64(msg.p),
        Float64(msg.s),
        haskey(msg, :x) ? String(msg.x) : "",
        conds,
        haskey(msg, :z) ? String(msg.z) : "",
        haskey(msg, :i) ? Int64(msg.i) : 0,
    )
end

# Quote frames carry the two sides with `b*`/`a*` prefixes; `c` is the quote
# condition list, as for trades.
function parse_alpaca_quote(msg, recv_ns::Int64; symbol::AbstractString = "")
    sym = isempty(symbol) ? String(msg.S) : String(symbol)
    conds = haskey(msg, :c) && msg.c !== nothing ? String.(msg.c) : String[]
    return Quote(
        sym,
        rfc3339_to_ns(String(msg.t)),
        recv_ns,
        Float64(msg.bp),
        Float64(msg.bs),
        haskey(msg, :bx) ? String(msg.bx) : "",
        Float64(msg.ap),
        Float64(msg.as),
        haskey(msg, :ax) ? String(msg.ax) : "",
        conds,
        haskey(msg, :z) ? String(msg.z) : "",
    )
end

# Bar frames reuse `c` for the CLOSING PRICE, not for conditions as trade and
# quote frames do. Mixing the two up is silent and produces plausible numbers,
# so the field is read explicitly here and nowhere else.
# The `b` channel carries one-minute bars stamped with the minute's opening
# instant, so the closing instant is that plus 60 s; daily and updated bars
# arrive on other channels and are not parsed here.
function parse_alpaca_bar(msg, recv_ns::Int64; symbol::AbstractString = "")
    sym = isempty(symbol) ? String(msg.S) : String(symbol)
    open_ns = rfc3339_to_ns(String(msg.t))
    return Bar(
        sym,
        open_ns,
        open_ns + 60 * NS_PER_SEC,
        recv_ns,
        Float64(msg.o),
        Float64(msg.h),
        Float64(msg.l),
        Float64(msg.c),
        Float64(msg.v),
        haskey(msg, :n) ? Int64(msg.n) : 0,
        haskey(msg, :vw) ? Float64(msg.vw) : NaN,
    )
end

# Core pagination loop: hands each page's parsed trades to `f` and returns
# the total row count. `start_str`/`end_str` are inclusive RFC 3339 bounds.
function _each_trades_page(
    f,
    p::AlpacaProvider,
    symbol::AbstractString;
    start_str::AbstractString,
    end_str::AbstractString,
    feed::AbstractString,
    page_limit::Integer,
    rate_sleep_s::Real,
)
    max_limit = provider_spec(Val(:alpaca)).max_page_limit
    1 <= page_limit <= max_limit || throw(
        ArgumentError("page_limit must be in 1:$max_limit for Alpaca, got $page_limit"),
    )
    url = "$(p.data_base)/v2/stocks/$(symbol)/trades"
    query = Dict{String,String}(
        "start" => String(start_str),
        "end" => String(end_str),
        "limit" => string(page_limit),
        "feed" => String(feed),
    )
    total = 0
    while true
        resp = _get_with_retry(url, rest_headers(p); query)
        # Assertions narrow the JSON value unions to what the documented
        # response shape guarantees; a violation is a protocol error and
        # should fail here rather than downstream.
        o = JSON3.read(resp.body)::JSON3.Object
        page = get(o, :trades, nothing)   # null/absent when the range has no data
        if page isa JSON3.Array && !isempty(page)
            trades = Trade[parse_alpaca_trade(msg, 0; symbol) for msg in page]
            total += length(trades)
            f(trades)
        end
        token = get(o, :next_page_token, nothing)
        (token === nothing || token == "") && break
        query["page_token"] = String(token::String)
        sleep(rate_sleep_s)
    end
    return total
end

"""
    historical_trades(p, symbol, start_date, end_date;
                      feed = "sip", page_limit = 10_000, rate_sleep_s = 0.35,
                      on_page = nothing, each_page = nothing)

Download all trades for `symbol` in `[start_date, end_date]` (inclusive,
exchange dates) from `/v2/stocks/{symbol}/trades`, following pagination
tokens until exhausted. `rate_sleep_s` throttles between pages (free tier:
200 requests/min). Backfilled records get `recv_ns = 0` — they never crossed
the wire. `on_page(n_page, n_total)` is called per page for progress.

By default all trades are accumulated and returned as a `Vector{Trade}`.
With `each_page` set, each page's trades are handed to
`each_page(::Vector{Trade})` instead and only the total row count is
returned — memory stays bounded by one page regardless of the range.
"""
function historical_trades(
    p::AlpacaProvider,
    symbol::AbstractString,
    start_date::Date,
    end_date::Date;
    feed::AbstractString = "sip",
    page_limit::Integer = 10_000,
    rate_sleep_s::Real = 0.35,
    on_page = nothing,
    each_page = nothing,
)
    acc = each_page === nothing ? Trade[] : nothing
    seen = 0
    total = _each_trades_page(
        p,
        symbol;
        start_str = ns_to_rfc3339(exchange_day_start_ns(p, start_date)),
        end_str = ns_to_rfc3339(exchange_day_start_ns(p, end_date + Day(1)) - 1),
        feed,
        page_limit,
        rate_sleep_s,
    ) do page
        seen += length(page)
        acc === nothing || append!(acc, page)
        each_page === nothing || each_page(page)
        on_page === nothing || on_page(length(page), seen)
    end
    return acc === nothing ? total : acc
end

"""
    historical_trade_count(p, symbol, start_str, end_str;
                           feed = "sip", page_limit = 10_000,
                           rate_sleep_s = 0.35) -> Int

Count trades on the historical tape for `symbol` over the inclusive
RFC 3339 window `[start_str, end_str]` without retaining them — the
reference side of live-capture coverage checks.
"""
historical_trade_count(
    p::AlpacaProvider,
    symbol::AbstractString,
    start_str::AbstractString,
    end_str::AbstractString;
    feed::AbstractString = "sip",
    page_limit::Integer = 10_000,
    rate_sleep_s::Real = 0.35,
) = _each_trades_page(
    _ -> nothing,
    p,
    symbol;
    start_str,
    end_str,
    feed,
    page_limit,
    rate_sleep_s,
)

# Alpaca v2 streaming protocol. Frames are JSON arrays of messages; the
# server opens with {"T":"success","msg":"connected"}, we reply with auth,
# then subscribe on {"T":"success","msg":"authenticated"}.
#
# Fatal protocol errors are carried out via a Ref and thrown AFTER
# HTTP.WebSockets.open returns: exceptions thrown inside the handler cross
# HTTP.jl's internal task boundary and may arrive wrapped (TaskFailedException
# etc.), which would defeat the caller's `isa FatalStreamError` dispatch.
function stream_protocol!(
    ch::Channel{Trade},
    p::AlpacaProvider,
    cfg::Config,
    s::LiveSession;
    on_quote = nothing,
    on_bar = nothing,
)
    fatal = Ref{Union{Nothing,FatalStreamError}}(nothing)
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
                            HTTP.WebSockets.send(
                                ws,
                                subscribe_payload(p, cfg.symbols, cfg.channels),
                            )
                        end
                    elseif T == "subscription"
                        @info "subscription confirmed" trades = get(msg, :trades, [])
                    elseif T == "error"
                        code = Int(get(msg, :code, 0))
                        m = String(get(msg, :msg, "unknown"))
                        code in FATAL_WS_CODES &&
                            (fatal[] = FatalStreamError("Alpaca error $code: $m"))
                        fatal[] === nothing && error("Alpaca stream error $code: $m")   # retryable
                    elseif T == "q"
                        on_quote === nothing || on_quote(parse_alpaca_quote(msg, now_ns()))
                    elseif T == "b"
                        on_bar === nothing || on_bar(parse_alpaca_bar(msg, now_ns()))
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
