# Live WebSocket source: connect → auth → subscribe → stream, with jittered
# exponential-backoff reconnection and a hard session deadline.
#
# The producer feeds a bounded Channel{Trade}; if consumers stall, `put!`
# blocks and TCP backpressure throttles the sender, so nothing is buffered
# without bound. A stall longer than `stream.stale_timeout_s` is ended by the
# watchdog instead, which severs the connection and lets the reconnect path
# resume.

"""
    AbstractProvider

Supertype for market-data provider adapters.

A concrete adapter supplies [`provider_spec`](@ref) and [`make_provider`](@ref)
for the name it answers to (the first registers it: see
[`registered_providers`](@ref)), [`exchange_tz`](@ref), [`stream_protocol!`](@ref),
[`market_clock`](@ref), [`historical_trades`](@ref) and its own message
parsing. It overrides [`always_open`](@ref), [`session_days`](@ref) and
[`feed_delay_ns`](@ref) where the venue departs from the defaults.

Beyond the exported names, the implementer interface is `public`:
[`stream_protocol!`](@ref), [`stream_websocket!`](@ref),
[`feed_delay_ns`](@ref), [`exchange_day_start_ns`](@ref),
[`spawn_watchdog`](@ref) and [`bump!`](@ref) are documented, versioned with
the package and reached qualified.

The adapters that ship are `providers/alpaca.jl` (US equities, New York
calendar, credentialed, closes overnight and on weekends) and
`providers/binance.jl` (crypto spot, UTC calendar, public, never closes).
"""
abstract type AbstractProvider end

"""
    ProviderSpec(; feeds, backfill_feeds, bulk_feeds = String[], tz,
                 credential_keys = String[], endpoint_keys = String[], max_page_limit,
                 regular_session, price_unit, size_unit, timestamp_unit)

What the configuration layer must know about a provider before any provider
object exists: which feed names it accepts, the calendar its tape is filed
under, the credentials and endpoint keys it reads, the largest REST page the
venue serves, and the venue's regular session.

`bulk_feeds` names the feeds the venue also publishes as daily bulk files,
which [`bulk_trades`](@ref) downloads; it is empty for a venue without such
a service, and the configuration refuses `backfill.source = "bulk"` for it.

`credential_keys` names the environment variables the adapter needs, which
[`load_credentials!`](@ref) reads from the configured environment file or
the process environment; it is empty for public market data, and then no
credential is asked for. `endpoint_keys` names the keys of the provider's own
configuration table `[<name>]`, endpoint roots as a rule; any other key in
that table is rejected.

`max_page_limit` exists because a venue may clamp an over-limit request
instead of rejecting it, and a clamped page is indistinguishable from the last
page of the tape.

`regular_session` is `(open, close)` in exchange-local hours, `(0.0, 24.0)`
for a venue that never closes. The multi-day figures lay their days out on
it, and `window = :regular` in [`eachday`](@ref) and
[`read_processed`](@ref) selects it. Nothing in acquisition depends on it,
since the market-hours guards come from the venue's own clock.

`price_unit` and `size_unit` are what the venue's prices and sizes are
denominated in, as the figure axes should name them: dollars and shares on a
US equity tape, quote asset and base asset on a crypto pair.

`timestamp_unit` is the resolution of the venue's exchange timestamps as the
adapter receives them: `"nanosecond"` on the SIP, `"microsecond"` on
Binance, which answers in milliseconds unless asked. It is written into
every session sidecar, so the clock a capture was quantized to is known
after the fact.
"""
Base.@kwdef struct ProviderSpec
    feeds::Vector{String}
    backfill_feeds::Vector{String}
    bulk_feeds::Vector{String} = String[]
    tz::TimeZone
    credential_keys::Vector{String} = String[]
    endpoint_keys::Vector{String} = String[]
    max_page_limit::Int
    regular_session::Tuple{Float64,Float64}
    price_unit::String
    size_unit::String
    timestamp_unit::String
end

"""
    provider_spec(name::AbstractString) -> ProviderSpec
    provider_spec(::Val{name}) -> ProviderSpec

Static description of the provider called `name`. Implemented per adapter;
defining the `Val{name}` method is what registers the adapter
([`registered_providers`](@ref)).
"""
provider_spec(::Val{P}) where {P} = throw(
    ArgumentError(
        "unknown provider \"$(P)\"; registered providers: $(join(registered_providers(), ", "))",
    ),
)

provider_spec(name::AbstractString) = provider_spec(Val(Symbol(name)))

"""
    registered_providers() -> Vector{String}

The provider names a configuration may select, sorted: one for every
`provider_spec(::Val{name})` method. The shipped adapters are `"alpaca"` and
`"binance"`; an adapter defined in another package is registered by that
method alone, and is listed, configurable and selectable once its package
is loaded.
"""
function registered_providers()
    names = String[]
    for m in methods(provider_spec)
        sig = Base.unwrap_unionall(m.sig)
        sig isa DataType && length(sig.parameters) == 2 || continue
        v = sig.parameters[2]
        (v isa DataType && v <: Val && length(v.parameters) == 1) || continue
        name = v.parameters[1]
        name isa Symbol && push!(names, String(name))
    end
    return sort!(names)
end

"""
    make_provider(::Val{name}, cfg::Config) -> AbstractProvider

Construct the provider called `name` from the configuration. An adapter whose
spec lists `credential_keys` reads them with [`load_credentials!`](@ref) here,
so a provider that needs none never asks. Implemented per adapter.

The fallback throws: a fallback that returned a value would widen every
caller's inferred provider type, and each provider method downstream would
carry a branch with no matching method.
"""
make_provider(::Val{P}, ::Config) where {P} = throw(
    ArgumentError(
        "unknown provider \"$(P)\"; registered providers: $(join(registered_providers(), ", "))",
    ),
)

"""
    exchange_tz(p::AbstractProvider) -> TimeZone

The calendar the provider's tape is filed under, which decides the date a
print belongs to. Implemented by each provider.
"""
function exchange_tz end

"""
    exchange_day_start_ns(p::AbstractProvider, d::Date) -> Int64

First instant of exchange date `d` on this provider's calendar, in ns since
the epoch.

A request window and the date its rows are filed under must come from the
same clock: windows on UTC days and rows on exchange dates agree only while
the exchange sits at UTC-4, and lose an hour a day otherwise.
`ZonedDateTime` resolves the offset per date, including across the 23- and
25-hour transition days.
"""
function exchange_day_start_ns(p::AbstractProvider, d::Date)
    tz = exchange_tz(p)
    return round(
        Int64,
        datetime2unix(DateTime(astimezone(ZonedDateTime(DateTime(d), tz), tz"UTC"))),
    ) * NS_PER_SEC
end

"""
    always_open(p::AbstractProvider) -> Bool

Whether the venue never closes. `true` suppresses the market-hours guards:
there is no open to wait for and no close to stop at, and a 24-hour venue
has no meaningful `next_close`. The session is then bounded by
`limits.max_session_hours` alone.
"""
always_open(::AbstractProvider) = false

"""
    session_days(p::AbstractProvider, start_date, end_date) -> Vector{Date}

The dates in `[start_date, end_date]` on which the venue trades, in order.

Defaults to every calendar date; a venue with closed days overrides it.
Requesting a day the venue was shut costs one empty response, while skipping a
day it traded loses that day's tape, as a hard-coded weekday filter would for a
24-hour venue.
"""
session_days(::AbstractProvider, start_date::Date, end_date::Date) =
    collect(start_date:Day(1):end_date)

"""
    bulk_trades(p::AbstractProvider, symbol, date; feed, dest_dir,
                each_page = nothing, page_rows = 100_000, previous_id = nothing)
        -> (; trades, prints, file, id_breaks, last_id)

The trades of `symbol` on exchange date `date` from the venue's daily bulk
file, for a provider whose [`ProviderSpec`](@ref) lists `feed` in
`bulk_feeds`. The file is downloaded into `dest_dir`, verified against the
venue's checksum and kept there as the vendor's record of the day; a
present, verified file is reused without a request. Rows are parsed in
pages of `page_rows`: with `each_page` every page is passed to it and
`trades` is empty, otherwise the trades are accumulated in `trades`; `prints`
counts them either way and `file` is the vendor file read. A day the venue
has not published yields no trades and `file = ""`. Backfilled prints carry
`recv_ns = 0`. `id_breaks` and `last_id` report the continuity of the
venue's trade ids, `previous_id` being the `last_id` of the day before; a
venue without ids returns `0` and `nothing`.

The default method throws an `ArgumentError`.
"""
bulk_trades(p::AbstractProvider, ::AbstractString, ::Date; kwargs...) =
    throw(ArgumentError("provider $(nameof(typeof(p))) publishes no bulk files"))

"""
    historical_trade_count(p::AbstractProvider, symbol, start_str, end_str; kwargs...) -> Int

Provider interface: count the prints of the historical tape for `symbol` over
the inclusive RFC 3339 window `[start_str, end_str]`, the reference side of
[`coverage_report`](@ref). The Alpaca adapter implements it against the
consolidated tape; a provider without a countable tape throws `ArgumentError`.
"""
historical_trade_count(
    p::AbstractProvider,
    symbol::AbstractString,
    start_str::AbstractString,
    end_str::AbstractString;
    kwargs...,
) = throw(
    ArgumentError(
        "$(nameof(typeof(p))) has no countable historical tape; coverage_report supports Alpaca only (a Binance bulk backfill checks the vendor checksum and counts trade-id breaks in its sidecar)",
    ),
)

"""
    feed_delay_ns(p::AbstractProvider) -> Int64

Intrinsic delay of the provider's configured feed (ns): the wall-clock lag
between an exchange event and its earliest possible arrival on the wire.
Zero for real-time feeds. Market-hours guards shift by this amount so a
delayed session waits out the silent post-open window and captures the
delayed tape tail after the close.
"""
feed_delay_ns(::AbstractProvider) = Int64(0)

"""
    LiveSession

Handle for a running live stream: the tick channel plus control state.
Obtain via [`live_source`](@ref); request shutdown with [`stop!`](@ref).

`stop` is atomic: it is written by whoever ends the session and read by the
producer, the watchdog and the guard tasks, which run on other threads. The
logging layer reads the same flag to mute transport-teardown noise, so each
session has its own flag.

`outcome` records how the producer ended: `:running` while it streams,
`:completed` when it stopped on request or at the deadline, `:failed` after a
fatal stream error, exhausted reconnection attempts or an unexpected error.

`opens` counts the connections that opened (a protocol loop's
[`spawn_watchdog`](@ref) call records each) and `opened_at` holds the
`time()` of the latest; the reconnect loop judges a connection by them.
`grace` records that [`stop!`](@ref) has armed its bound on the producer.
`ws` is written and read under `wslock`, which orders a stop against a
connection being registered.
"""
struct LiveSession
    channel::Channel{Trade}
    stop::Threads.Atomic{Bool}
    ws::Ref{Any}
    stats::Ref{NamedTuple{(:ticks, :frames, :reconnects),NTuple{3,Int}}}
    outcome::Ref{Symbol}
    opens::Threads.Atomic{Int}
    opened_at::Threads.Atomic{Float64}
    grace::Threads.Atomic{Bool}
    wslock::ReentrantLock
end

LiveSession(channel, stop, ws, stats, outcome = Ref(:running)) = LiveSession(
    channel,
    stop,
    ws,
    stats,
    outcome,
    Threads.Atomic{Int}(0),
    Threads.Atomic{Float64}(0.0),
    Threads.Atomic{Bool}(false),
    ReentrantLock(),
)

"""
    stop!(s::LiveSession)

Request shutdown: set the stop flag and sever the WebSocket's transport,
which ends the producer loop's read at once; the tick channel then closes
and the sinks drain. Returns `nothing`.

No closing handshake is attempted from here: HTTP.jl's `close(ws)` reads
until the peer's CLOSE frame arrives, which on a silent peer lasts until TCP
gives up, and it would read the socket concurrently with the protocol loop,
which can leave one of the two readers waiting forever.

A protocol loop built on [`stream_websocket!`](@ref) registers its
connection before the upgrade request is answered, so the stop reaches it from
then on. Before that, while TCP and TLS are set up (bounded by HTTP.jl's
`connect_timeout`, 30 s), and in an adapter with a loop of its own, there is
no socket to sever; if the channel is still open `STOP_GRACE_S` seconds after
the first request, it is closed here, so the consumers end whatever state the
producer is in.
"""
function stop!(s::LiveSession)
    # Under the lock the provider registers a connection with
    # (`_register_ws!`): either it sees the flag or this sees its socket.
    ws = lock(s.wslock) do
        s.stop[] = true
        s.ws[]
    end
    ws === nothing || _sever(ws)
    Threads.atomic_xchg!(s.grace, true) || Threads.@spawn begin
        sleep(STOP_GRACE_S)
        if isopen(s.channel)
            try
                @warn "producer did not end after the stop request; closing its channel" grace_s =
                    STOP_GRACE_S
            finally
                close(s.channel)
            end
        end
    end
    return nothing
end

# Provider side of the stop handshake: register `conn`, an `HTTP.Connection`,
# and test the stop flag under the session's lock. Returns `true` when the
# session is stopping, the connection then severed. `stream_websocket!` calls
# it once the upgrade request is on the wire, before the answer is read.
function _register_ws!(s::LiveSession, conn)
    stopping = lock(s.wslock) do
        s.ws[] = conn
        s.stop[]
    end
    stopping && _sever(conn)
    return stopping
end

# Seconds `stop!` waits for the producer to close the channel itself.
const STOP_GRACE_S = 5.0

# Seconds a closing handshake may take before the transport is severed: a
# healthy peer answers a CLOSE within one round trip.
const CLOSE_GRACE_S = 2.0

# Sever `conn` (an `HTTP.Connection`) from a task other than the one reading
# it. The reading task then meets the end of the stream, and a closing
# handshake still running in it finishes at once.
#
# Julia's `close` of a socket first waits for its queued writes to drain,
# which a peer that has stopped reading never lets happen once the send
# buffer is full. `shutdown(2)` on the descriptor beforehand fails the queued
# writes and ends the pending read, so the close returns at once.
function _sever(conn)
    tcp = _tcp_socket(conn)
    if tcp !== nothing && Sys.isunix()
        # Under the I/O lock, which libuv's close of the socket also runs
        # under: the descriptor cannot be closed and handed to another socket
        # between the lookup and the call. A closed handle yields -1 (EBADF).
        Base.iolock_begin()
        try
            isopen(tcp) && ccall(:shutdown, Cint, (Cint, Cint), Base._fd(tcp), 2)   # SHUT_RDWR
        catch e
            e isa InterruptException && rethrow()
        finally
            Base.iolock_end()
        end
    end
    try
        close(conn)
    catch e
        e isa InterruptException && rethrow()
    end
    return nothing
end

# The TCP socket under `x`: `x` itself, a field of it, or a field of one of
# its fields. An `HTTP.Connection` holds the socket or the TLS stream
# (OpenSSL.jl, MbedTLS.jl) that wraps it.
function _tcp_socket(x, depth::Int = 2)
    x isa TCPSocket && return x
    depth == 0 && return nothing
    x isa IO || return nothing
    for f in fieldnames(typeof(x))
        isdefined(x, f) || continue
        tcp = _tcp_socket(getfield(x, f), depth - 1)
        tcp === nothing || return tcp
    end
    return nothing
end

_close_ws(ws) = _sever(ws.io)

# Close `ws` with a CLOSE frame and wait up to `CLOSE_GRACE_S` for the peer's
# answer, then sever the transport and wait as long again for the handshake to
# end. HTTP.jl's `close(ws)` alone reads until the answer arrives, which on a
# silent peer lasts until TCP gives up. Returns the task that ran the
# handshake.
function _close_bounded(ws)
    closing = Threads.@spawn try
        close(ws)
    catch e
        e isa InterruptException && rethrow()
    end
    if timedwait(() -> istaskdone(closing), CLOSE_GRACE_S; pollint = 0.05) !== :ok
        _close_ws(ws)
        timedwait(() -> istaskdone(closing), CLOSE_GRACE_S; pollint = 0.05)
    end
    return closing
end

# Guard task: `stop!` the session once the wall clock passes `stop_ns`
# (ns since epoch). Shared by the market-hours guard and the session
# deadline. Returns the task; it exits early if the session stops first.
function _stop_at!(s::LiveSession, stop_ns::Int64, message::AbstractString)
    return Threads.@spawn begin
        while !s.stop[]
            remaining = (stop_ns - now_ns()) / 1e9
            remaining <= 0 && break
            sleep(min(remaining, 5.0))
        end
        if !s.stop[]
            @info message
            stop!(s)
        end
    end
end

"""
    schedule_close_stop!(s::LiveSession, close_ns; grace_s = 5.0) -> Task

Spawn a guard task that calls [`stop!`](@ref) on the session once the
wall clock passes `close_ns` (ns since epoch, e.g. the market's
`next_close`) plus `grace_s`. Without this, a streamer left unattended sits
on a silent overnight connection until the session deadline.
"""
function schedule_close_stop!(s::LiveSession, close_ns::Int64; grace_s::Real = 5.0)
    return _stop_at!(
        s,
        close_ns + round(Int64, grace_s * NS_PER_SEC),
        "market close reached; stopping session",
    )
end

"""
    stream_protocol!(ch, p::AbstractProvider, cfg, s::LiveSession;
                     on_quote = nothing, on_bar = nothing)

Provider interface: run one connect→auth→subscribe→stream cycle, pushing
normalized `Trade`s into `ch`. Must return on orderly close, throw
[`FatalStreamError`](@ref) on non-retryable protocol errors, and any other
exception on retryable transport failures. Implemented by each provider
(`providers/alpaca.jl`, `providers/binance.jl`). Both build on
[`stream_websocket!`](@ref), which handles the session's side of a
connection: the stop handshake, the watchdog, the bound on the upgrade, and
the record of the connection as opened that the reconnect counter and the
session status read. An adapter with a loop of its own calls
[`spawn_watchdog`](@ref) once connected, which makes that record.

`on_quote` and `on_bar`, when given, receive normalized [`Quote`](@ref) and
[`Bar`](@ref) values for the corresponding frames. Neither channel is
subscribed to by default, so neither callback fires unless
`stream.channels` asks for it.
"""
function stream_protocol! end

"""
    spawn_watchdog(ws, s::LiveSession, last_frame, alive, stale_timeout_s) -> Task

Provider interface: force-close `ws` once `last_frame[]` is older than
`stale_timeout_s` seconds. HTTP.jl 1.x WebSockets have no read idle timeout,
so a silently dead TCP connection would otherwise block the read loop
forever. A protocol loop spawns it on connect, writes `time()` into
`last_frame` (an atomic `Float64`) per frame, and before returning sets
`alive[] = false` and `wait`s the task; [`stream_websocket!`](@ref) does all
of this for the loops built on it. Both atomics are written by the
protocol loop and read by this task on another thread.

The call also records the connection as opened (`s.opens`, `s.opened_at`)
and restarts `last_frame` at that instant, so a connection the watchdog cuts
was open for at least `stale_timeout_s`.
"""
function spawn_watchdog(
    ws,
    s::LiveSession,
    last_frame::Threads.Atomic{Float64},
    alive::Threads.Atomic{Bool},
    stale_timeout_s::Float64,
)
    t = time()
    s.opened_at[] = t
    last_frame[] = t
    Threads.atomic_add!(s.opens, 1)
    return Threads.@spawn begin
        while alive[] && !s.stop[]
            if time() - last_frame[] > stale_timeout_s
                @warn "stale connection; closing" stale_timeout_s
                _close_ws(ws)
                break
            end
            sleep(0.25)   # fine-grained so connection teardown isn't held up
        end
    end
end

"""
    stream_websocket!(on_frame, s::LiveSession, url, stale_timeout_s;
                      headers = Pair{String,String}[]) -> Nothing

Provider interface: open a WebSocket to `url` and call `on_frame(ws, message)`
for every message received, until the session stops, the connection ends, or
`on_frame` returns a [`FatalStreamError`](@ref), which is then thrown here;
any other return value continues the stream. `ws` is the
`HTTP.WebSockets.WebSocket`, for the messages the venue expects in reply
(authentication, subscription). An exception thrown by `on_frame` ends the
connection as a retryable failure. HTTP.jl wraps it on the way out, which is
why a non-retryable condition is returned and not thrown. Once the session
is stopping, an error out of the connection is the sever's doing and the
function returns `nothing`; an interrupt is rethrown.

The function performs the session's side of a connection, which an adapter's
[`stream_protocol!`](@ref) then need not:

- the connection is registered with the session as soon as the upgrade
  request is sent, so [`stop!`](@ref) severs it whether or not the server
  has answered;
- an upgrade not answered within `stale_timeout_s` seconds severs the
  connection, which the reconnect loop counts as an attempt that never
  opened;
- once the upgrade is accepted, [`spawn_watchdog`](@ref) records the
  connection as opened and severs it when no message arrives for
  `stale_timeout_s` seconds; every message counts as a frame (`bump!`);
- the connection ends with a closing handshake bounded at a few seconds.

`headers` are added to the upgrade request.

```julia
function stream_protocol!(ch, p::VenueProvider, cfg, s::LiveSession; kw...)
    MarketTickStreamer.stream_websocket!(s, p.ws_url, cfg.stale_timeout_s) do ws, msg
        frame = JSON.parse(msg)
        haskey(frame, :error) && return FatalStreamError("venue error: \$(frame.error)")
        put!(ch, parse_venue_trade(frame, now_ns()))
        MarketTickStreamer.bump!(s; ticks = 1)
        return nothing
    end
end
```
"""
function stream_websocket!(
    on_frame,
    s::LiveSession,
    url::AbstractString,
    stale_timeout_s::Float64;
    headers = Pair{String,String}[],
)
    key = base64encode(rand(RandomDevice(), UInt8, 16))
    request_headers = [
        "Upgrade" => "websocket",
        "Connection" => "Upgrade",
        "Sec-WebSocket-Key" => key,
        "Sec-WebSocket-Version" => "13",
        headers...,
    ]
    fatal = Ref{Union{Nothing,FatalStreamError}}(nothing)
    # The client half of `HTTP.WebSockets.open`, taken here so that the
    # connection is in hand before the answer to the upgrade is read. HTTP.jl
    # does not retry a request with a stream handler; `retry = false` says so
    # explicitly, since the session's reconnect loop owns the attempts.
    try
        _websocket_request(on_frame, s, url, request_headers, key, stale_timeout_s, fatal)
    catch e
        # An interrupt that landed in this task arrives wrapped by HTTP.jl; it
        # leaves as itself, so no caller takes it for a disconnect.
        has_wrapped_exception(e, InterruptException) && throw(InterruptException())
        # A stop severs the transport, so whatever was reading it fails; after
        # a stop that failure is the orderly end.
        s.stop[] || rethrow()
    end
    fatal[] === nothing || throw(fatal[])
    return nothing
end

function _websocket_request(on_frame, s, url, request_headers, key, stale_timeout_s, fatal)
    HTTP.open("GET", url, request_headers; retry = false) do http
        conn = http.stream
        _register_ws!(s, conn) && return nothing      # stopped before the answer
        _await_upgrade(http, key, stale_timeout_s)
        http.ntoread = 0                     # the stream holds no HTTP body
        ws = HTTP.WebSockets.WebSocket(conn, http.message.request, http.message)
        last_frame = Threads.Atomic{Float64}(time())
        alive = Threads.Atomic{Bool}(true)
        watchdog = spawn_watchdog(ws, s, last_frame, alive, stale_timeout_s)
        try
            for msg in ws
                s.stop[] && break
                last_frame[] = time()
                bump!(s; frames = 1)
                r = on_frame(ws, msg)
                if r isa FatalStreamError
                    fatal[] = r
                    break
                end
            end
        finally
            alive[] = false
            wait(watchdog)
            _close_bounded(ws)
        end
    end
    return nothing
end

# Read the answer to the upgrade request on `http`, severing the connection
# if it has not arrived after `timeout_s`, and check it (RFC 6455, 4.1): status
# 101, `Upgrade: websocket`, `Connection: Upgrade` and the accept key derived
# from `key` (4.2.2). Throws on a refused or unanswered upgrade.
function _await_upgrade(http, key::AbstractString, timeout_s::Float64)
    answered = Threads.Atomic{Bool}(false)
    timer = Timer(timeout_s) do _
        if !answered[]
            @warn "upgrade not answered; closing the connection" timeout_s
            _sever(http.stream)
        end
    end
    try
        HTTP.startread(http)
    finally
        answered[] = true
        close(timer)
    end
    r = http.message
    accept = base64encode(sha1(key * "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
    ok =
        r.status == 101 &&
        lowercase(HTTP.header(r, "Upgrade")) == "websocket" &&
        _has_token(r, "Connection", "upgrade") &&
        HTTP.header(r, "Sec-WebSocket-Accept") == accept
    ok || error("WebSocket upgrade refused: HTTP $(r.status)")
    return nothing
end

# Whether the header `name` of message `m` lists `token` (case-insensitive),
# over every field of that name: a list may be split across several (RFC 9110,
# 5.3).
_has_token(m, name::AbstractString, token::AbstractString) = any(
    t -> lowercase(strip(t)) == token,
    Iterators.flatten(split(v, ',') for v in HTTP.headers(m, name)),
)

"""
    bump!(s::LiveSession; ticks = 0, frames = 0, reconnects = 0)

Provider interface: add to the session's counters. Single writer: only the
producer task calls it, so no lock is needed.
"""
function bump!(s::LiveSession; ticks = 0, frames = 0, reconnects = 0)
    st = s.stats[]
    s.stats[] = (;
        ticks = st.ticks + ticks,
        frames = st.frames + frames,
        reconnects = st.reconnects + reconnects,
    )
    return nothing
end

"""
    live_source(p::AbstractProvider, cfg::Config;
                on_quote = nothing, on_bar = nothing,
                stop = Threads.Atomic{Bool}(false)) -> LiveSession

Start the live producer task. Streams ticks into `session.channel` until the
session deadline (`limits.max_session_hours`), a [`stop!`](@ref) call, a
fatal protocol error, or reconnection exhaustion, whichever comes first.
The channel is closed on exit, which ends downstream consumers' loops.

`on_quote` and `on_bar` are handed normalized [`Quote`](@ref) and
[`Bar`](@ref) values when those channels are subscribed to
(`stream.channels`); neither is by default, and neither is persisted
(see [`Quote`](@ref)).

`stop` lets the caller own the session's stop flag, which [`run_stream`](@ref)
shares with its logger.

Reconnects with jittered exponential backoff
(`stream.reconnect_base_delay_s * 2^attempt`, capped at
`stream.reconnect_max_delay_s` before a jitter factor in [0.5, 1.5) drawn from a
generator seeded by `rng.seed`); the attempt counter resets after any connection
that delivered prints, or that opened and stayed open for
`stream.stale_timeout_s`, so a quiet market does not exhaust the attempts of
a healthy feed while an endpoint that never opens a connection still does.
"""
function live_source(
    p::AbstractProvider,
    cfg::Config;
    on_quote = nothing,
    on_bar = nothing,
    stop::Threads.Atomic{Bool} = Threads.Atomic{Bool}(false),
)
    ch = Channel{Trade}(cfg.channel_capacity)
    session = LiveSession(
        ch,
        stop,
        Ref{Any}(nothing),
        Ref((; ticks = 0, frames = 0, reconnects = 0)),
    )
    deadline = time() + cfg.max_session_hours * 3600
    # The reconnect loop below only tests the deadline between connections; a
    # healthy connection never returns to it, so a guard task enforces the
    # limit while the stream is up.
    _stop_at!(
        session,
        now_ns() + round(Int64, cfg.max_session_hours * 3600 * NS_PER_SEC),
        "session deadline reached; stopping session",
    )
    # The producer task is never waited on; `errormonitor` logs an error that
    # escapes it.
    errormonitor(
        Threads.@spawn begin
            attempt = 0
            rng = Xoshiro(UInt64(cfg.rng_seed))
            try
                while !session.stop[] && time() < deadline
                    ticks_before = session.stats[].ticks
                    opens_before = session.opens[]
                    try
                        stream_protocol!(ch, p, cfg, session; on_quote, on_bar)
                        session.stop[] || @warn "stream closed by server"
                    catch e
                        # An adapter with a loop of its own may let an interrupt
                        # out wrapped, as HTTP.jl hands it over.
                        has_wrapped_exception(e, InterruptException) &&
                            throw(InterruptException())
                        e isa FatalStreamError && rethrow()
                        session.stop[] && break
                        @warn "stream disconnected" exception = (e, catch_backtrace())
                    end
                    session.stop[] && break
                    # A connection that delivered prints was healthy, and so was one
                    # that opened and stayed open as long as the watchdog allows a
                    # silent one: a quiet symbol cut by the watchdog is not a failure
                    # to connect. No frame is asked for, since a Binance stream sends
                    # none until a trade. The time is counted from the opening, so an
                    # attempt that takes long to fail does not pass for a connection.
                    healthy =
                        session.stats[].ticks > ticks_before || (
                            session.opens[] > opens_before &&
                            time() - session.opened_at[] >= cfg.stale_timeout_s
                        )
                    healthy && (attempt = 0)
                    attempt += 1
                    if attempt > cfg.reconnect_max_retries
                        @error "reconnection attempts exhausted" attempt
                        session.outcome[] = :failed
                        break
                    end
                    bump!(session; reconnects = 1)
                    delay = _backoff_delay(
                        rng,
                        cfg.reconnect_base_delay_s,
                        attempt - 1,
                        cfg.reconnect_max_delay_s,
                    )
                    @info "reconnecting" attempt delay = round(delay; digits = 1)
                    # Slept in slices, so a stop request or the deadline ends the wait.
                    wake = min(time() + delay, deadline)
                    while !session.stop[] && time() < wake
                        sleep(clamp(wake - time(), 0.0, 0.25))
                    end
                end
            catch e
                # An unexpected error marks the session failed and is logged where
                # the session logs. An interrupt that landed in this task ends the
                # producer: it is rethrown, the outcome below reads `:completed`,
                # and the channel closes, which ends the session in order.
                e isa InterruptException && rethrow()
                session.outcome[] = :failed
                if e isa FatalStreamError
                    @error "fatal stream error; aborting" e.msg
                else
                    @error "live producer failed" exception = (e, catch_backtrace())
                end
            finally
                session.outcome[] === :running && (session.outcome[] = :completed)
                # The guard tasks end on the stop flag; without it they would outlive
                # a producer that ended on its own and stop a dead session later.
                session.stop[] = true
                close(ch)
            end
        end
    )
    return session
end
