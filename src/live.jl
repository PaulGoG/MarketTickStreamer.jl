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
for the name it answers to, [`exchange_tz`](@ref), [`stream_protocol!`](@ref),
[`market_clock`](@ref), [`historical_trades`](@ref) and its own message
parsing. It overrides [`always_open`](@ref), [`session_days`](@ref) and
[`feed_delay_ns`](@ref) where the venue departs from the defaults.

Beyond the exported names, the implementer interface is `public`:
[`stream_protocol!`](@ref), [`feed_delay_ns`](@ref),
[`exchange_day_start_ns`](@ref), [`spawn_watchdog`](@ref) and
[`bump!`](@ref) are documented, versioned with the package and reached
qualified.

The adapters that ship are `providers/alpaca.jl` (US equities, New York
calendar, credentialed, closes overnight and at weekends) and
`providers/binance.jl` (crypto spot, UTC calendar, public, never closes).
"""
abstract type AbstractProvider end

"""
    ProviderSpec(feeds, backfill_feeds, bulk_feeds, tz, needs_credentials, max_page_limit,
                 regular_session, price_unit, size_unit, timestamp_unit)

What the configuration layer must know about a provider *before* any provider
object exists: which feed names it accepts, the calendar its tape is filed
under, whether it needs credentials at all, the largest REST page the venue
serves, and the venue's regular session.

Without it the config layer would have to hard-code one vendor's vocabulary:
`provider.feed` would be validated against Alpaca's feed names for every
provider, and a 24-hour venue would have its days cut at midnight in New York.

`bulk_feeds` names the feeds the venue also publishes as daily bulk files,
which [`bulk_trades`](@ref) downloads; it is empty for a venue without such
a service, and the configuration refuses `backfill.source = "bulk"` for it.

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
adapter receives them — `"nanosecond"` on the SIP, `"microsecond"` on
Binance, which answers in milliseconds unless asked. It is written into
every session sidecar, so the clock a capture was quantized to is known
after the fact.
"""
struct ProviderSpec
    feeds::Vector{String}
    backfill_feeds::Vector{String}
    bulk_feeds::Vector{String}
    tz::TimeZone
    needs_credentials::Bool
    max_page_limit::Int
    regular_session::Tuple{Float64,Float64}
    price_unit::String
    size_unit::String
    timestamp_unit::String
end

"""Provider names this build knows how to construct."""
const KNOWN_PROVIDERS = ("alpaca", "binance")

"""
    provider_spec(name::AbstractString) -> ProviderSpec
    provider_spec(::Val{name}) -> ProviderSpec

Static description of the provider called `name`. Implemented per adapter.
"""
provider_spec(::Val{P}) where {P} = throw(
    ArgumentError(
        "unknown provider \"$(P)\"; known providers: $(join(KNOWN_PROVIDERS, ", "))",
    ),
)

provider_spec(name::AbstractString) = provider_spec(Val(Symbol(name)))

"""
    make_provider(::Val{name}, cfg, key, secret) -> AbstractProvider

Construct the provider called `name` from configuration and credentials.
`key`/`secret` are empty strings when the spec says none are needed.
Implemented per adapter.

The fallback throws rather than returning anything: a fallback with a value
would widen every caller's inferred provider type to include it, and then
each provider method downstream would carry a branch with no matching method.
"""
make_provider(::Val{P}, ::Config, ::AbstractString, ::AbstractString) where {P} = throw(
    ArgumentError(
        "unknown provider \"$(P)\"; known providers: $(join(KNOWN_PROVIDERS, ", "))",
    ),
)

"""
    exchange_tz(p::AbstractProvider) -> TimeZone

The calendar the provider's tape is filed under — the clock that decides which
date a print belongs to. Implemented by each provider.
"""
function exchange_tz end

"""
    exchange_day_start_ns(p::AbstractProvider, d::Date) -> Int64

First instant of exchange date `d` on this provider's calendar, in ns since
the epoch.

A request window and the key its rows are bucketed by must come from the same
clock. Building the window on UTC days while filing rows on exchange dates
agrees only while the exchange sits at UTC-4, and silently loses an hour a day
otherwise. `ZonedDateTime` resolves the offset per
date, including across the 23- and 25-hour transition days.
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
has no meaningful `next_close`. The session is then
bounded by `limits.max_session_hours` alone.
"""
always_open(::AbstractProvider) = false

"""
    session_days(p::AbstractProvider, start_date, end_date) -> Vector{Date}

The dates in `[start_date, end_date]` on which the venue trades, in order.

Defaults to every calendar date. A venue that rests must opt out, never the
reverse: requesting a day the venue was shut costs one empty response, while
skipping a day it traded loses that day's tape silently, as a hard-coded
weekday filter would for a 24-hour venue.
"""
session_days(::AbstractProvider, start_date::Date, end_date::Date) =
    collect(start_date:Day(1):end_date)

"""
    bulk_trades(p::AbstractProvider, symbol, date; feed, dest_dir,
                each_page = nothing, page_rows = 100_000) -> (; trades, prints, file)

The trades of `symbol` on exchange date `date` from the venue's daily bulk
file, for a provider whose [`ProviderSpec`](@ref) lists `feed` in
`bulk_feeds`. The file is downloaded into `dest_dir`, verified against the
venue's checksum and kept there as the vendor's record of the day; a
present, verified file is reused without a request. Rows are parsed in
pages of `page_rows`: with `each_page` every page is passed to it and
`trades` is empty, otherwise the trades are accumulated in `trades`; `prints`
counts them either way and `file` is the vendor file read. A day the venue
has not published yields no trades and `file = ""`. Backfilled prints carry
`recv_ns = 0`.

The default method throws: a venue without bulk files cannot be asked for
one.
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
        "$(nameof(typeof(p))) has no countable historical tape; coverage_report supports Alpaca only (Binance days are verified by vendor checksum and trade-id continuity)",
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
logging layer reads the same flag to mute transport-teardown noise, so the flag
belongs to one session and not to the process.

`outcome` records how the producer ended: `:running` while it streams,
`:completed` when it stopped on request, at the deadline or after the server
closed the stream, `:failed` after a fatal stream error or exhausted
reconnection attempts.
"""
struct LiveSession
    channel::Channel{Trade}
    stop::Threads.Atomic{Bool}
    ws::Ref{Any}
    stats::Ref{NamedTuple{(:ticks, :frames, :reconnects),NTuple{3,Int}}}
    outcome::Ref{Symbol}
end

LiveSession(channel, stop, ws, stats) = LiveSession(channel, stop, ws, stats, Ref(:running))

"""
    stop!(s::LiveSession)

Request shutdown: set the stop flag and close the underlying WebSocket,
which unblocks the producer loop; the tick channel then closes and the sinks
drain.
"""
function stop!(s::LiveSession)
    s.stop[] = true
    ws = s.ws[]
    if ws !== nothing
        try
            close(ws)
        catch e
            e isa InterruptException && rethrow()
        end
        # close() is a handshake; an unresponsive peer that never acks the
        # CLOSE frame would hang the read loop, so sever the transport after
        # a short grace if it is still open.
        Threads.@spawn begin
            sleep(5.0)
            try
                close(ws.io)
            catch e
                e isa InterruptException && rethrow()
            end
        end
    end
    return nothing
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
(`providers/alpaca.jl`, `providers/binance.jl`).

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
`alive[] = false` and `wait`s the task. Both atomics are written by the
protocol loop and read by this task on another thread.
"""
function spawn_watchdog(
    ws,
    s::LiveSession,
    last_frame::Threads.Atomic{Float64},
    alive::Threads.Atomic{Bool},
    stale_timeout_s::Float64,
)
    return Threads.@spawn begin
        while alive[] && !s.stop[]
            if time() - last_frame[] > stale_timeout_s
                @warn "stale connection; closing" stale_timeout_s
                try
                    close(ws)
                catch e
                    e isa InterruptException && rethrow()
                end
                break
            end
            sleep(0.25)   # fine-grained so connection teardown isn't held up
        end
    end
end

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
fatal protocol error, or reconnection exhaustion — whichever comes first.
The channel is closed on exit, which ends downstream consumers' loops.

`on_quote` and `on_bar` are handed normalized [`Quote`](@ref) and
[`Bar`](@ref) values when those channels are subscribed to
(`stream.channels`); neither is by default, and neither is persisted
(see [`Quote`](@ref)).

`stop` lets the caller own the session's stop flag, which [`run_stream`](@ref)
shares with its logger.

Reconnects with jittered exponential backoff
(`stream.reconnect_base_delay_s * 2^attempt`, capped at
`stream.reconnect_max_delay_s`, the jitter drawn from a generator seeded by
`rng.seed`); the attempt counter resets after any connection that
delivered data.
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
    Threads.@spawn begin
        attempt = 0
        rng = Xoshiro(UInt64(cfg.rng_seed))
        try
            while !session.stop[] && time() < deadline
                ticks_before = session.stats[].ticks
                try
                    stream_protocol!(ch, p, cfg, session; on_quote, on_bar)
                    session.stop[] || @warn "stream closed by server"
                catch e
                    (e isa FatalStreamError || e isa InterruptException) && rethrow()
                    session.stop[] && break
                    @warn "stream disconnected" exception = (e, catch_backtrace())
                end
                session.stop[] && break
                session.stats[].ticks > ticks_before && (attempt = 0)
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
                sleep(delay)
            end
        catch e
            e isa FatalStreamError || rethrow()
            session.outcome[] = :failed
            @error "fatal stream error; aborting" e.msg
        finally
            session.outcome[] === :running && (session.outcome[] = :completed)
            close(ch)
        end
    end
    return session
end
