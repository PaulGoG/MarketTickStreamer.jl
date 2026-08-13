# Live WebSocket source: connect → auth → subscribe → stream, with jittered
# exponential-backoff reconnection and a hard session deadline.
#
# The producer feeds a bounded Channel{Trade}; if consumers stall, `put!`
# blocks and TCP backpressure does the rest — no unbounded buffering.

"""
    AbstractProvider

Supertype for market-data provider adapters. A concrete provider implements
`ws_url`, `auth_payload`, `subscribe_payload`, `market_clock`,
`historical_trades` and message parsing (see `providers/alpaca.jl`).
"""
abstract type AbstractProvider end

"""
    feed_delay_ns(p::AbstractProvider) -> Int64

Intrinsic delay of the provider's configured feed (ns): the wall-clock lag
between an exchange event and its earliest possible arrival on the wire.
Zero for real-time feeds. Market-hours railings shift by this amount so a
delayed session waits out the silent post-open window and captures the
delayed tape tail after the close.
"""
feed_delay_ns(::AbstractProvider) = Int64(0)

"""
    FatalStreamError(msg)

A stream error that must NOT trigger reconnection (bad credentials,
connection-limit exceeded, invalid subscription). Aborts the session.
"""
struct FatalStreamError <: Exception
    msg::String
end

# Alpaca error codes that retrying cannot fix.
const FATAL_WS_CODES = (401, 402, 403, 404, 405, 406, 409, 410, 411)

# Set while a session shuts down deliberately; the logging layer uses it to
# suppress transport-teardown noise from HTTP.jl internals (an EOFError from
# a socket we closed ourselves is expected, not an incident).
const SHUTTING_DOWN = Ref(false)

"""
    LiveSession

Handle for a running live stream: the tick channel plus control state.
Obtain via [`live_source`](@ref); request shutdown with [`stop!`](@ref).
"""
struct LiveSession
    channel::Channel{Trade}
    stop::Ref{Bool}
    ws::Ref{Any}
    stats::Ref{NamedTuple{(:ticks, :frames, :reconnects),NTuple{3,Int}}}
end

"""
    stop!(s::LiveSession)

Signal graceful shutdown: sets the stop flag and closes the underlying
WebSocket, which unblocks the producer loop; the tick channel then closes,
letting sinks drain and finish.
"""
function stop!(s::LiveSession)
    s.stop[] = true
    SHUTTING_DOWN[] = true
    ws = s.ws[]
    if ws !== nothing
        try
            close(ws)
        catch
        end
        # close() is a handshake; an unresponsive peer that never acks the
        # CLOSE frame would hang the read loop, so sever the transport after
        # a short grace if it is still open.
        Threads.@spawn begin
            sleep(5.0)
            try
                close(ws.io)
            catch
            end
        end
    end
    return nothing
end

"""
    schedule_close_stop!(s::LiveSession, close_ns; grace_s = 5.0) -> Task

Spawn a guard task that gracefully [`stop!`](@ref)s the session once the
wall clock passes `close_ns` (ns since epoch, e.g. the market's
`next_close`) plus `grace_s`. Without this, a streamer left unattended sits
on a silent overnight connection until the session deadline.
"""
function schedule_close_stop!(s::LiveSession, close_ns::Int64; grace_s::Real = 5.0)
    return Threads.@spawn begin
        while !s.stop[]
            remaining = (close_ns - now_ns()) / 1e9 + grace_s
            remaining <= 0 && break
            sleep(min(remaining, 5.0))
        end
        if !s.stop[]
            @info "market close reached — stopping session"
            stop!(s)
        end
    end
end

"""
    stream_protocol!(ch, p::AbstractProvider, cfg, s::LiveSession)

Provider interface: run one connect→auth→subscribe→stream cycle, pushing
normalized `Trade`s into `ch`. Must return on orderly close, throw
[`FatalStreamError`](@ref) on non-retryable protocol errors, and any other
exception on retryable transport failures. Implemented per provider
(see `providers/alpaca.jl`).
"""
function stream_protocol! end

# Watchdog: force-close `ws` if `last_frame[]` goes stale — HTTP.jl 1.x
# websockets have no read idle timeout, and a silently dead TCP connection
# would otherwise block the read loop forever. Providers call this inside
# their protocol loop; set `alive[] = false` and `wait` it before returning.
function spawn_watchdog(
    ws,
    s::LiveSession,
    last_frame::Ref{Float64},
    alive::Ref{Bool},
    stale_timeout_s::Float64,
)
    return Threads.@spawn begin
        while alive[] && !s.stop[]
            if time() - last_frame[] > stale_timeout_s
                @warn "no frames for $(stale_timeout_s)s — closing stale connection"
                try
                    close(ws)
                catch
                end
                break
            end
            sleep(0.25)   # fine-grained so connection teardown isn't held up
        end
    end
end

# Lock-free single-writer stats bump (only the producer task mutates).
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
    live_source(p::AbstractProvider, cfg::Config) -> LiveSession

Start the live producer task. Streams ticks into `session.channel` until the
session deadline (`limits.max_session_hours`), a [`stop!`](@ref) call, a
fatal protocol error, or reconnection exhaustion — whichever comes first.
The channel is closed on exit so downstream consumers terminate cleanly.

Reconnects with jittered exponential backoff
(`stream.reconnect_base_delay_s * 2^attempt`, capped at
`stream.reconnect_max_delay_s`); the attempt counter resets after any
connection that actually delivered data.
"""
function live_source(p::AbstractProvider, cfg::Config)
    SHUTTING_DOWN[] = false
    ch = Channel{Trade}(cfg.channel_capacity)
    session = LiveSession(
        ch,
        Ref(false),
        Ref{Any}(nothing),
        Ref((; ticks = 0, frames = 0, reconnects = 0)),
    )
    deadline = time() + cfg.max_session_hours * 3600
    Threads.@spawn begin
        attempt = 0
        try
            while !session.stop[] && time() < deadline
                ticks_before = session.stats[].ticks
                try
                    stream_protocol!(ch, p, cfg, session)
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
                    break
                end
                bump!(session; reconnects = 1)
                delay =
                    min(
                        cfg.reconnect_base_delay_s * 2.0^(attempt - 1),
                        cfg.reconnect_max_delay_s,
                    ) * (0.5 + rand())
                @info "reconnecting" attempt delay = round(delay; digits = 1)
                sleep(delay)
            end
        catch e
            e isa FatalStreamError ? (@error "fatal stream error — aborting" e.msg) :
            rethrow()
        finally
            close(ch)
        end
    end
    return session
end
