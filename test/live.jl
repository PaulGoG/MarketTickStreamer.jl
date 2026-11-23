@testset "live E2E: quote and bar frames reach their callbacks" begin
    mktempdir() do dir
        ws_port = freeport(9651)
        plan = MockPlan(
            [[
                mock_trade("AAPL", 1),
                mock_quote("AAPL", 2),
                mock_bar("AAPL", 3),
                mock_trade("AAPL", 4),
            ]];
            fatal_after = 1,
        )
        ws, nconn = start_mock_ws(plan; port = ws_port)
        try
            cfg = load_config(
                mock_config_toml(dir; ws_port, rest_port = ws_port, max_retries = 0),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            quotes, bars = Quote[], Bar[]
            session = live_source(
                p,
                cfg;
                on_quote = q -> push!(quotes, q),
                on_bar = b -> push!(bars, b),
            )
            ticks = collect(session.channel)      # returns when the stream ends
            @test length(ticks) == 2              # trades still go to the channel
            @test length(quotes) == 1 && quotes[1].ask_price == 102.5
            @test length(bars) == 1 && bars[1].trade_count == 42
            @test all(q -> q.recv_ns > 0, quotes)
        finally
            close(ws)
        end
    end
end

@testset "live E2E: stream → reconnect → fatal stop → raw files" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(8951), freeport(8971)
        plan = MockPlan(
            [[mock_trade("AAPL", i) for i in 1:8], [mock_trade("MSFT", i) for i in 9:14]];
            fatal_after = 2,
        )               # 3rd connection → fatal 409
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(mock_config_toml(dir; ws_port, rest_port))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            result = run_stream(cfg; provider = p)
            @test result.ticks == 14                   # both batches, nothing lost
            @test result.status == "failed"
            @test nconn[] == 3                         # initial + 1 reconnect + fatal
            @test length(result.raw_files) == 1
            trades = read_raw(result.raw_files)
            @test length(trades) == 14
            @test count(t -> t.symbol == "AAPL", trades) == 8
            @test all(t -> t.recv_ns > 0, trades)
            # captured raw feeds straight into compaction + replay
            files = compact_raw(result.raw_files, joinpath(dir, "proc"); format = :csv)
            @test !isempty(files)
            @test length(collect(replay_source(result.raw_files; pace = :max))) == 14
            # A finished session read through the file-following source: same prints,
            # file order.
            @test collect(follow_source(cfg.raw_dir)) == trades
            # provenance sidecar written next to the raw files
            metas = filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))
            @test length(metas) == 1
            meta = TOML.parsefile(metas[1])
            @test meta["session"]["ticks"] == 14
            @test meta["session"]["raw_files"] == basename.(result.raw_files)
            @test meta["config"]["feed"] == "iex"
            @test meta["config"]["rng_seed"] == cfg.rng_seed
            @test meta["provenance"]["timestamp_unit"] == "nanosecond"
            prov = meta["provenance"]
            # git_available tells a real commit from the placeholders of a
            # copy that is not a checkout.
            @test prov["git_available"] isa Bool
            @test prov["git_available"] ? occursin(r"^[0-9a-f]{40}$", prov["git_commit"]) :
                  prov["git_commit"] == "unknown" && prov["git_dirty"] === false
            # hardware fingerprint: results attributable to config + commit + hardware
            hw = meta["hardware"]
            @test !isempty(hw["cpu_model"])
            @test hw["cpu_threads"] >= 1 && hw["julia_threads"] >= 1
            @test hw["blas_threads"] >= 1
            @test hw["total_memory_gib"] > 0
            @test occursin("Julia Version", hw["versioninfo"])
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live E2E: early market close stops the session" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9151), freeport(9171)
        # one batch, then the connection idles: only the close guard can end
        # this session before the 30 s linger or the session deadline.
        plan = MockPlan([[mock_trade("AAPL", i) for i in 1:3]]; linger_s = 30.0)
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port, close_in_s = 2.0)
        try
            cfg = load_config(mock_config_toml(dir; ws_port, rest_port))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            t0 = time()
            result = run_stream(cfg; provider = p)
            @test result.ticks == 3
            @test result.status == "completed"
            @test nconn[] == 1                 # close guard, not reconnect exhaustion
            @test time() - t0 < 20.0           # well before linger end and deadline
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live E2E: the session deadline stops a healthy connection" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9152), freeport(9172)
        # One batch, then the connection idles but stays up, and the market
        # stays open for an hour: neither a disconnect nor the close guard
        # ends this session, so only the deadline can, well inside the
        # 30 s linger.
        plan = MockPlan([[mock_trade("AAPL", i) for i in 1:3]]; linger_s = 30.0)
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(
                mock_config_toml(dir; ws_port, rest_port, max_session_hours = 2.0 / 3600),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            t0 = time()
            result = run_stream(cfg; provider = p)
            @test result.ticks == 3
            @test result.status == "completed"
            @test nconn[] == 1                 # stopped, not reconnected
            @test time() - t0 < 15.0
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live: a provider error is retried until the attempts run out" begin
    # Any error out of stream_protocol! other than FatalStreamError counts
    # as a disconnect; the session fails once the attempts are used up.
    @eval struct FailingProvider <: MarketTickStreamer.AbstractProvider end
    @eval MarketTickStreamer.stream_protocol!(ch, ::FailingProvider, cfg, s; kwargs...) =
        error("unexpected")
    mktempdir() do dir
        cfg = load_config(mock_config_toml(dir; ws_port = 9999, rest_port = 9998))
        s = @test_logs (:error, r"reconnection attempts exhausted") match_mode = :any begin
            s = live_source(FailingProvider(), cfg)
            collect(s.channel)
            s
        end
        @test s.outcome[] === :failed
        @test s.opens[] == 0
    end
end

@testset "live: the stop handshake severs a connection from either side" begin
    s = LiveSession(
        Channel{Trade}(1),
        Threads.Atomic{Bool}(false),
        Ref{Any}(nothing),
        Ref((; ticks = 0, frames = 0, reconnects = 0)),
    )
    # Registered first: stop! finds the socket and severs it.
    io1 = IOBuffer()
    @test !MarketTickStreamer._register_ws!(s, io1) && isopen(io1)
    stop!(s)
    @test !isopen(io1)
    # Registered after the stop: the provider severs it. It is registered
    # before the upgrade is answered, so it does not count as opened.
    io2 = IOBuffer()
    @test MarketTickStreamer._register_ws!(s, io2) && !isopen(io2)
    @test s.opens[] == 0
    close(s.channel)       # no producer here to close it
end

@testset "live: an orderly stop with a stalled consumer is not a failure" begin
    # The producer waits on a full channel nobody reads; stop!'s bound
    # closes the channel after STOP_GRACE_S and the producer ends on it.
    @eval struct StallProvider <: MarketTickStreamer.AbstractProvider end
    @eval function MarketTickStreamer.stream_protocol!(
        ch,
        ::StallProvider,
        cfg,
        s;
        kwargs...,
    )
        ws = (; io = IOBuffer())
        MarketTickStreamer._register_ws!(s, ws.io) && return nothing
        last_frame = Threads.Atomic{Float64}(time())
        alive = Threads.Atomic{Bool}(true)
        watchdog =
            MarketTickStreamer.spawn_watchdog(ws, s, last_frame, alive, cfg.stale_timeout_s)
        try
            put!(ch, sample_trade(1))
            put!(ch, sample_trade(2))      # the channel holds one: waits
        finally
            alive[] = false
            wait(watchdog)
        end
    end
    mktempdir() do dir
        cfg = load_config(
            mock_config_toml(dir; ws_port = 9999, rest_port = 9998, channel_capacity = 1),
        )
        s = live_source(StallProvider(), cfg)
        @test timedwait(() -> isready(s.channel), 10.0) === :ok
        stop!(s)
        limit = MarketTickStreamer.STOP_GRACE_S + 10
        @test timedwait(() -> s.outcome[] !== :running, limit) === :ok
        @test s.outcome[] === :completed && s.opens[] == 1
        @test take!(s.channel) == sample_trade(1)
    end
end

@testset "live: a sever is immediate and a close bounded, on every transport" begin
    # The peer never reads, and the client fills its send buffer first:
    # Julia's close of a socket would wait for those writes to drain.
    mktempdir() do dir
        sslconfig = self_signed_sslconfig(dir)
        silent(port; tls) = HTTP.WebSockets.listen!(
            "127.0.0.1",
            port;
            verbose = false,
            sslconfig = tls ? sslconfig : nothing,
        ) do ws
            HTTP.WebSockets.send(ws, "x")
            sleep(20)
        end
        ws_port, wss_port = freeport(9901), freeport(9911)
        servers = (silent(ws_port; tls = false), silent(wss_port; tls = true))
        grace = MarketTickStreamer.CLOSE_GRACE_S
        try
            for (url, tls) in (
                ("ws://127.0.0.1:$ws_port", nothing),
                ("wss://127.0.0.1:$wss_port", TLS_STACKS[1]),
                ("wss://127.0.0.1:$wss_port", TLS_STACKS[2]),
            )
                with_tls(something(tls, HTTP.SOCKET_TYPE_TLS[])) do
                    # A writer blocked on the full buffer, then each way out.
                    for way in (:sever, :close)
                        HTTP.WebSockets.open(url; suppress_close_error = true) do ws
                            first(ws)
                            filler = Threads.@spawn try
                                for _ in 1:1000
                                    HTTP.WebSockets.send(ws, repeat("y", 1 << 16))
                                end
                            catch
                            end
                            sleep(0.5)
                            if way === :sever
                                @test (@elapsed MarketTickStreamer._close_ws(ws)) < 0.5
                            else
                                elapsed =
                                    @elapsed closing = MarketTickStreamer._close_bounded(ws)
                                @test grace <= elapsed < grace + 1
                                @test istaskdone(closing)
                            end
                            @test timedwait(() -> istaskdone(filler), 5.0) === :ok
                        end
                    end
                end
            end
        finally
            foreach(close, servers)
        end
    end
    port = freeport(9881)
    server = HTTP.WebSockets.listen!("127.0.0.1", port; verbose = false) do ws
        HTTP.WebSockets.send(ws, "x")
        sleep(6)                 # never reads: the CLOSE frame goes unanswered
    end
    # A peer that reads answers the CLOSE at once.
    port2 = freeport(9891)
    server2 = HTTP.WebSockets.listen!("127.0.0.1", port2; verbose = false) do ws
        HTTP.WebSockets.send(ws, "x")
        for _ in ws
        end
    end
    grace = MarketTickStreamer.CLOSE_GRACE_S
    try
        HTTP.WebSockets.open("ws://127.0.0.1:$port") do ws
            first(ws)
            # A handshake would last as long as the server sleeps.
            elapsed = @elapsed MarketTickStreamer._close_ws(ws)
            @test elapsed < 4
        end
        HTTP.WebSockets.open("ws://127.0.0.1:$port") do ws
            first(ws)
            # The CLOSE goes unanswered: severed once the grace runs out.
            elapsed = @elapsed MarketTickStreamer._close_bounded(ws)
            @test grace <= elapsed < grace + 2
        end
        HTTP.WebSockets.open("ws://127.0.0.1:$port2") do ws
            first(ws)
            elapsed = @elapsed MarketTickStreamer._close_bounded(ws)
            @test elapsed < grace
        end
    finally
        close(server)
        close(server2)
    end
end

@testset "live: stream_websocket! is not retried by HTTP.jl, and is bounded" begin
    # The upgrade answer's Connection header is a token list, possibly split
    # across fields.
    upgrade(fields...) = MarketTickStreamer._has_token(
        HTTP.Response(101, ["Connection" => f for f in fields]),
        "Connection",
        "upgrade",
    )
    @test upgrade("Upgrade") && upgrade("keep-alive, Upgrade") && upgrade(" upgrade ")
    @test upgrade("keep-alive", "Upgrade")
    @test !upgrade("upgraded") && !upgrade("noupgrade") && !upgrade("keep-alive")
    @test !upgrade()
    mktempdir() do dir
        # A listener that accepts and drops at once: one call is one attempt,
        # the session's reconnect loop owns the retries.
        port = freeport(9921)
        listener = HTTP.Sockets.listen(HTTP.Sockets.ip"127.0.0.1", port)
        accepted = Threads.Atomic{Int}(0)
        acceptor = @async while isopen(listener)
            sock = try
                HTTP.Sockets.accept(listener)
            catch
                break
            end
            Threads.atomic_add!(accepted, 1)
            close(sock)
        end
        try
            @test_throws Exception MarketTickStreamer.stream_websocket!(
                (ws, msg) -> nothing,
                bare_session(),
                "ws://127.0.0.1:$port",
                30.0,
            )
            @test accepted[] == 1
        finally
            close(listener)
        end
        # Servers that read the upgrade request and never answer it.
        sslconfig = self_signed_sslconfig(dir)
        mute(port; tls) = HTTP.listen!(
            "127.0.0.1",
            port;
            verbose = false,
            sslconfig = tls ? sslconfig : nothing,
        ) do http
            sleep(30)
        end
        ws_port, wss_port = freeport(9931), freeport(9941)
        servers = (mute(ws_port; tls = false), mute(wss_port; tls = true))
        try
            for (url, tls) in (
                ("ws://127.0.0.1:$ws_port", nothing),
                ("wss://127.0.0.1:$wss_port", TLS_STACKS[1]),
                ("wss://127.0.0.1:$wss_port", TLS_STACKS[2]),
            )
                with_tls(something(tls, HTTP.SOCKET_TYPE_TLS[])) do
                    # Stopped already: returns without an error, and without
                    # waiting for the 30 s the upgrade is allowed (the bound
                    # leaves room for the first call's compilation).
                    s = bare_session()
                    stop!(s)
                    @test (@elapsed MarketTickStreamer.stream_websocket!(
                        (ws, msg) -> nothing,
                        s,
                        url,
                        30.0,
                    )) < 5
                    close(s.channel)
                    # Unanswered: cut after the timeout, a retryable error.
                    s = bare_session()
                    t =
                        @elapsed @test_throws Exception MarketTickStreamer.stream_websocket!(
                            (ws, msg) -> nothing,
                            s,
                            url,
                            0.5,
                        )
                    @test t < 5
                    @test s.opens[] == 0
                    close(s.channel)
                    # Stopped while unanswered: the stop reaches the request.
                    s = bare_session()
                    task = Threads.@spawn MarketTickStreamer.stream_websocket!(
                        (ws, msg) -> nothing,
                        s,
                        url,
                        30.0,
                    )
                    @test timedwait(() -> s.ws[] !== nothing, 10.0) === :ok
                    t0 = time()
                    stop!(s)
                    @test timedwait(() -> istaskdone(task), 5.0) === :ok
                    @test time() - t0 < 2
                    @test fetch(task) === nothing
                    close(s.channel)
                end
            end
        finally
            foreach(close, servers)
        end
    end
end

@testset "live E2E: watchdog severs a stalled stream" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9351), freeport(9371)
        # One batch, then the server holds the socket open and says
        # nothing. HTTP.jl 1.x has no read idle timeout, so only the
        # client-side watchdog can end this connection.
        plan = MockPlan(
            [[mock_trade("AAPL", i) for i in 1:3]];
            fatal_after = 1,
            linger_s = 20.0,
        )
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(
                mock_config_toml(
                    dir;
                    ws_port,
                    rest_port,
                    max_retries = 1,
                    stale_timeout_s = 1.0,
                ),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            t0 = time()
            result = run_stream(cfg; provider = p)
            @test result.ticks == 3            # the batch before the stall
            @test nconn[] == 2                 # severed, then reconnected
            @test time() - t0 < 15.0           # the watchdog, not the linger
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live E2E: market-closed guard and wait_for_open" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9451), freeport(9471)
        plan = MockPlan([[mock_trade("AAPL", i) for i in 1:3]]; fatal_after = 1)
        # Closed market, no waiting: the session must not open a socket.
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port, is_open = false)
        try
            cfg = load_config(mock_config_toml(dir; ws_port, rest_port))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            result = run_stream(cfg; provider = p)
            @test result.ticks == 0
            @test result.status == "completed"
            @test isempty(result.raw_files)
            @test nconn[] == 0
        finally
            close(ws)
            close(rest)
        end
    end
    mktempdir() do dir
        ws_port, rest_port = freeport(9491), freeport(9511)
        plan = MockPlan([[mock_trade("AAPL", i) for i in 1:3]]; fatal_after = 1)
        # Closed market, waiting enabled, opening bell already past: the
        # wait resolves to zero and the session proceeds to stream.
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port, is_open = false, open_in_s = -60.0)
        try
            cfg =
                load_config(mock_config_toml(dir; ws_port, rest_port, wait_for_open = true))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            t0 = time()
            result = run_stream(cfg; provider = p)
            @test result.ticks == 3
            @test nconn[] == 2                 # streamed, then scripted fatal
            @test time() - t0 < 30.0           # no real wait was served
        finally
            close(ws)
            close(rest)
        end
    end
    mktempdir() do dir
        rest_port = freeport(9531)
        # An hour before the open: the session waits without the data
        # tree's lock, and an interrupt ends the wait without a sidecar.
        rest = start_mock_rest(; port = rest_port, is_open = false, open_in_s = 3600.0)
        try
            cfg = load_config(
                mock_config_toml(dir; ws_port = 1, rest_port, wait_for_open = true),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            task = @async run_stream(cfg; provider = p)
            sleep(2.0)
            @test !istaskdone(task)
            close(acquire_session_lock(cfg.data_dir))   # free while waiting
            schedule(task, InterruptException(); error = true)
            result = fetch(task)
            @test result.status == "interrupted" && result.ticks == 0
            @test !isdir(cfg.raw_dir) || !any(endswith(".meta.toml"), readdir(cfg.raw_dir))
        finally
            close(rest)
        end
    end
end

@testset "live E2E: delayed feed shifts the close guard" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9551), freeport(9571)
        plan = MockPlan(
            [[mock_trade("AAPL", i) for i in 1:3]];
            fatal_after = 1,
            linger_s = 5.0,
        )
        ws, nconn = start_mock_ws(plan; port = ws_port)
        # A close two seconds out stops a real-time session almost at once
        # (asserted in the early-close testset above); on `delayed_sip`
        # the guard sits 900 s later, so the tape tail keeps arriving.
        rest = start_mock_rest(; port = rest_port, close_in_s = 2.0)
        try
            cfg =
                load_config(mock_config_toml(dir; ws_port, rest_port, feed = "delayed_sip"))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            @test MarketTickStreamer.feed_delay_ns(p) == 900 * MarketTickStreamer.NS_PER_SEC
            t0 = time()
            result = run_stream(cfg; provider = p)
            @test result.ticks == 3
            @test result.status == "failed"
            @test time() - t0 > 2.0            # outlived the unshifted close
            @test nconn[] == 2                 # ended by the fatal, not the guard
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live E2E: a quiet connection does not use up the reconnects" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9031), freeport(9041)
        # Handshake, then silence: the watchdog cuts every connection after
        # 1 s. Each stayed up that long, so none counts as a failed attempt,
        # and with one retry allowed the session still runs to its deadline.
        plan = MockPlan([Any[]]; fatal_after = 1_000, linger_s = 30.0)
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(
                mock_config_toml(
                    dir;
                    ws_port,
                    rest_port,
                    max_retries = 1,
                    stale_timeout_s = 1.0,
                    max_session_hours = 0.0015,         # 5.4 s
                ),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            result = run_stream(cfg; provider = p)
            @test result.status == "completed"
            @test nconn[] >= 3              # two attempts would have been all
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live E2E: an endpoint that never upgrades uses up the reconnects" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9061), freeport(9066)
        # Accepts TCP and drops it after 1.2 s without answering the
        # upgrade: each attempt lasts longer than stale_timeout_s, yet no
        # connection opened, so none resets the attempt counter.
        listener = HTTP.Sockets.listen(HTTP.Sockets.ip"127.0.0.1", ws_port)
        accepted = Threads.Atomic{Int}(0)
        acceptor = @async while isopen(listener)
            sock = try
                HTTP.Sockets.accept(listener)
            catch
                break
            end
            Threads.atomic_add!(accepted, 1)
            @async (sleep(1.2); close(sock))
        end
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(
                mock_config_toml(
                    dir;
                    ws_port,
                    rest_port,
                    max_retries = 1,
                    stale_timeout_s = 1.0,
                    max_session_hours = 0.003,         # 10.8 s
                ),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            result = run_stream(cfg; provider = p)
            @test result.status == "failed"
            @test accepted[] == 2
        finally
            close(listener)
            close(rest)
        end
    end
end

@testset "live E2E: a session ends when an upgrade is never answered" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9071), freeport(9076)
        # Accepts TCP and never answers the upgrade. The connection is
        # registered before the answer is read, so the deadline's stop
        # severs it and the session ends without waiting out the grace
        # period stop! allows a producer it cannot reach.
        listener = HTTP.Sockets.listen(HTTP.Sockets.ip"127.0.0.1", ws_port)
        held = Any[]
        acceptor = @async while isopen(listener)
            sock = try
                HTTP.Sockets.accept(listener)
            catch
                break
            end
            push!(held, sock)
        end
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(
                mock_config_toml(dir; ws_port, rest_port, max_session_hours = 0.001),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            t0 = time()
            result = run_stream(cfg; provider = p)
            # 3.6 s to the deadline; the grace period would add 5 s more
            @test time() - t0 < 3.6 + MarketTickStreamer.STOP_GRACE_S
            @test result.ticks == 0
            @test result.status == "failed"          # it never connected
        finally
            foreach(close, held)
            close(listener)
            close(rest)
        end
    end
end

@testset "live E2E: an unanswered upgrade is cut and retried" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9081), freeport(9086)
        # Accepts TCP and never answers the upgrade. Each attempt is cut
        # after stale_timeout_s, so the attempts run out long before the
        # deadline, which is all that would end a wait on the answer.
        listener = HTTP.Sockets.listen(HTTP.Sockets.ip"127.0.0.1", ws_port)
        held = Any[]
        acceptor = @async while isopen(listener)
            sock = try
                HTTP.Sockets.accept(listener)
            catch
                break
            end
            push!(held, sock)
        end
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(
                mock_config_toml(
                    dir;
                    ws_port,
                    rest_port,
                    max_retries = 1,
                    stale_timeout_s = 1.0,
                    max_session_hours = 0.01,          # 36 s
                ),
            )
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            t0 = time()
            result = run_stream(cfg; provider = p)
            @test time() - t0 < 20
            @test result.status == "failed"
            @test length(held) == 2
        finally
            foreach(close, held)
            close(listener)
            close(rest)
        end
    end
end

@testset "live E2E: a stop severs a wss:// connection read on another thread" begin
    # Every other mock speaks plain TCP. Here the server speaks TLS with a
    # self-signed certificate, which the client is told not to verify, and
    # the session is stopped from this task while the producer reads on a
    # worker thread, once through each TLS stack HTTP.jl offers.
    mktempdir() do dir
        sslconfig = self_signed_sslconfig(dir)
        ws_port = freeport(9091)
        frames = [mock_binance_frame("BTCUSDT", k) for k in 1:3]
        server = start_mock_binance_ws(
            ws_port;
            frames,
            linger_s = 30.0,
            fatal_after = 10,
            sslconfig,
        )
        cfg = load_config(
            mock_binance_config_toml(
                dir;
                ws_port,
                rest_port = 9999,
                ws_scheme = "wss",
                max_session_hours = 0.05,
            ),
        )
        try
            for tls in TLS_STACKS
                with_tls(tls) do
                    s = live_source(BinanceProvider(cfg), cfg)
                    got = Trade[]
                    for _ in 1:3
                        push!(got, take!(s.channel))
                    end
                    @test length(got) == 3
                    @test s.ws[].io isa tls
                    t0 = time()
                    stop!(s)
                    @test timedwait(() -> !isopen(s.channel), 10.0) === :ok
                    # Ended by the sever, not by the channel close stop!
                    # falls back on after STOP_GRACE_S.
                    @test time() - t0 < MarketTickStreamer.STOP_GRACE_S
                    @test timedwait(() -> s.outcome[] !== :running, 5.0) === :ok
                    @test s.outcome[] === :completed && s.opens[] == 1
                end
            end
        finally
            close(server)
        end
    end
end

@testset "live E2E: prints without a recorded connection complete a session" begin
    # An adapter that never calls spawn_watchdog records no opening; its
    # prints alone show that the session reached the venue.
    @eval struct FeedProvider <: MarketTickStreamer.AbstractProvider end
    @eval MarketTickStreamer.always_open(::FeedProvider) = true
    @eval function MarketTickStreamer.stream_protocol!(
        ch,
        ::FeedProvider,
        cfg,
        s;
        kwargs...,
    )
        foreach(i -> put!(ch, sample_trade(i)), 1:3)
        while !s.stop[]
            sleep(0.05)
        end
    end
    mktempdir() do dir
        cfg = load_config(
            mock_config_toml(
                dir;
                ws_port = 9999,
                rest_port = 9998,
                max_session_hours = 0.0005,       # 1.8 s
            ),
        )
        result = run_stream(cfg; provider = FeedProvider())
        @test result.status == "completed" && result.ticks == 3
    end
end

@testset "live E2E: a connection limit after a reconnect is retried" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9011), freeport(9021)
        # The first connection streams and closes; the next ones meet 406,
        # as the server would while it still counts the first. Retried
        # until the two allowed attempts are spent.
        plan = MockPlan([[mock_trade("AAPL", 1)]]; fatal_after = 1, fatal_code = 406)
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(mock_config_toml(dir; ws_port, rest_port, max_retries = 2))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            result = run_stream(cfg; provider = p)
            @test result.status == "failed" && result.ticks == 1
            @test nconn[] == 3
        finally
            close(ws)
            close(rest)
        end
    end
    mktempdir() do dir
        ws_port, rest_port = freeport(9001), freeport(9006)
        # On the first connection the limit is another client's: fatal.
        plan = MockPlan([[mock_trade("AAPL", 1)]]; fatal_after = 0, fatal_code = 406)
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(mock_config_toml(dir; ws_port, rest_port, max_retries = 2))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            result = run_stream(cfg; provider = p)
            @test result.status == "failed" && nconn[] == 1
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live E2E: auth failure is fatal, no retry storm" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9051), freeport(9071)
        plan = MockPlan([[mock_trade("AAPL", 1)]])
        ws, nconn = start_mock_ws(plan; port = ws_port)
        rest = start_mock_rest(; port = rest_port)
        try
            cfg = load_config(mock_config_toml(dir; ws_port, rest_port))
            p = AlpacaProvider(cfg, "wrongkey", "wrongsecret")
            result = run_stream(cfg; provider = p)
            @test result.ticks == 0
            @test result.status == "failed"
            @test nconn[] == 1                         # 402 → FatalStreamError → no reconnect
            meta = only(
                filter(f -> endswith(f, ".meta.toml"), readdir(cfg.raw_dir; join = true)),
            )
            @test TOML.parsefile(meta)["session"]["status"] == "failed"
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "live: an interrupt that lands in the producer ends it, without a reconnect" begin
    # Thrown inside the connection, an interrupt comes out of HTTP.jl wrapped
    # as a request error, which the reconnect loop took for a disconnect.
    port = freeport(9881)
    nconn = Threads.Atomic{Int}(0)
    server = HTTP.WebSockets.listen!("127.0.0.1", port; verbose = false) do ws
        Threads.atomic_add!(nconn, 1)
        HTTP.WebSockets.send(ws, "x")
        for _ in ws
        end
    end
    @eval struct InterruptedProvider <: MarketTickStreamer.AbstractProvider
        url::String
    end
    @eval MarketTickStreamer.stream_protocol!(
        ch,
        p::InterruptedProvider,
        cfg,
        s;
        kwargs...,
    ) = MarketTickStreamer.stream_websocket!(
        (ws, msg) -> throw(InterruptException()),
        s,
        p.url,
        cfg.stale_timeout_s,
    )
    try
        url = "ws://127.0.0.1:$port"
        @test_throws InterruptException MarketTickStreamer.stream_websocket!(
            (ws, msg) -> throw(InterruptException()),
            bare_session(),
            url,
            5.0,
        )
        mktempdir() do dir
            cfg = load_config(mock_config_toml(dir; ws_port = 9999, rest_port = 9998))
            before = nconn[]
            s = live_source(InterruptedProvider(url), cfg)
            @test isempty(collect(s.channel))            # the channel closes
            @test nconn[] == before + 1                  # one connection
            @test s.stats[].reconnects == 0 && s.outcome[] === :interrupted
            # Through a whole session the sidecar says so, which is what marks
            # the days of the capture incomplete when they are compacted.
            toml = mock_config_toml(
                mkpath(joinpath(dir, "run"));
                ws_port = 9999,
                rest_port = 9998,
                require_market_open = false,
            )
            write(
                toml,
                replace(
                    read(toml, String),
                    "wait_for_open = false" => "wait_for_open = false\nstop_at_market_close = false",
                ),
            )
            result = run_stream(load_config(toml); provider = InterruptedProvider(url))
            @test result.status == "interrupted" && result.ticks == 0
            sidecar = only(
                filter(
                    endswith(".meta.toml"),
                    readdir(joinpath(dirname(toml), "data", "raw"); join = true),
                ),
            )
            @test TOML.parsefile(sidecar)["session"]["status"] == "interrupted"
        end
    finally
        close(server)
    end
end
