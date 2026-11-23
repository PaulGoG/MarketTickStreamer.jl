@testset "close guard + disk guard" begin
    s = LiveSession(
        Channel{Trade}(1),
        Threads.Atomic{Bool}(false),
        Ref{Any}(nothing),
        Ref((; ticks = 0, frames = 0, reconnects = 0)),
    )
    t = schedule_close_stop!(s, now_ns() + 300_000_000; grace_s = 0.0)   # closes in 0.3 s
    wait(t)
    @test s.stop[]
    close(s.channel)       # no producer here to close it
    @test free_disk_gb(pwd()) > 0.0
    # feed delay drives the delayed-tape market-hours guard shifts
    mk_provider(feed) = AlpacaProvider(MOCK_KEY, MOCK_SECRET, feed, "", "", "")
    @test MarketTickStreamer.feed_delay_ns(mk_provider("iex")) == 0
    @test MarketTickStreamer.feed_delay_ns(mk_provider("delayed_sip")) ==
          900 * MarketTickStreamer.NS_PER_SEC
end

@testset "backfill: per-day loop, page streaming, resume, feed naming" begin
    mktempdir() do dir
        ws_port, rest_port = freeport(9251), freeport(9271)
        hits = Ref(0)
        ws, _ = start_mock_ws(MockPlan([[]]); port = ws_port)
        rest = start_mock_rest(; port = rest_port, hits)
        try
            cfg = load_config(mock_config_toml(dir; ws_port, rest_port))
            p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
            processed = run_backfill(cfg; provider = p)
            @test length(processed) == 2                 # AAPL + MSFT, one day
            @test hits[] > 0
            # session id carries the backfill feed (sip), not the live feed
            raws = filter(f -> startswith(f, "backfill_alpaca_sip_"), readdir(cfg.raw_dir))
            @test !isempty(raws)
            # resume: a second run skips fully-processed (symbol, day) pairs
            h1 = hits[]
            processed2 = run_backfill(cfg; provider = p)
            @test isempty(processed2)
            @test hits[] == h1                           # no REST traffic at all
            # sidecars: completed status, backfill session id
            metas = filter(endswith(".meta.toml"), readdir(cfg.raw_dir))
            @test length(metas) == 2                     # one per run (2nd wrote no raw)
            m = TOML.parsefile(joinpath(cfg.raw_dir, sort(metas)[1]))
            @test m["session"]["status"] == "completed"
        finally
            close(ws)
            close(rest)
        end
    end
end

@testset "backfill: a killed run resumes from what was compacted" begin
    mktempdir() do dir
        # Two trading days, one symbol. The mock serves the first day's two
        # pages and then refuses, so day one completes and day two dies
        # mid-download, as a multi-hour download does when it is killed.
        rest_port = freeport(9771)
        hits = Ref(0)
        rest = start_mock_rest(; port = rest_port, hits, fail_after = 2)
        cfg = load_config(
            mock_config_toml(
                dir;
                ws_port = rest_port,
                rest_port,
                symbols = ["AAPL"],
                start_date = "2026-07-29",
                end_date = "2026-07-30",
            ),
        )
        p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
        try
            @test_throws Exception run_backfill(cfg; provider = p)
            # The completed day is compacted the moment it finishes; the
            # interrupted one is not, which is exactly what resume reads.
            @test isfile(joinpath(cfg.processed_dir, "AAPL", "2026-07-29.csv"))
            @test !isfile(joinpath(cfg.processed_dir, "AAPL", "2026-07-30.csv"))
            @test hits[] == 3                       # two pages, then the refusal
            # The sidecar of a run that died says so, and pins its environment.
            meta = TOML.parsefile(
                only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
            )
            @test meta["session"]["status"] == "failed"
            manifest = meta["provenance"]["manifest"]
            @test !isempty(manifest)
            @test isfile(joinpath(cfg.raw_dir, manifest))
            # No zero-byte stub left behind by the day that wrote nothing.
            @test all(
                f -> filesize(f) > 0,
                filter(endswith(".jsonl"), readdir(cfg.raw_dir; join = true)),
            )
        finally
            close(rest)
        end

        # A healthy server, same data tree: the finished day is skipped and
        # only the missing one is fetched.
        rest_port2 = freeport(9791)
        hits2 = Ref(0)
        rest2 = start_mock_rest(; port = rest_port2, hits = hits2)
        try
            cfg2 = load_config(
                mock_config_toml(
                    dir;
                    ws_port = rest_port2,
                    rest_port = rest_port2,
                    symbols = ["AAPL"],
                    start_date = "2026-07-29",
                    end_date = "2026-07-30",
                ),
            )
            p2 = AlpacaProvider(cfg2, MOCK_KEY, MOCK_SECRET)
            processed = run_backfill(cfg2; provider = p2)
            @test hits2[] == 2                      # one day only, two pages
            @test length(processed) == 1
            @test isfile(joinpath(cfg2.processed_dir, "AAPL", "2026-07-30.csv"))
            # Both days are now present, each filed under its own date.
            @test length(readdir(joinpath(cfg2.processed_dir, "AAPL"))) == 2
        finally
            close(rest2)
        end
    end
end

@testset "REST policy: timeouts and transport retry" begin
    @test MarketTickStreamer.RestPolicy() == MarketTickStreamer.RestPolicy(30, 10, 5)
    @test_throws ArgumentError MarketTickStreamer.RestPolicy(0, 10, 5)
    @test_throws ArgumentError MarketTickStreamer.RestPolicy(30, 0, 5)
    @test_throws ArgumentError MarketTickStreamer.RestPolicy(30, 10, -1)
    @test_throws ArgumentError MarketTickStreamer.RestPolicy(
        30,
        10,
        5;
        max_download_rate = 0,
    )
    @test MarketTickStreamer.RestPolicy(30, 10, 5; max_download_rate = 1e6) !=
          MarketTickStreamer.RestPolicy()
    for n in (0, 9)
        @test_throws ArgumentError MarketTickStreamer.RestPolicy(
            30,
            10,
            5;
            download_connections = n,
        )
    end
    @test MarketTickStreamer.RestPolicy(30, 10, 5; download_connections = 3) !=
          MarketTickStreamer.RestPolicy()
    # A server that accepts the request and goes silent once, then answers.
    port = freeport(8961)
    hits = Threads.Atomic{Int}(0)
    server = HTTP.serve!("127.0.0.1", port; verbose = false) do _
        Threads.atomic_add!(hits, 1) == 0 && sleep(3.0)   # returns the old value
        return HTTP.Response(200, "ok")
    end
    try
        policy = MarketTickStreamer.RestPolicy(1, 1, 2)
        resp = MarketTickStreamer._get_with_retry(
            "http://127.0.0.1:$port/x",
            Pair{String,String}[];
            policy,
        )
        @test String(resp.body) == "ok"
        @test hits[] == 2
    finally
        close(server)
    end
    # Nothing listening: retried, then the transport error surfaces.
    dead = freeport(8971)
    t0 = time()
    @test_throws HTTP.ConnectError MarketTickStreamer._get_with_retry(
        "http://127.0.0.1:$dead/x",
        Pair{String,String}[];
        policy = MarketTickStreamer.RestPolicy(1, 1, 1),
    )
    @test time() - t0 >= 0.5                     # one backoff was taken
    # A negative Retry-After falls back to the backoff schedule instead of
    # reaching `sleep` as a negative duration.
    port = freeport(8966)
    hits429 = Threads.Atomic{Int}(0)
    server = HTTP.serve!("127.0.0.1", port; verbose = false) do _
        Threads.atomic_add!(hits429, 1) == 0 &&
            return HTTP.Response(429, ["Retry-After" => "-1"], "slow down")
        return HTTP.Response(200, "ok")
    end
    try
        policy = MarketTickStreamer.RestPolicy(1, 1, 2)
        resp = MarketTickStreamer._get_with_retry(
            "http://127.0.0.1:$port/x",
            Pair{String,String}[];
            policy,
        )
        @test resp.status == 200
    finally
        close(server)
    end
    # A Retry-After beyond ten minutes is not waited out inside a request.
    port = freeport(8961)
    server = HTTP.serve!("127.0.0.1", port; verbose = false) do _
        HTTP.Response(429, ["Retry-After" => "601"], "banned")
    end
    try
        t0 = time()
        @test_throws HTTP.StatusError MarketTickStreamer._get_with_retry(
            "http://127.0.0.1:$port/x",
            Pair{String,String}[];
            policy = MarketTickStreamer.RestPolicy(1, 1, 2),
        )
        @test time() - t0 < 5
    finally
        close(server)
    end
    # An interrupt that arrives inside a request comes out of HTTP.jl wrapped
    # as a transport error. It is rethrown as itself, not retried.
    calls = Ref(0)
    @test_throws InterruptException MarketTickStreamer._with_retry(
        MarketTickStreamer.RestPolicy(1, 1, 3),
    ) do
        calls[] += 1
        throw(HTTP.RequestError(HTTP.Request("GET", "/x"), InterruptException()))
    end
    @test calls[] == 1
    # A chunk's timeout: the policy's timeout plus 1.25 times the transfer
    # at the slower of 50 kB/s and the cap, 30 + ceil(1.25 × 5) s here.
    @test MarketTickStreamer._chunk_timeout(
        MarketTickStreamer.RestPolicy(30, 10, 0),
        100_000;
        rate = 20_000.0,
    ) == 37
    # Backoff jitter is a deterministic function of the seed, within
    # [0.5, 1.5) of the capped exponential.
    d = [MarketTickStreamer._backoff_delay(Xoshiro(7), 1.0, k, 60.0) for k in 0:5]
    @test d == [MarketTickStreamer._backoff_delay(Xoshiro(7), 1.0, k, 60.0) for k in 0:5]
    @test all(0.5 * 2.0^k <= d[k+1] < 1.5 * 2.0^k for k in 0:5)
    @test 30.0 <= MarketTickStreamer._backoff_delay(Xoshiro(7), 1.0, 20, 60.0) < 90.0
    # A provider built from a configuration carries its [rest] table, and
    # its generator is seeded from [rng] seed.
    mktempdir() do dir
        p = joinpath(dir, "c.toml")
        write(
            p,
            "[stream]\nsymbols = [\"A\"]\n[rng]\nseed = 5\n[rest]\nrequest_timeout_s = 7\nmax_retries = 0\n",
        )
        cfg = load_config(p)
        prov = AlpacaProvider(cfg, "k", "s")
        @test prov.rest == MarketTickStreamer.RestPolicy(7, 10, 0)
        @test rand(MarketTickStreamer.RestPolicy(cfg).rng) ==
              rand(MarketTickStreamer.RestPolicy(cfg).rng)
        @test rand(MarketTickStreamer.RestPolicy(cfg).rng) != rand(Xoshiro(5))
    end
    @test AlpacaProvider("k", "s", "iex", "a", "b", "c").rest ==
          MarketTickStreamer.RestPolicy()
end

@testset "logging is scoped to the session" begin
    ws_port, rest_port = freeport(8981), freeport(8991)
    rest = start_mock_rest(; port = rest_port, trades_per_page = 3)
    try
        mktempdir() do dir
            cfg = load_config(
                mock_config_toml(
                    dir;
                    ws_port,
                    rest_port,
                    symbols = ["AAPL"],
                    log_to_file = true,
                ),
            )
            provider = withenv("ALPACA_API_KEY_ID" => "k", "ALPACA_SECRET_KEY" => "s") do
                MarketTickStreamer.make_provider(Val(:alpaca), cfg)
            end
            @test provider.key == "k" && provider.secret == "s"
            before = Base.CoreLogging.global_logger()
            run_backfill(cfg; provider)
            @test Base.CoreLogging.global_logger() === before
            logs = filter(endswith(".log"), readdir(cfg.log_dir; join = true))
            @test length(logs) == 1
            # The handle was closed with the session.
            if Sys.islinux()
                open_fds = [
                    try
                        readlink(f)
                    catch
                        ""
                    end for f in readdir("/proc/self/fd"; join = true)
                ]
                @test !(only(logs) in open_fds)
            end
        end
    finally
        close(rest)
    end
    # The shutdown flag belongs to one session: muting HTTP teardown noise
    # in one leaves another session's records alone.
    mktempdir() do dir
        p = joinpath(dir, "c.toml")
        write(
            p,
            "[stream]\nsymbols = [\"A\"]\n[logging]\nlevel = \"info\"\nlog_dir = \"logs\"\n",
        )
        cfg = load_config(p)
        stopping, running = Threads.Atomic{Bool}(true), Threads.Atomic{Bool}(false)
        la, ioa = setup_logging(cfg; session_id = "a", shutting_down = stopping)
        lb, iob = setup_logging(cfg; session_id = "b", shutting_down = running)
        redirect_stderr(devnull) do
            for logger in (la, lb)
                Base.CoreLogging.with_logger(logger) do
                    @warn "socket teardown" _module = HTTP
                    @warn "own record"
                end
            end
            # Terminal control sequences never reach the log file.
            Base.CoreLogging.with_logger(lb) do
                @info "color \e[31mred\e[0m" note = "\e[1mbold\e[0m"
            end
        end
        close(ioa)
        close(iob)
        a = read(joinpath(cfg.log_dir, "a.log"), String)
        b = read(joinpath(cfg.log_dir, "b.log"), String)
        @test !occursin("socket teardown", a) && occursin("own record", a)
        @test occursin("socket teardown", b) && occursin("own record", b)
        @test occursin("color red", b) && occursin("note=bold", b)
        @test !occursin("\e[", b)
    end
    # An (exception, backtrace) pair is logged to file as its message, on one line.
    let err = try
            error("boom\nsecond line")
        catch e
            (e, catch_backtrace())
        end
        @test MarketTickStreamer._log_value(err) == "boom second line"
        @test MarketTickStreamer._log_value(3) == "3"
    end
end

@testset "alpaca REST: clock + paginated historical trades" begin
    rest_port = freeport(8931)
    rest = start_mock_rest(; port = rest_port, trades_per_page = 3)
    try
        p = AlpacaProvider(
            MOCK_KEY,
            MOCK_SECRET,
            "iex",
            "http://127.0.0.1:$rest_port",
            "http://127.0.0.1:$rest_port",
            "ws://127.0.0.1:$rest_port/v2",
        )
        clock = market_clock(p)
        @test clock.is_open
        # The condition decoder is per tape, which is why it is fetched.
        cmap = condition_map(p; tape = "A")
        @test cmap["@"] == "Regular Sale"
        @test haskey(cmap, "B")                        # CTA average price
        @test haskey(condition_map(p; tape = "C"), "I")
        @test_throws ArgumentError condition_map(p; ticktype = "bars")
        r = historical_trades(
            p,
            "AAPL",
            Date(2026, 7, 29),
            Date(2026, 7, 29);
            page_limit = 3,
            rate_sleep_s = 0.0,
        )
        trades = r.trades
        @test length(trades) == 6 && r.prints == 6     # two pages followed
        @test all(t -> t.symbol == "AAPL", trades)
        @test all(t -> t.recv_ns == 0, trades)         # backfill marker
        @test trades[1].time_ns % 10 == 9              # nanosecond digit survived
    finally
        close(rest)
    end
end

@testset "session envelope: an interrupt in a task the body waits on; resume by symbol" begin
    mktempdir() do dir
        cfg = load_config(mock_config_toml(dir; ws_port = 9999, rest_port = 9998))
        # The interrupt arrives as the task's failure, and is one all the same.
        sid = "wrapped_interrupt"
        out = MarketTickStreamer._with_session(cfg, sid; result = String[]) do _
            fetch(Threads.@spawn throw(InterruptException()))
        end
        @test out == String[]
        meta = TOML.parsefile(joinpath(cfg.raw_dir, "$sid.meta.toml"))
        @test meta["session"]["status"] == "interrupted"
        @test !isfile(joinpath(cfg.data_dir, ".session.lock"))
        # `resume` finds a day under the symbol as the adapter files it.
        day = Date(2026, 7, 29)
        mkpath(joinpath(cfg.processed_dir, "BTCUSDT"))
        touch(joinpath(cfg.processed_dir, "BTCUSDT", "$day.csv"))
        @test MarketTickStreamer._processed_exists(cfg, "btcusdt", day)
        @test MarketTickStreamer._processed_exists(cfg, "BTCUSDT", day)
        @test !MarketTickStreamer._processed_exists(cfg, "ethusdt", day)
    end
end
