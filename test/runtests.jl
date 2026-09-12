using Test
using Aqua
using ExplicitImports
using JET
using Dates
using TimeZones
using CSV
using DataFrames
using TOML
using MarketTickStreamer

include("mock_alpaca.jl")

# Ports unlikely to collide; bumped per-run if busy.
function freeport(start)
    p = start
    while true
        try
            s = HTTP.Sockets.listen(HTTP.Sockets.ip"127.0.0.1", p)
            close(s)
            return p
        catch
            p += 1
        end
    end
end

@testset "MarketTickStreamer" begin

    @testset "static QA (Aqua)" begin
        Aqua.test_all(MarketTickStreamer)
    end

    @testset "static QA (ExplicitImports)" begin
        # The owner and public-access checks are deliberately not asserted:
        # the plotting stack's surface is re-exported (Makie names through
        # CairoMakie, `save` through FileIO, `@L_str` through LaTeXStrings),
        # and a few stable non-public names are load-bearing here
        # (`HTTP.StatusError`, `Arrow.Table`, `Base.gc_live_bytes`).
        @test check_no_implicit_imports(MarketTickStreamer) === nothing
        @test check_no_stale_explicit_imports(MarketTickStreamer) === nothing
        @test check_no_self_qualified_accesses(MarketTickStreamer) === nothing
    end

    @testset "static QA (JET)" begin
        # Restricted to this module: the dependency tree reports a hundred-odd
        # findings of its own, none of them actionable here. JET follows the
        # compiler, so a Julia upgrade may surface new reports — which is the
        # reason to run it.
        JET.test_package(MarketTickStreamer; target_modules = (MarketTickStreamer,))
    end

    @testset "schema / timestamps" begin
        ns = rfc3339_to_ns("2026-07-30T14:30:00.123456789Z")
        @test ns % MarketTickStreamer.NS_PER_SEC == 123_456_789
        @test ns_to_rfc3339(ns) == "2026-07-30T14:30:00.123456789Z"
        # lossless round-trip at all fractional widths
        for frac in ("1", "123", "123456", "999999999")
            s = "2020-01-02T03:04:05.$(rpad(frac, 9, '0'))Z"
            @test ns_to_rfc3339(rfc3339_to_ns(s)) == s
        end
        # numeric offsets normalize to UTC
        @test rfc3339_to_ns("2026-07-30T10:30:00-04:00") ==
              rfc3339_to_ns("2026-07-30T14:30:00Z")
        @test rfc3339_to_ns("2026-07-30T16:30:00+02:00") ==
              rfc3339_to_ns("2026-07-30T14:30:00Z")
        # no fractional part
        @test rfc3339_to_ns("2026-07-30T14:30:00Z") % MarketTickStreamer.NS_PER_SEC == 0
        @test_throws ArgumentError rfc3339_to_ns("garbage")
        @test_throws ArgumentError rfc3339_to_ns("2026-07-30 14:30")
        # 2026-07-30 20:30 UTC is 16:30 in New York (same trading day),
        # but 2026-07-31 01:00 UTC is still 2026-07-30 in New York.
        @test trading_date(rfc3339_to_ns("2026-07-30T20:30:00Z")) == Date(2026, 7, 30)
        @test trading_date(rfc3339_to_ns("2026-07-31T01:00:00Z")) == Date(2026, 7, 30)
        @test now_ns() > rfc3339_to_ns("2026-01-01T00:00:00Z")
    end

    @testset "config" begin
        cfg = load_config()   # the repo's own config must always be valid
        @test cfg.provider == "alpaca"
        @test !isempty(cfg.symbols)
        @test cfg.feed in ("iex", "delayed_sip", "sip")
        mktempdir() do dir
            bad = joinpath(dir, "bad.toml")
            write(bad, "[provider]\nfeed = \"nope\"\n[stream]\nsymbols = [\"A\"]\n")
            @test_throws ArgumentError load_config(bad)
            write(bad, "[stream]\nsymbols = []\n")
            @test_throws ArgumentError load_config(bad)
            write(bad, "[stream]\nsymbols = [\"A\"]\nchannels = [\"trades\", \"news\"]\n")
            @test_throws ArgumentError load_config(bad)
            @test_throws ArgumentError load_config(joinpath(dir, "missing.toml"))
        end
        mktempdir() do dir
            base = "[stream]\nsymbols = [\"A\"]\n"
            exchange_today = Date(now(tz"America/New_York"))
            p = joinpath(dir, "dates.toml")
            write(p, base * "[backfill]\nstart_date = \"today-2d\"\nend_date = \"today\"\n")
            c = load_config(p)
            @test c.backfill_start == exchange_today - Day(2)
            @test c.backfill_end == exchange_today
            write(p, base * "[backfill]\nstart_date = \"today\"\nend_date = \"today\"\n")
            @test load_config(p).backfill_start == exchange_today
            write(
                p,
                base *
                "[backfill]\nstart_date = \"2026-08-12\"\nend_date = \"2026-08-13\"\n",
            )
            c = load_config(p)
            @test c.backfill_start == Date(2026, 8, 12)
            @test c.backfill_end == Date(2026, 8, 13)
            write(p, base * "[backfill]\nstart_date = \"yesterday\"\n")
            @test_throws ArgumentError load_config(p)
            write(p, base * "[backfill]\nstart_date = \"today+2d\"\n")
            @test_throws ArgumentError load_config(p)
            write(p, base * "[backfill]\nstart_date = \"today\"\nend_date = \"today-1d\"\n")
            @test_throws ArgumentError load_config(p)
        end
    end

    sample_trade(i; sym = "AAPL") = Trade(
        sym,
        1_753_886_600_000_000_000 + i * 1_000_000,
        1_753_886_600_100_000_000 + i * 1_000_000,
        100.0 + i,
        10.0 * i,
        "V",
        ["@", "I"],
        "C",
        i,
    )

    @testset "sinks: NDJSON round-trip, rolling, recovery" begin
        t = sample_trade(1)
        @test json_to_trade(trade_to_json(t)) == t
        mktempdir() do dir
            sink = open_raw_sink(dir, "sess"; max_mb = 1)
            write_batch!(sink, [sample_trade(i) for i in 1:100])
            close_sink!(sink)
            # never reopen: a second sink with the same prefix gets a new part
            sink2 = open_raw_sink(dir, "sess"; max_mb = 1)
            @test sink2.path != sink.path
            close_sink!(sink2)
            trades = read_raw(sink.path)
            @test length(trades) == 100
            @test trades[7] == sample_trade(7)
            # corrupt-line recovery: truncate mid-line, append garbage
            open(sink.path, "a") do io
                print(io, "{\"symbol\":\"AAPL\",\"time_ns\":123")   # no newline, cut off
            end
            @test length(read_raw(sink.path)) == 100
        end
    end

    @testset "sinks: run_sink! batching via channel close" begin
        mktempdir() do dir
            sink = open_raw_sink(dir, "batch")
            ch = Channel{Trade}(1000)
            task = Threads.@spawn run_sink!(
                ch,
                sink;
                flush_interval_s = 10.0,
                flush_max_ticks = 25,
            )
            for i in 1:60
                put!(ch, sample_trade(i))
            end
            close(ch)                       # shutdown signal → final flush
            @test fetch(task) == 60
            close_sink!(sink)
            @test length(read_raw(sink.path)) == 60
        end
    end

    @testset "compaction" begin
        mktempdir() do dir
            sink = open_raw_sink(dir, "c")
            write_batch!(
                sink,
                [sample_trade(i; sym = iseven(i) ? "MSFT" : "AAPL") for i in 1:20],
            )
            close_sink!(sink)
            out = joinpath(dir, "processed")
            files = compact_raw([sink.path], out; format = "csv")
            @test length(files) == 2                       # one per symbol, same day
            @test all(isfile, files)
            # safesave: recompacting must not overwrite
            files2 = compact_raw([sink.path], out; format = "csv")
            @test isempty(intersect(files, files2))
            @test any(occursin("#2", f) for f in files2)
            arrow_files = compact_raw([sink.path], joinpath(dir, "arrow"); format = "arrow")
            @test length(arrow_files) == 2
        end
    end

    @testset "replay" begin
        mktempdir() do dir
            sink = open_raw_sink(dir, "r")
            write_batch!(sink, [sample_trade(i) for i in 1:50])
            close_sink!(sink)
            # max pace: everything, in recv order
            got = collect(replay_source(sink.path; pace = "max"))
            @test length(got) == 50
            @test issorted(got; by = t -> t.recv_ns)
            # recorded pace at high compression still delivers everything
            got2 = collect(replay_source(sink.path; pace = "recorded", speed = 1e9))
            @test length(got2) == 50
            @test_throws ArgumentError replay_source(sink.path; pace = "warp")
        end
    end

    @testset "quality: dedup + session report" begin
        base = rfc3339_to_ns("2026-07-30T14:30:00Z")
        mk(i; sym = "AAPL", dt = 1_000_000_000, recv_off = 2_000_000) = Trade(
            sym,
            base + i * dt,
            base + i * dt + recv_off,
            100.0 + i,
            10.0,
            "V",
            ["@"],
            "C",
            i,
        )
        trades = [mk(i) for i in 1:20]
        dup_set = vcat(trades, trades[5:8])                  # reconnection double-delivery
        @test length(deduplicate_trades(dup_set)) == 20
        @test deduplicate_trades(dup_set) == trades                # order preserved, first kept
        mktempdir() do dir
            sink = open_raw_sink(dir, "q")
            # AAPL: 4 dupes, one 120 s gap, one out-of-order pair, one negative latency
            aapl = vcat(trades, trades[1:4])
            push!(aapl, mk(21; dt = 1_000_000_000))
            aapl[end] = Trade(
                "AAPL",
                base + 200 * 1_000_000_000,
                base + 200 * 1_000_000_000 - 5_000_000,
                99.0,
                1.0,
                "V",
                String[],
                "C",
                999,
            )   # +120s gap, latency -5 ms
            push!(aapl, mk(3))                               # out-of-order arrival (t < previous)
            # MSFT: pure backfill (recv_ns = 0)
            msft = [
                Trade("MSFT", base + i * 1_000_000_000, 0, 300.0, 5.0, "V", ["@"], "C", i) for i in 1:5
            ]
            write_batch!(sink, vcat(aapl, msft))
            close_sink!(sink)
            rep = session_report(sink.path; gap_threshold_s = 60.0)
            @test nrow(rep) == 2
            a = rep[rep.symbol .== "AAPL", :][1, :]
            @test a.n_duplicates == 5                        # 4 re-sent + mk(3) re-sent
            @test a.n_gaps == 1 && a.max_gap_s > 100
            @test a.n_out_of_order >= 1
            @test a.n_negative_latency == 1
            @test a.median_latency_ms ≈ 2.0 atol = 0.5
            m = rep[rep.symbol .== "MSFT", :][1, :]
            @test isnan(m.median_latency_ms)                 # backfill: no latency defined
            # compaction deduplicates by default
            files = compact_raw([sink.path], joinpath(dir, "p"); format = "csv")
            df = CSV.read(files[findfirst(contains("AAPL"), files)], DataFrame)
            @test nrow(df) == 21                             # 26 raw AAPL rows − 5 dupes
            # over-budget inputs reroute through spill compaction transparently
            spill_files =
                compact_raw([sink.path], joinpath(dir, "p2"); mem_fraction = 1e-12)
            @test length(spill_files) == length(files)
        end
    end

    @testset "close guard + disk guard" begin
        s = LiveSession(
            Channel{Trade}(1),
            Ref(false),
            Ref{Any}(nothing),
            Ref((; ticks = 0, frames = 0, reconnects = 0)),
        )
        t = schedule_close_stop!(s, now_ns() + 300_000_000; grace_s = 0.0)   # closes in 0.3 s
        wait(t)
        @test s.stop[]
        @test free_disk_gb(pwd()) > 0.0
        # feed delay drives the delayed-tape railing shifts
        mk_provider(feed) = AlpacaProvider(MOCK_KEY, MOCK_SECRET, feed, "", "", "")
        @test MarketTickStreamer.feed_delay_ns(mk_provider("iex")) == 0
        @test MarketTickStreamer.feed_delay_ns(mk_provider("delayed_sip")) ==
              900 * MarketTickStreamer.NS_PER_SEC
    end

    @testset "tee fan-out" begin
        src = Channel{Trade}(100)
        outs = tee(src, 3; capacity = 100)
        consumers = [Threads.@spawn collect(o) for o in outs]
        expected = [sample_trade(i) for i in 1:30]
        foreach(t -> put!(src, t), expected)
        close(src)
        for c in consumers
            got = fetch(c)
            @test got == expected          # every consumer sees every tick, in order
        end
    end

    @testset "robustness: reconcile, lock, RAM guard, spill compaction, lossy tee" begin
        mktempdir() do dir
            # crash-only reconciliation: a "running" sidecar from a dead process
            write(
                joinpath(dir, "dead.meta.toml"),
                """
[session]
id = "dead"
status = "running"
pid = 99999999
[provenance]
hostname = "$(gethostname())"
""",
            )
            write(joinpath(dir, "dead_part001.jsonl"), "")
            @test reconcile_sessions!(dir) == 1
            meta = TOML.parsefile(joinpath(dir, "dead.meta.toml"))
            @test meta["session"]["status"] == "aborted"
            @test haskey(meta["session"], "reconciled_utc")
            @test reconcile_sessions!(dir) == 0              # idempotent
            # single-instance lock
            l1 = acquire_session_lock(dir)
            @test_throws ErrorException acquire_session_lock(dir)
            close(l1)
            l2 = acquire_session_lock(dir)                   # released → reacquirable
            close(l2)
            # RAM ceiling fails loudly
            @test_throws ErrorException check_live_heap(0; context = "test")
            @test check_live_heap(1e9) === nothing
        end
        # spill compaction produces the same result as in-memory compaction
        mktempdir() do dir
            sink = open_raw_sink(dir, "sp")
            write_batch!(
                sink,
                [sample_trade(i; sym = iseven(i) ? "MSFT" : "AAPL") for i in 1:40],
            )
            write_batch!(sink, [sample_trade(i) for i in 1:5])   # duplicates
            close_sink!(sink)
            mem = compact_raw([sink.path], joinpath(dir, "mem"); format = "csv")
            spill = compact_raw(
                [sink.path],
                joinpath(dir, "spill");
                format = "csv",
                mem_fraction = 1e-12,
            )            # force the spill path
            @test length(mem) == length(spill) == 2
            for (m, s) in zip(mem, spill)
                @test CSV.read(m, DataFrame) == CSV.read(s, DataFrame)
            end
        end
        # lossy tee: persistence output receives everything, saturated lossy
        # output drops instead of stalling the fan-out
        src = Channel{Trade}(100)
        outs = tee(src, 2; capacity = 5, lossy = [false, true])
        keeper = Threads.@spawn collect(outs[1])
        foreach(i -> put!(src, sample_trade(i)), 1:50)
        close(src)
        @test length(fetch(keeper)) == 50
        @test length(collect(outs[2])) <= 5                  # never consumed → capped
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
                raws =
                    filter(f -> startswith(f, "backfill_alpaca_sip_"), readdir(cfg.raw_dir))
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

    @testset "monitor: attach-mode tailing" begin
        mktempdir() do dir
            sink = open_raw_sink(dir, "mon")
            write_batch!(sink, [sample_trade(i) for i in 1:30])
            write_batch!(sink, [sample_trade(i; sym = "MSFT") for i in 1:15])
            close_sink!(sink)
            sink2 = open_raw_sink(dir, "mon")        # second part of the same session
            write_batch!(sink2, [sample_trade(i; sym = "MSFT") for i in 16:20])
            close_sink!(sink2)
            @test MarketTickStreamer.latest_session_prefix(dir) == "mon"
            buf = IOBuffer()
            st = monitor_raw(
                dir;
                refresh_s = 0.01,
                iterations = 1,
                from_start = true,
                io = buf,
            )
            @test st.total == 50
            @test st.per_symbol["AAPL"] == 30 && st.per_symbol["MSFT"] == 20
            out = String(take!(buf))
            @test occursin("AAPL", out) && occursin("Ticks", out)
            # incremental tailing: a full new line plus a torn line
            open(sink2.path, "a") do io
                println(io, trade_to_json(sample_trade(99)))
                print(io, "{\"symbol\":\"AAPL\",\"time_")
            end
            MarketTickStreamer._ingest!(st)
            @test st.total == 51                     # torn line carried, not counted
            open(sink2.path, "a") do io
                println(io, "ns\":1}")               # completes to malformed record → skipped
            end
            MarketTickStreamer._ingest!(st)
            @test st.total == 51
            # default attach starts at end of file
            st2 = monitor_raw(dir; refresh_s = 0.01, iterations = 1, io = IOBuffer())
            @test st2.total == 0
        end
    end

    @testset "viz: axis utilities, decimation, tail fit, figure smoke" begin
        M = MarketTickStreamer
        # HH:MM domain ticks
        vals, labels = M._hhmm_ticks(9.5, 16.0)
        @test labels[1] == "10:00" && labels[end] == "16:00"
        # log ticks: plain decimals on short spans, exponent + collapse on long
        v, l = M._log_ticks(0.5, 80.0)
        @test "1" in l &&
              "2" in l &&
              "50" in l &&
              !any(occursin("10^", string(x)) for x in l)
        v, l = M._log_ticks(1e-7, 10.0)
        @test "1" in l && "10" in l                          # 10^0 and 10^1 collapse
        @test any(x -> occursin("10^{-6}", string(x)), l)
        # thinning preserves ends; decimation preserves extrema
        xs = collect(1.0:10_000.0)
        tx, ty = M._thin(xs, xs; cap = 500)
        @test length(tx) <= 810 && tx[1] == 1.0 && tx[end] == 10_000.0
        dx, dy = M._decimate_minmax(xs, sin.(xs); nbins = 50)
        @test length(dx) <= 100
        @test maximum(dy) ≈ maximum(sin.(xs)) atol = 1e-3
        # tail fit recovers a known power law
        n = 5000
        p = collect(n:-1:1) ./ n
        x = p .^ (-1 / 2.5)                                  # exact alpha = 2.5
        fit = M._tail_fit(sort(x), sort(p; rev = true))
        @test fit !== nothing && isapprox(fit.α, 2.5; atol = 0.1)
        # figure smoke tests (layout only; no file I/O)
        trades = [sample_trade(i) for i in 1:200]
        @test session_figure(trades) isa M.CairoMakie.Figure
        df = DataFrame(
            symbol = fill("AAPL", 100),
            time_ns = [1_753_886_600_000_000_000 + i * 10_000_000_000 for i in 1:100],
            recv_ns = zeros(Int64, 100),
            price = 100.0 .+ sin.(1:100),
            size = Float64.(mod1.(7 .* (1:100), 100)),
            exchange = fill("V", 100),
            conditions = fill("@", 100),
            tape = fill("C", 100),
            id = collect(1:100),
        )
        days = [(Date(2026, 7, 30), df), (Date(2026, 7, 31), df)]
        @test overview_figure("AAPL", days) isa M.CairoMakie.Figure
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
            trades = historical_trades(
                p,
                "AAPL",
                Date(2026, 7, 29),
                Date(2026, 7, 29);
                page_limit = 3,
                rate_sleep_s = 0.0,
            )
            @test length(trades) == 6                      # two pages followed
            @test all(t -> t.symbol == "AAPL", trades)
            @test all(t -> t.recv_ns == 0, trades)         # backfill marker
            @test trades[1].time_ns % 10 == 9              # nanosecond digit survived
        finally
            close(rest)
        end
    end

    @testset "live E2E: stream → reconnect → fatal stop → raw files" begin
        mktempdir() do dir
            ws_port, rest_port = freeport(8951), freeport(8971)
            plan = MockPlan(
                [
                    [mock_trade("AAPL", i) for i in 1:8],
                    [mock_trade("MSFT", i) for i in 9:14],
                ];
                fatal_after = 2,
            )               # 3rd connection → fatal 406
            ws, nconn = start_mock_ws(plan; port = ws_port)
            rest = start_mock_rest(; port = rest_port)
            try
                cfg = load_config(mock_config_toml(dir; ws_port, rest_port))
                p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
                result = run_stream(cfg; provider = p)
                @test result.ticks == 14                   # both batches, nothing lost
                @test nconn[] == 3                         # initial + 1 reconnect + fatal
                @test length(result.raw_files) == 1
                trades = read_raw(result.raw_files)
                @test length(trades) == 14
                @test count(t -> t.symbol == "AAPL", trades) == 8
                @test all(t -> t.recv_ns > 0, trades)
                # captured raw feeds straight into compaction + replay
                files = compact_raw(result.raw_files, joinpath(dir, "proc"); format = "csv")
                @test !isempty(files)
                @test length(collect(replay_source(result.raw_files; pace = "max"))) == 14
                # provenance sidecar written next to the raw files
                metas = filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))
                @test length(metas) == 1
                meta = TOML.parsefile(metas[1])
                @test meta["session"]["ticks"] == 14
                @test meta["session"]["raw_files"] == basename.(result.raw_files)
                @test meta["config"]["feed"] == "iex"
                @test occursin(
                    r"^([0-9a-f]{40}|unknown)$",
                    meta["provenance"]["git_commit"],
                )
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

    @testset "live E2E: early market close stops the session gracefully" begin
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
                @test nconn[] == 1                 # close guard, not reconnect exhaustion
                @test time() - t0 < 20.0           # well before linger end and deadline
            finally
                close(ws)
                close(rest)
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

    @testset "live E2E: market-closed railing and wait_for_open" begin
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
                cfg = load_config(
                    mock_config_toml(dir; ws_port, rest_port, wait_for_open = true),
                )
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
    end

    @testset "live E2E: delayed feed shifts the close railing" begin
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
                cfg = load_config(
                    mock_config_toml(dir; ws_port, rest_port, feed = "delayed_sip"),
                )
                p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
                @test MarketTickStreamer.feed_delay_ns(p) ==
                      900 * MarketTickStreamer.NS_PER_SEC
                t0 = time()
                result = run_stream(cfg; provider = p)
                @test result.ticks == 3
                @test time() - t0 > 2.0            # outlived the unshifted close
                @test nconn[] == 2                 # ended by the fatal, not the guard
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
                @test nconn[] == 1                         # 402 → FatalStreamError → no reconnect
            finally
                close(ws)
                close(rest)
            end
        end
    end

end
