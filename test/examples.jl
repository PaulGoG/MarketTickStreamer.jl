@testset "examples: the worked consumer runs and adds up" begin
    # The example is the manual's claim about the interface, in code, so
    # the suite executes it rather than trusting it to stay true.
    mktempdir() do dir
        sink = open_raw_sink(dir, "example")
        # Ten prints a millisecond apart; two carry the odd-lot condition,
        # so the two populations differ by a known amount.
        trades = [
            Trade(
                "AAPL",
                1_753_886_600_000_000_000 + i * 1_000_000,
                1_753_886_600_100_000_000 + i * 1_000_000,
                100.0 + i,
                10.0,
                "V",
                i in (4, 7) ? ["I"] : ["@"],
                "C",
                i,
            ) for i in 1:10
        ]
        write_batch!(sink, trades)
        close_sink!(sink)

        mod = Module(:WaitingTimesExample)
        Base.include(mod, joinpath(@__DIR__, "..", "examples", "waiting_times.jl"))
        w = example_call(mod, :consume, [sink.path])
        @test w.n_prints == 10
        @test length(w.gaps) == 9                  # one gap fewer than prints
        # 1 ms apart in exchange time
        @test all(x -> isapprox(x, 0.001; rtol = 1e-12), w.gaps)
        @test w.n_nonpositive == 0

        pf = example_call(mod, :consume, [sink.path]; price_forming_only = true)
        @test pf.n_prints == 8                     # the two odd lots are excluded
        @test length(pf.gaps) == 7
        @test sum(pf.gaps) ≈ 0.009 atol = 1e-9     # gaps still span the session

        # A late print is measured against the latest timestamp seen and
        # does not move it: one non-positive gap, no spurious 5 s wait.
        w = example_call(mod, :WaitingTimes)
        for s in (4.999, 10.0, 10.001, 5.0, 10.002)
            t = Trade("AAPL", round(Int64, s * 1e9), 0, 1.0, 1.0, "V", String[], "C", 0)
            example_call(mod, :observe!, w, t)
        end
        @test all(isapprox.(w.gaps, [5.001, 0.001, 0.001]; atol = 1e-9))
        @test w.n_nonpositive == 1
    end
end

@testset "example: bounded-memory live diagnostics" begin
    mktempdir() do dir
        sink = open_raw_sink(dir, "diag")
        # 1 ms apart in exchange time, price 101…110, size 10 throughout,
        # so the VWAP is the plain mean price and every gap is 1 ms.
        write_batch!(
            sink,
            [
                Trade(
                    "AAPL",
                    1_753_886_600_000_000_000 + i * 1_000_000,
                    1_753_886_600_100_000_000 + i * 1_000_000,
                    100.0 + i,
                    10.0,
                    "V",
                    ["@"],
                    "C",
                    i,
                ) for i in 1:10
            ],
        )
        close_sink!(sink)

        mod = Module(:LiveDiagnosticsExample)
        Base.include(mod, joinpath(@__DIR__, "..", "examples", "live_diagnostics.jl"))
        d, persisted, seen = example_call(mod, :consume, [sink.path])
        @test persisted == 10                      # persistence is lossless
        @test seen <= persisted                    # the diagnostic tap may drop
        @test OnlineStats.nobs(d.Δt_mean) == seen - 1
        @test OnlineStats.value(d.Δt_mean) ≈ 0.001 rtol = 1e-12
        @test OnlineStats.value(d.Δt_extrema).max ≈ 0.001 rtol = 1e-12
        @test d.n_nonpositive == 0
        # Equal sizes make the VWAP the arithmetic mean of the prices seen.
        @test example_call(mod, :vwap, d) ≈ 101.0 + (seen - 1) / 2 rtol = 1e-12
        @test OnlineStats.value(d.volume) ≈ 10.0 * seen rtol = 1e-12

        # A late print counts as one non-positive gap and moves neither the
        # reference nor the start of the span: 5 prints over 5.003 s.
        late = example_call(mod, :LiveDiagnostics)
        for s in (4.999, 10.0, 10.001, 5.0, 10.002)
            t = Trade("AAPL", round(Int64, s * 1e9), 0, 1.0, 1.0, "V", String[], "C", 0)
            example_call(mod, :observe!, late, t)
        end
        @test late.n_nonpositive == 1 && OnlineStats.nobs(late.Δt_mean) == 3
        @test example_call(mod, :print_rate, late) ≈ 5 / 5.003 rtol = 1e-9
        # A late first print: the span starts at the earliest timestamp seen.
        early = example_call(mod, :LiveDiagnostics)
        for s in (10.0, 5.0, 10.001)
            t = Trade("AAPL", round(Int64, s * 1e9), 0, 1.0, 1.0, "V", String[], "C", 0)
            example_call(mod, :observe!, early, t)
        end
        @test example_call(mod, :print_rate, early) ≈ 3 / 5.001 rtol = 1e-9

        # Gaps swept log-uniformly over five orders of magnitude
        # (100 µs … 10 s), the shape that breaks a P² sketch and the reason
        # the example bins in log10. Deterministic, so the expected quantiles are exact.
        spread_trades(n) = begin
            gaps_ns = [round(Int64, 10^(-4 + 5 * (i - 1) / (n - 1)) * 1e9) for i in 1:n]
            t = 1_753_886_600_000_000_000
            out = Trade[]
            for (i, g) in enumerate(gaps_ns)
                t += g
                push!(out, Trade("AAPL", t, 0, 100.0, 10.0, "V", ["@"], "C", i))
            end
            (out, gaps_ns ./ 1e9)
        end

        trades_s, gaps_s = spread_trades(2000)
        s = open_raw_sink(dir, "diagspread")
        write_batch!(s, trades_s)
        close_sink!(s)
        acc, persisted_s, seen_s = example_call(mod, :consume, [s.path])
        @test persisted_s == 2000 && seen_s == 2000   # far below the tap capacity
        # The first trade opens the series, so its gap is not observed.
        exact = sort(gaps_s[2:end])
        for τ in (0.5, 0.9, 0.99)
            got = example_call(mod, :waiting_quantile, acc, τ)
            want = exact[max(1, round(Int, τ * length(exact)))]
            # Bin centers over five orders of magnitude: accurate to a bin
            # width, not to the digit. A P² sketch on these gaps misses by 10x+.
            @test 0.8 * want <= got <= 1.25 * want
        end
        # Both calls go through invokelatest: the example's methods are
        # younger than this world, the constructor included.
        empty_acc = example_call(mod, :LiveDiagnostics)
        @test isnan(example_call(mod, :waiting_quantile, empty_acc, 0.5))
        @test_throws ArgumentError example_call(mod, :waiting_quantile, acc, 1.5)
        @test_throws ArgumentError example_call(mod, :LiveDiagnostics; n_bins = 2)

        # The example shows: once the histogram has filled its bin
        # budget the footprint stops growing, so a longer stream of the
        # same shape costs exactly the same state.
        foot = map((2000, 6000)) do n
            tr, _ = spread_trades(n)
            sk = open_raw_sink(dir, "diaglong$(n)")
            write_batch!(sk, tr)
            close_sink!(sk)
            a, _, _ = example_call(mod, :consume, [sk.path])
            Base.summarysize(a)
        end
        @test foot[1] == foot[2]
    end
end

@testset "example: following a session from its files" begin
    mktempdir() do dir
        cfg = load_config(mock_config_toml(dir; ws_port = 1, rest_port = 1))
        raw = cfg.raw_dir
        mkpath(raw)
        sid = "follow_example"
        meta = start_session_meta(cfg, sid; started_utc = Dates.now(UTC))
        # Two prints before 10:01 and one after, size 10 throughout, so each
        # row's VWAP is the mean of the prices folded so far.
        prints = [
            Trade("AAPL", rfc3339_to_ns(ts), 0, price, 10.0, "V", ["@"], "C", i) for
            (i, (ts, price)) in enumerate([
                ("2026-01-05T10:00:30Z", 100.0),
                ("2026-01-05T10:00:45Z", 102.0),
                ("2026-01-05T10:01:10Z", 104.0),
            ])
        ]
        part = joinpath(raw, "$(sid)_part001.jsonl")
        write(part, join(trade_to_json.(prints) .* "\n"))
        finalize_session_meta(meta, "completed"; ticks = 3, raw_files = [part])

        mod = Module(:FollowSessionExample)
        Base.include(mod, joinpath(@__DIR__, "..", "examples", "follow_session.jl"))
        buf = IOBuffer()
        n = example_call(mod, :follow, raw; session = sid, io = buf, poll_s = 0.02)
        @test n == 3
        out = String(take!(buf))
        rows = filter(!isempty, split(out, '\n'))
        @test length(rows) == 2
        @test occursin("2026-01-05T10:01:00", rows[1])
        @test occursin("prints=2", rows[1]) && occursin("vwap=101.0000", rows[1])
        @test occursin("2026-01-05T10:01:10", rows[2])
        @test occursin("prints=3", rows[2]) && occursin("vwap=102.0000", rows[2])
        @test occursin("final", rows[2])
    end
end
