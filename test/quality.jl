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
        # AAPL: 4 duplicates, one 120 s gap, one out-of-order pair, one negative latency
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
            Trade("MSFT", base + i * 1_000_000_000, 0, 300.0, 5.0, "V", ["@"], "C", i)
            for i in 1:5
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
        files = compact_raw([sink.path], joinpath(dir, "p"); format = :csv)
        df = CSV.read(files[findfirst(contains("AAPL"), files)], DataFrame)
        @test nrow(df) == 21                             # 26 raw AAPL rows − 5 duplicates
        # over-budget inputs reroute through spill compaction transparently
        spill_files = compact_raw([sink.path], joinpath(dir, "p2"); mem_fraction = 1e-12)
        @test length(spill_files) == length(files)
        # Both paths log the dropped duplicates per symbol and day, and
        # count the prints they stored.
        for mem_fraction in (0.5, 1e-12)
            logger = Test.TestLogger(; min_level = Base.CoreLogging.Info)
            c = Base.CoreLogging.with_logger(logger) do
                MarketTickStreamer._compact_raw(
                    [sink.path],
                    mktempdir(dir);
                    format = :csv,
                    mem_fraction,
                )
            end
            rec = only(filter(r -> r.message == "dropped duplicate prints", logger.logs))
            @test rec.kwargs[:symbol] == "AAPL"
            @test rec.kwargs[:day] == Date(2026, 7, 30)
            @test rec.kwargs[:count] == 5
            @test c.rows == 21 + 5                       # AAPL + MSFT stored
        end
    end
end

@testset "quality: condition-code price eligibility" begin
    mk(tape, conds) = Trade("AAPL", 1, 2, 100.0, 10.0, "V", conds, tape, 1)
    @test price_forming(mk("A", ["@"]))
    @test price_forming(mk("A", String[]))
    @test !price_forming(mk("A", ["I"]))           # odd lot
    @test !price_forming(mk("A", ["@", "I"]))      # any one condition disqualifies
    # The same character differs by tape: "B" is the CTA average-price
    # modifier, and is not an exclusion on the UTP tape.
    @test !price_forming(mk("A", ["B"]))
    @test price_forming(mk("C", ["B"]))
    @test !price_forming(mk("C", ["I"]))
    # Its UTP counterpart is "W", which CTA does not define.
    @test !price_forming(mk("C", ["W"]))
    @test !price_forming(mk("C", ["T", "W"]))
    @test price_forming(mk("A", ["W"]))
    # Prices fixed earlier or elsewhere never set the last sale on any tape.
    for tape in ("A", "B", "C"), code in ("4", "P", "Z")
        @test !price_forming(mk(tape, ["@", code]))
    end
    # Extended hours alone does not disqualify, nor do the auction prints.
    @test price_forming(mk("C", ["@", "T"]))
    @test price_forming(mk("A", [" ", "F", "T"]))
    @test all(
        price_forming(mk(tape, [c])) for tape in ("A", "C") for
        c in ("O", "5", "6", "X", "L")
    )
    # A tape with no list gives no basis to exclude, so the print is kept.
    @test price_forming(mk("Z", ["I"]))

    trades = [mk("A", ["@"]), mk("A", ["I"]), mk("C", ["@"]), mk("C", ["4", "W"])]
    @test length(filter_price_forming(trades)) == 2
    @test filter_price_forming(trades; non_price = Dict("A" => ["@"])) ==
          [trades[2], trades[3], trades[4]]
    @test filter_price_forming(Trade[]) == Trade[]

    # The session report counts what survives, per symbol.
    mktempdir() do dir
        sink = open_raw_sink(dir, "cond")
        write_batch!(sink, trades)
        close_sink!(sink)
        rep = session_report([sink.path])
        @test "n_price_forming" in names(rep)
        @test rep.n_trades[1] == 4
        @test rep.n_price_forming[1] == 2
    end

    # Config-driven, so the set used lands in the session sidecar.
    @test load_config(MarketTickStreamer.DEFAULT_CONFIG).non_price_conditions ==
          NON_PRICE_CONDITIONS
    mktempdir() do dir
        p = joinpath(dir, "q.toml")
        base = "[stream]\nsymbols = [\"A\"]\n"
        write(p, base * "[quality.non_price_conditions]\nA = [\"X\"]\n")
        @test load_config(p).non_price_conditions == Dict("A" => ["X"])
        write(p, base * "[quality.non_price_conditions]\nA = \"X\"\n")
        @test_throws ArgumentError load_config(p)
        write(p, base * "[quality.non_price_conditions]\nA = [7]\n")
        @test_throws ArgumentError load_config(p)
    end
end

@testset "quality: the lists follow the tape plans' sale-condition matrices" begin
    # The consolidated "update last" column of each matrix, transcribed from
    # CTS Pillar Output Specification 2.11b (29 January 2026), p. 82, and
    # UTP Data Feed Services Specification 4.1 (September 2026), 3.14.2.
    # :first is "no, unless it is the only (CTS note 2) or first (UTP
    # footnote 1) qualifying trade"; :late is CTS note 3 and UTP footnote 2,
    # a late report that may still update it; :tbd a placeholder.
    cts = Dict(
        " " => :yes,
        "B" => :no,
        "C" => :no,
        "E" => :yes,
        "F" => :yes,
        "H" => :no,
        "I" => :no,
        "K" => :yes,
        "L" => :late,
        "M" => :no,
        "N" => :no,
        "O" => :yes,
        "P" => :first,
        "Q" => :no,
        "R" => :no,
        "T" => :no,
        "U" => :no,
        "V" => :no,
        "X" => :yes,
        "Z" => :first,
        "4" => :first,
        "5" => :yes,
        "6" => :yes,
        "7" => :no,
        "8" => :no,
        "9" => :yes,
    )
    utp = Dict(
        "@" => :yes,
        "A" => :yes,
        "B" => :yes,
        "C" => :no,
        "D" => :yes,
        "E" => :tbd,
        "F" => :yes,
        "G" => :first,
        "H" => :no,
        "I" => :no,
        "K" => :yes,
        "L" => :late,
        "M" => :no,
        "N" => :no,
        "O" => :yes,
        "P" => :first,
        "Q" => :no,
        "R" => :no,
        "S" => :yes,
        "T" => :no,
        "U" => :no,
        "V" => :no,
        "W" => :no,
        "X" => :yes,
        "Y" => :yes,
        "Z" => :first,
        "1" => :yes,
        "4" => :first,
        "5" => :yes,
        "6" => :yes,
        "7" => :no,
        "8" => :tbd,
        "9" => :yes,
    )
    # The rule the docstring states, then its two departures: T is kept,
    # 9 is listed.
    listed(matrix) = Set(c for (c, v) in matrix if v in (:no, :first))
    expected(matrix) = union(setdiff(listed(matrix), ["T"]), ["9"])
    @test Set(NON_PRICE_CONDITIONS["A"]) == expected(cts)
    @test Set(NON_PRICE_CONDITIONS["B"]) == expected(cts)
    @test Set(NON_PRICE_CONDITIONS["C"]) == expected(utp)
    @test allunique(NON_PRICE_CONDITIONS["A"]) && allunique(NON_PRICE_CONDITIONS["C"])
end

@testset "quality: the round lot is read off the tape" begin
    mk(sz, conds) =
        Trade("AAPL", 1_753_886_600_000_000_000, 0, 100.0, sz, "V", conds, "C", 1)
    # One share more than the largest print still flagged an odd lot.
    ts = [mk(99.0, ["I"]), mk(100.0, ["@"]), mk(40.0, ["I"]), mk(250.0, ["@"])]
    @test observed_round_lot(ts) == 100.0
    # The tiered regime: nothing above 39 carries the flag.
    ts40 = [mk(39.0, ["I"]), mk(40.0, ["@"]), mk(12.0, ["I"])]
    @test observed_round_lot(ts40) == 40.0
    # No flagged print means no basis to state a lot.
    @test isnan(observed_round_lot([mk(100.0, ["@"])]))
    @test isnan(observed_round_lot(Trade[]))

    # It reaches the session report: a redefinition
    # that moves the price-forming population is otherwise invisible.
    mktempdir() do dir
        sink = open_raw_sink(dir, "rl")
        write_batch!(sink, ts)
        close_sink!(sink)
        rep = session_report(sink.path)
        @test rep[1, :round_lot] == 100.0
        @test rep[1, :n_price_forming] == 2      # the two unflagged prints
    end
end

@testset "quality: capture coverage against the historical tape" begin
    rest_port = freeport(8936)
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
        # The mock's tape for AAPL on 2026-07-29: two pages of three, ids 1…6,
        # page-local index j = 1…3 at 15:0j:0j.123456789Z on both pages, so
        # every request on that day returns 6 prints spanning 15:01:01 to
        # 15:03:03. The capture misses id 2 from the middle: captured 5,
        # reference 6.
        tape = [
            (id = 3 * (pg - 1) + j, j = j, price = 200.0 + 3 * (pg - 1) + j) for
            pg in 1:2 for j in 1:3
        ]
        captured = [
            Trade(
                "AAPL",
                rfc3339_to_ns("2026-07-29T15:0$(r.j):0$(r.j).123456789Z"),
                1_785_337_000_000_000_000 + r.id,
                r.price,
                5.0 * r.j,
                "V",
                ["@"],
                "C",
                r.id,
            ) for r in tape if r.id != 2
        ]
        mktempdir() do dir
            path = joinpath(dir, "capture.jsonl")
            write(path, join(trade_to_json.(captured) .* "\n"))
            cov = coverage_report(path, p; page_limit = 3, rate_sleep_s = 0.0)
            @test size(cov, 1) == 1
            row = only(eachrow(cov))
            @test row.symbol == "AAPL"
            @test row.captured == 5
            @test row.reference == 6
            @test row.coverage == round(5 / 6; digits = 4)
        end
    finally
        close(rest)
    end
end
