@testset "replay" begin
    mktempdir() do dir
        sink = open_raw_sink(dir, "r")
        write_batch!(sink, [sample_trade(i) for i in 1:50])
        close_sink!(sink)
        # max pace: everything, in recv order
        got = collect(replay_source(sink.path; pace = :max))
        @test length(got) == 50
        @test issorted(got; by = t -> t.recv_ns)
        # recorded pace at high compression still delivers everything
        got2 = collect(replay_source(sink.path; pace = :recorded, speed = 1e9))
        @test length(got2) == 50
        @test_throws ArgumentError replay_source(sink.path; pace = :warp)
        # A mode is a Symbol; the 1.x string spelling is a type error.
        @test_throws TypeError replay_source(sink.path; pace = "max")
        @test_throws ArgumentError replay_source(sink.path; clock = :wall)
        # A backfilled recording has no receive clock. `auto` must pace it
        # on exchange time rather than emit at full speed; `recv` refuses.
        spaced(n, gap_ns) = [
            Trade(
                "AAPL",
                1_753_886_600_000_000_000 + i * gap_ns,
                0,
                100.0,
                1.0,
                "V",
                String[],
                "C",
                i,
            ) for i in 0:n
        ]
        function paced_span(path; kwargs...)
            ch = replay_source(path; pace = :recorded, kwargs...)
            take!(ch)
            t0 = time()
            n = 1
            for _ in ch
                n += 1
            end
            return n, time() - t0
        end
        bsink = open_raw_sink(dir, "b")
        write_batch!(bsink, spaced(25, 20_000_000))
        close_sink!(bsink)
        n, span = paced_span(bsink.path)
        @test n == 26
        @test 0.45 <= span <= 1.0               # 25 gaps of 20 ms
        @test_throws ArgumentError replay_source(bsink.path; clock = :recv)
        # Absolute schedule: 400 gaps of 2.5 ms take 1 s. Sleeping each gap
        # on its own adds one timer overshoot per gap and runs markedly long.
        dsink = open_raw_sink(dir, "d")
        write_batch!(dsink, spaced(400, 2_500_000))
        close_sink!(dsink)
        n, span = paced_span(dsink.path; clock = :exchange)
        @test n == 401
        @test 0.95 <= span <= 1.2
        # max_gap_s: two bursts of ten 20 ms gaps, 30 s apart. Replayed
        # faithfully that is 30.4 s; with the gap capped at 0.1 s, 0.5 s.
        burst = spaced(10, 20_000_000)
        later(t) = Trade(
            t.symbol,
            t.time_ns + 30_200_000_000,
            t.recv_ns,
            t.price,
            t.size,
            t.exchange,
            t.conditions,
            t.tape,
            t.id + 100,
        )
        gsink = open_raw_sink(dir, "g")
        write_batch!(gsink, [burst; later.(burst)])
        close_sink!(gsink)
        n, span = paced_span(gsink.path; max_gap_s = 0.1)
        @test n == 22
        @test 0.45 <= span <= 0.8
        @test_throws ArgumentError replay_source(gsink.path; max_gap_s = 0)
        @test_throws ArgumentError replay_source(gsink.path; max_gap_s = NaN)
    end
end

@testset "replay: processed day files are streamed, one schedule across days" begin
    day_ns = 86_400 * 1_000_000_000
    t0 = 1_753_886_600_000_000_000                       # 2025-07-30 14:43:20 UTC
    mk(i, sym, day; conds = ["@"]) = Trade(
        sym,
        t0 + day * day_ns + i * 1_000_000,
        0,
        100.0 + i,
        1.0 * i,
        "V",
        conds,
        "C",
        i,
    )
    trades = [
        [
            mk(i, "AAPL", d; conds = isodd(i) ? ["@", "I"] : String[]) for d in 0:2 for
            i in 1:2:9
        ]
        [mk(i, "MSFT", d; conds = ["4"]) for d in 0:1 for i in 2:2:8]
    ]
    expected = sort(trades; by = t -> t.time_ns)
    mktempdir() do dir
        sink = open_raw_sink(dir, "corpus")
        write_batch!(sink, trades)
        close_sink!(sink)
        for format in (:csv, :arrow)
            out = joinpath(dir, string(format))
            compact_raw([sink.path], out; format, tz = tz"UTC")
            aapl = processed_files(out, "AAPL")
            @test basename.(aapl) ==
                  ["2025-07-30.$format", "2025-07-31.$format", "2025-08-01.$format"]
            @test length(processed_files(out, "AAPL"; from = Date(2025, 7, 31))) == 2
            @test length(processed_files(out, "AAPL"; to = Date(2025, 7, 30))) == 1
            @test_throws ArgumentError processed_files(out, "NVDA")
            # One symbol: the days in order, every field intact, an empty
            # condition list and an all-numeric one included.
            got = collect(replay_source(aapl; pace = :max))
            @test got == filter(t -> t.symbol == "AAPL", expected)
            # Two symbols: merged on the clock within each day.
            both = [aapl; processed_files(out, "MSFT")]
            @test collect(replay_source(both; pace = :max)) == expected
            @test collect(replay_source(reverse(both); pace = :max)) == expected
        end
        out = joinpath(dir, "arrow")
        files = processed_files(out, "AAPL")
        # Backups are not replayed, raw and processed do not mix, the
        # receive clock needs receive timestamps.
        @test_throws ArgumentError replay_source([files[1], sink.path]; pace = :max)
        @test_throws ArgumentError replay_source(
            [joinpath(out, "AAPL", "2025-07-30_#1.arrow")];
            pace = :max,
        )
        @test_throws ArgumentError replay_source([
            joinpath(out, "AAPL", "2025-01-01.arrow"),
        ])
        @test_throws Exception collect(replay_source(files; pace = :max, clock = :recv))
        # Streaming: a day is opened only when the one before is spent. With
        # the second day unreadable the first still arrives in full, and
        # the failure reaches the consumer afterwards.
        broken = joinpath(dir, "broken", "AAPL")
        mkpath(broken)
        cp(files[1], joinpath(broken, basename(files[1])))
        write(joinpath(broken, basename(files[2])), "not an arrow file")
        ch = replay_source(
            processed_files(dirname(broken), "AAPL");
            pace = :max,
            capacity = 1,
        )
        first_day = [take!(ch) for _ in 1:5]
        @test first_day == filter(t -> t.symbol == "AAPL", expected)[1:5]
        @test_throws Exception take!(ch)
        # One schedule across days: 86 400 s between sessions replayed at
        # 1e6x is 0.0864 s, not zero and not per-day re-anchored.
        two = processed_files(out, "AAPL"; to = Date(2025, 7, 31))
        t_start = time()
        n = length(collect(replay_source(two; pace = :recorded, speed = 1.0e6)))
        elapsed = time() - t_start
        @test n == 10
        @test 0.08 <= elapsed < 1.0
        # The cap carries across days: at 1e5x the closure alone would take
        # 0.864 s, capped at 60 s it takes 0.6 ms.
        t_start = time()
        n = length(
            collect(replay_source(two; pace = :recorded, speed = 1.0e5, max_gap_s = 60.0)),
        )
        elapsed = time() - t_start
        @test n == 10
        @test elapsed < 0.4
    end
end

@testset "corpus: inventory, day iteration, windowed and filtered reads" begin
    day_ns = 86_400 * 1_000_000_000
    t0 = 1_753_886_600_000_000_000                       # 2025-07-30 14:43:20 UTC
    mk(i, sym, day, conds) = Trade(
        sym,
        t0 + day * day_ns + i * 1_000_000,
        0,
        100.0 + i,
        1.0 * i,
        "V",
        conds,
        "C",
        i,
    )
    # AAPL: six prints a day for three days, odd i regular sales, even i odd
    # lots; MSFT: two derivatively priced prints a day for two days.
    aapl = [mk(i, "AAPL", d, isodd(i) ? ["@"] : ["@", "I"]) for d in 0:2 for i in 1:6]
    msft = [mk(i, "MSFT", d, ["4"]) for d in 0:1 for i in 7:8]
    all_trades = sort([aapl; msft]; by = t -> t.time_ns)
    dates = [Date(2025, 7, 30), Date(2025, 7, 31), Date(2025, 8, 1)]
    on(day, ts) = filter(t -> trading_date(t.time_ns; tz = tz"UTC") == day, ts)
    h(ms) = ms / 3_600_000                               # exchange-local hours
    h_t0 = h(53_000_000)                                 # 14:43:20 as hours
    mktempdir() do dir
        sink = open_raw_sink(dir, "corpus")
        write_batch!(sink, [aapl; msft])
        close_sink!(sink)
        for format in (:csv, :arrow)
            out = joinpath(dir, string(format))
            compact_raw([sink.path], out; format, tz = tz"UTC")
            # Inventory from metadata: counts, span, bytes of the canonical files.
            inv = corpus_inventory(out)
            @test names(inv) == [
                "symbol",
                "format",
                "n_days",
                "first_date",
                "last_date",
                "n_prints",
                "bytes",
            ]
            @test inv.symbol == ["AAPL", "MSFT"]
            @test inv.format == [format, format]
            @test inv.n_days == [3, 2]
            @test inv.first_date == [Date(2025, 7, 30), Date(2025, 7, 30)]
            @test inv.last_date == [Date(2025, 8, 1), Date(2025, 7, 31)]
            @test inv.n_prints == [18, 4]
            @test inv.bytes == [
                sum(filesize, processed_files(out, "AAPL")),
                sum(filesize, processed_files(out, "MSFT")),
            ]
            @test corpus_inventory(out; symbols = "MSFT").n_prints == [4]
            @test_throws ArgumentError corpus_inventory(out; symbols = ["NVDA"])
            # One symbol: the days in order, every print, in exchange-time order.
            it = eachday(out, "AAPL")
            @test length(it) == 3
            @test eltype(it) == Tuple{Date,Vector{Trade}}
            @test occursin("3 days", sprint(show, it))
            got = collect(it)
            @test first.(got) == dates
            @test [last(g) for g in got] == [on(d, aapl) for d in dates]
            # Two symbols: merged on exchange time within each day; the
            # third day has AAPL only.
            both = collect(eachday(out, ["MSFT", "AAPL"]))
            @test [last(g) for g in both] == [on(d, all_trades) for d in dates]
            @test length(eachday(out, ["AAPL", "MSFT"]; from = Date(2025, 7, 31))) == 2
            @test length(eachday(out, "AAPL"; to = Date(2025, 7, 30))) == 1
            @test read_processed(out, ["AAPL", "MSFT"]) == all_trades
            @test read_processed(processed_files(out, "AAPL")) == aapl
            # Window: half-open on the exchange-local clock, here UTC.
            # [t0, t0 + 4 ms) keeps i = 1, 2, 3 of every day.
            w = read_processed(out, "AAPL"; window = (h_t0, h(53_000_004)), tz = tz"UTC")
            @test length(w) == 9
            @test all(t -> t.id in 1:3, w)
            # The open bound is inclusive: a window opening on the first
            # print keeps it.
            @test length(
                read_processed(out, "AAPL"; window = (h(53_000_001), 24.0), tz = tz"UTC"),
            ) == 18
            # Nothing before t0: every day is yielded, empty.
            empty_days = collect(eachday(out, "AAPL"; window = (0.0, h_t0), tz = tz"UTC"))
            @test length(empty_days) == 3
            @test all(isempty ∘ last, empty_days)
            @test_throws ArgumentError eachday(out, "AAPL"; window = (16.0, 9.5))
            @test_throws ArgumentError eachday(out, "AAPL"; window = (-1.0, 5.0))
            @test_throws ArgumentError eachday(out, "AAPL"; window = (0.0, 25.0))
            # Population: odd lots and derivatively priced prints drop out;
            # an empty condition table excludes nothing.
            pf = read_processed(out, ["AAPL", "MSFT"]; price_forming = true)
            @test length(pf) == 9
            @test all(t -> t.symbol == "AAPL" && t.conditions == ["@"], pf)
            @test length(
                read_processed(
                    out,
                    ["AAPL", "MSFT"];
                    price_forming = true,
                    non_price = Dict{String,Vector{String}}(),
                ),
            ) == 22
            # Memory guard: refused up front without filters, while
            # accumulating with them; the fraction itself is validated.
            @test_throws ResourceError read_processed(out, "AAPL"; mem_fraction = 1e-15)
            @test_throws ResourceError read_processed(
                out,
                "AAPL";
                price_forming = true,
                mem_fraction = 1e-15,
            )
            @test_throws ArgumentError read_processed(out, "AAPL"; mem_fraction = 0.0)
            @test_throws ArgumentError read_processed(out, "AAPL"; mem_fraction = 1.5)
            @test_throws ArgumentError eachday(out, String[])
            @test_throws ArgumentError eachday(out, "NVDA")
            @test_throws ArgumentError eachday([sink.path])
            @test_throws ArgumentError eachday([
                joinpath(out, "AAPL", "2025-01-01.$format"),
            ])
        end
        # Backups are neither counted nor read.
        out = joinpath(dir, "arrow")
        cp(
            joinpath(out, "AAPL", "2025-07-30.arrow"),
            joinpath(out, "AAPL", "2025-07-30_#1.arrow"),
        )
        @test corpus_inventory(out).n_days == [3, 2]
        @test corpus_inventory(out).bytes[1] == sum(filesize, processed_files(out, "AAPL"))
        # An empty tree is an empty table with the same columns; a
        # missing one is an error.
        mkdir(joinpath(dir, "empty"))
        @test size(corpus_inventory(joinpath(dir, "empty"))) == (0, 7)
        @test_throws ArgumentError corpus_inventory(joinpath(dir, "nowhere"))
        # The columnar form round-trips through the processed schema.
        df = trades_table(all_trades)
        @test names(df) == [
            "symbol",
            "time_ns",
            "recv_ns",
            "price",
            "size",
            "exchange",
            "conditions",
            "tape",
            "id",
        ]
        @test df.conditions[1] == "@" && "@|I" in df.conditions && "4" in df.conditions
        @test MarketTickStreamer._columns_to_trades(
            df.symbol,
            df.time_ns,
            df.recv_ns,
            df.price,
            df.size,
            df.exchange,
            df.conditions,
            df.tape,
            df.id,
        ) == all_trades
        @test size(trades_table(Trade[])) == (0, 9)
        # Configuration form: tree, calendar, condition lists and the
        # provider's regular session come from the file. 14:43 UTC is
        # 10:43 New York, inside (9.5, 16.0).
        p = joinpath(dir, "corpus.toml")
        write(p, "[stream]\nsymbols = [\"AAPL\", \"MSFT\"]\n[storage]\ndata_dir = \".\"\n")
        cfg = load_config(p)
        @test cfg.processed_dir == joinpath(dir, "processed")
        compact_raw([sink.path], cfg.processed_dir; format = :csv, tz = tz"UTC")
        @test corpus_inventory(cfg).symbol == ["AAPL", "MSFT"]
        @test read_processed(cfg) == all_trades
        @test length(read_processed(cfg, "AAPL"; window = :regular)) == 18
        @test length(
            read_processed(cfg, "AAPL"; window = :regular, price_forming = true),
        ) == 9
        @test length(collect(eachday(cfg; window = :regular))) == 3
        @test_throws ArgumentError read_processed(cfg, "AAPL"; window = :all)
        write(
            p,
            "[stream]\nsymbols = [\"AAPL\"]\n[storage]\ndata_dir = \".\"\n" *
            "[limits]\nread_mem_fraction = 1e-15\n",
        )
        @test_throws ResourceError read_processed(load_config(p))
    end
    # The window is resolved on each date's own clock: 09:30 New York is
    # 14:30 UTC under standard time, 13:30 UTC under daylight time, and a
    # full local day is 23 h on 2026-03-08 and 25 h on 2026-11-01.
    ny = tz"America/New_York"
    @test MarketTickStreamer._window_ns(Date(2026, 3, 6), (9.5, 16.0), ny) ==
          (rfc3339_to_ns("2026-03-06T14:30:00Z"), rfc3339_to_ns("2026-03-06T21:00:00Z"))
    @test MarketTickStreamer._window_ns(Date(2026, 3, 9), (9.5, 16.0), ny) ==
          (rfc3339_to_ns("2026-03-09T13:30:00Z"), rfc3339_to_ns("2026-03-09T20:00:00Z"))
    lo, hi = MarketTickStreamer._window_ns(Date(2026, 3, 8), (0.0, 24.0), ny)
    @test hi - lo == 23 * 3_600 * 1_000_000_000
    lo, hi = MarketTickStreamer._window_ns(Date(2026, 11, 1), (0.0, 24.0), ny)
    @test hi - lo == 25 * 3_600 * 1_000_000_000
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
        st = monitor_raw(dir; refresh_s = 0.01, iterations = 1, from_start = true, io = buf)
        @test st.total == 50
        @test st.per_symbol["AAPL"] == 30 && st.per_symbol["MSFT"] == 20
        out = String(take!(buf))
        @test occursin("AAPL", out) && occursin("Ticks", out)
        # UnicodePlots is not loaded yet: the dashboard is plain text.
        @test Base.get_extension(MarketTickStreamer, :MarketTickStreamerUnicodePlotsExt) ===
              nothing
        @test occursin("Ticks by symbol:", out)
        @test occursin(r"AAPL\s+30", out) && occursin(r"MSFT\s+20", out)
        @test !occursin("┌", out)
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

@testset "follow: a running session's raw parts, torn lines, roll, end by sidecar" begin
    mktempdir() do dir
        cfg = load_config(mock_config_toml(dir; ws_port = 1, rest_port = 1))
        raw = cfg.raw_dir
        mkpath(raw)
        line(i) = trade_to_json(sample_trade(i)) * "\n"
        append(path, s) = open(io -> write(io, s), path, "a")
        sid = "follow_unit"
        meta = start_session_meta(cfg, sid; started_utc = Dates.now(UTC))

        # Three prints and the first half of a fourth: the torn line is held.
        p1 = joinpath(raw, "$(sid)_part001.jsonl")
        l4 = line(4)
        cut = length(l4) ÷ 2
        write(p1, line(1) * line(2) * line(3) * l4[1:cut])
        ch = follow_source(raw; session = sid, poll_s = 0.02)
        @test [take!(ch) for _ in 1:3] == [sample_trade(i) for i in 1:3]
        # Its newline arrives with the next flush.
        append(p1, l4[(cut+1):end] * line(5))
        @test take!(ch) == sample_trade(4)
        @test take!(ch) == sample_trade(5)
        # The sink rolls to a second part.
        p2 = joinpath(raw, "$(sid)_part002.jsonl")
        write(p2, line(6) * line(7) * line(8))
        @test [take!(ch) for _ in 1:3] == [sample_trade(i) for i in 6:8]
        # A line that does not parse is skipped.
        append(p2, "{not json}\n" * line(9))
        @test take!(ch) == sample_trade(9)
        # The sidecar leaves `running`: the channel closes after the drain.
        finalize_session_meta(meta, "completed"; ticks = 9, raw_files = [p1, p2])
        @test collect(ch) == Trade[]
        @test !isopen(ch)

        # A finished session is read whole; from the end it is empty; the
        # default session is the latest one.
        @test collect(follow_source(raw; session = sid, poll_s = 0.02)) ==
              [sample_trade(i) for i in 1:9]
        @test collect(
            follow_source(raw; session = sid, from_start = false, poll_s = 0.02),
        ) == Trade[]
        @test length(collect(follow_source(raw; poll_s = 0.02))) == 9

        @test_throws ArgumentError follow_source(raw; session = "nobody")
        @test_throws ArgumentError follow_source(raw; session = sid, poll_s = 0)

        # A backfill day file carries symbol and date after the session id;
        # its sidecar is found either way.
        sid2 = "follow_backfill"
        meta2 = start_session_meta(cfg, sid2; started_utc = Dates.now(UTC))
        finalize_session_meta(meta2, "completed"; ticks = 2, raw_files = String[])
        write(joinpath(raw, "$(sid2)_AAPL_2026-01-02_part001.jsonl"), line(1) * line(2))
        @test length(collect(follow_source(raw; session = "$(sid2)_AAPL_2026-01-02"))) == 2
        @test length(collect(follow_source(raw; session = sid2))) == 2

        # A session still marked running whose files stop growing: the
        # channel closes with an error past the idle timeout.
        sid3 = "follow_idle"
        start_session_meta(cfg, sid3; started_utc = Dates.now(UTC))
        write(joinpath(raw, "$(sid3)_part001.jsonl"), line(1))
        ch3 = follow_source(raw; session = sid3, poll_s = 0.02, idle_timeout_s = 0.1)
        @test take!(ch3) == sample_trade(1)
        stalled = try
            collect(ch3)
            nothing
        catch e
            e isa TaskFailedException ? e.task.exception : e
        end
        @test stalled isa SessionError && endswith(stalled.path, "$(sid3).meta.toml")
        @test !isopen(ch3)
    end
end

@testset "follow: attached in the middle of a line" begin
    # Attached while a line is being written: it is read whole once its
    # newline arrives, not counted as a corrupt fragment.
    mktempdir() do raw
        part = joinpath(raw, "attach_part001.jsonl")
        l1, l2 =
            trade_to_json(sample_trade(1)) * "\n", trade_to_json(sample_trade(2)) * "\n"
        write(part, l1 * l2[1:10])
        tail = MarketTickStreamer.RawTail(raw, "attach")
        MarketTickStreamer._skip_to_end!(tail)
        open(io -> write(io, l2[11:end]), part, "a")
        got = Trade[]
        @test MarketTickStreamer._drain!(t -> push!(got, t), tail) == (1, 0)
        @test got == [sample_trade(2)]
    end
    # A part deleted while it is drained (keep_raw = false compacts a day
    # and removes it) is read to the end through the handle already open.
    # Lines the writer appends before the deletion are read too.
    mktempdir() do raw
        part = joinpath(raw, "gone_part001.jsonl")
        write(part, join(trade_to_json(sample_trade(i)) * "\n" for i in 1:20))
        tail = MarketTickStreamer.RawTail(raw, "gone")
        got = Trade[]
        n, bad = MarketTickStreamer._drain!(tail; block = 100) do t
            push!(got, t)
            if isfile(part)
                open(part, "a") do io
                    foreach(i -> println(io, trade_to_json(sample_trade(i))), 21:25)
                end
                rm(part)
            end
        end
        @test (n, bad) == (25, 0) && got == [sample_trade(i) for i in 1:25]
    end
    # The last line end lies before the final window: a line longer than
    # the window is being written.
    mktempdir() do raw
        part = joinpath(raw, "long_part001.jsonl")
        write(part, "first\n" * "x"^300)
        @test MarketTickStreamer._last_line_end(part; window = 100) == 6
        write(part, "y"^300)
        @test MarketTickStreamer._last_line_end(part; window = 100) == 0
        # A part deleted since it was listed holds no line, and no error.
        @test MarketTickStreamer._last_line_end(joinpath(raw, "missing.jsonl")) === Int64(0)
    end
end

@testset "follow: the latest session of a backfill" begin
    # A backfill names its parts <id>_<SYMBOL>_<DATE>: the session is the
    # longest sidecar id that prefixes the newest part.
    mktempdir() do d
        sid = "backfill_alpaca_sip_20260101-000000"
        write(joinpath(d, "$sid.meta.toml"), "[session]\nstatus = \"completed\"\n")
        write(joinpath(d, "$(sid)_AAPL_2026-01-02_part001.jsonl"), "")
        @test MarketTickStreamer.latest_session_prefix(d) == sid
    end
    mktempdir() do d                 # no sidecar: the part's own prefix
        write(joinpath(d, "x_part001.jsonl"), "")
        @test MarketTickStreamer.latest_session_prefix(d) == "x"
    end
end

@testset "tee: a consumer that closes its output does not end the others" begin
    src = Channel{Trade}(10)
    outs = tee(src, 2; capacity = 100)
    close(outs[2])                                 # the analysis tap dies
    for i in 1:20
        put!(src, sample_trade(i))
    end
    close(src)
    @test length(collect(outs[1])) == 20           # persistence got everything
    # Every output closed: the fan-out stops instead of draining into nothing.
    src = Channel{Trade}(10)
    outs = tee(src, 1; capacity = 100)
    close(outs[1])
    put!(src, sample_trade(1))
    close(src)
    @test isempty(collect(outs[1]))
end

@testset "replay: a stamp behind the cursor leaves the schedule alone" begin
    # On the receive clock the first print of a day file can precede the last
    # of the day before. With a gap cap, moving the cursor back to it would
    # make the next gap look longer than the cap and shift every later print.
    s = 1_000_000_000
    sch = MarketTickStreamer.ReplaySchedule(1.0, 1.0)            # speed 1, gaps capped at 1 s
    stamps = [0.0, 0.8, 1.5, 0.3, 1.8, 2.5]
    due = [MarketTickStreamer._due!(sch, round(Int64, x * s)) for x in stamps]
    @test sch.skipped_ns == 0
    # a wall clock in Float64 seconds resolves 238 ns at this epoch
    @test due .- sch.t0_wall ≈ stamps atol = 1e-6
    # A gap above the cap is still shortened, measured from the latest stamp.
    @test MarketTickStreamer._due!(sch, round(Int64, 5.5 * s)) - sch.t0_wall ≈ 3.5 atol =
        1e-6
    @test sch.skipped_ns == 2s
end

@testset "corpus: a window bound inside a daylight-saving hour" begin
    ny = tz"America/New_York"
    hours(d, w) = (r = MarketTickStreamer._window_ns(d, w, ny); (r[2] - r[1]) / 3.6e12)
    utc(d, h) = Int64(Dates.value(DateTime(d) + Hour(h) - DateTime(1970, 1, 1))) * 1_000_000
    # 2026-03-08: 02:00 to 03:00 does not exist. A bound inside it is the
    # instant of the jump, 07:00 UTC.
    spring = Date(2026, 3, 8)
    @test MarketTickStreamer._window_ns(spring, (2.5, 4.0), ny) ==
          (utc(spring, 7), utc(spring, 8))
    @test hours(spring, (2.0, 4.0)) == 1.0
    @test hours(spring, (0.0, 24.0)) == 23.0
    # 2026-11-01: 01:00 to 02:00 occurs twice. A bound inside it is the first
    # occurrence, so 01:30 (05:30 UTC) to 03:00 (08:00 UTC) spans 2.5 h.
    fall = Date(2026, 11, 1)
    @test hours(fall, (1.5, 3.0)) == 2.5
    @test hours(fall, (0.0, 24.0)) == 25.0
    @test hours(fall, (9.5, 16.0)) == 6.5
    # a calendar without transitions
    @test MarketTickStreamer._window_ns(fall, (1.5, 3.0), tz"UTC") ==
          (utc(fall, 1) + 30 * 60 * 10^9, utc(fall, 3))
    # A calendar that sets its clock forward at midnight (Cuba, 2019-03-10):
    # the date starts at 01:00, and a day start is asked for every date.
    @test MarketTickStreamer._zoned(DateTime(2019, 3, 10), tz"America/Havana") ==
          ZonedDateTime(2019, 3, 10, 1, tz"America/Havana")
end

@testset "tee: a consumer that fails while the fan-out waits on it" begin
    # A channel bound to a consumer task that failed is closed with that
    # task's exception, and a `put!` waiting for room on it throws the
    # exception itself, not an `InvalidStateException`.
    src = Channel{Trade}(10)
    outs = tee(src, 2; capacity = 1)
    got = Trade[]
    reader = Threads.@spawn for t in outs[1]
        push!(got, t)
    end
    foreach(i -> put!(src, sample_trade(i)), 1:3)      # the second output fills; the fan-out waits
    @test timedwait(() -> length(got) >= 2, 10.0) === :ok
    close(outs[2], ErrorException("consumer died"))
    foreach(i -> put!(src, sample_trade(i)), 4:8)
    close(src)
    wait(reader)
    @test got == [sample_trade(i) for i in 1:8]         # the other output saw every print
    @test_throws DimensionMismatch tee(Channel{Trade}(1), 2; lossy = [true])
end
