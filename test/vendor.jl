@testset "itch50: executions priced off the book, crosses, odd lots, breaks" begin
    (; be, alpha, msg, R, A, F, E, C, X, D, U, P, Q, B, S, H) = ITCHBytes
    @test length(R(1, "AAPL", 'Q', 100)) == 41 && length(A(1, 0, 1, 'B', 1, "A", 1)) == 38
    @test length(E(1, 0, 1, 1, 1)) == 33 && length(Q(1, 0, 1, "A", 1, 1, 'O')) == 42
    # Market Category → tape: Nasdaq C, NYSE A, NYSE American and the other
    # exchanges B (as the SIP itself stamps them), unknown categories "".
    @test [MarketTickStreamer._itch50_tape(UInt8(c)) for c in "QGSNAPZVMX "] == ["C", "C", "C", "A", "B", "B", "B", "B", "B", "", ""]
    T0 = 34_200_000_000_000                                        # 09:30 ET in ns
    # The day starts at New York midnight: UTC-5 in winter, UTC-4 in summer.
    utc(dt) = Dates.value(dt - DateTime(1970)) * 1_000_000
    @test MarketTickStreamer._itch50_day_start_ns(Date(2025, 12, 9)) ==
          utc(DateTime(2025, 12, 9, 5))
    @test MarketTickStreamer._itch50_day_start_ns(Date(2025, 7, 1)) ==
          utc(DateTime(2025, 7, 1, 4))
    day0 = 1_577_682_000_000_000_000                               # 2019-12-30 05:00 UTC
    stream = vcat(
        S('O'),
        R(1, "AAPL", 'Q', 100),
        R(2, "IBM", 'N', 100),
        R(3, "SPY", 'P', 100),
        R(4, "ZZZ", 'Q', 0),
        H(1),
        A(1, T0, 10, 'B', 500, "AAPL", 2_900_000),
        F(1, T0, 11, 'S', 100, "AAPL", 2_901_000),
        E(1, T0 + 1, 10, 100, 1001),
        E(1, T0 + 2, 10, 50, 1002),
        X(1, T0 + 3, 10, 350),
        E(1, T0 + 4, 10, 10, 1003),                                # dead order: unresolved
        U(1, T0 + 5, 11, 12, 200, 2_899_500),
        C(1, T0 + 6, 12, 200, 1004, 'Y', 2_899_000),
        A(2, T0 + 7, 13, 'B', 300, "IBM", 1_300_000),
        C(2, T0 + 8, 13, 100, 1005, 'N', 1_300_500),               # non-printable
        P(2, T0 + 9, 75, "IBM", 1_300_250, 1006),
        Q(3, T0 + 10, 0, "SPY", 3_200_000, 1007, 'O'),             # zero shares
        Q(3, T0 + 11, 1_000_000, "SPY", 3_200_000, 1008, 'C'),
        B(1, T0 + 12, 1004),
        A(4, T0 + 13, 20, 'B', 50, "ZZZ", 100_000),
        E(4, T0 + 14, 20, 50, 1009),
        Q(1, T0 + 15, 10, "AAPL", 2_900_000, 1010, 'H'),
        D(1, T0 + 16, 12),
        S('C'),
    )
    mktempdir() do dir
        raw = joinpath(dir, "day.bin")
        write(raw, stream)
        gz = joinpath(dir, "day.gz")
        open(GzipCompressorStream, gz, "w") do io
            write(io, stream)
        end
        date = Date(2019, 12, 30)
        r = itch50_trades(raw, date; symbols = ["AAPL", "IBM", "SPY"])
        @test (r.messages, r.prints, r.broken, r.unresolved, r.symbols, r.truncated) ==
              (25, 5, 1, 1, 3, false)
        got = r.trades
        @test [t.id for t in got] == [1001, 1002, 1006, 1008, 1010]
        @test got[1] == Trade(
            "AAPL",
            day0 + T0 + 1,
            0,
            290.0,
            100.0,
            "Q",
            ["displayed", "sell"],
            "C",
            1001,
        )
        @test got[2].conditions == ["displayed", "sell", "I"] && got[2].size == 50.0
        @test got[3] ==
              Trade("IBM", day0 + T0 + 9, 0, 130.025, 75.0, "Q", ["hidden", "I"], "A", 1006)
        @test got[4] ==
              Trade("SPY", day0 + T0 + 11, 0, 320.0, 1.0e6, "Q", ["cross_close"], "B", 1008)
        @test got[5].conditions == ["cross_halt", "I"] && got[5].symbol == "AAPL"
        # gzip input decodes identically
        @test itch50_trades(gz, date; symbols = ["AAPL", "IBM", "SPY"]).trades == got
        # every symbol, and an unknown round lot never marks an odd lot
        all_r = itch50_trades(raw, date)
        @test [t.id for t in all_r.trades] == [1001, 1002, 1006, 1008, 1009, 1010]
        @test all_r.trades[5] == Trade(
            "ZZZ",
            day0 + T0 + 14,
            0,
            10.0,
            50.0,
            "Q",
            ["displayed", "sell"],
            "C",
            1009,
        )
        @test all_r.symbols == 4
        # broken prints: marked, or kept untouched in one pass
        m = itch50_trades(raw, date; symbols = ["AAPL"], broken = :mark)
        @test [t.id for t in m.trades] == [1001, 1002, 1004, 1010]
        @test m.trades[3].conditions == ["improved", "buy", "broken"] && m.broken == 1
        @test m.trades[3].price == 289.9 && m.trades[3].size == 200.0
        k = itch50_trades(raw, date; symbols = ["AAPL"], broken = :keep)
        @test k.trades[3].conditions == ["improved", "buy"] && k.broken == 1
        # pages stream through each_page; the returned trades are then empty
        pages = Vector{Trade}[]
        s = itch50_trades(
            raw,
            date;
            symbols = ["AAPL", "IBM", "SPY"],
            page_rows = 2,
            each_page = p -> push!(pages, copy(p)),
        )
        @test length.(pages) == [2, 2, 1] && isempty(s.trades) && s.prints == 5
        @test reduce(vcat, pages) == got
        # a file cut inside a message: what precedes it is read, and the cut is reported
        cut = joinpath(dir, "cut.bin")
        write(cut, stream[1:(end-30)])
        c = itch50_trades(cut, date; broken = :keep)
        @test c.truncated && c.prints == 7 && c.messages == 23
        # a file cut between two messages, before the End of Messages event
        # (the last 14 bytes): every print is there, and the file is incomplete
        full = itch50_trades(raw, date; broken = :keep)
        cut2 = joinpath(dir, "cut2.bin")
        write(cut2, stream[1:(end-14)])
        c2 = itch50_trades(cut2, date; broken = :keep)
        @test !full.truncated && c2.truncated
        @test c2.prints == full.prints && c2.messages == full.messages - 1
        # a gzip member cut short, as a partial download is: read to the cut, reported
        gzcut = joinpath(dir, "cut.gz")
        write(gzcut, read(gz)[1:(end-12)])
        gc = itch50_trades(gzcut, date; broken = :keep)
        @test gc.truncated && gc.messages <= 25
        @test_throws ArgumentError itch50_trades(raw, date; broken = :void)
        @test_throws ArgumentError itch50_trades(raw, date; page_rows = 0)
        # Breaks of two symbols, one selected: the count follows the
        # selection in every mode (one AAPL break, the IBM one excluded).
        two = joinpath(dir, "two.bin")
        write(
            two,
            vcat(
                R(1, "AAPL", 'Q', 100),
                R(2, "IBM", 'N', 100),
                A(1, T0, 10, 'B', 500, "AAPL", 2_900_000),
                E(1, T0 + 1, 10, 100, 2001),
                A(2, T0 + 2, 13, 'B', 300, "IBM", 1_300_000),
                E(2, T0 + 3, 13, 100, 2002),
                B(1, T0 + 4, 2001),
                B(2, T0 + 5, 2002),
            ),
        )
        @test itch50_trades(two, date; symbols = ["AAPL"], broken = :keep).broken == 1
        @test itch50_trades(two, date; symbols = ["AAPL"], broken = :mark).broken == 1
        # A Stock Directory message repeated for one locate counts the symbol once.
        rep = joinpath(dir, "rep.bin")
        write(rep, vcat(R(1, "AAPL", 'Q', 100), R(1, "AAPL", 'Q', 100), S('C')))
        @test itch50_trades(rep, date; symbols = ["AAPL"]).symbols == 1
        @test itch50_trades(rep, date).symbols == 1
        # A Broken Trade cut to type, locate and tracking (5 bytes) is
        # malformed, not a truncation: the length prefix frames it whole.
        short = joinpath(dir, "short.bin")
        write(short, vcat(stream, be(5, 2), UInt8('B'), be(1, 2), be(0, 2)))
        @test_throws IntegrityError itch50_trades(short, date)
        # Buffer boundaries: 33 messages of eight types, the longest the
        # 46-byte P (prefix included), so a 48-byte chunk holds every one.
        msgs = [
            S('O'),
            R(1, "AAPL", 'Q', 100),
            R(2, "IBM", 'N', 100),
            H(1),
            [A(1, T0 + i, 100 + i, 'B', 100, "AAPL", 2_900_000) for i in 1:6]...,
            [E(1, T0 + 10 + i, 100 + i, 50, 3000 + i) for i in 1:6]...,
            [P(2, T0 + 20 + i, 10, "IBM", 1_300_000, 4000 + i) for i in 1:6]...,
            [Q(1, T0 + 30 + i, 100, "AAPL", 2_900_000, 5000 + i, 'I') for i in 1:4]...,
            [D(1, T0 + 40 + i, 100 + i) for i in 1:6]...,
            S('C'),
        ]
        @test length(msgs) == 33 && maximum(length, msgs) == 46
        long = joinpath(dir, "long.bin")
        write(long, reduce(vcat, msgs))
        # (type byte, length) of every message handed over, and the count.
        collect_scan(chunk) = open(long) do io
            seen = Tuple{UInt8,Int}[]
            scan = MarketTickStreamer._itch50_scan(io; chunk) do buf, i, len
                push!(seen, (buf[i], len))
            end
            (seen, scan.messages)
        end
        ref_scan = collect_scan(8 << 20)
        @test ref_scan[2] == 33
        @test ref_scan[1] == [(x[3], length(x) - 2) for x in msgs]
        for c in (48, 50, 64, 67, 129)
            @test collect_scan(c) == ref_scan
        end
        @test_throws IntegrityError collect_scan(16)           # a message exceeds it
    end
    @test itch50_date("12302019.NASDAQ_ITCH50.gz") == Date(2019, 12, 30)
    @test itch50_date("/x/S112825-v50.txt.gz") == Date(2025, 11, 28)
    @test_throws ArgumentError itch50_date("itch50_05_15.gz")
end

@testset "iex hist: pcap and pcapng framing, IEX-TP sessions, trade reports and breaks" begin
    (;
        le,
        be,
        sym,
        S,
        Qm,
        T,
        B,
        segment,
        frame,
        pad4,
        shb,
        idb,
        epb,
        pcap_header,
        pcap_record,
    ) = IEXBytes
    t0 = 1_577_975_400_000_000_000                         # 2020-01-02 14:30 UTC
    seg_hb = segment(0x8003, 0, 1, []; send = -1)          # pre-session heartbeat
    seg1 = segment(0x8003, 7, 1, [S('O', t0), Qm("AAPL", t0 + 1)])
    seg2 = segment(
        0x8003,
        7,
        3,
        [
            T("AAPL", 0x00, t0 + 2, 100, 1_500_000, 1),
            T("AAPL", 0x20, t0 + 3, 7, 1_500_100, 2),
            T("MSFT", 0xc0, t0 + 4, 200, 3_000_000, 3),
        ],
    )
    seg3 = segment(
        0x8003,
        7,
        6,
        [B("AAPL", t0 + 5, 2), T("SPY", 0x08, t0 + 6, 1000, 4_000_000, 4)],
    )
    # gap: 8 expected
    seg4 = segment(0x8003, 7, 10, [T("AAPL", 0x10, t0 + 7, 50, 1_500_200, 5)])
    ng = vcat(
        shb(),
        idb(),
        epb(frame(seg_hb)),
        epb(frame(seg1)),
        epb(frame(seg2)),
        epb(frame(seg3)),
        epb(frame(seg3)),                                   # seg3 twice: a duplicate
        epb(frame(seg4)),
    )
    mktempdir() do dir
        f = joinpath(dir, "day.pcapng")
        write(f, ng)
        r = iex_hist_trades(f; symbols = ["AAPL", "SPY"])
        @test r.feed == "TOPS1.6"
        @test (
            r.packets,
            r.segments,
            r.messages,
            r.prints,
            r.broken,
            r.gaps,
            r.duplicates,
            r.truncated,
        ) == (6, 4, 8, 3, 1, 1, 1, false)
        @test r.trades == [
            Trade("AAPL", t0 + 2, 0, 150.0, 100.0, "V", String[], "", 1),
            Trade("SPY", t0 + 6, 0, 400.0, 1000.0, "V", ["X"], "", 4),
            Trade("AAPL", t0 + 7, 0, 150.02, 50.0, "V", ["8"], "", 5),
        ]
        all_r = iex_hist_trades(f; tape = Dict("MSFT" => "C"))
        @test [t.id for t in all_r.trades] == [1, 3, 4, 5]
        @test all_r.trades[2] ==
              Trade("MSFT", t0 + 4, 0, 300.0, 200.0, "V", ["F", "T"], "C", 3)
        m = iex_hist_trades(f; symbols = ["AAPL"], broken = :mark, tape = "C")
        @test [t.id for t in m.trades] == [1, 2, 5] && m.trades[2].conditions == ["I", "broken"]
        @test all(t.tape == "C" for t in m.trades)
        k = iex_hist_trades(f; symbols = ["AAPL"], broken = :keep)
        @test [t.id for t in k.trades] == [1, 2, 5] &&
              k.trades[2].conditions == ["I"] &&
              k.broken == 1
        # A Trade Break for MSFT as well: with AAPL selected, :keep still
        # counts the one AAPL break only.
        seg5 = segment(0x8003, 7, 11, [B("MSFT", t0 + 8, 3)])
        f2 = joinpath(dir, "two_breaks.pcapng")
        write(f2, vcat(ng, epb(frame(seg5))))
        @test iex_hist_trades(f2; symbols = ["AAPL"], broken = :keep).broken == 1
        # A protocol the reader does not know (DEEP+ is 0x8005) is skipped and
        # reported, not read as an empty capture without a word.
        f3 = joinpath(dir, "unknown.pcapng")
        write(
            f3,
            vcat(
                shb(),
                idb(),
                epb(frame(segment(0x8005, 7, 1, [T("AAPL", 0, t0, 100, 1_000_000, 1)]))),
            ),
        )
        got3 = @test_logs (:warn, r"another protocol") iex_hist_trades(f3)
        @test got3.prints == 0 && got3.feed == ""
        # A segment whose payload ends before its message count: the missing
        # message is counted as a gap, though the sequence numbers agree.
        short = segment(0x8003, 7, 1, [T("AAPL", 0, t0, 100, 1_000_000, 1)])
        short[15:16] .= le(2, 2)                     # claims two messages
        # the next segment follows the claimed count: no second gap
        after = segment(0x8003, 7, 3, [T("AAPL", 0, t0 + 1, 100, 1_000_000, 2)])
        f4 = joinpath(dir, "short.pcapng")
        write(f4, vcat(shb(), idb(), epb(frame(short)), epb(frame(after))))
        got4 = iex_hist_trades(f4)
        @test got4.prints == 2 && got4.messages == 2 && got4.gaps == 1
        # Buffer boundaries: the frames handed over do not depend on the
        # chunk, down to the largest block of the capture (header included).
        frames = [frame(s) for s in (seg_hb, seg1, seg2, seg3, seg3, seg4)]
        blocks = vcat([shb(), idb()], epb.(frames))
        @test reduce(vcat, blocks) == ng
        largest = maximum(length, blocks)
        packet_scan(chunk) = open(f) do io
            lens = Int[]
            scan = MarketTickStreamer._iex_packets(io; chunk) do buf, i, n
                push!(lens, n)
            end
            (lens, scan.packets)
        end
        @test packet_scan(8 << 20) == (length.(frames), 6)
        for c in (largest, largest + 5, 2 * largest - 1)
            @test packet_scan(c) == (length.(frames), 6)
        end
        # gzip, as published
        gz = joinpath(dir, "day.pcap.gz")
        open(GzipCompressorStream, gz, "w") do io
            write(io, ng)
        end
        @test iex_hist_trades(gz; symbols = ["AAPL", "SPY"]).trades == r.trades
        # pages
        pages = Vector{Trade}[]
        s = iex_hist_trades(f; page_rows = 3, each_page = p -> push!(pages, copy(p)))
        @test length.(pages) == [3, 1] && isempty(s.trades) && s.prints == 4
        # a capture cut inside a packet block
        cut = joinpath(dir, "cut.pcapng")
        write(cut, ng[1:(end-40)])
        c = iex_hist_trades(cut; broken = :keep)
        @test c.truncated && c.prints == 4 && c.packets == 5
        # classic pcap, 802.1Q-tagged frames, TOPS 1.5 with its reserved bytes
        seg15 = segment(
            0x8002,
            9,
            1,
            [T("ZIEXT", 0x00, t0, 100, 990_500, 429_974; reserved = 4)],
        )
        classic = vcat(
            pcap_header(),
            pcap_record(frame(seg15; vlan = true)),
            pcap_record(frame(seg_hb)),
        )
        p = joinpath(dir, "day.pcap")
        write(p, classic)
        c15 = iex_hist_trades(p)
        @test c15.feed == "TOPS1.5" && c15.packets == 2 && c15.segments == 1
        @test c15.trades ==
              [Trade("ZIEXT", t0, 0, 99.05, 100.0, "V", String[], "", 429_974)]
        # the TOPS specification's own Trade Report example, byte for byte
        golden = UInt8[
            0x54,
            0x00,
            0xc3,
            0xdf,
            0xf7,
            0x05,
            0xa2,
            0x86,
            0x6d,
            0x14,
            0x5a,
            0x49,
            0x45,
            0x58,
            0x54,
            0x20,
            0x20,
            0x20,
            0x64,
            0x00,
            0x00,
            0x00,
            0x24,
            0x1d,
            0x0f,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x96,
            0x8f,
            0x06,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ]
        g = joinpath(dir, "golden.pcapng")
        write(g, vcat(shb(), idb(), epb(frame(segment(0x8003, 1, 1, [golden])))))
        gt = only(iex_hist_trades(g).trades)
        @test gt.symbol == "ZIEXT" && gt.time_ns == 1_471_980_683_662_974_915
        @test gt.price == 99.05 &&
              gt.size == 100.0 &&
              gt.id == 429_974 &&
              isempty(gt.conditions)
        @test_throws ArgumentError iex_hist_trades(f; tape = 3)
        @test_throws ArgumentError iex_hist_trades(f; broken = :void)
        # not a capture at all
        bad = joinpath(dir, "bad.bin")
        write(bad, zeros(UInt8, 64))
        @test_throws IntegrityError iex_hist_trades(bad)
    end
end

@testset "vendor readers on published bytes" begin
    # Fixtures, generators and provenance: test/fixtures/PROVENANCE.md. The
    # committed IEX bytes are checked against facts of the day that do not
    # come from the reader (session hours, AAPL's 100-share round lot); where
    # the local SIP reference exists, every print is also checked against the
    # consolidated tape's record of the same execution: price and size equal,
    # the SIP's stamp after the venue's, the SIP's T, I, O and 6 flags.
    fixtures = joinpath(@__DIR__, "fixtures")
    reference = joinpath(fixtures, "sip_AAPL_20251209.toml")
    sip = isfile(reference) ? TOML.parsefile(reference) : nothing
    ns(dt) = Dates.value(dt - DateTime(1970)) * 1_000_000
    regular = ns(DateTime(2025, 12, 9, 14, 30)):(ns(DateTime(2025, 12, 9, 21))-1)
    function against_sip(trades, rows, bound)
        @test length(rows["price"]) == length(trades)
        for (k, t) in enumerate(trades)
            f = rows["flags"][k]
            @test t.price == rows["price"][k] && t.size == rows["size"][k]
            @test 0 < rows["time_ns"][k] - t.time_ns < bound
            @test ("I" in t.conditions) == occursin('I', f)
            @test ("cross_open" in t.conditions) == occursin('O', f)
            @test ("cross_close" in t.conditions) == occursin('6', f)
        end
    end

    q = iex_hist_trades(joinpath(fixtures, "iex_AAPL_20251209.pcapng"); symbols = ["AAPL"])
    @test (q.prints, q.packets, q.segments) == (10, 10, 10)
    @test q.gaps == 4                     # the excerpt skips the frames between
    @test all(t -> t.symbol == "AAPL" && t.exchange == "V", q.trades)
    @test issorted(map(t -> t.time_ns, q.trades)) && allunique(map(t -> t.id, q.trades))
    for t in q.trades
        @test ("T" in t.conditions) == !(t.time_ns in regular)
        @test ("I" in t.conditions) == (t.size < 100)
    end
    # Imported, the excerpt's sequence gaps mark the day, and a recompaction
    # of the import's raw parts keeps the marker.
    mktempdir() do dir
        p = joinpath(dir, "c.toml")
        write(
            p,
            "[stream]\nsymbols = [\"AAPL\"]\n[storage]\ndata_dir = \"data\"\nprocessed_format = \"csv\"\n[logging]\nlog_to_file = false\n",
        )
        cfg = load_config(p)
        files = run_import(
            cfg,
            joinpath(fixtures, "iex_AAPL_20251209.pcapng");
            format = :iex,
            date = Date(2025, 12, 9),
        )
        @test length(files) == 1
        marker = only(files) * ".incomplete.toml"
        @test occursin("4 sequence gaps", TOML.parsefile(marker)["reason"])
        compact_raw(cfg, filter(endswith(".jsonl"), readdir(cfg.raw_dir; join = true)))
        @test occursin("4 sequence gaps", TOML.parsefile(marker)["reason"])
    end
    if sip === nothing
        @test_skip "local SIP reference not present"
    else
        against_sip(q.trades, sip["iex"], 1_000_000)
        @test all(
            k -> ("T" in q.trades[k].conditions) == occursin('T', sip["iex"]["flags"][k]),
            1:10,
        )
    end

    itch = joinpath(fixtures, "itch50_AAPL_20251209.bin")
    if isfile(itch)
        r = itch50_trades(itch, Date(2025, 12, 9); symbols = ["AAPL"])
        @test (r.messages, r.prints, r.unresolved, r.symbols, r.truncated) ==
              (37, 22, 0, 1, false)
        @test all(t -> t.tape == "C" && t.exchange == "Q", r.trades)
        crosses = filter(t -> startswith(t.conditions[1], "cross"), r.trades)
        @test [c.conditions[1] for c in crosses] == ["cross_open", "cross_close"]
        @test crosses[1].time_ns - regular[1] in 0:60_000_000_000
        @test crosses[2].time_ns - (regular[end] + 1) in 0:60_000_000_000
        for t in r.trades
            t in crosses || @test ("I" in t.conditions) == (t.size < 100)
        end
        sip === nothing || haskey(sip, "itch") && against_sip(r.trades, sip["itch"], 25_000)
    else
        # Nasdaq's sample files carry no licence; the excerpt is cut locally
        # by test/fixtures/extract_itch50.jl.
        @test_skip "ITCH 5.0 golden excerpt not present"
    end
end

@testset "fetch: resumable Range downloads, announced sizes, CRC-32C" begin
    payload = rand(Xoshiro(3), UInt8, 3 * 1024 * 1024 + 123)
    crc = string(crc32c(payload); base = 16, pad = 8)
    crc_b64 = base64encode([UInt8((crc32c(payload) >> (8 * (3 - k))) & 0xff) for k in 0:3])
    ranges = String[]
    ranges_lock = ReentrantLock()
    port = freeport(8981)
    router = HTTP.Router()
    HTTP.register!(
        router,
        "HEAD",
        "/f/{name}",
        req -> HTTP.Response(
            200,
            [
                "Content-Length" => string(length(payload)),
                "x-goog-hash" => "crc32c=$crc_b64,md5=AAAA",
            ],
        ),
    )
    HTTP.register!(
        router,
        "GET",
        "/f/{name}",
        function (req)
            name = HTTP.getparams(req)["name"]
            name == "missing.gz" && return HTTP.Response(404, "Not Found")
            r = HTTP.header(req, "Range", "")
            lock(() -> push!(ranges, r), ranges_lock)
            # a host that fails for good past 2 MiB
            name == "failing.gz" &&
                parse(Int, split(split(r, '=')[2], '-')[1]) >= 2 << 20 &&
                return HTTP.Response(403, "Forbidden")
            # a host that answers a range without saying which
            name == "norange.gz" && return HTTP.Response(206, payload[1:1000])
            # a host that ignores the requested start: the client must not splice
            name == "misaligned.gz" && return HTTP.Response(
                206,
                ["Content-Range" => "bytes 0-$(length(payload)-1)/$(length(payload))"],
                payload,
            )
            if isempty(r)
                return HTTP.Response(
                    200,
                    ["x-goog-hash" => "crc32c=$crc_b64,md5=AAAA"],
                    payload,
                )
            end
            lo, hi = split(split(r, '=')[2], '-')
            from = parse(Int, lo)
            from >= length(payload) && return HTTP.Response(
                416,
                ["Content-Range" => "bytes */$(length(payload))"],
            )
            to =
                isempty(hi) ? length(payload) - 1 :
                min(parse(Int, hi), length(payload) - 1)
            return HTTP.Response(
                206,
                [
                    "Content-Range" => "bytes $from-$to/$(length(payload))",
                    "x-goog-hash" => "crc32c=$crc_b64,md5=AAAA",
                ],
                payload[(from+1):(to+1)],
            )
        end,
    )
    HTTP.register!(
        router,
        "GET",
        "/api/1.0/hist",
        function (req)
            d = get(HTTP.queryparams(HTTP.URI(req.target)), "date", "")
            d == "20200102" || return HTTP.Response(404, "Not Found")
            return HTTP.Response(
                200,
                JSON.json([
                    Dict(
                        "link" => "http://127.0.0.1:$port/f/20200102_IEXTP1_DEEP1.0.pcap.gz",
                        "date" => d,
                        "feed" => "DEEP",
                        "version" => "1.0",
                        "protocol" => "IEXTP1",
                        "size" => string(length(payload)),
                    ),
                    Dict(
                        "link" => "http://127.0.0.1:$port/f/20200102_IEXTP1_TOPS1.6.pcap.gz",
                        "date" => d,
                        "feed" => "TOPS",
                        "version" => "1.6",
                        "protocol" => "IEXTP1",
                        "size" => string(length(payload)),
                    ),
                ]),
            )
        end,
    )
    server = HTTP.serve!(router, "127.0.0.1", port; verbose = false)
    policy = MarketTickStreamer.RestPolicy(10, 5, 1)
    api = "http://127.0.0.1:$port/api/1.0"
    try
        mktempdir() do dir
            # ITCH: a partial file is resumed with a Range request and the size checked
            url = "http://127.0.0.1:$port/f/12302019.NASDAQ_ITCH50.gz"
            partial = joinpath(dir, "itch50", "12302019.NASDAQ_ITCH50.gz.partial")
            mkpath(dirname(partial))
            write(partial, payload[1:1_000_000])
            p = fetch_itch50(url, dir; policy)
            @test p == joinpath(dir, "itch50", "12302019.NASDAQ_ITCH50.gz")
            @test read(p) == payload && !isfile(partial)
            @test ranges == ["bytes=1000000-9388607"]
            @test fetch_itch50(url, dir; policy) == p && ranges == ["bytes=1000000-9388607"]   # reused
            @test fetch_itch50("http://127.0.0.1:$port/f/missing.gz", dir; policy) ===
                  nothing
            # IEX: index, feed selection, download, CRC-32C
            idx = iex_hist_index(Date(2020, 1, 2); api_base = api, policy)
            @test [e["feed"] for e in idx] == ["DEEP", "TOPS"]
            @test iex_hist_index(Date(2020, 1, 3); api_base = api, policy) === nothing
            empty!(ranges)
            q = fetch_iex_hist(Date(2020, 1, 2), dir; feed = "tops", api_base = api, policy)
            @test q == joinpath(dir, "iex", "20200102_IEXTP1_TOPS1.6.pcap.gz") &&
                  read(q) == payload
            @test ranges == ["bytes=0-8388607"]
            @test fetch_iex_hist(
                Date(2020, 1, 2),
                dir;
                feed = "TOPS",
                api_base = api,
                policy,
            ) == q
            @test ranges == ["bytes=0-8388607"]              # verified copy reused
            @test fetch_iex_hist(Date(2020, 1, 3), dir; api_base = api, policy) === nothing
            @test_throws VendorError fetch_iex_hist(
                Date(2020, 1, 2),
                dir;
                feed = "DPLS",
                api_base = api,
                policy,
            )
            # a corrupted copy of the right size is re-downloaded
            bad = copy(payload)
            bad[1] ⊻= 0xff
            write(q, bad)
            @test fetch_iex_hist(Date(2020, 1, 2), dir; api_base = api, policy) == q
            @test read(q) == payload
            # A transfer resumed from a complete partial file meets only a 416,
            # which carries no checksum: the host is asked for it, and a
            # corrupted partial of the right size is caught and removed.
            deep = joinpath(dir, "iex", "20200102_IEXTP1_DEEP1.0.pcap.gz")
            write(deep * ".partial", bad)
            @test_throws IntegrityError fetch_iex_hist(
                Date(2020, 1, 2),
                dir;
                feed = "deep",
                api_base = api,
                policy,
            )
            @test !isfile(deep)
            # chunked transfer: one bounded request per chunk, the last one clipped
            empty!(ranges)
            chunked = joinpath(dir, "chunked.bin")
            r3 = MarketTickStreamer._download_resumable(
                url,
                chunked,
                policy;
                chunk_bytes = 1 << 20,
            )
            @test r3.found && read(chunked) == payload
            @test ranges == [
                "bytes=0-1048575",
                "bytes=1048576-2097151",
                "bytes=2097152-3145727",
                "bytes=3145728-4194303",
            ]
            # a 206 whose range does not start where the partial ends is refused
            mis = joinpath(dir, "misaligned.bin")
            write(mis * ".partial", payload[1:1_000_000])
            @test_throws VendorError MarketTickStreamer._download_resumable(
                "http://127.0.0.1:$port/f/misaligned.gz",
                mis,
                policy,
            )
            @test filesize(mis * ".partial") == 1_000_000      # untouched, for the next attempt
            # and so is a 206 that names no range at all, with or without a
            # partial file to continue
            nr = joinpath(dir, "norange.bin")
            norange = "http://127.0.0.1:$port/f/norange.gz"
            @test_throws "refusing to splice" MarketTickStreamer._download_resumable(
                norange,
                nr,
                policy,
            )
            write(nr * ".partial", payload[1:500])
            @test_throws "refusing to splice" MarketTickStreamer._download_resumable(
                norange,
                nr,
                policy,
            )
            @test filesize(nr * ".partial") == 500 && !isfile(nr)
            @test MarketTickStreamer._content_range_start(
                "bytes 1000000-2097151/3145851",
            ) == 1_000_000
            @test MarketTickStreamer._content_range_total("bytes 0-9/3145851") == 3145851
            @test MarketTickStreamer._content_range_total("") == -1
            # the policy's 10 s, plus 8 MiB at 50 kB/s (167.8 s) with a quarter
            # added: 10 + ceil(209.7)
            @test MarketTickStreamer._chunk_timeout(policy, 8 << 20) == 10 + 210
            # a download cap paces the body: 3.1 MB at 4 MB/s takes at least
            # 0.79 s (a local transfer otherwise takes milliseconds), and a
            # cap below 50 kB/s stretches the chunk timeout to match
            capped = MarketTickStreamer.RestPolicy(10, 5, 1; max_download_rate = 4e6)
            t = @elapsed r4 = MarketTickStreamer._download_resumable(
                url,
                joinpath(dir, "capped.bin"),
                capped,
            )
            @test r4.found && read(joinpath(dir, "capped.bin")) == payload
            @test t >= length(payload) / 4e6
            # several connections: the first chunk alone (it names the
            # length), the rest claimed concurrently and appended in order
            par = MarketTickStreamer.RestPolicy(10, 5, 1; download_connections = 3)
            empty!(ranges)
            pp = joinpath(dir, "parallel.bin")
            r5 = MarketTickStreamer._download_resumable(url, pp, par; chunk_bytes = 1 << 20)
            @test r5.found && read(pp) == payload && !isfile(pp * ".partial")
            @test ranges[1] == "bytes=0-1048575"
            @test sort(ranges[2:end]) == [
                "bytes=1048576-2097151",
                "bytes=2097152-3145727",
                "bytes=3145728-3145850",
            ]
            # a partial file left by a sequential transfer resumes in parallel
            pr = joinpath(dir, "parallel-resumed.bin")
            write(pr * ".partial", payload[1:1_000_000])
            empty!(ranges)
            @test MarketTickStreamer._download_resumable(
                url,
                pr,
                par;
                chunk_bytes = 1 << 20,
            ).found && read(pr) == payload
            @test ranges[1] == "bytes=1000000-2048575" && length(ranges) == 3
            # a transfer reports its progress every `progress_s` seconds
            @test_logs (:info, "downloading") match_mode = :any MarketTickStreamer._download_resumable(
                url,
                joinpath(dir, "progress.bin"),
                par;
                chunk_bytes = 1 << 20,
                progress_s = 0,
            )
            # the cap bounds the sum of the connections
            cappar = MarketTickStreamer.RestPolicy(
                10,
                5,
                1;
                max_download_rate = 4e6,
                download_connections = 2,
            )
            t = @elapsed MarketTickStreamer._download_resumable(
                url,
                joinpath(dir, "capped-parallel.bin"),
                cappar;
                chunk_bytes = 1 << 20,
            )
            @test read(joinpath(dir, "capped-parallel.bin")) == payload
            @test t >= length(payload) / 4e6
            # a connection that fails for good ends the transfer; what was
            # appended is a prefix of the file, whole chunks only
            pf = joinpath(dir, "failing.bin")
            @test_throws VendorError MarketTickStreamer._download_resumable(
                "http://127.0.0.1:$port/f/failing.gz",
                pf,
                par;
                chunk_bytes = 1 << 20,
            )
            kept = read(pf * ".partial")
            @test length(kept) in (1 << 20, 2 << 20) &&
                  kept == payload[1:length(kept)] &&
                  !isfile(pf)
            # a connection that breaks mid-chunk resumes from its last byte
            dropped = Ref(false)
            drop_ranges = String[]
            drop_port = freeport(8991)
            drop_server = HTTP.serve!(
                "127.0.0.1",
                drop_port;
                stream = true,
                verbose = false,
            ) do http::HTTP.Stream
                r = HTTP.header(http.message, "Range", "")
                lock(() -> push!(drop_ranges, r), ranges_lock)
                lo, hi = split(split(r, '=')[2], '-')
                from, to = parse(Int, lo), min(parse(Int, hi), length(payload) - 1)
                body = payload[(from+1):(to+1)]
                HTTP.setstatus(http, 206)
                HTTP.setheader(
                    http,
                    "Content-Range" => "bytes $from-$to/$(length(payload))",
                )
                HTTP.setheader(http, "Content-Length" => string(length(body)))
                HTTP.startwrite(http)
                if from == 2 << 20 && !dropped[]
                    dropped[] = true
                    write(http, body[1:(length(body)÷2)])
                    close(http.stream)
                else
                    write(http, body)
                end
            end
            try
                pd = joinpath(dir, "dropped.bin")
                @test MarketTickStreamer._download_resumable(
                    "http://127.0.0.1:$drop_port/f/dropped.gz",
                    pd,
                    MarketTickStreamer.RestPolicy(10, 5, 2; download_connections = 3);
                    chunk_bytes = 1 << 20,
                ).found && read(pd) == payload
                @test dropped[] && "bytes=2621440-3145727" in drop_ranges
            finally
                close(drop_server)
            end
            # A connection that breaks after a chunk's last byte (a chunked answer
            # without its end): the retry has nothing left to ask for, and no
            # second request goes out.
            tail_hits = Threads.Atomic{Int}(0)
            tail_port = freeport(8995)
            tail_server = HTTP.serve!(
                "127.0.0.1",
                tail_port;
                stream = true,
                verbose = false,
            ) do http::HTTP.Stream
                Threads.atomic_add!(tail_hits, 1)
                lo, hi =
                    split(split(HTTP.header(http.message, "Range", ""), '=')[2], '-')
                from, to = parse(Int, lo), parse(Int, hi)
                HTTP.setstatus(http, 206)
                HTTP.setheader(
                    http,
                    "Content-Range" => "bytes $from-$to/$(length(payload))",
                )
                HTTP.startwrite(http)
                write(http, payload[(from+1):(to+1)])
                close(http.stream)
            end
            try
                chunk = MarketTickStreamer._fetch_range(
                    "http://127.0.0.1:$tail_port/f/tail.gz",
                    Int64(0),
                    Int64(999),
                    MarketTickStreamer.RestPolicy(10, 5, 2),
                    Inf,
                )
                @test chunk == payload[1:1000] && tail_hits[] == 1
            finally
                close(tail_server)
            end
            # A 404 whose connection breaks before the answer ends is retried,
            # and the retry's answer decides: the file is there after all.
            flaky_hits = Threads.Atomic{Int}(0)
            flaky_port = freeport(8997)
            flaky_server = HTTP.serve!(
                "127.0.0.1",
                flaky_port;
                stream = true,
                verbose = false,
            ) do http::HTTP.Stream
                if Threads.atomic_add!(flaky_hits, 1) == 0
                    HTTP.setstatus(http, 404)
                    HTTP.startwrite(http)
                    write(http, "not yet")
                    close(http.stream)
                    return nothing
                end
                lo, hi =
                    split(split(HTTP.header(http.message, "Range", ""), '=')[2], '-')
                from, to = parse(Int, lo), min(parse(Int, hi), length(payload) - 1)
                if from >= length(payload)
                    HTTP.setstatus(http, 416)
                    HTTP.startwrite(http)
                    return nothing
                end
                body = payload[(from+1):(to+1)]
                HTTP.setstatus(http, 206)
                HTTP.setheader(
                    http,
                    "Content-Range" => "bytes $from-$to/$(length(payload))",
                )
                HTTP.setheader(http, "Content-Length" => string(length(body)))
                HTTP.startwrite(http)
                write(http, body)
                return nothing
            end
            try
                pf2 = joinpath(dir, "flaky.bin")
                r = MarketTickStreamer._download_resumable(
                    "http://127.0.0.1:$flaky_port/f/flaky.gz",
                    pf2,
                    MarketTickStreamer.RestPolicy(10, 5, 2);
                    chunk_bytes = 1 << 20,
                )
                @test r.found && read(pf2) == payload
            finally
                close(flaky_server)
            end
            slow = MarketTickStreamer.RestPolicy(10, 5, 1; max_download_rate = 10_000)
            # 8 MiB at the 10 kB/s cap is 838.9 s: 10 + ceil(1.25 × 838.9)
            @test MarketTickStreamer._chunk_timeout(slow, 8 << 20) == 10 + 1049
            @test MarketTickStreamer._crc32c_from_headers([
                "X-Goog-Hash" => "crc32c=$crc_b64,md5=AAAA",
            ]) == crc
            @test MarketTickStreamer._crc32c_from_headers(["Content-Type" => "x"]) ===
                  nothing
        end
    finally
        close(server)
    end
end

@testset "import: a vendor day through sink, compaction and sidecar" begin
    msg = ITCHBytes.msg
    E = ITCHBytes.E
    R(loc, stock) = ITCHBytes.R(loc, stock, 'Q', 100)
    A(loc, ts, ref, sh, px) = ITCHBytes.A(loc, ts, ref, 'S', sh, "", px)
    T0 = 34_200_000_000_000
    stream = vcat(
        msg('S', 0, 0, [UInt8('O')]),
        R(1, "AAPL"),
        R(2, "MSFT"),
        A(1, T0, 1, 1000, 2_900_000),
        A(2, T0, 2, 1000, 1_600_000),
        [E(1, T0 + k, 1, 100, 100 + k) for k in 1:3]...,
        [E(2, T0 + k, 2, 100, 200 + k) for k in 1:2]...,
        msg('S', 0, 0, [UInt8('C')]),
    )
    mktempdir() do dir
        file = joinpath(dir, "12302019.NASDAQ_ITCH50.gz")
        open(GzipCompressorStream, file, "w") do io
            write(io, stream)
        end
        p = joinpath(dir, "c.toml")
        write(
            p,
            "[stream]\nsymbols = [\"AAPL\", \"MSFT\"]\n[storage]\ndata_dir = \"data\"\nprocessed_format = \"csv\"\n[logging]\nlog_to_file = false\n",
        )
        cfg = load_config(p)
        files = run_import(cfg, file; format = :itch50)
        @test length(files) == 2
        @test sort(basename.(dirname.(files))) == ["AAPL", "MSFT"]
        @test all(endswith("2019-12-30.csv"), files)
        @test length(collect(replay_source(files; pace = :max))) == 5
        metas = filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))
        @test length(metas) == 1
        meta = TOML.parsefile(only(metas))
        @test startswith(meta["session"]["id"], "import_itch50_")
        @test meta["session"]["status"] == "completed"
        @test meta["session"]["ticks"] == 5 && meta["session"]["ticks_processed"] == 5
        src = meta["session"]["source"]
        @test src["file"] == basename(file) && src["format"] == "itch50"
        @test src["bytes"] == filesize(file) && length(src["sha256"]) == 64
        @test src["messages"] == 11 && src["prints"] == 5 && src["truncated"] == false
        # The same day without its End of Messages event, in a tree of its
        # own: imported, and marked incomplete; a recompaction of its raw
        # parts keeps the marker; the whole file needs `replace`, as for any
        # day in the tree, and clears the marker.
        cut_file = joinpath(dir, "cut", "12302019.NASDAQ_ITCH50.gz")
        mkpath(dirname(cut_file))
        open(GzipCompressorStream, cut_file, "w") do io
            write(io, stream[1:(end-14)])           # the last message is S('C')
        end
        pc = joinpath(dirname(cut_file), "c.toml")
        write(
            pc,
            "[stream]\nsymbols = [\"AAPL\", \"MSFT\"]\n[storage]\ndata_dir = \"data\"\nprocessed_format = \"csv\"\n[logging]\nlog_to_file = false\n",
        )
        cfg_cut = load_config(pc)
        cut_files = run_import(cfg_cut, cut_file; format = :itch50)
        @test all(endswith("2019-12-30.csv"), cut_files) && length(cut_files) == 2
        aapl = joinpath(cfg_cut.processed_dir, "AAPL")
        @test isfile(joinpath(aapl, "2019-12-30.csv.incomplete.toml"))
        @test length(processed_files(cfg_cut.processed_dir, "AAPL")) == 1
        @test isempty(processed_files(cfg_cut.processed_dir, "AAPL"; complete = true))
        compact_raw(
            cfg_cut,
            filter(endswith(".jsonl"), readdir(cfg_cut.raw_dir; join = true)),
        )
        @test isfile(joinpath(aapl, "2019-12-30.csv.incomplete.toml"))
        whole = joinpath(dirname(cut_file), "whole", "12302019.NASDAQ_ITCH50.gz")
        mkpath(dirname(whole))
        cp(file, whole)
        @test_throws ArgumentError run_import(cfg_cut, whole; format = :itch50)
        run_import(cfg_cut, whole; format = :itch50, replace = true)
        @test !isfile(joinpath(aapl, "2019-12-30.csv.incomplete.toml"))
        @test length(processed_files(cfg_cut.processed_dir, "AAPL"; complete = true)) == 1
        @test length(meta["session"]["raw_files"]) == 1
        # a second import of one symbol without the raw copy: processed again
        # (safesave keeps the first), raw parts gone, the sidecar lists none.
        # the day exists: refused unless the displacement is explicit
        @test_throws ArgumentError run_import(
            cfg,
            file;
            format = :itch50,
            symbols = ["AAPL"],
        )
        # The readers select symbols case-insensitively, so the collision
        # check does as well.
        @test_throws ArgumentError run_import(
            cfg,
            file;
            format = :itch50,
            symbols = ["aapl"],
        )
        # Arguments are checked before the session exists: nothing is left behind.
        raw_before = readdir(cfg.raw_dir)
        @test_throws "broken must be" run_import(
            cfg,
            file;
            format = :itch50,
            broken = :discard,
        )
        @test_throws "tape must be" run_import(cfg, file; format = :iex, tape = 1)
        @test readdir(cfg.raw_dir) == raw_before
        # An import files prints on the New York calendar and refuses another one.
        utc_dir = joinpath(dir, "utc")
        mkpath(utc_dir)
        cfg_utc = load_config(mock_binance_config_toml(utc_dir; ws_port = 1, rest_port = 1))
        @test_throws ArgumentError run_import(cfg_utc, file; format = :itch50)
        files2 = run_import(
            cfg,
            file;
            format = :itch50,
            symbols = ["AAPL"],
            keep_raw = false,
            replace = true,
        )
        @test length(files2) == 1
        raws = filter(endswith(".jsonl"), readdir(cfg.raw_dir; join = true))
        @test length(raws) == 1                       # the first session's part only
        metas = filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))
        @test length(metas) == 2
        @test_throws ArgumentError run_import(cfg, file; format = :csv)
        # an IEX file whose name carries no date needs `date`
        anon = joinpath(dir, "capture.pcap.gz")
        write(anon, zeros(UInt8, 16))
        @test_throws ArgumentError run_import(cfg, anon; format = :iex)
        @test iex_hist_date("20200102_IEXTP1_TOPS1.6.pcap.gz") == Date(2020, 1, 2)
        @test_throws ArgumentError iex_hist_date("capture.pcap.gz")
        @test_throws ArgumentError run_import(
            cfg,
            joinpath(dir, "missing.gz");
            format = :iex,
        )
    end
end
