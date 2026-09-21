using Test
using AllocCheck
using Aqua
using ExplicitImports
using JET
using OnlineStats: OnlineStats
using Dates
using TimeZones
using CSV
using DataFrames
using TOML
using MarketTickStreamer

include("mock_alpaca.jl")
include("mock_binance.jl")

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

@testset "MarketTickStreamer" begin

    @testset "static QA (Aqua)" begin
        Aqua.test_all(MarketTickStreamer)
    end

    @testset "static QA (ExplicitImports)" begin
        # The owner and public-access checks are deliberately not asserted:
        # a few stable non-public names are load-bearing here
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

    @testset "static QA (allocation-free hot paths)" begin
        # These three run once per print on the ingest and compaction paths.
        # Zero allocations is the measured state, asserted here so an
        # accidental boxing or a dynamic dispatch shows up as a test failure.
        @test isempty(check_allocs(MarketTickStreamer.ns_to_datetime, (Int64,)))
        @test isempty(check_allocs(MarketTickStreamer.now_ns, ()))
        @test isempty(check_allocs(MarketTickStreamer.trading_date, (Int64,)))
        # `rfc3339_to_ns` and `ns_to_rfc3339` are deliberately NOT asserted:
        # the first matches a regular expression and validates through
        # `DateTime`, the second builds a String. At 438 ns and 304 ns against
        # network-bound ingestion, hand-rolling either to reach zero is not
        # justified by measurement — see bench/benchmarks.jl.
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
        # The last nanosecond of an exchange day must file under that day.
        # Routing through `ns_to_datetime` divides by 1e9 in floating point and
        # rounds to the millisecond, which pushed this instant into the next
        # date; `trading_date` floors in integer arithmetic instead.
        @test trading_date(rfc3339_to_ns("2026-07-31T03:59:59.999999999Z")) ==
              Date(2026, 7, 30)                      # 23:59:59.999999999 EDT
        @test trading_date(rfc3339_to_ns("2026-07-31T04:00:00Z")) == Date(2026, 7, 31)
        @test trading_date(rfc3339_to_ns("2025-11-04T04:59:59.999999999Z")) ==
              Date(2025, 11, 3)                      # the same instant in EST
        @test trading_date(rfc3339_to_ns("2025-11-04T05:00:00Z")) == Date(2025, 11, 4)
    end

    @testset "backfill window follows exchange dates, not UTC days" begin
        # The request window has to use the convention `trading_date` files
        # rows under. A UTC calendar day equals an exchange date only while
        # New York is UTC-4, so from November to March a UTC-day request
        # returned the previous date's last post-market hour and stopped an
        # hour short of its own — splitting each date across two requests.
        alpaca = AlpacaProvider("k", "s", "sip", "", "", "")
        bounds(d) = (
            MarketTickStreamer.exchange_day_start_ns(alpaca, d),
            MarketTickStreamer.exchange_day_start_ns(alpaca, d + Day(1)) - 1,
        )
        for d in (
            Date(2026, 7, 1),      # EDT, UTC-4
            Date(2025, 11, 3),     # EST, UTC-5
            Date(2025, 11, 2),     # 25-hour day, DST ends
            Date(2026, 3, 8),      # 23-hour day, DST begins
        )
            lo, hi = bounds(d)
            @test trading_date(lo) == d          # first instant of the date
            @test trading_date(hi) == d          # last instant of the date
            @test trading_date(lo - 1) == d - Day(1)
            @test trading_date(hi + 1) == d + Day(1)
        end
        # Local midnight in each offset regime.
        @test ns_to_rfc3339(bounds(Date(2026, 7, 1))[1]) == "2026-07-01T04:00:00.000000000Z"
        @test ns_to_rfc3339(bounds(Date(2025, 11, 3))[1]) ==
              "2025-11-03T05:00:00.000000000Z"
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
        mktempdir() do dir
            base = "[stream]\nsymbols = [\"A\"]\n"
            binance = "[provider]\nname = \"binance\"\n"
            p = joinpath(dir, "bounds.toml")
            rejected(body) = (write(p, body); @test_throws ArgumentError load_config(p))
            # a misspelt table or key must not fall back to a default
            rejected("[strem]\nsymbols = [\"A\"]\n")
            rejected(base * "stale_timout_s = 5.0\n")
            # types
            rejected(base * "stale_timeout_s = \"5\"\n")
            rejected(base * "require_market_open = 1\n")
            rejected(base * "[limits]\nchannel_capacity = 2.5\n")
            rejected("[stream]\nsymbols = \"A\"\n")
            rejected(base * "[alpaca]\ndata_base = 8080\n")
            # bounds
            rejected(base * "reconnect_max_retries = -1\n")
            rejected(base * "reconnect_base_delay_s = 2.0\nreconnect_max_delay_s = 1.0\n")
            rejected(base * "[storage]\nflush_max_ticks = 0\n")
            rejected(base * "[limits]\nmax_session_hours = 0.0\n")
            rejected(base * "[backfill]\nrate_limit_sleep_s = -0.1\n")
            rejected(base * "[replay]\nclock = \"wall\"\n")
            # the page limit is the provider's: Binance clamps an over-limit
            # request without an error, which would truncate every day
            rejected(base * "[backfill]\npage_limit = 10001\n")
            rejected(binance * base * "[backfill]\npage_limit = 1001\n")
            write(p, binance * base)
            @test load_config(p).backfill_page_limit == 1000
            write(p, base)
            c = load_config(p)
            @test c.backfill_page_limit == 10_000
            @test c.replay_clock == "auto"
            # a TOML date is accepted as well as a date string
            write(p, base * "[backfill]\nstart_date = 2026-08-12\nend_date = 2026-08-12\n")
            @test load_config(p).backfill_start == Date(2026, 8, 12)
        end
    end

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
            # safesave: a recompaction takes the canonical names and the files
            # it displaces survive as numbered backups.
            before = Dict(f => read(f) for f in files)
            files2 = compact_raw([sink.path], out; format = "csv")
            @test files2 == files
            for f in files
                base, ext = splitext(f)
                @test isfile(f)
                @test read(base * "_#1" * ext) == before[f]
            end
            @test !any(occursin(".partial", f) for (_, _, fs) in walkdir(out) for f in fs)
            arrow_files = compact_raw([sink.path], joinpath(dir, "arrow"); format = "arrow")
            @test length(arrow_files) == 2
        end
    end

    @testset "safesave: newest canonical, backups numbered, failed write harmless" begin
        safesave = MarketTickStreamer._safesave
        mktempdir() do dir
            p = joinpath(dir, "x.csv")
            @test safesave(tmp -> write(tmp, "one"), p) == p
            @test read(p, String) == "one"
            @test readdir(dir) == ["x.csv"]                # first write leaves no backup
            safesave(tmp -> write(tmp, "two"), p)
            safesave(tmp -> write(tmp, "three"), p)
            @test read(p, String) == "three"
            @test read(joinpath(dir, "x_#1.csv"), String) == "one"   # oldest first
            @test read(joinpath(dir, "x_#2.csv"), String) == "two"
            # A writer that dies leaves the canonical file as it was, and no debris.
            @test_throws ErrorException safesave(p) do tmp
                write(tmp, "half")
                error("writer died")
            end
            @test read(p, String) == "three"
            @test sort(readdir(dir)) == ["x.csv", "x_#1.csv", "x_#2.csv"]
            # The temporary name keeps the extension last: FileIO infers the
            # format of a figure from it.
            seen = Ref("")
            safesave(tmp -> (seen[] = tmp; write(tmp, "four")), p)
            @test endswith(seen[], ".partial.csv")
            @test dirname(seen[]) == dir
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
            @test_throws ArgumentError replay_source(sink.path; clock = "wall")
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
                ch = replay_source(path; pace = "recorded", kwargs...)
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
            @test_throws ArgumentError replay_source(bsink.path; clock = "recv")
            # Absolute schedule: 400 gaps of 2.5 ms take 1 s. Sleeping each gap
            # on its own adds one timer overshoot per gap and runs ~50 % long.
            dsink = open_raw_sink(dir, "d")
            write_batch!(dsink, spaced(400, 2_500_000))
            close_sink!(dsink)
            n, span = paced_span(dsink.path; clock = "exchange")
            @test n == 401
            @test 0.95 <= span <= 1.2
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

    @testset "schema: quote and bar frames" begin
        wire(m) = JSON3.read(JSON3.write(m))
        q = MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("AAPL", 1)), 7)
        @test q.symbol == "AAPL"
        @test q.bid_price == 101.0 && q.ask_price == 101.5
        @test q.bid_size == 1.0 && q.ask_size == 2.0
        @test q.bid_exchange == "V" && q.ask_exchange == "W"
        @test q.conditions == ["R"] && q.tape == "C"
        @test q.recv_ns == 7
        @test q.time_ns == rfc3339_to_ns("2026-07-30T14:30:01.000000001Z")

        b = MarketTickStreamer.parse_alpaca_bar(wire(mock_bar("AAPL", 1)), 9)
        # `c` on a bar frame is the closing price, not a condition list.
        @test b.open == 100.0 && b.high == 101.0 && b.low == 99.0 && b.close == 100.5
        @test b.volume == 1000.0 && b.trade_count == 42 && b.vwap == 100.25
        @test b.recv_ns == 9
        # A streamed bar is one minute wide and stamped with its opening instant.
        @test b.close_ns - b.time_ns == 60 * 1_000_000_000
        later = Bar(
            b.symbol,
            b.time_ns,
            b.close_ns + 1,
            b.recv_ns,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.trade_count,
            b.vwap,
        )
        @test later != b && hash(later) != hash(b)

        # Value semantics, as for Trade: equal content compares equal.
        @test q == MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("AAPL", 1)), 7)
        @test b == MarketTickStreamer.parse_alpaca_bar(wire(mock_bar("AAPL", 1)), 9)
        @test hash(q) ==
              hash(MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("AAPL", 1)), 7))
        @test q != MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("MSFT", 1)), 7)
    end

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

    @testset "resampling: transaction, volume and value clocks" begin
        mk(id, price, size; t = id, sym = "AAPL") =
            Trade(sym, Int64(t), 0, price, size, "V", String[], "C", id)
        ts = [mk(1, 10.0, 1.0), mk(2, 11.0, 1.0), mk(3, 9.0, 1.0), mk(4, 12.0, 1.0)]

        b = tick_bars(ts, 2)
        @test length(b) == 2
        @test b[1].open == 10.0 && b[1].close == 11.0
        @test b[1].high == 11.0 && b[1].low == 10.0
        @test b[1].trade_count == 2 && b[1].volume == 2.0
        @test b[1].time_ns == 1                       # bar opens at its first print
        @test b[1].close_ns == 2                      # and closes at the print completing it
        @test b[2].time_ns == 3 && b[2].close_ns == 4
        @test all(b[i].close_ns <= b[i+1].time_ns for i in 1:(length(b)-1))
        @test b[1].vwap == 10.5
        @test b[2].open == 9.0 && b[2].close == 12.0
        @test b[2].low == 9.0 && b[2].high == 12.0
        @test all(x -> x.recv_ns == 0, b)             # derived: never crossed the wire

        # A trailing bar short of its threshold is not comparable to the rest.
        @test length(tick_bars(ts, 3)) == 1
        @test length(tick_bars(ts, 3; keep_partial = true)) == 2
        @test tick_bars(ts, 3; keep_partial = true)[2].trade_count == 1
        partial = tick_bars(ts, 3; keep_partial = true)[2]
        @test partial.time_ns == partial.close_ns == 4    # a one-print bar has no duration

        # Volume clock: 3 + 3 closes the first bar, the remaining 3 is partial.
        vs = [mk(1, 10.0, 3.0), mk(2, 10.0, 3.0), mk(3, 10.0, 3.0)]
        @test length(volume_bars(vs, 6.0)) == 1
        @test volume_bars(vs, 6.0)[1].volume == 6.0
        @test volume_bars(vs, 6.0; keep_partial = true)[2].volume == 3.0

        # Value clock weights each print by price: 100 | 1 + 100.
        ds = [mk(1, 100.0, 1.0), mk(2, 1.0, 1.0), mk(3, 100.0, 1.0)]
        db = dollar_bars(ds, 100.0)
        @test length(db) == 2
        @test db[1].trade_count == 1 && db[2].trade_count == 2

        # Bars are built on exchange time, not arrival order.
        shuffled = [mk(2, 11.0, 1.0; t = 2), mk(1, 10.0, 1.0; t = 1)]
        @test tick_bars(shuffled, 2)[1].open == 10.0
        @test tick_bars(shuffled, 2)[1].close == 11.0

        @test tick_bars(Trade[], 2) == Bar[]
        @test_throws ArgumentError tick_bars(ts, 0)
        @test_throws ArgumentError volume_bars(ts, -1.0)
        @test_throws ArgumentError dollar_bars(ts, 0.0)
        @test_throws ArgumentError tick_bars(
            [mk(1, 1.0, 1.0), mk(2, 1.0, 1.0; sym = "MSFT")],
            1,
        )
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
        @test NON_PRICE_CONDITIONS["A"] == NON_PRICE_CONDITIONS["B"]
        @test setdiff(NON_PRICE_CONDITIONS["C"], NON_PRICE_CONDITIONS["A"]) == ["G", "W"]
        @test setdiff(NON_PRICE_CONDITIONS["A"], NON_PRICE_CONDITIONS["C"]) == ["B"]
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
        @test load_config().non_price_conditions == NON_PRICE_CONDITIONS
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

        # It reaches the session report, which is the point: a redefinition
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
        # The spill copy is the size of the input, so it must never default to
        # tempdir(): /tmp is a tmpfs sized at half of RAM on systemd
        # distributions, and spilling a 26 GB corpus there took the machine
        # down. It goes beside out_dir instead, and is refused without room.
        @test MarketTickStreamer._spill_parent("/data/processed", nothing) == "/data/"
        @test MarketTickStreamer._spill_parent("/data/processed/", nothing) == "/data/"
        @test MarketTickStreamer._spill_parent("/data/processed", "/mnt/scratch") ==
              "/mnt/scratch"
        @test_throws ErrorException MarketTickStreamer._ensure_spill_space(
            "/mnt/small",
            10 * 2^30,
            2 * 2^30,
        )
        @test MarketTickStreamer._ensure_spill_space("/mnt/big", 10 * 2^30, 40 * 2^30) ===
              nothing
        # the message names the offending filesystem and the shortfall
        let e = try
                MarketTickStreamer._ensure_spill_space("/mnt/small", 10 * 2^30, 2 * 2^30)
            catch err
                sprint(showerror, err)
            end
            @test contains(e, "/mnt/small") && contains(e, "scratch_dir")
        end
        # scratch_dir is honored end to end: the directory is created where
        # asked, and only the temporary subdirectory inside it is removed.
        mktempdir() do dir
            sink = open_raw_sink(dir, "sc")
            write_batch!(sink, [sample_trade(i) for i in 1:20])
            close_sink!(sink)
            scratch = joinpath(dir, "nested", "scratch")
            files = compact_raw(
                [sink.path],
                joinpath(dir, "out");
                format = "csv",
                mem_fraction = 1e-12,
                scratch_dir = scratch,
            )
            @test length(files) == 1
            @test isdir(scratch)
            @test isempty(filter(startswith("compact_spill_"), readdir(scratch)))
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

    @testset "backfill: a killed run resumes from what was compacted" begin
        mktempdir() do dir
            # Two trading days, one symbol. The mock serves the first day's two
            # pages and then refuses, so day one completes and day two dies
            # mid-download — the shape of a multi-hour download that is killed.
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
            w = Base.invokelatest(mod.consume, [sink.path])
            @test w.n_prints == 10
            @test length(w.gaps) == 9                  # one gap fewer than prints
            @test all(≈(0.001), w.gaps)                # 1 ms apart in exchange time
            @test w.n_nonpositive == 0

            pf = Base.invokelatest(mod.consume, [sink.path]; price_forming_only = true)
            @test pf.n_prints == 8                     # the two odd lots are excluded
            @test length(pf.gaps) == 7
            @test sum(pf.gaps) ≈ 0.009 atol = 1e-9     # gaps still span the session
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
            d, persisted, seen = Base.invokelatest(mod.consume, [sink.path])
            @test persisted == 10                      # persistence is lossless
            @test seen <= persisted                    # the diagnostic tap may drop
            @test OnlineStats.nobs(d.Δt_mean) == seen - 1
            @test OnlineStats.value(d.Δt_mean) ≈ 0.001
            @test OnlineStats.value(d.Δt_extrema).max ≈ 0.001
            @test d.n_nonpositive == 0
            # Equal sizes make the VWAP the arithmetic mean of the prices seen.
            @test Base.invokelatest(mod.vwap, d) ≈ 101.0 + (seen - 1) / 2
            @test OnlineStats.value(d.volume) ≈ 10.0 * seen

            # Gaps swept log-uniformly over five decades (100 µs … 10 s), the
            # shape that breaks a P² sketch and the reason the example bins in
            # log10. Deterministic, so the expected quantiles are exact.
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
            acc, persisted_s, seen_s = Base.invokelatest(mod.consume, [s.path])
            @test persisted_s == 2000 && seen_s == 2000   # far below the tap capacity
            # The first trade opens the series, so its gap is not observed.
            exact = sort(gaps_s[2:end])
            for τ in (0.5, 0.9, 0.99)
                got = Base.invokelatest(mod.waiting_quantile, acc, τ)
                want = exact[max(1, round(Int, τ * length(exact)))]
                # Bin centres over five decades: accurate to a bin width, not
                # to the digit. A P² sketch on these gaps misses by 10x+.
                @test 0.8 * want <= got <= 1.25 * want
            end
            # Both calls go through invokelatest: the example's methods are
            # younger than this world, the constructor included.
            empty_acc = Base.invokelatest(mod.LiveDiagnostics)
            @test isnan(Base.invokelatest(mod.waiting_quantile, empty_acc, 0.5))
            @test_throws ArgumentError Base.invokelatest(mod.waiting_quantile, acc, 1.5)
            @test_throws ArgumentError Base.invokelatest(mod.LiveDiagnostics; n_bins = 2)

            # The point of the example: once the histogram has filled its bin
            # budget the footprint stops growing, so a longer stream of the
            # same shape costs exactly the same state.
            foot = map((2000, 6000)) do n
                tr, _ = spread_trades(n)
                sk = open_raw_sink(dir, "diaglong$(n)")
                write_batch!(sk, tr)
                close_sink!(sk)
                a, _, _ = Base.invokelatest(mod.consume, [sk.path])
                Base.summarysize(a)
            end
            @test foot[1] == foot[2]
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
            # UnicodePlots is not loaded yet: the dashboard is plain text.
            @test Base.get_extension(
                MarketTickStreamer,
                :MarketTickStreamerUnicodePlotsExt,
            ) === nothing
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

    @testset "diagnostics: clock ticks, decimation, tail fit, formatting" begin
        M = MarketTickStreamer
        # HH:MM domain ticks
        vals, labels = M._hhmm_ticks(9.5, 16.0)
        @test labels[1] == "10:00" && labels[end] == "16:00"
        # a whole day is labelled every four hours, which fits a half-width panel
        @test M._hhmm_ticks(0.0, 24.0)[2] ==
              ["00:00", "04:00", "08:00", "12:00", "16:00", "20:00", "24:00"]
        # Pooled waiting times: a closure between days is not a waiting time,
        # midnight on a venue that never closes is.
        day(d, ts) = (d, DataFrame(time_ns = Int64.(ts) .* 1_000_000_000))
        days = [
            day(Date(2026, 9, 18), [10, 11, 13]),
            day(Date(2026, 9, 19), [20, 24]),
            day(Date(2026, 9, 21), [50, 51]),          # the 20th is missing
        ]
        gaps, excluded = M._pooled_gaps(days; continuous = false)
        @test gaps == [1.0, 2.0, 4.0, 1.0] && excluded == 2
        gaps, excluded = M._pooled_gaps(days; continuous = true)
        @test gaps == [1.0, 2.0, 7.0, 4.0, 1.0] && excluded == 1
        @test M._pooled_gaps(days[1:1]; continuous = true) == ([1.0, 2.0], 0)
        # thinning preserves ends; decimation preserves extrema
        xs = collect(1.0:10_000.0)
        tx, ty = M._thin(xs, xs; cap = 500)
        @test length(tx) <= 810 && tx[1] == 1.0 && tx[end] == 10_000.0
        dx, dy = M._decimate_minmax(xs, sin.(xs); nbins = 50)
        @test length(dx) <= 100
        @test maximum(dy) ≈ maximum(sin.(xs)) atol = 1e-3
        # Hill estimator on the quantiles of a Pareto law with alpha = 2.5
        n = 5000
        x = sort!([((i - 0.5) / n)^(-1 / 2.5) for i in 1:n])
        fit = M._tail_fit(x)
        @test fit !== nothing
        @test isapprox(fit.α, 2.5; rtol = 0.02)
        @test fit.σ ≈ fit.α / sqrt(500)                      # k = top 10 %
        @test M._tail_fit(x[1:40]) === nothing               # too few samples
        @test M._tail_fit(fill(100.0, 500)) === nothing      # degenerate tail
        # annotations: value and uncertainty share their decimals
        @test M._value_pm(1.4294, 0.0041) == ("1.429", "0.004")
        @test M._value_pm(1.8676, 0.0684) == ("1.87", "0.07")
        @test M._value_pm(1.4, 0.2) == ("1.4", "0.2")
        @test M._si_seconds(4.6e-4) == "460 μs"
        @test M._si_seconds(2.5) == "2.5 s"
    end

    @testset "figures: without CairoMakie the stubs say what to load" begin
        @test Base.get_extension(MarketTickStreamer, :MarketTickStreamerMakieExt) ===
              nothing
        for call in (
            () -> session_figure([sample_trade(1)]),
            () -> tick_theme(),
            () -> save_session_figures(String[], mktempdir()),
        )
            err = try
                call()
            catch e
                e
            end
            @test err isa MethodError
            @test occursin("load CairoMakie", sprint(showerror, err))
        end
        # The hint is specific to the figure functions.
        err = try
            price_forming(1)
        catch e
            e
        end
        @test err isa MethodError && !occursin("CairoMakie", sprint(showerror, err))
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
                    mock_config_toml(
                        dir;
                        ws_port,
                        rest_port,
                        max_session_hours = 2.0 / 3600,
                    ),
                )
                p = AlpacaProvider(cfg, MOCK_KEY, MOCK_SECRET)
                t0 = time()
                result = run_stream(cfg; provider = p)
                @test result.ticks == 3
                @test nconn[] == 1                 # stopped, not reconnected
                @test time() - t0 < 15.0
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


    @testset "provider registry and the second provider" begin
        # The config layer must not know one vendor's vocabulary. Before this
        # was dispatched, `provider.feed` was validated against Alpaca's feed
        # names for every provider, and a 24-hour venue's days were cut at
        # midnight in New York.
        @test_throws ArgumentError provider_spec("kraken")
        @test provider_spec("alpaca").tz == tz"America/New_York"
        @test provider_spec("binance").tz == tz"UTC"
        @test provider_spec("alpaca").needs_credentials
        @test !provider_spec("binance").needs_credentials

        bp = BinanceProvider("aggTrade", "http://x", "ws://x")
        ap = AlpacaProvider("k", "s", "sip", "", "", "")
        @test always_open(bp) && !always_open(ap)
        @test exchange_tz(bp) == tz"UTC"

        # A 24-hour venue trades on weekends; an equity venue does not. The
        # default is every day, so a provider that rests must opt out — the
        # reverse default silently drops two days in seven.
        @test length(session_days(bp, Date(2026, 1, 2), Date(2026, 1, 6))) == 5
        @test session_days(ap, Date(2026, 1, 2), Date(2026, 1, 6)) ==
              [Date(2026, 1, 2), Date(2026, 1, 5), Date(2026, 1, 6)]

        # UTC has no DST, so every Binance day is exactly 24 h.
        d0 = MarketTickStreamer.exchange_day_start_ns(bp, Date(2026, 1, 15))
        d1 = MarketTickStreamer.exchange_day_start_ns(bp, Date(2026, 1, 16))
        @test d1 - d0 == 86_400 * MarketTickStreamer.NS_PER_SEC
        @test ns_to_rfc3339(d0) == "2026-01-15T00:00:00.000000000Z"
        # The New York equivalents are not 24 h apart across a DST boundary.
        n0 = MarketTickStreamer.exchange_day_start_ns(ap, Date(2025, 11, 2))
        n1 = MarketTickStreamer.exchange_day_start_ns(ap, Date(2025, 11, 3))
        @test n1 - n0 == 25 * 3600 * MarketTickStreamer.NS_PER_SEC

        @test market_clock(bp) == (; is_open = true, next_open = "", next_close = "")

        # An unknown provider must fail at construction with a message naming
        # it, not with a MethodError deep in the session. The fallback throws
        # rather than returning, so the provider type stays narrow: a fallback
        # with a value widens every caller's inference and leaves each
        # downstream provider call with a no-matching-method branch.
        cfg_now = load_config()
        @test_throws ArgumentError MarketTickStreamer.make_provider(
            Val(:kraken),
            cfg_now,
            "",
            "",
        )
        @test Base.infer_return_type(MarketTickStreamer._provider_from_config, (Config,)) ==
              Union{AlpacaProvider,BinanceProvider}
    end

    @testset "binance: frame and REST parsing" begin
        f = mock_binance_frame("BTCUSDT", 4)
        t = MarketTickStreamer.parse_binance_trade(JSON3.read(JSON3.write(f)), 99)
        @test t.symbol == "BTCUSDT"
        @test t.recv_ns == 99
        @test t.exchange == "BINANCE" && t.tape == "SPOT"
        @test t.id == 4
        # Prices and sizes arrive as strings and must survive as Float64.
        @test t.price ≈ 64_004.0 && t.size ≈ 0.004
        # Milliseconds on the wire, nanoseconds in the schema.
        @test t.time_ns == Int64(f.T) * 1_000_000
        @test t.time_ns % 1_000_000 == 0
        # `m = true` means the buyer was the maker, so the seller crossed.
        @test t.conditions == (f.m ? ["sell"] : ["buy"])
        # k = 6 is even, so the mock sets m = false: the buyer crossed.
        @test MarketTickStreamer.parse_binance_trade(
            JSON3.read(JSON3.write(mock_binance_frame("BTCUSDT", 6))),
            0,
        ).conditions == ["buy"]
        # No tape entry means nothing to exclude on: every crypto print is
        # price-forming, there being no odd lots or late prints.
        @test price_forming(t)
        # A raw `trade` frame carries `t` for the id where aggTrade carries `a`.
        raw = (;
            e = "trade",
            E = f.T,
            s = "BTCUSDT",
            t = 77,
            p = "1.5",
            q = "2.0",
            T = f.T,
            m = false,
            M = true,
        )
        @test MarketTickStreamer.parse_binance_trade(JSON3.read(JSON3.write(raw)), 0).id ==
              77
    end

    @testset "binance: id-seeded backfill pagination" begin
        rest_port = freeport(9811)
        per_day = 7
        server = start_mock_binance_rest(rest_port; trades_per_day = per_day)
        try
            p = BinanceProvider("aggTrade", "http://127.0.0.1:$rest_port", "ws://unused")
            # One UTC date, walked in pages of 3 — the seed request uses
            # startTime, every later one uses fromId.
            got = historical_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH,
                MOCK_BINANCE_EPOCH;
                page_limit = 3,
                rate_sleep_s = 0.0,
            )
            @test length(got) == per_day
            @test all(t -> trading_date(t.time_ns; tz = tz"UTC") == MOCK_BINANCE_EPOCH, got)
            @test issorted([t.time_ns for t in got])
            @test [t.id for t in got] == collect(1:per_day)
            @test all(t -> t.recv_ns == 0, got)        # never crossed the wire

            # Two dates must give exactly two days of tape, weekend included.
            two = historical_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH,
                MOCK_BINANCE_EPOCH + Day(1);
                page_limit = 3,
                rate_sleep_s = 0.0,
            )
            @test length(two) == 2 * per_day
            @test length(unique(trading_date(t.time_ns; tz = tz"UTC") for t in two)) == 2

            # Streaming mode returns the count and retains nothing.
            pages = Vector{Int}()
            n = historical_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH,
                MOCK_BINANCE_EPOCH;
                page_limit = 3,
                rate_sleep_s = 0.0,
                each_page = pg -> push!(pages, length(pg)),
            )
            @test n == per_day && sum(pages) == per_day && length(pages) > 1

            # Only aggTrade can seek by time; asking for the raw feed must say so.
            @test_throws ArgumentError historical_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH,
                MOCK_BINANCE_EPOCH;
                feed = "trade",
            )
        finally
            close(server)
        end
    end

    @testset "binance: end-to-end backfill on the UTC calendar" begin
        mktempdir() do dir
            rest_port = freeport(9831)
            per_day = 7
            server = start_mock_binance_rest(rest_port; trades_per_day = per_day)
            try
                cfg = load_config(
                    mock_binance_config_toml(
                        dir;
                        ws_port = 9999,
                        rest_port = rest_port,
                        start_date = string(MOCK_BINANCE_EPOCH),
                        end_date = string(MOCK_BINANCE_EPOCH + Day(2)),
                    ),
                )
                @test cfg.provider == "binance"
                @test cfg.exchange_tz == tz"UTC"
                files = run_backfill(cfg)
                # Three consecutive dates, one file each — including the
                # weekend, which an equity day loop would have skipped.
                @test length(files) == 3
                days = sort([basename(f)[1:10] for f in files])
                @test days == [string(MOCK_BINANCE_EPOCH + Day(i)) for i in 0:2]
                rows = CSV.read(first(sort(files)), DataFrame)
                @test nrow(rows) == per_day
                @test all(rows.tape .== "SPOT") && all(rows.exchange .== "BINANCE")
            finally
                close(server)
            end
        end
    end

    @testset "binance: live stream needs no auth handshake" begin
        mktempdir() do dir
            ws_port = freeport(9851)
            frames = [mock_binance_frame("BTCUSDT", k) for k in 1:5]
            server = start_mock_binance_ws(ws_port; frames = frames)
            try
                cfg = load_config(
                    mock_binance_config_toml(
                        dir;
                        ws_port = ws_port,
                        rest_port = 9999,
                        symbols = ["BTCUSDT"],
                    ),
                )
                p = BinanceProvider(cfg)
                s = live_source(p, cfg)
                got = collect(s.channel)
                @test length(got) == 5
                @test all(t -> t.symbol == "BTCUSDT", got)
                @test all(t -> t.recv_ns > 0, got)     # live prints are stamped
                @test [t.id for t in got] == collect(1:5)
            finally
                close(server)
            end
        end
    end

end

# The extensions load with their trigger packages. Everything above ran
# without them, which is the state a consumer without plotting is in.
using CairoMakie: CairoMakie
using UnicodePlots: UnicodePlots

@testset "MarketTickStreamer extensions" begin
    makie_ext = Base.get_extension(MarketTickStreamer, :MarketTickStreamerMakieExt)
    plots_ext = Base.get_extension(MarketTickStreamer, :MarketTickStreamerUnicodePlotsExt)

    @testset "both load with their trigger" begin
        @test makie_ext isa Module
        @test plots_ext isa Module
    end

    @testset "static QA (ExplicitImports)" begin
        # Owner checks are not asserted for the figure extension: the plotting
        # surface is re-exported (Makie names through CairoMakie, `save`
        # through FileIO, `@L_str` through LaTeXStrings).
        for ext in (makie_ext, plots_ext)
            @test check_no_implicit_imports(ext) === nothing
            @test check_no_stale_explicit_imports(ext) === nothing
            @test check_no_self_qualified_accesses(ext) === nothing
        end
    end

    @testset "static QA (JET)" begin
        # `report_package` does not reach extension modules, so their entry
        # points are analysed call by call.
        trades = [sample_trade(i) for i in 1:60]
        JET.@test_call target_modules = (makie_ext,) tick_theme()
        JET.@test_call target_modules = (makie_ext,) session_figure(trades)
        JET.@test_call target_modules = (makie_ext,) save_session_figures(
            String[],
            mktempdir(),
        )
        JET.@test_call target_modules = (plots_ext,) plots_ext.draw_rate_history(
            IOBuffer(),
            [1.0, 2.0],
            300.0,
        )
        JET.@test_call target_modules = (plots_ext,) plots_ext.draw_symbol_counts(
            IOBuffer(),
            ["AAPL"],
            [1],
        )
    end

    @testset "figures: axis labelling" begin
        E = makie_ext
        # log ticks: plain decimals on short spans, exponent + collapse on long
        v, l = E._log_ticks(0.5, 80.0)
        @test "1" in l &&
              "2" in l &&
              "50" in l &&
              !any(occursin("10^", string(x)) for x in l)
        v, l = E._log_ticks(1e-7, 10.0)
        @test "1" in l && "10" in l                          # 10^0 and 10^1 collapse
        @test any(x -> occursin("10^{-6}", string(x)), l)
        # intermediates carry their own mantissa, never the rounded decade
        v, l = E._log_ticks(3e-6, 2e-4)
        @test allunique(string.(l))
        @test any(x -> occursin("2\\times10^{-5}", string(x)), l)
        @test E._pow10_label(5, 0) == "5" && E._pow10_label(2, 1) == "20"
        @test E._pow10_label(2, -1) == "0.2" && E._pow10_label(1, 1) == "10"
        v, l = E._log_ticks(1.3e-4, 1.0)                     # four decades: decades only
        @test length(v) == 4 && allunique(string.(l))
        # one exponent per axis: the rate is rescaled, not the tick labels
        @test E._rate_scale(2.2e4)[1] == 1e3
        @test E._rate_scale(800)[1] == 1.0
    end

    @testset "figures: theme, layout smoke, session window, files" begin
        theme = tick_theme()
        @test theme isa CairoMakie.Makie.Attributes
        @test theme.fontsize[] == 26
        @test haskey(theme, :fonts)                          # Computer Modern faces
        trades = [sample_trade(i) for i in 1:200]
        @test session_figure(trades) isa CairoMakie.Figure
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
        @test overview_figure("AAPL", days) isa CairoMakie.Figure
        # A venue that never closes lays its days out on the whole UTC day.
        @test overview_figure("BTCUSDT", days; tz = tz"UTC", session = (0.0, 24.0)) isa
              CairoMakie.Figure
        @test_throws ArgumentError overview_figure("AAPL", days; session = (16.0, 9.5))
        @test_throws ArgumentError overview_figure("AAPL", days; session = (0.0, 25.0))
        @test provider_spec("alpaca").regular_session == (9.5, 16.0)
        @test provider_spec("binance").regular_session == (0.0, 24.0)
        @test provider_spec("alpaca").size_unit == "shares"
        @test provider_spec("binance").price_unit == "quote asset"
        @test session_figure(
            trades;
            price_unit = "quote asset",
            size_unit = "base asset",
        ) isa CairoMakie.Figure
        # Files land under their canonical names, with no partial left behind.
        mktempdir() do dir
            sink = open_raw_sink(dir, "fig")
            write_batch!(sink, trades)
            close_sink!(sink)
            out = joinpath(dir, "plots")
            written =
                save_session_figures([sink.path], out; formats = ("png",), min_trades = 10)
            @test length(written) == 1 && all(isfile, written)
            @test readdir(out) == basename.(written)
        end
    end

    @testset "monitor: terminal plots" begin
        mktempdir() do dir
            sink = open_raw_sink(dir, "mon")
            write_batch!(sink, [sample_trade(i) for i in 1:30])
            close_sink!(sink)
            buf = IOBuffer()
            monitor_raw(dir; refresh_s = 0.01, iterations = 2, from_start = true, io = buf)
            out = String(take!(buf))
            @test occursin("Ticks by symbol", out) && occursin("┌", out)   # bar chart frame
            @test occursin("Ticks/s", out)                                  # rate history
        end
    end
end
