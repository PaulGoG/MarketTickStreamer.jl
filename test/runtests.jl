using Test
using Dates
using TimeZones
using CSV
using DataFrames
using TickStreamer

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

@testset "TickStreamer" begin

@testset "schema / timestamps" begin
    ns = rfc3339_to_ns("2026-07-30T14:30:00.123456789Z")
    @test ns % TickStreamer.NS_PER_SEC == 123_456_789
    @test ns_to_rfc3339(ns) == "2026-07-30T14:30:00.123456789Z"
    # lossless round-trip at all fractional widths
    for frac in ("1", "123", "123456", "999999999")
        s = "2020-01-02T03:04:05.$(rpad(frac, 9, '0'))Z"
        @test ns_to_rfc3339(rfc3339_to_ns(s)) == s
    end
    # numeric offsets normalize to UTC
    @test rfc3339_to_ns("2026-07-30T10:30:00-04:00") == rfc3339_to_ns("2026-07-30T14:30:00Z")
    @test rfc3339_to_ns("2026-07-30T16:30:00+02:00") == rfc3339_to_ns("2026-07-30T14:30:00Z")
    # no fractional part
    @test rfc3339_to_ns("2026-07-30T14:30:00Z") % TickStreamer.NS_PER_SEC == 0
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
end

sample_trade(i; sym = "AAPL") = Trade(sym, 1_753_886_600_000_000_000 + i * 1_000_000,
                                      1_753_886_600_100_000_000 + i * 1_000_000,
                                      100.0 + i, 10.0 * i, "V", ["@", "I"], "C", i)

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
        task = Threads.@spawn run_sink!(ch, sink; flush_interval_s = 10.0, flush_max_ticks = 25)
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
        write_batch!(sink, [sample_trade(i; sym = iseven(i) ? "MSFT" : "AAPL") for i in 1:20])
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
    mk(i; sym = "AAPL", dt = 1_000_000_000, recv_off = 2_000_000) =
        Trade(sym, base + i * dt, base + i * dt + recv_off, 100.0 + i, 10.0, "V", ["@"], "C", i)
    trades = [mk(i) for i in 1:20]
    dup_set = vcat(trades, trades[5:8])                  # reconnection double-delivery
    @test length(dedup_trades(dup_set)) == 20
    @test dedup_trades(dup_set) == trades                # order preserved, first kept
    mktempdir() do dir
        sink = open_raw_sink(dir, "q")
        # AAPL: 4 dupes, one 120 s gap, one out-of-order pair, one negative latency
        aapl = vcat(trades, trades[1:4])
        push!(aapl, mk(21; dt = 1_000_000_000))
        aapl[end] = Trade("AAPL", base + 200 * 1_000_000_000, base + 200 * 1_000_000_000 - 5_000_000,
                          99.0, 1.0, "V", String[], "C", 999)   # +120s gap, latency -5 ms
        push!(aapl, mk(3))                               # out-of-order arrival (t < previous)
        # MSFT: pure backfill (recv_ns = 0)
        msft = [Trade("MSFT", base + i * 1_000_000_000, 0, 300.0, 5.0, "V", ["@"], "C", i)
                for i in 1:5]
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
        # compaction dedups by default
        files = compact_raw([sink.path], joinpath(dir, "p"); format = "csv")
        df = CSV.read(files[findfirst(contains("AAPL"), files)], DataFrame)
        @test nrow(df) == 21                             # 26 raw AAPL rows − 5 dupes
        # memory guard refuses absurd budgets
        @test_throws ErrorException compact_raw([sink.path], joinpath(dir, "p2");
                                                mem_fraction = 1e-12)
    end
end

@testset "close guard + disk guard" begin
    s = LiveSession(Channel{Trade}(1), Ref(false), Ref{Any}(nothing),
                    Ref((; ticks = 0, frames = 0, reconnects = 0)))
    t = schedule_close_stop!(s, now_ns() + 300_000_000; grace_s = 0.0)   # closes in 0.3 s
    wait(t)
    @test s.stop[]
    @test free_disk_gb(pwd()) > 0.0
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

@testset "alpaca REST: clock + paginated historical trades" begin
    rest_port = freeport(8931)
    rest = start_mock_rest(; port = rest_port, trades_per_page = 3)
    try
        p = AlpacaProvider(MOCK_KEY, MOCK_SECRET, "iex",
                           "http://127.0.0.1:$rest_port", "http://127.0.0.1:$rest_port",
                           "ws://127.0.0.1:$rest_port/v2")
        clock = market_clock(p)
        @test clock.is_open
        trades = historical_trades(p, "AAPL", Date(2026, 7, 29), Date(2026, 7, 29);
                                   page_limit = 3, rate_sleep_s = 0.0)
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
        plan = MockPlan([[mock_trade("AAPL", i) for i in 1:8],
                         [mock_trade("MSFT", i) for i in 9:14]];
                        fatal_after = 2)               # 3rd connection → fatal 406
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
        finally
            close(ws); close(rest)
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
            close(ws); close(rest)
        end
    end
end

end
