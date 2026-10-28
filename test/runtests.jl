using Test
# AllocCheck builds on GPUCompiler, whose LLVM bindings trail each new Julia
# release; the CI pre-release leg therefore runs without it. Any other
# environment missing it is a broken test setup.
const ALLOCCHECK_AVAILABLE = Base.find_package("AllocCheck") !== nothing
if ALLOCCHECK_AVAILABLE
    using AllocCheck
elseif isempty(VERSION.prerelease)
    error(
        "AllocCheck is missing from the test environment; only a Julia pre-release runs without it",
    )
end
using Aqua
using ExplicitImports
using JET
# JET follows the compiler and loads as empty stubs on Julia versions it does
# not support yet. The pre-release leg then skips its analyses; on a release
# they run regardless, so an unsupported JET fails instead of passing silently.
const JET_ENABLED = JET.JET_AVAILABLE || isempty(VERSION.prerelease)
using OnlineStats: OnlineStats
using Dates
using TimeZones
using CSV
using DataFrames
using TOML
using Random: Xoshiro
using Base64: base64encode
using CRC32c: crc32c
using CodecZlib: GzipCompressorStream
using MarketTickStreamer
using MarketTickStreamer:
    RawSink,
    open_raw_sink,
    write_batch!,
    close_sink!,
    run_sink!,
    free_disk_gb,
    check_live_heap,
    schedule_close_stop!,
    session_id,
    setup_logging,
    write_session_meta,
    start_session_meta,
    finalize_session_meta,
    reconcile_sessions!,
    acquire_session_lock

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

# Example scripts are loaded into fresh modules at run time, so their bindings
# live in a newer world than this file's: both the lookup and the call go
# through `invokelatest`, or Julia 1.12 warns about the binding access.
example_call(mod::Module, name::Symbol, args...; kw...) =
    Base.invokelatest(Base.invokelatest(getglobal, mod, name), args...; kw...)

@testset "MarketTickStreamer" begin

    @testset "static QA (Aqua)" begin
        Aqua.test_all(MarketTickStreamer)
    end

    @testset "static QA (ExplicitImports)" begin
        # The owner and public-access checks are not asserted:
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
        if JET_ENABLED
            JET.test_package(MarketTickStreamer; target_modules = (MarketTickStreamer,))
        else
            @test_skip "JET package analysis (JET unavailable)"
        end
    end

    @testset "static QA (allocation-free hot paths)" begin
        # These three run once per print on the ingest and compaction paths.
        # Zero allocations is the measured state, asserted here so an
        # accidental boxing or a dynamic dispatch shows up as a test failure.
        if ALLOCCHECK_AVAILABLE
            @test isempty(check_allocs(MarketTickStreamer.ns_to_datetime, (Int64,)))
            @test isempty(check_allocs(MarketTickStreamer.now_ns, ()))
            @test isempty(check_allocs(MarketTickStreamer.trading_date, (Int64,)))
        else
            @test_skip "allocation-free hot paths (AllocCheck unavailable)"
        end
        # `rfc3339_to_ns` and `ns_to_rfc3339` are not asserted: the first
        # matches a regular expression and validates through `DateTime`, the
        # second builds a String. At a few hundred nanoseconds each against
        # network-bound ingestion, hand-rolling either to reach zero is not
        # justified by measurement — see bench/benchmarks.jl.
    end

    @testset "exceptions: one supertype, typed fields, plain messages" begin
        for T in (
            ResourceError,
            IntegrityError,
            VendorError,
            SessionError,
            CredentialError,
            FatalStreamError,
        )
            @test T <: MarketTickStreamerError <: Exception
        end
        @test sprint(showerror, ResourceError(:disk, "only 1.2 GiB free")) ==
              "ResourceError: only 1.2 GiB free"
        @test sprint(showerror, FatalStreamError("auth failed")) ==
              "FatalStreamError: auth failed"
        # A refusal names its resource: a handler branches on a field, not on text.
        err = try
            check_live_heap(0; context = "test")
        catch e
            e
        end
        @test err isa ResourceError && err.resource === :heap
        @test occursin("during test", err.msg)
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
        # The last nanosecond of an exchange day files under that day:
        # `trading_date` floors in integer arithmetic, never rounding up a date.
        @test trading_date(rfc3339_to_ns("2026-07-31T03:59:59.999999999Z")) ==
              Date(2026, 7, 30)                      # 23:59:59.999999999 EDT
        @test trading_date(rfc3339_to_ns("2026-07-31T04:00:00Z")) == Date(2026, 7, 31)
        @test trading_date(rfc3339_to_ns("2025-11-04T04:59:59.999999999Z")) ==
              Date(2025, 11, 3)                      # the same instant in EST
        @test trading_date(rfc3339_to_ns("2025-11-04T05:00:00Z")) == Date(2025, 11, 4)
    end

    @testset "backfill window follows exchange dates, not UTC days" begin
        # The request window of a date spans exactly the instants
        # `trading_date` files under it, in every offset regime.
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
            rejected(base * "[replay]\nmax_gap_s = 0.0\n")
            rejected(base * "[replay]\nmax_gap_s = nan\n")
            rejected(base * "[limits]\nread_mem_fraction = 0.0\n")
            rejected(base * "[limits]\nread_mem_fraction = 1.5\n")
            # the page limit is the provider's: Binance clamps an over-limit
            # request without an error, which would truncate every day
            rejected(base * "[backfill]\npage_limit = 10001\n")
            rejected(binance * base * "[backfill]\npage_limit = 1001\n")
            # The bulk source is a provider capability: Alpaca has none,
            # Binance has it for both feeds, REST for aggTrade only.
            rejected(base * "[backfill]\nsource = \"ftp\"\n")
            rejected(base * "[backfill]\nsource = \"bulk\"\n")
            rejected(binance * base * "[backfill]\nsource = \"rest\"\nfeed = \"trade\"\n")
            write(p, binance * base * "[backfill]\nsource = \"bulk\"\nfeed = \"trade\"\n")
            c = load_config(p)
            @test c.backfill_source === :bulk && c.backfill_feed == "trade"
            @test endswith(c.bulk_dir, joinpath("data", "bulk"))
            @test c.backfill_keep_raw
            # Dropping the raw copy needs the vendor file as the record.
            rejected(binance * base * "[backfill]\nkeep_raw = false\n")
            rejected(binance * base * "[backfill]\nkeep_raw = \"no\"\n")
            write(p, binance * base * "[backfill]\nsource = \"bulk\"\nkeep_raw = false\n")
            @test !load_config(p).backfill_keep_raw
            write(p, base)
            @test load_config(p).backfill_source === :rest
            write(p, binance * base)
            @test load_config(p).backfill_page_limit == 1000
            write(p, base)
            c = load_config(p)
            @test c.backfill_page_limit == 10_000
            @test c.replay_clock === :auto
            @test c.replay_max_gap_s == Inf
            @test c.read_mem_fraction == 0.5
            write(p, base * "[replay]\nmax_gap_s = 3600\n")
            @test load_config(p).replay_max_gap_s == 3600.0
            # a TOML date is accepted as well as a date string
            write(p, base * "[backfill]\nstart_date = 2026-08-12\nend_date = 2026-08-12\n")
            @test load_config(p).backfill_start == Date(2026, 8, 12)
        end
        mktempdir() do dir
            p = joinpath(dir, "rng.toml")
            base = "[stream]\nsymbols = [\"A\"]\n"
            write(p, base * "[rng]\nseed = 42\n")
            @test load_config(p).rng_seed == 42
            write(p, base)
            a, b = load_config(p).rng_seed, load_config(p).rng_seed
            @test a >= 0 && b >= 0 && a != b      # drawn from the OS at each load
            write(p, base * "[rng]\nseed = -1\n")
            @test_throws ArgumentError load_config(p)
            write(p, base * "[rng]\nseed = \"7\"\n")
            @test_throws ArgumentError load_config(p)
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
            files = compact_raw([sink.path], out; format = :csv)
            @test length(files) == 2                       # one per symbol, same day
            @test all(isfile, files)
            # safesave: a recompaction takes the canonical names and the files
            # it displaces survive as numbered backups.
            before = Dict(f => read(f) for f in files)
            files2 = compact_raw([sink.path], out; format = :csv)
            @test files2 == files
            for f in files
                base, ext = splitext(f)
                @test isfile(f)
                @test read(base * "_#1" * ext) == before[f]
            end
            @test !any(occursin(".partial", f) for (_, _, fs) in walkdir(out) for f in fs)
            arrow_files = compact_raw([sink.path], joinpath(dir, "arrow"); format = :arrow)
            @test length(arrow_files) == 2
            # Arrow stores the low-cardinality columns dictionary-encoded and
            # reads back the same table the CSV holds, compressed or not.
            zstd_files = compact_raw(
                [sink.path],
                joinpath(dir, "zstd");
                format = :arrow,
                compression = :zstd,
            )
            for (c, a, z) in zip(files, arrow_files, zstd_files)
                csv = CSV.read(c, DataFrame; types = Dict(:conditions => String))
                tbl = MarketTickStreamer.Arrow.Table(a)
                @test tbl.conditions isa MarketTickStreamer.Arrow.DictEncoded
                @test tbl.symbol isa MarketTickStreamer.Arrow.DictEncoded
                @test !(tbl.price isa MarketTickStreamer.Arrow.DictEncoded)
                for f in (a, z)
                    df = MarketTickStreamer._read_processed(f)
                    @test df.time_ns == csv.time_ns && df.price == csv.price
                    @test df.symbol == csv.symbol && df.conditions == csv.conditions
                end
            end
            @test_throws ArgumentError compact_raw(
                [sink.path],
                joinpath(dir, "x");
                compression = :zstd,
            )
            @test_throws ArgumentError compact_raw(
                [sink.path],
                joinpath(dir, "x");
                format = :arrow,
                compression = :gzip,
            )
            # Empty text reads back as "", whether the file quotes it or leaves
            # it unquoted, which CSV.jl reads as `missing`.
            t1 = Trade(
                "AAPL",
                1_753_886_600_000_000_000,
                0,
                100.0,
                1.0,
                "V",
                String[],
                "C",
                1,
            )
            t2 = Trade("AAPL", 1_753_886_600_001_000_000, 0, 100.5, 2.0, "", ["@"], "", 2)
            p = MarketTickStreamer._write_group(
                [t1, t2],
                dir,
                "EMPTY",
                Date(2025, 7, 30),
                :csv,
            )
            @test MarketTickStreamer._trades_from_processed(p) == [t1, t2]
            write(
                p,
                "symbol,time_ns,recv_ns,price,size,exchange,conditions,tape,id\n" *
                "AAPL,1753886600000000000,0,100.0,1.0,V,,C,1\n" *
                "AAPL,1753886600001000000,0,100.5,2.0,,@,,2\n",
            )
            @test MarketTickStreamer._trades_from_processed(p) == [t1, t2]
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
                mk(i, "AAPL", d; conds = isodd(i) ? ["@", "I"] : String[]) for d in 0:2
                for i in 1:2:9
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
                # One symbol: the days in order, every field intact — including
                # an empty condition list and an all-numeric one.
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
                collect(
                    replay_source(two; pace = :recorded, speed = 1.0e5, max_gap_s = 60.0),
                ),
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
                w = read_processed(
                    out,
                    "AAPL";
                    window = (h_t0, h(53_000_004)),
                    tz = tz"UTC",
                )
                @test length(w) == 9
                @test all(t -> t.id in 1:3, w)
                # The open bound is inclusive: a window opening on the first
                # print keeps it.
                @test length(
                    read_processed(
                        out,
                        "AAPL";
                        window = (h(53_000_001), 24.0),
                        tz = tz"UTC",
                    ),
                ) == 18
                # Nothing before t0: every day is yielded, empty.
                empty_days =
                    collect(eachday(out, "AAPL"; window = (0.0, h_t0), tz = tz"UTC"))
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
            compact_raw([sink.path], out; format = :arrow, tz = tz"UTC")
            @test isfile(joinpath(out, "AAPL", "2025-07-30_#1.arrow"))
            @test corpus_inventory(out).n_days == [3, 2]
            @test corpus_inventory(out).bytes[1] ==
                  sum(filesize, processed_files(out, "AAPL"))
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
            write(
                p,
                "[stream]\nsymbols = [\"AAPL\", \"MSFT\"]\n[storage]\ndata_dir = \".\"\n",
            )
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
            files = compact_raw([sink.path], joinpath(dir, "p"); format = :csv)
            df = CSV.read(files[findfirst(contains("AAPL"), files)], DataFrame)
            @test nrow(df) == 21                             # 26 raw AAPL rows − 5 duplicates
            # over-budget inputs reroute through spill compaction transparently
            spill_files =
                compact_raw([sink.path], joinpath(dir, "p2"); mem_fraction = 1e-12)
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
                rec =
                    only(filter(r -> r.message == "dropped duplicate prints", logger.logs))
                @test rec.kwargs[:symbol] == "AAPL"
                @test rec.kwargs[:day] == Date(2026, 7, 30)
                @test rec.kwargs[:count] == 5
                @test c.rows == 21 + 5                       # AAPL + MSFT stored
            end
        end
    end

    @testset "schema: quote and bar frames" begin
        wire(m) = JSON.parse(JSON.json(m))
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

    @testset "resampling: matching events" begin
        mk(id, t, price, size; sym = "BTCUSDT") =
            Trade(sym, Int64(t), 0, price, size, "BINANCE", ["buy"], "SPOT", id)
        # A taker order filled against three resting orders is three prints at
        # one instant; the next two instants hold one and two fills.
        ts = [
            mk(1, 100, 10.0, 1.0),
            mk(2, 100, 10.5, 2.0),
            mk(3, 100, 11.0, 1.0),
            mk(4, 250, 9.0, 4.0),
            mk(5, 400, 9.5, 1.0),
            mk(6, 400, 9.5, 1.0),
        ]
        ev = event_bars(ts)
        @test length(ev) == 3
        @test [e.time_ns for e in ev] == [100, 250, 400]
        @test all(e.time_ns == e.close_ns for e in ev)      # an event has no duration
        @test ev[1].open == 10.0 && ev[1].close == 11.0
        @test ev[1].high == 11.0 && ev[1].low == 10.0
        @test ev[1].volume == 4.0 && ev[1].trade_count == 3
        @test ev[1].vwap ≈ (10.0 + 21.0 + 11.0) / 4.0
        @test ev[2].trade_count == 1 && ev[2].vwap == 9.0
        @test ev[3].trade_count == 2 && ev[3].volume == 2.0
        @test all(e.recv_ns == 0 for e in ev)
        # Waiting times between events are the gaps between instants.
        @test diff([e.time_ns for e in ev]) == [150, 150]
        # Exchange time decides the events, arrival order the fill sequence
        # within one: a tie split across the input is still one event.
        parts = [ts[4], ts[1], ts[5], ts[2], ts[6], ts[3]]
        @test event_bars(parts) == ev
        @test event_bars(Trade[]) == Bar[]
        @test_throws ArgumentError event_bars([
            mk(1, 1, 1.0, 1.0),
            mk(2, 1, 1.0, 1.0; sym = "ETHUSDT"),
        ])
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
        @test free_disk_gb(pwd()) > 0.0
        # feed delay drives the delayed-tape market-hours guard shifts
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

    @testset "snapshots: period boundaries on the exchange clock, final at close" begin
        P = 1_000_000_000                       # one second
        at(τ, i) = Trade("AAPL", τ, 0, 100.0, 1.0, "V", String[], "C", i)
        times = [500, 700, 2100, 2200, 5900] .* 1_000_000
        src = Channel{Trade}(10)
        foreach(((i, τ),) -> put!(src, at(τ, i)), enumerate(times))
        close(src)
        snaps = Tuple{Int64,Int,Bool}[]
        volume = 0.0
        n = snapshots(src, P; on_snapshot = (a, k, f) -> push!(snaps, (a, k, f))) do t
            volume += t.size
        end
        @test n == 5 && volume == 5.0
        @test snaps == [
            (1P, 2, false),
            (2P, 2, false),                      # a window without prints
            (3P, 4, false),
            (4P, 4, false),
            (5P, 4, false),
            (5_900_000_000, 5, true),
        ]
        # A late print folds into the open window; the boundary never retreats.
        src = Channel{Trade}(10)
        foreach(
            ((i, τ),) -> put!(src, at(τ, i)),
            enumerate([1500, 2500, 1800] .* 1_000_000),
        )
        close(src)
        empty!(snaps)
        snapshots(src, P; on_snapshot = (a, k, f) -> push!(snaps, (a, k, f))) do _
        end
        @test snaps == [(2P, 1, false), (1_800_000_000, 3, true)]
        # The receipt clock, and an empty stream makes no call.
        src = Channel{Trade}(10)
        put!(src, Trade("AAPL", 0, 3_200_000_000, 1.0, 1.0, "V", String[], "C", 1))
        close(src)
        empty!(snaps)
        snapshots(
            src,
            P;
            clock = :recv,
            on_snapshot = (a, k, f) -> push!(snaps, (a, k, f)),
        ) do _
        end
        @test snaps == [(3_200_000_000, 1, true)]
        src = Channel{Trade}(1)
        close(src)
        empty!(snaps)
        @test snapshots(
            identity,
            src,
            P;
            on_snapshot = (a, k, f) -> push!(snaps, (a, k, f)),
        ) == 0
        @test isempty(snaps)
        @test_throws ArgumentError snapshots(
            identity,
            Channel{Trade}(1),
            0;
            on_snapshot = identity,
        )
        @test_throws ArgumentError snapshots(
            identity,
            Channel{Trade}(1),
            P;
            on_snapshot = identity,
            clock = :wall,
        )
    end

    @testset "reconcile, lock, RAM guard, spill compaction, lossy tee" begin
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
            @test_throws SessionError acquire_session_lock(dir)
            close(l1)
            l2 = acquire_session_lock(dir)                   # released → reacquirable
            close(l2)
            # RAM ceiling fails loudly
            @test_throws ResourceError check_live_heap(0; context = "test")
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
            mem = compact_raw([sink.path], joinpath(dir, "mem"); format = :csv)
            spill = compact_raw(
                [sink.path],
                joinpath(dir, "spill");
                format = :csv,
                mem_fraction = 1e-12,
            )            # force the spill path
            @test length(mem) == length(spill) == 2
            for (m, s) in zip(mem, spill)
                @test CSV.read(m, DataFrame) == CSV.read(s, DataFrame)
            end
        end
        # The spill copy is the size of the input, so it goes beside out_dir,
        # never to tempdir() (a RAM-backed tmpfs on systemd distributions), and
        # is refused without room.
        @test MarketTickStreamer._spill_parent("/data/processed", nothing) == "/data/"
        @test MarketTickStreamer._spill_parent("/data/processed/", nothing) == "/data/"
        @test MarketTickStreamer._spill_parent("/data/processed", "/mnt/scratch") ==
              "/mnt/scratch"
        @test_throws ResourceError MarketTickStreamer._ensure_spill_space(
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
                format = :csv,
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
            w = example_call(mod, :consume, [sink.path])
            @test w.n_prints == 10
            @test length(w.gaps) == 9                  # one gap fewer than prints
            @test all(≈(0.001), w.gaps)                # 1 ms apart in exchange time
            @test w.n_nonpositive == 0

            pf = example_call(mod, :consume, [sink.path]; price_forming_only = true)
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
            d, persisted, seen = example_call(mod, :consume, [sink.path])
            @test persisted == 10                      # persistence is lossless
            @test seen <= persisted                    # the diagnostic tap may drop
            @test OnlineStats.nobs(d.Δt_mean) == seen - 1
            @test OnlineStats.value(d.Δt_mean) ≈ 0.001
            @test OnlineStats.value(d.Δt_extrema).max ≈ 0.001
            @test d.n_nonpositive == 0
            # Equal sizes make the VWAP the arithmetic mean of the prices seen.
            @test example_call(mod, :vwap, d) ≈ 101.0 + (seen - 1) / 2
            @test OnlineStats.value(d.volume) ≈ 10.0 * seen

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
                # Bin centres over five orders of magnitude: accurate to a bin
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
            @test length(
                collect(follow_source(raw; session = "$(sid2)_AAPL_2026-01-02")),
            ) == 2
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

    @testset "diagnostics: clock ticks, decimation, tail fit, formatting" begin
        M = MarketTickStreamer
        # HH:MM domain ticks
        vals, labels = M._hhmm_ticks(9.5, 16.0)
        @test labels[1] == "10:00" && labels[end] == "16:00"
        # a whole day is labeled every four hours, which fits a half-width panel
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

    @testset "config: paths follow the file, new tables, credentials file" begin
        mktempdir() do dir
            base = "[stream]\nsymbols = [\"A\"]\n"
            p = joinpath(dir, "conf", "my.toml")
            mkpath(dirname(p))
            # Relative paths are relative to the configuration file.
            write(
                p,
                base *
                "[storage]\ndata_dir = \"../d\"\n[logging]\nlog_dir = \"l\"\n" *
                "[credentials]\nenv_file = \"secrets/keys.env\"\n",
            )
            cfg = load_config(p)
            @test cfg.data_dir == joinpath(dir, "d")
            @test cfg.raw_dir == joinpath(dir, "d", "raw")
            @test cfg.log_dir == joinpath(dir, "conf", "l")
            @test cfg.env_file == joinpath(dir, "conf", "secrets", "keys.env")
            # Defaults sit beside the file too, and absolute paths are kept.
            write(p, base)
            cfg = load_config(p)
            @test cfg.data_dir == joinpath(dir, "conf", "data")
            @test cfg.env_file == joinpath(dir, "conf", ".env")
            write(p, base * "[storage]\ndata_dir = \"$(joinpath(dir, "abs"))\"\n")
            @test load_config(p).data_dir == joinpath(dir, "abs")
            # The same file read through a relative path resolves identically.
            cd(dirname(p)) do
                @test load_config("my.toml").data_dir == joinpath(dir, "abs")
            end

            # New keys: defaults, then every documented bound.
            write(p, base)
            cfg = load_config(p)
            @test (
                cfg.rest_request_timeout_s,
                cfg.rest_connect_timeout_s,
                cfg.rest_max_retries,
                cfg.rest_max_download_mb_s,
                cfg.rest_download_connections,
            ) == (30, 10, 5, 0.0, 1)
            @test MarketTickStreamer.RestPolicy(cfg).max_download_rate == Inf
            write(p, base * "[rest]\nmax_download_mb_s = 5\ndownload_connections = 4\n")
            @test MarketTickStreamer.RestPolicy(load_config(p)).max_download_rate == 5e6
            @test MarketTickStreamer.RestPolicy(load_config(p)).download_connections == 4
            @test cfg.max_open_spill_files == 256
            @test cfg.compact_mem_fraction == 0.5
            @test cfg.spill_headroom == 1.1
            @test cfg.compact_footprint_factor == 4.0
            @test cfg.processed_compression === :none
            write(
                p,
                base *
                "[storage]\nprocessed_format = \"arrow\"\nprocessed_compression = \"zstd\"\n",
            )
            @test load_config(p).processed_compression === :zstd
            for bad in (
                "[storage]\nprocessed_compression = \"zstd\"\n",          # csv cannot honor it
                "[storage]\nprocessed_format = \"arrow\"\nprocessed_compression = \"gzip\"\n",
                "[rest]\nrequest_timeout_s = 0\n",
                "[rest]\nrequest_timeout_s = 1.5\n",
                "[rest]\nconnect_timeout_s = 0\n",
                "[rest]\nmax_retries = -1\n",
                "[rest]\nretries = 3\n",
                "[rest]\nmax_download_mb_s = -1.0\n",
                "[rest]\nmax_download_mb_s = inf\n",
                "[rest]\nmax_download_mb_s = \"5\"\n",
                "[rest]\ndownload_connections = 0\n",
                "[rest]\ndownload_connections = 9\n",
                "[rest]\ndownload_connections = 2.0\n",
                "[limits]\nmax_open_spill_files = 0\n",
                "[limits]\ncompact_mem_fraction = 0.0\n",
                "[limits]\ncompact_mem_fraction = 1.5\n",
                "[limits]\nspill_headroom = 0.9\n",
                "[limits]\ncompact_footprint_factor = 0\n",
                "[credentials]\nenv_file = \"\"\n",
                "[credentials]\nenv_file = 3\n",
            )
                write(p, base * bad)
                @test_throws ArgumentError load_config(p)
            end

            # Credentials come from the file the configuration names.
            write(p, base * "[credentials]\nenv_file = \"keys.env\"\n")
            write(
                joinpath(dir, "conf", "keys.env"),
                "ALPACA_API_KEY_ID=key-from-file\nALPACA_SECRET_KEY=secret-from-file\n",
            )
            chmod(joinpath(dir, "conf", "keys.env"), 0o600)
            withenv("ALPACA_API_KEY_ID" => nothing, "ALPACA_SECRET_KEY" => nothing) do
                @test load_credentials!(load_config(p)) ==
                      ("key-from-file", "secret-from-file")
            end
        end
        # The shipped configuration is the working one in a clone, where its
        # relative paths land in the repository, and a template elsewhere.
        root = pkgdir(MarketTickStreamer)
        @test !MarketTickStreamer._installed_in_depot()
        @test MarketTickStreamer.DEFAULT_CONFIG == joinpath(root, "config", "config.toml")
        shipped = load_config()
        @test shipped.data_dir == joinpath(root, "data")
        @test shipped.log_dir == joinpath(root, "logs")
        @test shipped.env_file == joinpath(root, ".env")
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
        @test_throws Exception MarketTickStreamer._get_with_retry(
            "http://127.0.0.1:$dead/x",
            Pair{String,String}[];
            policy = MarketTickStreamer.RestPolicy(1, 1, 1),
        )
        @test time() - t0 >= 0.5                     # one backoff was taken
        # Backoff jitter is a deterministic function of the seed, within
        # [0.5, 1.5) of the capped exponential.
        d = [MarketTickStreamer._backoff_delay(Xoshiro(7), 1.0, k, 60.0) for k in 0:5]
        @test d ==
              [MarketTickStreamer._backoff_delay(Xoshiro(7), 1.0, k, 60.0) for k in 0:5]
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
                provider = MarketTickStreamer.make_provider(Val(:alpaca), cfg, "k", "s")
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
                    @info "colour \e[31mred\e[0m" note = "\e[1mbold\e[0m"
                end
            end
            close(ioa)
            close(iob)
            a = read(joinpath(cfg.log_dir, "a.log"), String)
            b = read(joinpath(cfg.log_dir, "b.log"), String)
            @test !occursin("socket teardown", a) && occursin("own record", a)
            @test occursin("socket teardown", b) && occursin("own record", b)
            @test occursin("colour red", b) && occursin("note=bold", b)
            @test !occursin("\e[", b)
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

    @testset "spill compaction within a file-handle budget" begin
        day_ns = 86_400 * 1_000_000_000
        mk(i, sym, day) = Trade(
            sym,
            1_753_886_600_000_000_000 + day * day_ns + i * 1_000_000,
            0,
            100.0 + i,
            1.0 * i,
            "V",
            ["@"],
            "C",
            i,
        )
        # Three symbols over two days, interleaved so that with two handles
        # every group is evicted and reopened many times.
        trades = [mk(i, sym, day) for i in 1:40 for day in 0:1 for sym in ("A", "B", "C")]
        mktempdir() do dir
            sink = open_raw_sink(dir, "lru")
            write_batch!(sink, trades)
            close_sink!(sink)
            mem = compact_raw([sink.path], joinpath(dir, "mem"); format = :csv)
            spill = compact_raw(
                [sink.path],
                joinpath(dir, "spill");
                format = :csv,
                mem_fraction = 1e-12,
                max_open_spill_files = 2,
            )
            @test length(mem) == length(spill) == 6
            @test all(read(a) == read(b) for (a, b) in zip(mem, spill))
            @test !any(startswith("compact_spill_"), readdir(dir))   # scratch removed
            pool = MarketTickStreamer.SpillPool(mktempdir(dir), 2)
            for key in
                (("A", Date(2026, 1, 1)), ("B", Date(2026, 1, 1)), ("C", Date(2026, 1, 1)))
                println(MarketTickStreamer._spill_handle!(pool, key), "x")
                @test length(pool.handles) <= 2
            end
            @test length(pool.seen) == 3
            @test !haskey(pool.handles, ("A", Date(2026, 1, 1)))      # least recently written
            MarketTickStreamer._close_all!(pool)
            @test isempty(pool.handles)
            for bad in (
                (; mem_fraction = 0.0),
                (; mem_fraction = 1.5),
                (; footprint_factor = 0.0),
                (; spill_headroom = 0.5),
                (; max_open_spill_files = 0),
            )
                @test_throws ArgumentError compact_raw(
                    [sink.path],
                    joinpath(dir, "x");
                    bad...,
                )
            end
            # The configuration-driven form files under the provider's calendar.
            p = joinpath(dir, "c.toml")
            write(
                p,
                "[provider]\nname = \"binance\"\nfeed = \"aggTrade\"\n[stream]\nsymbols = [\"A\"]\n" *
                "[storage]\ndata_dir = \"out\"\n[backfill]\nfeed = \"aggTrade\"\npage_limit = 1000\n" *
                "[limits]\nmax_open_spill_files = 2\n",
            )
            cfg = load_config(p)
            written = compact_raw(cfg, [sink.path])
            @test all(startswith(cfg.processed_dir), written)
            # 2025-07-30 14:43 UTC: one UTC date per day offset, as under New York here
            @test length(written) == 6
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
                @test prov["git_available"] ?
                      occursin(r"^[0-9a-f]{40}$", prov["git_commit"]) :
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
                meta = only(
                    filter(
                        f -> endswith(f, ".meta.toml"),
                        readdir(cfg.raw_dir; join = true),
                    ),
                )
                @test TOML.parsefile(meta)["session"]["status"] == "failed"
            finally
                close(ws)
                close(rest)
            end
        end
    end


    @testset "provider registry and a second provider" begin
        # Feed names, calendar and credentials come from each provider's spec,
        # never from one vendor's vocabulary.
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
        # No countable historical tape: coverage_report is refused, not a MethodError.
        @test_throws ArgumentError historical_trade_count(
            bp,
            "BTCUSDT",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        )

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
        t = MarketTickStreamer.parse_binance_trade(JSON.parse(JSON.json(f)), 99)
        @test t.symbol == "BTCUSDT"
        @test t.recv_ns == 99
        @test t.exchange == "BINANCE" && t.tape == "SPOT"
        @test t.id == 4
        # Prices and sizes arrive as strings and must survive as Float64.
        @test t.price ≈ 64_004.0 && t.size ≈ 0.004
        # Milliseconds on the wire by default, nanoseconds in the schema.
        @test t.time_ns == Int64(f.T) * 1_000_000
        @test t.time_ns % 1_000_000 == 0
        # The adapter asks for microseconds; a frame in either unit lands on
        # the same instant, and the stream URL carries the request.
        fu = mock_binance_frame("BTCUSDT", 4; unit = :us)
        @test fu.T == f.T * 1000
        @test MarketTickStreamer.parse_binance_trade(JSON.parse(JSON.json(fu)), 0).time_ns ==
              t.time_ns
        @test MarketTickStreamer._binance_ns(1_791_590_429_424) == 1_791_590_429_424_000_000
        @test MarketTickStreamer._binance_ns(1_791_590_429_424_532) ==
              1_791_590_429_424_532_000
        p0 = BinanceProvider("trade", "http://x", "ws://y")
        url = MarketTickStreamer.ws_url(p0, ["BTCUSDT", "ETHUSDT"])
        @test occursin("streams=btcusdt@trade/ethusdt@trade", url)
        @test endswith(url, "&timeUnit=MICROSECOND")
        # `m = true` means the buyer was the maker, so the seller crossed.
        @test t.conditions == (f.m ? ["sell"] : ["buy"])
        # k = 6 is even, so the mock sets m = false: the buyer crossed.
        @test MarketTickStreamer.parse_binance_trade(
            JSON.parse(JSON.json(mock_binance_frame("BTCUSDT", 6))),
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
        @test MarketTickStreamer.parse_binance_trade(JSON.parse(JSON.json(raw)), 0).id == 77
    end

    @testset "binance: id-seeded backfill pagination" begin
        rest_port = freeport(9811)
        per_day = 7
        units = String[]
        server =
            start_mock_binance_rest(rest_port; trades_per_day = per_day, seen_units = units)
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
            ).trades
            @test length(got) == per_day
            @test all(t -> trading_date(t.time_ns; tz = tz"UTC") == MOCK_BINANCE_EPOCH, got)
            @test issorted([t.time_ns for t in got])
            @test [t.id for t in got] == collect(1:per_day)
            @test all(t -> t.recv_ns == 0, got)        # never crossed the wire
            # Every request asked for microseconds, the mock answered in them,
            # and the instants are the tape's.
            @test !isempty(units) && all(==("MICROSECOND"), units)
            @test all(t -> t.time_ns == mock_binance_row(t.id, per_day).T * 1_000_000, got)

            # Two dates must give exactly two days of tape, weekend included.
            two = historical_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH,
                MOCK_BINANCE_EPOCH + Day(1);
                page_limit = 3,
                rate_sleep_s = 0.0,
            ).trades
            @test length(two) == 2 * per_day
            @test length(unique(trading_date(t.time_ns; tz = tz"UTC") for t in two)) == 2

            # Streaming mode counts the prints and retains none.
            pages = Vector{Int}()
            r = historical_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH,
                MOCK_BINANCE_EPOCH;
                page_limit = 3,
                rate_sleep_s = 0.0,
                each_page = pg -> push!(pages, length(pg)),
            )
            @test r.prints == per_day && isempty(r.trades)
            @test sum(pages) == per_day && length(pages) > 1

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
                meta = TOML.parsefile(
                    only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
                )
                @test meta["provenance"]["timestamp_unit"] == "microsecond"
            finally
                close(server)
            end
        end
    end

    @testset "binance: bulk dump parsing" begin
        d = Date(2026, 1, 1)
        # The reader's page state: `kept` holds the trades, `prints` counts them.
        parse_pages(text; feed = "aggTrade", kw...) = MarketTickStreamer._parse_bulk_csv(
            Vector{UInt8}(codeunits(text)),
            "BTCUSDT",
            d,
            feed;
            name = "x.csv",
            kw...,
        )
        parse_csv(text; kw...) = parse_pages(text; kw...).kept
        us = 1_767_225_600_000_000 + 5_000_000           # 2026-01-01T00:00:05Z, µs
        ms = 1_767_225_600_000 + 5_000                   # the same instant, ms
        agg = "7,64000.5,0.25,10,12,$us,True,True\n8,64001.0,0.5,13,13,$(us + 1_000),False,True\n"
        got = parse_csv(agg)
        @test length(got) == 2
        @test got[1] == Trade(
            "BTCUSDT",
            1_767_225_605_000_000_000,
            0,
            64000.5,
            0.25,
            "BINANCE",
            ["sell"],
            "SPOT",
            7,
        )
        @test got[2].conditions == ["buy"]
        @test got[2].time_ns == 1_767_225_605_001_000_000 && got[2].id == 8
        # Millisecond timestamps, the unit before 2025-01-01, read to the same instant.
        @test parse_csv("7,64000.5,0.25,10,12,$ms,true,True\n")[1].time_ns == got[1].time_ns
        # A header line is skipped; the trades feed has seven columns.
        header = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker,is_best_match\n"
        @test length(parse_csv(header * agg)) == 2
        tr = parse_csv("10,64000.5,0.145,9280.07,$us,False,True\n"; feed = "trade")
        @test tr[1].id == 10 && tr[1].size == 0.145 && tr[1].conditions == ["buy"]
        # Paging: the count comes back and every page reaches the callback.
        pages = Vector{Int}()
        r = parse_pages(agg^3; each_page = pg -> push!(pages, length(pg)), page_rows = 4)
        @test r.prints == 6 && isempty(r.kept) && pages == [4, 2]
        # A wrong column count and a row outside the day are errors, not silence.
        @test_throws IntegrityError parse_csv("7,64000.5,0.25,$us,True\n")
        @test_throws IntegrityError parse_csv(
            "7,64000.5,0.25,10,12,$(us + 86_400_000_000),True,True\n",
        )
    end

    @testset "binance: bulk files match the REST tape, are verified and kept" begin
        mktempdir() do dir
            rest_port = freeport(9871)
            per_day = 7
            server = start_mock_binance_rest(
                rest_port;
                trades_per_day = per_day,
                missing_dates = [MOCK_BINANCE_EPOCH + Day(2)],
            )
            base = "http://127.0.0.1:$rest_port"
            dest = joinpath(dir, "bulk")
            try
                p = BinanceProvider("aggTrade", base, "ws://unused", base)
                @test MarketTickStreamer.bulk_url(p, "btcusdt", MOCK_BINANCE_EPOCH) ==
                      "$base/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-01-01.zip"
                @test_throws ArgumentError MarketTickStreamer.bulk_url(
                    p,
                    "BTCUSDT",
                    MOCK_BINANCE_EPOCH;
                    feed = "kline",
                )
                b = bulk_trades(p, "BTCUSDT", MOCK_BINANCE_EPOCH; dest_dir = dest)
                rest = historical_trades(
                    p,
                    "BTCUSDT",
                    MOCK_BINANCE_EPOCH,
                    MOCK_BINANCE_EPOCH;
                    page_limit = 3,
                    rate_sleep_s = 0.0,
                ).trades
                @test b.trades == rest && b.prints == length(rest)
                zip =
                    joinpath(dest, "binance", "BTCUSDT", "BTCUSDT-aggTrades-2026-01-01.zip")
                @test b.file == zip
                @test isfile(zip) && isfile(zip * ".CHECKSUM") && !isfile(zip * ".partial")
                # An interrupted download resumes from its partial file with a
                # Range request and still passes the checksum.
                full = read(zip)
                rm(zip)
                write(zip * ".partial", full[1:(length(full)÷2)])
                @test bulk_trades(p, "BTCUSDT", MOCK_BINANCE_EPOCH; dest_dir = dest).trades ==
                      rest
                @test read(zip) == full && !isfile(zip * ".partial")
                # The trades feed is the same tape under its own column layout.
                tr = bulk_trades(
                    p,
                    "BTCUSDT",
                    MOCK_BINANCE_EPOCH + Day(1);
                    feed = "trade",
                    dest_dir = dest,
                ).trades
                @test length(tr) == per_day
                @test [t.id for t in tr] == collect((per_day+1):2per_day)
                @test isfile(
                    joinpath(dest, "binance", "BTCUSDT", "BTCUSDT-trades-2026-01-02.zip"),
                )
                # Streaming counts the prints and keeps none; an unpublished
                # day is empty, names no file and leaves none behind.
                pages = Vector{Int}()
                r = bulk_trades(
                    p,
                    "BTCUSDT",
                    MOCK_BINANCE_EPOCH;
                    dest_dir = dest,
                    each_page = pg -> push!(pages, length(pg)),
                    page_rows = 3,
                )
                @test r.prints == per_day && isempty(r.trades) && pages == [3, 3, 1]
                gone = MOCK_BINANCE_EPOCH + Day(2)
                @test bulk_trades(p, "BTCUSDT", gone; dest_dir = dest) ==
                      (; trades = Trade[], prints = 0, file = "")
                @test bulk_trades(
                    p,
                    "BTCUSDT",
                    gone;
                    dest_dir = dest,
                    each_page = identity,
                ).prints == 0
                @test !isfile(
                    joinpath(
                        dest,
                        "binance",
                        "BTCUSDT",
                        "BTCUSDT-aggTrades-2026-01-03.zip",
                    ),
                )
            finally
                close(server)
            end
            # A verified file is reused with the venue unreachable.
            p = BinanceProvider("aggTrade", base, "ws://unused", base, RestPolicy(1, 1, 0))
            @test bulk_trades(p, "BTCUSDT", MOCK_BINANCE_EPOCH; dest_dir = dest).prints ==
                  per_day
            # A checksum that does not match discards the download and fails the day.
            rest_port = freeport(9881)
            server = start_mock_binance_rest(
                rest_port;
                trades_per_day = per_day,
                corrupt_checksum = true,
            )
            try
                base = "http://127.0.0.1:$rest_port"
                p = BinanceProvider("aggTrade", base, "ws://unused", base)
                bad = joinpath(dir, "bad")
                @test_throws IntegrityError bulk_trades(
                    p,
                    "BTCUSDT",
                    MOCK_BINANCE_EPOCH;
                    dest_dir = bad,
                )
                @test !isfile(
                    joinpath(bad, "binance", "BTCUSDT", "BTCUSDT-aggTrades-2026-01-01.zip"),
                )
            finally
                close(server)
            end
        end
    end

    @testset "binance: end-to-end bulk backfill, then resume" begin
        mktempdir() do dir
            rest_port = freeport(9891)
            per_day = 7
            # The second day's file repeats its first three rows, as a real
            # vendor file did; compaction drops them and the sidecar says so.
            repeated = MOCK_BINANCE_EPOCH + Day(1)
            server = start_mock_binance_rest(
                rest_port;
                trades_per_day = per_day,
                repeat_head = Dict(repeated => 3),
            )
            try
                cfg = load_config(
                    mock_binance_config_toml(
                        dir;
                        ws_port = 9999,
                        rest_port = rest_port,
                        source = "bulk",
                        backfill_feed = "trade",
                        start_date = string(MOCK_BINANCE_EPOCH),
                        end_date = string(MOCK_BINANCE_EPOCH + Day(2)),
                    ),
                )
                @test cfg.backfill_source === :bulk && cfg.backfill_feed == "trade"
                files = run_backfill(cfg)
                @test length(files) == 3
                rows = CSV.read(first(sort(files)), DataFrame)
                @test nrow(rows) == per_day
                @test rows.id == collect(1:per_day) && all(rows.recv_ns .== 0)
                @test all(rows.tape .== "SPOT") && all(rows.exchange .== "BINANCE")
                @test all(
                    isfile(
                        joinpath(
                            cfg.bulk_dir,
                            "binance",
                            "BTCUSDT",
                            "BTCUSDT-trades-$(MOCK_BINANCE_EPOCH + Day(i)).zip",
                        ),
                    ) for i in 0:2
                )
                raw = filter(f -> endswith(f, ".jsonl"), readdir(cfg.raw_dir))
                @test !isempty(raw) && all(startswith("backfill_bulk_"), raw)
                rep = CSV.read(
                    joinpath(cfg.processed_dir, "BTCUSDT", "$(repeated).csv"),
                    DataFrame,
                )
                @test nrow(rep) == per_day && allunique(rep.id)
                meta = TOML.parsefile(
                    only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
                )
                @test meta["session"]["ticks"] == 3 * per_day + 3        # received
                @test meta["session"]["ticks_processed"] == 3 * per_day  # stored
                # Resume: every day is under processed/, nothing is redone.
                @test isempty(run_backfill(cfg))
            finally
                close(server)
            end
        end
    end

    @testset "binance: bulk backfill without the raw copy" begin
        mktempdir() do dir
            rest_port = freeport(9911)
            per_day = 7
            days = [MOCK_BINANCE_EPOCH + Day(i) for i in 0:1]
            toml(; resume) = mock_binance_config_toml(
                dir;
                ws_port = 9999,
                rest_port,
                source = "bulk",
                backfill_feed = "trade",
                start_date = string(first(days)),
                end_date = string(last(days)),
                keep_raw = false,
                resume,
            )
            server = start_mock_binance_rest(rest_port; trades_per_day = per_day)
            cfg = load_config(toml(; resume = true))
            local files
            try
                files = run_backfill(cfg)
            finally
                close(server)
            end
            @test length(files) == 2
            @test all(f -> nrow(CSV.read(f, DataFrame)) == per_day, files)
            # Only the sidecar and its manifest remain in raw/; the vendor
            # files stay as the record of each day.
            @test isempty(filter(endswith(".jsonl"), readdir(cfg.raw_dir)))
            meta = TOML.parsefile(
                only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
            )
            @test meta["session"]["ticks_processed"] == 2 * per_day
            @test isempty(meta["session"]["raw_files"])
            @test all(
                isfile(
                    joinpath(cfg.bulk_dir, "binance", "BTCUSDT", "BTCUSDT-trades-$d.zip"),
                ) for d in days
            )
            # With the server gone, a rerun that does not resume rebuilds every
            # day from the kept files; the displaced outputs become backups.
            rebuilt = run_backfill(load_config(toml(; resume = false)))
            @test sort(rebuilt) == sort(files)
            backup(f) = string(first(splitext(f)), "_#1", last(splitext(f)))
            @test all(
                f ->
                    isfile(backup(f)) &&
                    CSV.read(f, DataFrame) == CSV.read(backup(f), DataFrame),
                rebuilt,
            )
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

    @testset "binance: a stream error frame is fatal, no retry storm" begin
        mktempdir() do dir
            ws_port = freeport(9852)
            frames = [mock_binance_frame("BTCUSDT", k) for k in 1:5]
            server = start_mock_binance_ws(ws_port; frames = frames)
            try
                cfg = load_config(
                    mock_binance_config_toml(
                        dir;
                        ws_port = ws_port,
                        rest_port = 9999,
                        symbols = ["BTCUSDT"],
                        reconnect_max_retries = 3,
                    ),
                )
                s = live_source(BinanceProvider(cfg), cfg)
                got = collect(s.channel)
                @test length(got) == 5
                # The first connection closes after its frames (retryable); the
                # second answers with the error frame: one reconnect, then abort.
                @test s.stats[].reconnects == 1
                @test s.outcome[] === :failed
            finally
                close(server)
            end
        end
    end

    @testset "itch50: executions priced off the book, crosses, odd lots, breaks" begin
        be(x, n) = UInt8[(UInt64(x) >> (8 * (n - k))) & 0xff for k in 1:n]
        alpha(s, n) = UInt8.(collect(rpad(s, n)))
        msg(type, locate, ts, body) = (
            m = vcat(UInt8(type), be(locate, 2), be(0, 2), be(ts, 6), body);
            vcat(be(length(m), 2), m)
        )
        R(loc, stock, cat, lot) = msg(
            'R',
            loc,
            0,
            vcat(
                alpha(stock, 8),
                UInt8(cat),
                UInt8(' '),
                be(lot, 4),
                UInt8('N'),
                UInt8(' '),
                alpha("", 2),
                UInt8('P'),
                alpha("", 4),
                be(0, 4),
                UInt8('N'),
            ),
        )
        A(loc, ts, ref, side, sh, stock, px) = msg(
            'A',
            loc,
            ts,
            vcat(be(ref, 8), UInt8(side), be(sh, 4), alpha(stock, 8), be(px, 4)),
        )
        F(loc, ts, ref, side, sh, stock, px) = msg(
            'F',
            loc,
            ts,
            vcat(
                be(ref, 8),
                UInt8(side),
                be(sh, 4),
                alpha(stock, 8),
                be(px, 4),
                alpha("NSDQ", 4),
            ),
        )
        E(loc, ts, ref, sh, match) =
            msg('E', loc, ts, vcat(be(ref, 8), be(sh, 4), be(match, 8)))
        C(loc, ts, ref, sh, match, printable, px) = msg(
            'C',
            loc,
            ts,
            vcat(be(ref, 8), be(sh, 4), be(match, 8), UInt8(printable), be(px, 4)),
        )
        X(loc, ts, ref, sh) = msg('X', loc, ts, vcat(be(ref, 8), be(sh, 4)))
        D(loc, ts, ref) = msg('D', loc, ts, be(ref, 8))
        U(loc, ts, orig, new, sh, px) =
            msg('U', loc, ts, vcat(be(orig, 8), be(new, 8), be(sh, 4), be(px, 4)))
        P(loc, ts, sh, stock, px, match) = msg(
            'P',
            loc,
            ts,
            vcat(be(0, 8), UInt8('B'), be(sh, 4), alpha(stock, 8), be(px, 4), be(match, 8)),
        )
        Q(loc, ts, sh, stock, px, match, cross) = msg(
            'Q',
            loc,
            ts,
            vcat(be(sh, 8), alpha(stock, 8), be(px, 4), be(match, 8), UInt8(cross)),
        )
        B(loc, ts, match) = msg('B', loc, ts, be(match, 8))
        S(event) = msg('S', 0, 0, [UInt8(event)])
        H(loc) = msg('H', loc, 0, zeros(UInt8, 14))                    # skipped by length
        @test length(R(1, "AAPL", 'Q', 100)) == 41 &&
              length(A(1, 0, 1, 'B', 1, "A", 1)) == 38
        @test length(E(1, 0, 1, 1, 1)) == 33 && length(Q(1, 0, 1, "A", 1, 1, 'O')) == 42
        # Market Category → tape: Nasdaq C, NYSE A, NYSE American and the other
        # exchanges B (as the SIP itself stamps them), unknown categories "".
        @test [MarketTickStreamer._itch50_tape(UInt8(c)) for c in "QGSNAPZVM "] == ["C", "C", "C", "A", "B", "B", "B", "B", "", ""]
        T0 = 34_200_000_000_000                                        # 09:30 ET in ns
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
            @test got[3] == Trade(
                "IBM",
                day0 + T0 + 9,
                0,
                130.025,
                75.0,
                "Q",
                ["hidden", "I"],
                "A",
                1006,
            )
            @test got[4] == Trade(
                "SPY",
                day0 + T0 + 11,
                0,
                320.0,
                1.0e6,
                "Q",
                ["cross_close"],
                "B",
                1008,
            )
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
            # a gzip member cut short, as a partial download is: read to the cut, reported
            gzcut = joinpath(dir, "cut.gz")
            write(gzcut, read(gz)[1:(end-12)])
            gc = itch50_trades(gzcut, date; broken = :keep)
            @test gc.truncated && gc.messages <= 25
            @test_throws ArgumentError itch50_trades(raw, date; broken = :void)
            @test_throws ArgumentError itch50_trades(raw, date; page_rows = 0)
        end
        @test itch50_date("12302019.NASDAQ_ITCH50.gz") == Date(2019, 12, 30)
        @test itch50_date("/x/S112825-v50.txt.gz") == Date(2025, 11, 28)
        @test_throws ArgumentError itch50_date("itch50_05_15.gz")
    end

    @testset "iex hist: pcap and pcapng framing, IEX-TP sessions, trade reports and breaks" begin
        le(x, n) = (
            u = x < 0 ? reinterpret(UInt64, Int64(x)) : UInt64(x);
            UInt8[(u >> (8 * (k - 1))) & 0xff for k in 1:n]
        )
        be(x, n) = UInt8[(UInt64(x) >> (8 * (n - k))) & 0xff for k in 1:n]
        sym(s) = UInt8.(collect(rpad(s, 8)))
        S(ev, ts) = vcat(UInt8('S'), UInt8(ev), le(ts, 8))
        Qm(s, ts) = vcat(
            UInt8('Q'),
            0x00,
            le(ts, 8),
            sym(s),
            le(100, 4),
            le(1_000_000, 8),
            le(1_000_100, 8),
            le(100, 4),
        )
        T(s, flags, ts, size, px, id; reserved = 0) = vcat(
            UInt8('T'),
            UInt8(flags),
            le(ts, 8),
            sym(s),
            le(size, 4),
            le(px, 8),
            le(id, 8),
            zeros(UInt8, reserved),
        )
        B(s, ts, id) =
            vcat(UInt8('B'), 0x00, le(ts, 8), sym(s), le(1, 4), le(0, 8), le(id, 8))
        function segment(proto, session, seq, msgs; send = 0)
            payload =
                isempty(msgs) ? UInt8[] :
                reduce(vcat, [vcat(le(length(m), 2), m) for m in msgs])
            return vcat(
                0x01,
                0x00,
                le(proto, 2),
                le(1, 4),
                le(session, 4),
                le(length(payload), 2),
                le(length(msgs), 2),
                le(0, 8),
                le(seq, 8),
                le(send, 8),
                payload,
            )
        end
        function frame(seg; vlan = false)
            udp = vcat(be(10377, 2), be(10377, 2), be(8 + length(seg), 2), be(0, 2), seg)
            ip = vcat(
                0x45,
                0x00,
                be(20 + length(udp), 2),
                be(0, 2),
                be(0, 2),
                0x40,
                0x11,
                be(0, 2),
                UInt8[23, 226, 155, 131],
                UInt8[233, 215, 21, 3],
                udp,
            )
            eth = vcat(
                UInt8[0x01, 0x00, 0x5e, 0x57, 0x15, 0x03],
                UInt8[0, 0x11, 0x22, 0x33, 0x44, 0x55],
            )
            return vlan ? vcat(eth, be(0x8100, 2), be(1017, 2), be(0x0800, 2), ip) :
                   vcat(eth, be(0x0800, 2), ip)
        end
        pad4(v) = vcat(v, zeros(UInt8, (4 - length(v) % 4) % 4))
        shb() = vcat(
            le(0x0A0D0D0A, 4),
            le(28, 4),
            le(0x1A2B3C4D, 4),
            le(1, 2),
            le(0, 2),
            le(-1, 8),
            le(28, 4),
        )
        idb() = vcat(le(1, 4), le(20, 4), le(1, 2), le(0, 2), le(65535, 4), le(20, 4))
        function epb(data)
            body = vcat(
                le(0, 4),
                le(0, 4),
                le(0, 4),
                le(length(data), 4),
                le(length(data), 4),
                pad4(data),
            )
            return vcat(
                le(6, 4),
                le(8 + length(body) + 4, 4),
                body,
                le(8 + length(body) + 4, 4),
            )
        end
        pcap_header() = vcat(
            le(0xa1b2c3d4, 4),
            le(2, 2),
            le(4, 2),
            le(0, 4),
            le(0, 4),
            le(65535, 4),
            le(1, 4),
        )
        pcap_record(data) =
            vcat(le(0, 4), le(0, 4), le(length(data), 4), le(length(data), 4), data)
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

    @testset "fetch: resumable Range downloads, announced sizes, CRC-32C" begin
        payload = rand(Xoshiro(3), UInt8, 3 * 1024 * 1024 + 123)
        crc = string(crc32c(payload); base = 16, pad = 8)
        crc_b64 =
            base64encode([UInt8((crc32c(payload) >> (8 * (3 - k))) & 0xff) for k in 0:3])
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
        server = HTTP.serve!(router, "127.0.0.1", port)
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
                @test fetch_itch50(url, dir; policy) == p &&
                      ranges == ["bytes=1000000-9388607"]   # reused
                @test fetch_itch50("http://127.0.0.1:$port/f/missing.gz", dir; policy) ===
                      nothing
                # IEX: index, feed selection, download, CRC-32C
                idx = iex_hist_index(Date(2020, 1, 2); api_base = api, policy)
                @test [e["feed"] for e in idx] == ["DEEP", "TOPS"]
                @test iex_hist_index(Date(2020, 1, 3); api_base = api, policy) === nothing
                empty!(ranges)
                q = fetch_iex_hist(
                    Date(2020, 1, 2),
                    dir;
                    feed = "tops",
                    api_base = api,
                    policy,
                )
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
                @test fetch_iex_hist(Date(2020, 1, 3), dir; api_base = api, policy) ===
                      nothing
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
                @test MarketTickStreamer._content_range_start(
                    "bytes 1000000-2097151/3145851",
                ) == 1_000_000
                @test MarketTickStreamer._content_range_total("bytes 0-9/3145851") ==
                      3145851
                @test MarketTickStreamer._content_range_total("") == -1
                @test MarketTickStreamer._chunk_timeout(policy, 8 << 20) ==
                      cld(8 << 20, 50_000)
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
                r5 = MarketTickStreamer._download_resumable(
                    url,
                    pp,
                    par;
                    chunk_bytes = 1 << 20,
                )
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
                slow = MarketTickStreamer.RestPolicy(10, 5, 1; max_download_rate = 10_000)
                @test MarketTickStreamer._chunk_timeout(slow, 8 << 20) ==
                      cld(8 << 20, 10_000)
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
        be(x, n) = UInt8[(UInt64(x) >> (8 * (n - k))) & 0xff for k in 1:n]
        alpha(s, n) = UInt8.(collect(rpad(s, n)))
        msg(type, locate, ts, body) = (
            m = vcat(UInt8(type), be(locate, 2), be(0, 2), be(ts, 6), body);
            vcat(be(length(m), 2), m)
        )
        R(loc, stock) = msg(
            'R',
            loc,
            0,
            vcat(
                alpha(stock, 8),
                UInt8('Q'),
                UInt8(' '),
                be(100, 4),
                UInt8('N'),
                UInt8(' '),
                alpha("", 2),
                UInt8('P'),
                alpha("", 4),
                be(0, 4),
                UInt8('N'),
            ),
        )
        A(loc, ts, ref, sh, px) = msg(
            'A',
            loc,
            ts,
            vcat(be(ref, 8), UInt8('S'), be(sh, 4), alpha("", 8), be(px, 4)),
        )
        E(loc, ts, ref, sh, match) =
            msg('E', loc, ts, vcat(be(ref, 8), be(sh, 4), be(match, 8)))
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
            @test length(meta["session"]["raw_files"]) == 1
            # a second import of one symbol without the raw copy: processed again (safesave
            # keeps the first), raw parts gone, the sidecar lists none. Session ids
            # are second-resolved, so a second import in the same second would
            # share the first one's prefix.
            sleep(1.1)
            # the day exists: refused unless the displacement is explicit
            @test_throws ArgumentError run_import(
                cfg,
                file;
                format = :itch50,
                symbols = ["AAPL"],
            )
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
        if JET_ENABLED
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
        else
            @test_skip "JET analyses of the extensions (JET unavailable)"
        end
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
        # intermediates carry their own mantissa, never the rounded power of ten
        v, l = E._log_ticks(3e-6, 2e-4)
        @test allunique(string.(l))
        @test any(x -> occursin("2\\times10^{-5}", string(x)), l)
        @test E._pow10_label(5, 0) == "5" && E._pow10_label(2, 1) == "20"
        @test E._pow10_label(2, -1) == "0.2" && E._pow10_label(1, 1) == "10"
        v, l = E._log_ticks(1.3e-4, 1.0)                     # four orders: 10^k only
        @test length(v) == 4 && allunique(string.(l))
        # one exponent per axis: the rate is rescaled, not the tick labels
        @test E._rate_scale(2.2e4)[1] == 1e3
        @test E._rate_scale(800)[1] == 1.0
    end

    @testset "figures: theme, layout, session window, files" begin
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
