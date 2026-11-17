# Benchmarks for the acquisition hot paths: timestamp parsing, per-line
# serialization, sink throughput, compaction, replay, the corpus read
# (`read_processed`), the quality report and the vendor day-file readers.
# These live outside the test suite so the suite stays fast.
#
#     julia bench/benchmarks.jl
include(joinpath(@__DIR__, "activate.jl"))

using BenchmarkTools
using Logging: NullLogger, with_logger
using MarketTickStreamer
using MarketTickStreamer: open_raw_sink, write_batch!, close_sink!
# The byte writers of the reader tests build the vendor files below.
include(joinpath(@__DIR__, "..", "test", "vendor_bytes.jl"))

const suite = BenchmarkGroup()

# Deterministic synthetic tape — no RNG, so runs compare directly.
make_trade(i) = Trade(
    iseven(i) ? "AAPL" : "MSFT",
    1_753_886_600_000_000_000 + i * 1_000_000,
    1_753_886_600_100_000_000 + i * 1_000_000,
    100.0 + (i % 97) / 100,
    100.0 * (1 + i % 13),
    "V",
    ["@", "I"],
    "C",
    i,
)

const TRADES = [make_trade(i) for i in 1:10_000]
const DUPED = vcat(TRADES, TRADES[1:1000])
const STAMP = "2026-07-30T14:30:00.123456789Z"
const STAMP_NS = rfc3339_to_ns(STAMP)
const LINE = trade_to_json(TRADES[1])
# The same content as a `SubString` of a large parent. A `SubString` line must
# parse at the cost of a `String`; the pair keeps a regression on that visible.
const PARENT = join((trade_to_json(t) for t in TRADES), '\n')
const SUBLINE = SubString(PARENT, 1, ncodeunits(LINE))

# One raw session file, reused by every file-driven benchmark below.
const BENCH_DIR = mktempdir()
const RAW_FILE = let sink = open_raw_sink(BENCH_DIR, "bench")
    write_batch!(sink, TRADES)
    close_sink!(sink)
    sink.path
end
# One processed Arrow day of the synthetic tape: the file every query reads.
const PROCESSED_FILE = with_logger(NullLogger()) do
    first(compact_raw([RAW_FILE], joinpath(BENCH_DIR, "processed"); format = :arrow))
end

# One synthetic ITCH 5.0 day: two symbols, 25 000 orders, each added, then
# executed at its display price (E), at another price (C) and beside a hidden
# trade (P): 100 004 messages, 75 000 prints. Uncompressed, so the reader's
# decoding is measured and not gzip.
const ITCH_FILE = let I = ITCHBytes
    T0 = 34_200_000_000_000                    # 09:30 New York, ns since midnight
    io = IOBuffer()
    write(io, I.S('O'), I.R(1, "AAPL", 'Q', 100), I.R(2, "MSFT", 'Q', 100))
    for k in 1:25_000
        loc, stock = isodd(k) ? (1, "AAPL") : (2, "MSFT")
        px = 1_000_000 + 100 * (k % 50)
        ts = T0 + 1_000 * k
        write(
            io,
            I.A(loc, ts, k, 'B', 300, stock, px),
            I.E(loc, ts + 1, k, 100, 3k),
            I.C(loc, ts + 2, k, 100, 3k + 1, 'Y', px + 100),
            I.P(loc, ts + 3, 100, stock, px, 3k + 2),
        )
    end
    write(io, I.S('C'))
    path = joinpath(BENCH_DIR, "12302019.NASDAQ_ITCH50")
    write(path, take!(io))
    path
end
const ITCH_DATE = itch50_date(ITCH_FILE)
# One synthetic IEX HIST capture: pcapng, TOPS 1.6, 5 000 segments of ten
# Trade Reports each, 50 000 prints.
const IEX_FILE = let I = IEXBytes
    t0 = 1_577_975_400_000_000_000             # 2020-01-02 14:30 UTC
    io = IOBuffer()
    write(io, I.shb(), I.idb())
    for s in 1:5_000
        msgs = [
            I.T(
                isodd(j) ? "AAPL" : "MSFT",
                0x00,
                t0 + 10s + j,
                100,
                1_500_000 + j,
                10s + j,
            ) for j in 0:9
        ]
        write(io, I.epb(I.frame(I.segment(0x8003, 7, 10 * (s - 1) + 1, msgs))))
    end
    path = joinpath(BENCH_DIR, "day.pcapng")
    write(path, take!(io))
    path
end

suite["schema"] = BenchmarkGroup()
suite["schema"]["rfc3339_to_ns"] = @benchmarkable rfc3339_to_ns($STAMP)
suite["schema"]["ns_to_rfc3339"] = @benchmarkable ns_to_rfc3339($STAMP_NS)
suite["schema"]["trading_date"] = @benchmarkable trading_date($STAMP_NS)

suite["serialization"] = BenchmarkGroup()
suite["serialization"]["trade_to_json"] = @benchmarkable trade_to_json($(TRADES[1]))
suite["serialization"]["json_to_trade"] = @benchmarkable json_to_trade($LINE)
suite["serialization"]["json_to_trade_substring"] = @benchmarkable json_to_trade($SUBLINE)

suite["sink"] = BenchmarkGroup()
# Batch write of 10k prints, sink construction and teardown excluded.
suite["sink"]["write_batch_10k"] = @benchmarkable(
    write_batch!(sink, $TRADES),
    setup = (dir = mktempdir(); sink = open_raw_sink(dir, "bench")),
    teardown = (close_sink!(sink); rm(dir; recursive = true, force = true)),
    samples = 20,
    evals = 1,
)

suite["compaction"] = BenchmarkGroup()
suite["compaction"]["read_raw_10k"] = @benchmarkable read_raw([$RAW_FILE])
suite["compaction"]["deduplicate_11k"] = @benchmarkable deduplicate_trades($DUPED)
suite["compaction"]["compact_csv_10k"] = @benchmarkable(
    compact_raw([$RAW_FILE], out; format = :csv),
    setup = (out = mktempdir()),
    teardown = (rm(out; recursive = true, force = true)),
    samples = 10,
    evals = 1,
)

suite["replay"] = BenchmarkGroup()
# Unpaced drain: the ceiling on how fast a consumer can be fed from disk.
suite["replay"]["max_rate_10k"] = @benchmarkable(
    length(collect(replay_source([$RAW_FILE]; pace = :max))),
    samples = 10,
    evals = 1,
)

suite["corpus"] = BenchmarkGroup()
# Materializing one processed day: the cost every query pays per day and file.
suite["corpus"]["read_processed_arrow_5k"] =
    @benchmarkable read_processed([$PROCESSED_FILE])

suite["quality"] = BenchmarkGroup()
suite["quality"]["session_report_10k"] =
    @benchmarkable(session_report([$RAW_FILE]), samples = 10, evals = 1,)

suite["vendor"] = BenchmarkGroup()
suite["vendor"]["itch50_100k_messages"] =
    @benchmarkable(itch50_trades($ITCH_FILE, $ITCH_DATE), samples = 10, evals = 1)
suite["vendor"]["iex_hist_50k_prints"] =
    @benchmarkable(iex_hist_trades($IEX_FILE), samples = 10, evals = 1)

# Run only when invoked as a script. Logging is muted: the benchmarked
# functions log, and their records would drown the progress output.
if abspath(PROGRAM_FILE) == @__FILE__
    println("Running benchmarks...")
    try
        results = with_logger(NullLogger()) do
            run(suite; verbose = true)
        end
        display(results)
        println()
    finally
        rm(BENCH_DIR; recursive = true, force = true)
    end
end
