# Benchmarks for the acquisition hot paths: timestamp parsing, per-line
# serialization, sink throughput, compaction, replay and the quality report.
# These live outside the test suite so the suite stays fast.
#
#     julia bench/benchmarks.jl
include(joinpath(@__DIR__, "activate.jl"))

using BenchmarkTools
using Logging: NullLogger, with_logger
using MarketTickStreamer

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
# The same content as a `SubString` of a large parent. JSON3 parsing a
# SubString of a big String was measured at roughly 2000x the per-line cost,
# which is why `json_to_trade` materializes its argument first; the pair keeps
# that regression visible.
const PARENT = join((trade_to_json(t) for t in TRADES), '\n')
const SUBLINE = SubString(PARENT, 1, ncodeunits(LINE))

# One raw session file, reused by every file-driven benchmark below.
const BENCH_DIR = mktempdir()
const RAW_FILE = let sink = open_raw_sink(BENCH_DIR, "bench")
    write_batch!(sink, TRADES)
    close_sink!(sink)
    sink.path
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
    compact_raw([$RAW_FILE], out; format = "csv"),
    setup = (out = mktempdir()),
    teardown = (rm(out; recursive = true, force = true)),
    samples = 10,
    evals = 1,
)

suite["replay"] = BenchmarkGroup()
# Unpaced drain: the ceiling on how fast a consumer can be fed from disk.
suite["replay"]["max_rate_10k"] = @benchmarkable(
    length(collect(replay_source([$RAW_FILE]; pace = "max"))),
    samples = 10,
    evals = 1,
)

suite["quality"] = BenchmarkGroup()
suite["quality"]["session_report_10k"] =
    @benchmarkable(session_report([$RAW_FILE]), samples = 10, evals = 1,)

# Run only when invoked as a script; the benchmarked functions log, and that
# chatter would drown the progress output.
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
