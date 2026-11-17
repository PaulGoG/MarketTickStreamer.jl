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

# Ports unlikely to collide; the next free one within 200 of `start`.
function freeport(start)
    last = min(start + 200, 65535)
    for p in start:last
        try
            s = HTTP.Sockets.listen(HTTP.Sockets.ip"127.0.0.1", p)
            close(s)
            return p
        catch e
            e isa Base.IOError || rethrow()
        end
    end
    error("no free port in $start:$last")
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

# One file per subsystem. `Pkg.test(; test_args = ["live", "vendor"])` runs
# only the files named; with no arguments every file runs, followed by the
# extensions.
const TEST_FILES = [
    "qa",
    "schema",
    "config",
    "storage",
    "sources",
    "quality",
    "analysis",
    "examples",
    "sessions",
    "live",
    "providers",
    "vendor",
    "extensions",
]
const SELECTED = isempty(ARGS) ? TEST_FILES : ARGS
for name in SELECTED
    name in TEST_FILES || throw(ArgumentError("no test file $name; one of $TEST_FILES"))
end

@testset "MarketTickStreamer" begin
    for name in SELECTED
        name == "extensions" || include("$name.jl")
    end
end

# The extensions load with their trigger packages, after everything above has
# run without them, which is the state a consumer without plotting is in.
"extensions" in SELECTED && include("extensions.jl")
