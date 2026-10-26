# Replay a recorded session as a live-like stream.
#
#   julia scripts/replay.jl <raw_file.jsonl>... | <YYYY-MM-DD.csv|.arrow>... [--config path/to/config.toml]
#
# Prints each tick at the configured pace ([replay] in config.toml). This is
# the template for attaching real-time analysis consumers: swap the printing
# loop for any function of `Channel{Trade}`.

include(joinpath(@__DIR__, "activate.jl"))
using MarketTickStreamer
using Printf
include(joinpath(@__DIR__, "cli.jl"))

function main()
    (; opts, positional) = parse_cli(ARGS; valued = ("config",))
    cfg = load_config(config_path(opts))
    files = positional
    isempty(files) && (
        println(
            "Usage: julia scripts/replay.jl <raw_file.jsonl>... | <YYYY-MM-DD.csv|.arrow>...",
        );
        return
    )
    ch = replay_source(
        files;
        pace = cfg.replay_pace,
        speed = cfg.replay_speed,
        max_gap_s = cfg.replay_max_gap_s,
        clock = cfg.replay_clock,
        capacity = cfg.channel_capacity,
    )
    n = 0
    for t in ch
        n += 1
        @printf(
            "[%s] %-6s  %10.4f × %-7.0f (%s)\n",
            ns_to_rfc3339(t.time_ns),
            t.symbol,
            t.price,
            t.size,
            t.exchange
        )
    end
    println("Replayed $n ticks.")
end

run_entrypoint(main)
