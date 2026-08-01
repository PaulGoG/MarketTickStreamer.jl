# Replay a recorded session as a live-like stream (smoke view).
#
#   julia scripts/replay.jl <raw_file.jsonl>... [--config path/to/config.toml]
#
# Prints each tick at the configured pace ([replay] in config.toml). This is
# the template for attaching real-time analysis consumers: swap the printing
# loop for any function of `Channel{Trade}`.

include(joinpath(@__DIR__, "startup.jl"))
using MarketTickStreamer
using Printf

function main()
    cfg_path = joinpath(@__DIR__, "..", "config", "config.toml")
    files = String[]
    i = 1
    while i <= length(ARGS)
        if ARGS[i] == "--config"
            cfg_path = ARGS[i + 1]
            i += 2
        else
            push!(files, ARGS[i])
            i += 1
        end
    end
    cfg = load_config(cfg_path)
    isempty(files) &&
        (println("Usage: julia scripts/replay.jl <raw_file.jsonl>..."); return)
    ch = replay_source(files; pace = cfg.replay_pace, speed = cfg.replay_speed,
                       capacity = cfg.channel_capacity)
    n = 0
    for t in ch
        n += 1
        @printf("[%s] %-6s  %10.4f × %-7.0f (%s)\n",
                ns_to_rfc3339(t.time_ns), t.symbol, t.price, t.size, t.exchange)
    end
    println("Replayed $n ticks.")
end

main()
