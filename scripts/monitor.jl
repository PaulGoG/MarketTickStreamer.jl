# Attach a live terminal dashboard to the most recent (or named) session by
# tailing its raw NDJSON files. Read-only — safe to run beside a running
# live capture or backfill; Ctrl-C detaches without affecting the session.
#
#   julia scripts/monitor.jl [session_prefix] [--config path] [--full] [--iterations N]
#
# --full ingests the existing files first (session totals; one full read);
# the default attaches at the current end of file.

include(joinpath(@__DIR__, "..", "activate.jl"))
using MarketTickStreamer

function main()
    cfg_path = joinpath(@__DIR__, "..", "config", "config.toml")
    session = nothing
    from_start = false
    iterations = nothing
    i = 1
    while i <= length(ARGS)
        if ARGS[i] == "--config"
            cfg_path = ARGS[i+1]
            i += 2
        elseif ARGS[i] == "--full"
            from_start = true
            i += 1
        elseif ARGS[i] == "--iterations"
            iterations = parse(Int, ARGS[i+1])
            i += 2
        else
            session = ARGS[i]
            i += 1
        end
    end
    cfg = load_config(cfg_path)
    monitor_raw(
        cfg.raw_dir;
        session,
        refresh_s = cfg.monitor_refresh_s,
        top_symbols = cfg.monitor_top_symbols,
        rate_window_s = cfg.monitor_rate_window_s,
        from_start,
        iterations,
    )
end

run_entrypoint(main)
