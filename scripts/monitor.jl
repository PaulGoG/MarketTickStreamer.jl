# Attach a live terminal dashboard to the most recent (or named) session by
# tailing its raw NDJSON files. Read-only, so it can run beside a
# live capture or backfill; Ctrl-C detaches without affecting the session.
#
#   julia scripts/monitor.jl [session_prefix] [--config path] [--full] [--iterations N]
#
# --full ingests the existing files first (session totals; one full read);
# the default attaches at the current end of file.

include(joinpath(@__DIR__, "activate.jl"))
using MarketTickStreamer
using UnicodePlots: UnicodePlots   # activates the dashboard plots
include(joinpath(@__DIR__, "cli.jl"))

function main()
    (; opts, flags, positional) =
        parse_cli(ARGS; valued = ("config", "iterations"), flags = ("full",))
    length(positional) <= 1 ||
        throw(ArgumentError("at most one session prefix, got $(length(positional))"))
    session = isempty(positional) ? nothing : first(positional)
    from_start = "full" in flags
    iterations = haskey(opts, "iterations") ? parse(Int, opts["iterations"]) : nothing
    cfg = load_config(config_path(opts))
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
