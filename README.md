# MarketTickStreamer.jl

[![CI](https://github.com/PaulGoG/MarketTickStreamer.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/PaulGoG/MarketTickStreamer.jl/actions/workflows/CI.yml)
[![codecov](https://codecov.io/gh/PaulGoG/MarketTickStreamer.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/PaulGoG/MarketTickStreamer.jl)

Provider-agnostic tick-by-tick market data acquisition in pure Julia, built
for scientific time series research: live WebSocket streaming, historical
REST backfill, append-only raw persistence, compaction to analysis-ready
files, and paced replay of recorded sessions for reproducible development of
real-time analysis methods.

## File structure

```
.
├── Project.toml            # package manifest (HTTP 1.x pinned; see docs/design.md §6)
├── Manifest.toml           # exact resolved versions — the portability guarantee
├── .JuliaFormatter.toml    # committed formatting configuration (JuliaFormatter.jl)
├── .env.example            # credential template → copy to .env (gitignored)
├── activate.jl             # silent environment activation for interactive work
├── config/
│   └── config.toml         # ALL tunables: provider, symbols, storage, limits, replay
├── src/
│   ├── MarketTickStreamer.jl     # module root: imports, exports, includes
│   ├── schema.jl           # Trade struct; Int64-ns timestamps; RFC 3339 parsing
│   ├── config.jl           # TOML loading/validation; .env credential loading
│   ├── sinks.jl            # raw NDJSON sink (append-only, rolling); compaction
│   ├── quality.jl          # dedup, session QA report, disk-space probe
│   ├── monitor.jl          # attach-mode terminal dashboard (tails raw files)
│   ├── replay.jl           # recorded-session replay source (paced or max-rate)
│   ├── live.jl             # provider-agnostic live source: reconnect, watchdog,
│   │                       #   market-close guard
│   ├── pipeline.jl         # session orchestration: run_stream / run_backfill; tee
│   ├── visualization.jl    # CairoMakie session diagnostics (price, activity, CCDFs)
│   └── providers/
│       └── alpaca.jl       # Alpaca adapter: REST clock/trades + v2 WS protocol
├── scripts/
│   ├── startup.jl          # silent environment activation (included by all scripts)
│   ├── stream.jl           # live capture session
│   ├── backfill.jl         # historical trade download
│   ├── compact.jl          # raw NDJSON → per-symbol per-day CSV/Arrow
│   ├── replay.jl           # replay a recorded session to the terminal
│   ├── monitor.jl          # live dashboard for a running session (read-only)
│   └── visualize.jl        # QA report + per-symbol per-day diagnostic figures
├── test/
│   ├── runtests.jl         # unit + end-to-end tests
│   └── mock_alpaca.jl      # in-process mock of Alpaca REST + WebSocket APIs
├── docs/
│   └── design.md           # architecture, data flow, decisions
├── .github/workflows/
│   └── CI.yml              # test + coverage on push/PR (single stable-Julia job)
├── LICENSE                 # MIT
├── CHANGELOG.md            # release history (Keep a Changelog)
├── CITATION.cff            # citation metadata
├── data/                   # (gitignored, created on demand) raw/ + processed/
├── plots/                  # (gitignored) rendered diagnostic figures
└── logs/                   # (gitignored) per-session log files
```

## Setup

Requires Julia ≥ 1.11 (developed and Manifest-pinned on 1.13). From a clone:

```julia
using Pkg
Pkg.activate("."); Pkg.instantiate()   # or: julia -i activate.jl
```

Credentials: `cp .env.example .env`, fill in `ALPACA_API_KEY_ID` /
`ALPACA_SECRET_KEY` (free keys: https://alpaca.markets; paper-account keys
work — point `[alpaca] trading_base` at the paper endpoint, as the shipped
config does). Feeds on the free tier: `iex` (real-time, single venue) and
`delayed_sip` (consolidated tape, 15 min delayed — the default here, and
empirically print-complete against the historical tape); `sip` (real-time
consolidated) requires a paid subscription.

## Entry points

All behavior is driven by `config/config.toml`; every script accepts an
alternative config path. Each script activates and instantiates the project
environment itself (`scripts/startup.jl`), so a fresh clone needs no manual
environment step.

```bash
# live capture (Ctrl-C drains buffers and exits cleanly)
julia --threads=auto scripts/stream.jl

# historical backfill over [backfill.start_date, backfill.end_date]
julia scripts/backfill.jl

# compact raw session files into per-symbol per-day CSV/Arrow
julia scripts/compact.jl data/raw/<session>_part001.jsonl

# replay a recorded session as a live-like paced stream
julia scripts/replay.jl data/raw/<session>_part001.jsonl

# QA report (dupes, gaps, ordering, latency) + diagnostic figures → plots/
julia scripts/visualize.jl data/raw/<session>_part001.jsonl

# live dashboard for a running capture/backfill (separate terminal, read-only;
# tick rate, tape head/lag, per-symbol counts; --full for session totals)
julia scripts/monitor.jl

# test suite (offline: unit + mock-server end-to-end + Aqua static QA)
julia --project=. -e 'using Pkg; Pkg.test()'
```

For interactive work against the test environment, activate it with
`TestEnv.jl` (`using TestEnv; TestEnv.activate()`) from the project REPL.

From the REPL, the same entry points are `run_stream(cfg)`,
`run_backfill(cfg)`, `compact_raw(files, out)`, and
`replay_source(files)` — the latter returns a `Channel{Trade}`
indistinguishable from the live source, which is how real-time analysis
consumers are developed offline (see `docs/design.md`).

## Data layout

- `data/raw/` — append-only NDJSON, one normalized `Trade` per line,
  session-stamped filenames, size-rolled parts, never overwritten. Source of
  truth; line-by-line recoverable after a crash.
- `data/raw/<session_id>.meta.toml` — provenance sidecar per session:
  session summary, git commit, Julia/package versions, hardware fingerprint
  (CPU, memory, thread counts, `versioninfo`), full effective configuration
  snapshot.
- `data/processed/SYMBOL/YYYY-MM-DD.csv|.arrow` — compacted, time-sorted
  per-day files for the analysis pipeline. Existing files are never
  overwritten (` #N` suffix siblings instead).
- Timestamps are Int64 nanoseconds since the UNIX epoch (UTC) everywhere;
  `time_ns` is the exchange timestamp, `recv_ns` local receive time
  (`0` marks backfilled records).

## Testing

The suite runs fully offline: unit tests plus end-to-end runs (stream →
reconnect → fatal stop → raw files → compaction → replay) against an
in-process mock of the Alpaca REST + WebSocket APIs — no credentials or
network needed — plus static package QA via Aqua.jl.

## Status

| Component | State |
|---|---|
| Live trade streaming (Alpaca IEX/SIP) | working, tested against mock |
| Reconnection, stale-connection watchdog, graceful shutdown | working, tested |
| Market-hours railings (wait-for-open, auto-stop at close, incl. half-day early close) | working, tested |
| Delayed-feed handling (railings shifted by feed delay; tape tail captured) | working, tested |
| Per-session provenance sidecars (`.meta.toml`) | working, tested |
| Resource guards (disk space, RAM ceiling with auto-GC, channel lag, REST backoff) | working, tested |
| Crash-only session lifecycle (running/completed/interrupted sidecars + startup reconciliation) | working, tested |
| Page-streaming, per-day resumable backfill | working, tested |
| Spill compaction (bounded memory for arbitrarily large raw inputs) | working, tested |
| Single-instance lock per data tree | working, tested |
| Capture-coverage cross-check against the historical tape (`coverage_report`) | working |
| Lossy-tap option in `tee` fan-out (persistence always wins) | working, tested |
| Batched raw NDJSON persistence + rolling | working, tested |
| Historical backfill (paginated REST) | working, tested against mock |
| Compaction to CSV/Arrow with duplicate removal | working, tested |
| Session QA report (dupes/gaps/ordering/latency) | working, tested |
| Diagnostic figures (price, activity, Δt & size CCDFs; HH:MM axes, decade log ticks, tail-exponent annotations) | working, inspected |
| Multi-day overview figures (trading-time price, activity heatmap, intra-session waiting-time CCDF) | working, inspected |
| Live monitoring dashboard (attach-mode, UnicodePlots) | working, tested |
| Paced replay | working, tested |
| Quotes (`q`) / bars (`b`) normalization | accepted on the wire, not yet normalized |
| Real-time analysis consumers | not started — attach via `tee`/`replay_source` |
| Credential validation (paper account): clock REST, historical SIP REST, WS auth on `iex` and `delayed_sip` | verified 2026-08-01 |
| Live capture validation against real Alpaca feed | verified 2026-08-06: 3 h `delayed_sip` session, 24 symbols, 3.17 M ticks; QA clean (0 duplicates/out-of-order/gaps), print-complete against the historical tape |

## License

MIT — see `LICENSE`.
