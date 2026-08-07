# MarketTickStreamer.jl

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
├── .env.example            # credential template → copy to .env (gitignored)
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
│   ├── viz.jl              # CairoMakie session diagnostics (price, activity, CCDFs)
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
├── data/                   # (gitignored, created on demand) raw/ + processed/
├── plots/                  # (gitignored) rendered diagnostic figures
└── logs/                   # (gitignored) per-session log files
```

## Setup

```julia
using Pkg
Pkg.activate("."); Pkg.instantiate()
```

Credentials: `cp .env.example .env`, fill in `ALPACA_API_KEY_ID` /
`ALPACA_SECRET_KEY` (free keys: https://alpaca.markets). The free tier
streams the IEX single-exchange feed; set `provider.feed = "sip"` in
`config/config.toml` only with a paid subscription.

## Usage

All behavior is driven by `config/config.toml`; every script accepts an
alternative config path.

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
```

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
  session summary, git commit, Julia/package versions, full effective
  configuration snapshot.
- `data/processed/SYMBOL/YYYY-MM-DD.csv|.arrow` — compacted, time-sorted
  per-day files for the analysis pipeline. Existing files are never
  overwritten (` #N` suffix siblings instead).
- Timestamps are Int64 nanoseconds since the UNIX epoch (UTC) everywhere;
  `time_ns` is the exchange timestamp, `recv_ns` local receive time
  (`0` marks backfilled records).

## Testing

```julia
using Pkg; Pkg.test()
```

The suite includes full end-to-end runs (stream → reconnect → fatal stop →
raw files → compaction → replay) against an in-process mock of the Alpaca
REST + WebSocket APIs — no credentials or network needed.

## Status

| Component | State |
|---|---|
| Live trade streaming (Alpaca IEX/SIP) | working, tested against mock |
| Reconnection, stale-connection watchdog, graceful shutdown | working, tested |
| Market-hours railings (wait-for-open, auto-stop at close, incl. half-day early close) | working, tested |
| Delayed-feed handling (railings shifted by feed delay; tape tail captured) | working, tested |
| Per-session provenance sidecars (`.meta.toml`) | working, tested |
| Resource guards (disk space, compaction RAM, channel lag, REST backoff) | working, tested |
| Batched raw NDJSON persistence + rolling | working, tested |
| Historical backfill (paginated REST) | working, tested against mock |
| Compaction to CSV/Arrow with duplicate removal | working, tested |
| Session QA report (dupes/gaps/ordering/latency) | working, tested |
| Diagnostic figures (price, activity, Δt & size CCDFs) | working, inspected |
| Live monitoring dashboard (attach-mode, UnicodePlots) | working, tested |
| Paced replay | working, tested |
| Quotes (`q`) / bars (`b`) normalization | accepted on the wire, not yet normalized |
| Real-time analysis consumers | not started — attach via `tee`/`replay_source` |
| Credential validation (paper account): clock REST, historical SIP REST, WS auth on `iex` and `delayed_sip` | verified 2026-08-01 |
| Live capture validation against real Alpaca feed | pending (market hours) |

Provider research and the phased action plan are maintained in the
workspace notes, outside this repository.
