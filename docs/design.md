# MarketTickStreamer — Design

Goal: acquire tick-by-tick (trade-level) market data — live and historical —
as the substrate for scientifically testing exotic time series analysis
methods on a real-time data flux. This document records the architecture and
the reasoning behind each decision; the README covers usage.

## 1. Data flow

```
                       ┌────────────────────────────────────────────────────┐
 config/config.toml ──▶│                 MarketTickStreamer                 │
 .env (credentials) ──▶│                                                    │
                       │   AbstractProvider  (Alpaca today; adapter per     │
                       │        │            provider, multiple dispatch)   │
                       │        ▼                                           │
                       │   ┌─ live_source ──── WebSocket, reconnect,        │
                       │   │                   watchdog, session deadline   │
                       │   ├─ replay_source ── recorded NDJSON, paced       │
                       │   └─ historical_trades ── REST, paginated          │
                       │        │                                           │
                       │        ▼   Channel{Trade}  (bounded, typed)        │
                       │   ┌────┴────── tee (fan-out, lossless) ───────┐    │
                       │   ▼                                           ▼    │
                       │  run_sink! ─▶ data/raw/*.jsonl      analysis taps  │
                       │  (batched, append-only)             (future)       │
                       │        │                                           │
                       │        ▼ compact_raw                               │
                       │  data/processed/SYMBOL/DATE.csv|.arrow             │
                       └────────────────────────────────────────────────────┘
```

The load-bearing abstraction is `Channel{Trade}`: live, replay, and (via a
trivial wrapper) backfill all produce the same typed stream, so a real-time
analysis consumer developed against `replay_source` runs unchanged against
the live feed. Backpressure is intrinsic — bounded channels block producers
instead of dropping or buffering unboundedly.

## 2. Canonical schema

`Trade(symbol, time_ns, recv_ns, price, size, exchange, conditions, tape, id)`
— an immutable struct with concrete field types, provider-normalized.

- **Timestamps are `Int64` nanoseconds since the UNIX epoch (UTC).**
  `Dates.DateTime` is millisecond-resolution and would silently truncate the
  nanosecond RFC 3339 timestamps Alpaca sends; a custom parser
  (`rfc3339_to_ns`) round-trips losslessly (tested at every fractional
  width). This representation maps directly onto Arrow `Timestamp(ns)` and
  DuckDB `TIMESTAMP_NS` later.
- Two clocks per tick: `time_ns` (exchange event time) and `recv_ns` (local
  receipt). Their difference measures pipeline latency; replay pacing uses
  `recv_ns` (the flux as experienced); `recv_ns = 0` marks backfilled rows.

## 3. Persistence: two layers

1. **Raw** — append-only NDJSON, one `Trade` per line, session-stamped
   filenames with size-based part rolling, never reopened or overwritten.
   Chosen over binary formats for crash-tolerance (a torn final line loses
   one tick, and `read_raw` skips it with a warning — tested) and
   human-inspectability. Writes are batched (`flush_max_ticks` /
   `flush_interval_s`) to keep I/O off the hot path.
2. **Processed** — per-symbol, per-trading-day (America/New_York calendar)
   CSV or Arrow, time-sorted, produced by `compact_raw`. CSV is the default
   per project convention; Arrow is one config switch away when volume
   demands it. Existing files get ` #N` siblings, never overwritten.

Scale-up path (not yet needed at a-few-symbols IEX volume): swap the raw
layer to `Arrow.append` on stream-format files and query with DuckDB.jl —
the `Trade`-channel interface isolates that change to `sinks.jl`.

## 4. Concurrency model

Plain Julia tasks, no locks. The producer (WebSocket read loop) `put!`s into
the bounded channel; the sink task drains it with a poll/batch loop
(0.05 s idle poll — granularity is irrelevant against a 30 s flush
interval). **Closing the channel is the only shutdown signal**: producer
exit (deadline, fatal error, retry exhaustion, `stop!`) closes it, the sink
drains what remains, flushes, and returns its count. Ctrl-C is converted to
`InterruptException` (`Base.exit_on_sigint(false)`) and takes the same
path — no data is lost on interrupt (tested).

## 5. Failure handling

- **Reconnection**: jittered exponential backoff
  (`base·2^attempt`, capped), attempt counter reset after any connection
  that delivered data; bounded by `reconnect_max_retries` and the hard
  session deadline `limits.max_session_hours`.
- **Fatal vs retryable**: Alpaca error codes 401–411 (bad auth, connection
  limit, bad subscription) abort immediately — retrying them is a ban risk,
  not resilience. The fatal signal is carried out of the WebSocket handler
  by value, not by throw: exceptions crossing HTTP.jl's internal task
  boundary arrive wrapped, which would defeat `isa` dispatch (this was a
  real bug, caught by the mock-server tests).
- **Stale connections**: HTTP.jl 1.x websockets have no read-idle timeout, so
  a watchdog task force-closes the socket after `stream.stale_timeout_s`
  without a frame, triggering the normal reconnect path.
- **Market-hours railings**: `/v2/clock` gates connection
  (`stream.require_market_open`); `stream.wait_for_open` sleeps until the
  next open instead of exiting; `stream.stop_at_market_close` schedules a
  graceful stop at `next_close` so an unattended session doesn't idle on a
  dead overnight connection. Half-days and holidays are handled implicitly
  because the clock endpoint, not a local calendar, is authoritative
  (early-close behavior is tested end-to-end against a mock clock).
  Delayed feeds (`delayed_sip`, 15 min) shift both railings by the feed
  delay via `feed_delay_ns`: the open-wait extends past the silent
  post-open window and the close guard runs to `next_close + delay + 60 s`
  so the delayed tape tail is captured, not truncated.
- **Shutdown robustness**: `close(ws)` is a handshake; a peer that never
  acks the CLOSE frame would hang the read loop, so `stop!` severs the raw
  transport after a 5 s grace if the graceful close has not completed.
- **Resource guards**: free-disk check before and during a session
  (`limits.min_free_disk_gb`, mid-session breach stops the stream
  gracefully); compaction reroutes raw inputs whose estimated footprint
  exceeds half of free RAM through bounded-memory spill compaction; a channel-occupancy warning fires when the sink
  lags the stream (>80% capacity); REST calls back off exponentially on
  429/5xx honoring `Retry-After`.
- **Data integrity**: compaction removes exact duplicate prints
  (reconnection double-delivery, overlapping backfill/live captures);
  `session_report` audits every capture for duplicates, exchange-time gaps,
  out-of-order delivery, and clock skew (negative receive latency) before
  the data is used for science.
- **Provenance, crash-only**: every stream/backfill session writes its
  `<session_id>.meta.toml` sidecar at START (`status = "running"`; git
  commit + dirty flag, versions, hostname, pid, full config snapshot) and
  finalizes it at exit (`completed` / `interrupted`, counts). Startup
  reconciliation relabels sidecars of dead processes `aborted` and reports
  zero-byte raw stubs — provenance survives SIGKILL and power loss, which
  graceful-path handling alone cannot (threaded Ctrl-C delivery is
  best-effort in Julia).
- **Backfill robustness**: per-(symbol, trading-day) download loop with
  page-streamed sink writes — memory is bounded by one REST page plus the
  configured heap ceiling (`limits.max_live_heap_mb`, auto-GC then loud
  stop); `backfill.resume` skips pairs already processed, so a rerun after
  any abort resumes. Compaction spills to per-group files when the input
  exceeds the in-memory budget, bounding memory by the largest single
  (symbol, day) group.
- **Single-instance lock**: a PID-file lock per data tree prevents two
  sessions from interleaving on one raw directory; stale locks from dead
  processes break automatically.

## 6. Dependency decisions (verified 2026-07-30)

| Choice | Rationale |
|---|---|
| `HTTP.jl` pinned to 1.x (resolved 1.11) | 2.x is a weeks-old breaking rewrite (Reseau.jl backend). For unattended capture, battle-tested wins. Migration path documented: kwarg renames (`readtimeout`→`read_idle_timeout` etc.); our watchdog becomes redundant on 2.x. |
| `JSON3.jl` over new `JSON.jl` 1.x | JSON.jl 1.x rewrite benches ~10× slower for hot typed materialization; JSON3 is maintenance-mode but stable. Revisit when JSON.jl closes the gap. |
| No `WebSockets.jl` | Abandoned (last release 2022). `HTTP.WebSockets` is the ecosystem standard. |
| No `AlpacaMarkets.jl` | REST-only, single maintainer, pins that conflict with a modern stack. Direct HTTP+JSON is ~200 lines and fully under test. |
| `DotEnv.jl` 1.0 (`load!`) | Registered, stable, zero-issue scope. |
| Custom ns timestamps over `NanoDates.jl` | One regex + integer math, zero deps on the hot path; NanoDates remains an option as a display layer. |
| Hand-rolled backoff | `Base.retry`/Retry.jl don't fit a stateful reconnect loop with attempt-reset semantics. |

## 7. Testing strategy

`test/mock_alpaca.jl` implements the Alpaca REST + WebSocket v2 protocol
in-process (scriptable per-connection behavior: batches, then fatal codes),
so the full pipeline — connect, auth, subscribe, stream, reconnect, fatal
stop, batched persistence, compaction, replay — runs end-to-end in CI with
no credentials and no network. Unit tests cover timestamp round-trips at
every fractional width, offset normalization, config validation, sink
rolling/never-reopen, corrupt-line recovery, safesave compaction, `tee`
fan-out, and replay pacing.

## 8. Visualization

Two figure families with identical styling (Computer Modern, boxed axes,
no titles, `HH:MM` exchange-time axes, decade log ticks with 2×/5×
intermediates and `10⁰ → 1`, `10¹ → 10` collapse, in-axis count and fitted
tail-exponent annotations):

- **Session figures** — one 2×2 panel per (symbol, trading day) from raw
  captures: price path (min–max decimated), trades per minute, and
  survival functions (CCDFs) of inter-arrival times and trade sizes on
  log-log axes — the distributions whose heavy tails matter first for
  exotic time series methods, and which linear histograms hide.
- **Overview figures** — one panel per symbol across many days from the
  processed tree: price on a concatenated *trading-time* axis (overnight
  gaps removed, session boundaries marked), a day × session-minute
  activity heatmap, and pooled **intra-session** waiting-time and size
  CCDFs — overnight gaps are excluded by construction because they would
  contaminate the waiting-time tail.

CCDF point clouds are deterministically thinned (tail-preserving) and
price paths min–max decimated, so million-tick figures stay vector-light.
PDF + PNG (`px_per_unit = 4`) via `scripts/visualize.jl` (raw-file mode
prints the `session_report` QA table; `--overview` sweeps the processed
tree with symbol/date filters).

## 9. Live monitoring

`monitor.jl` implements attach-mode monitoring: a separate read-only
process tails the session's raw NDJSON part files (incremental offsets,
torn-line carries, roll-aware) and renders an in-terminal dashboard
(UnicodePlots — headless-safe): tick totals and rate history, tape-head
timestamp and lag, per-symbol counts. Attaching from outside the producer
was chosen over an in-process panel because it cannot interfere with the
capture, its logging, or its progress output (a §9 requirement of the
project standards), it works identically for live capture and backfill,
and it can attach to or detach from a session at any point — including
sessions started by another process. Refresh cadence and panel sizing come
from the `[monitor]` config table; `scripts/monitor.jl` is the entry point.

