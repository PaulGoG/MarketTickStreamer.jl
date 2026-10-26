# Architecture

The package acquires tick-by-tick (trade-level) market data, live and
historical, as the substrate for testing exotic time series analysis methods
on a real-time data flux. Below are the architecture and the reasons for
each decision; configuration and entry points are on the
[usage page](usage.md).

## 1. Data flow

```
                       ┌──────────────────────────────────────────────────────────────────┐
 config/config.toml ──▶│                        MarketTickStreamer                        │
 .env (credentials) ──▶│                                                                  │
                       │  AbstractProvider: the Alpaca and Binance adapters, by dispatch  │
                       │      ▼                                         ▼                 │
                       │  live_source                      historical_trades, bulk_trades │
                       │  WebSocket                        REST pages or daily dumps      │
                       │      │                            itch50_trades, iex_hist_trades │
                       │      ▼                            vendor day files               │
                       │  Channel{Trade}  bounded, typed                │                 │
                       │      │                                         │ pages           │
                       │      ▼                                         │                 │
                       │     tee ──────────▶ analysis taps              │                 │
                       │      │                                         │                 │
                       │      ▼                                         ▼                 │
                       │  run_sink!                               write_batch!            │
                       │      │                                         │                 │
                       │      └─────────────────▶ raw/ ◀────────────────┘                 │
                       │             NDJSON parts + <id>.meta.toml                        │
                       │                           │                                      │
                       │           ┌───────────────┼───────────────┐                      │
                       │           ▼               ▼               ▼                      │
                       │     replay_source    compact_raw    follow_source                │
                       │   ▶ Channel{Trade}        ▼        ▶ Channel{Trade} in           │
                       │                      processed/      another process             │
                       │                SYMBOL/DATE.csv|.arrow                            │
                       │                           │                                      │
                       │                           ▼                                      │
                       │        eachday, read_processed, replay_source                    │
                       └──────────────────────────────────────────────────────────────────┘
```

The central abstraction is `Channel{Trade}`: live, replay and follow produce
the same typed stream, so a real-time analysis consumer developed against
`replay_source` runs unchanged against the live feed or against a capture
running in another process. Backfill and import write through the same sink
as a capture, so everything downstream of the raw layer is one path.
Backpressure is intrinsic: bounded channels block producers instead of
dropping or buffering unboundedly.

## 2. Canonical schema

`Trade(symbol, time_ns, recv_ns, price, size, exchange, conditions, tape, id)`
— an immutable struct with concrete field types, provider-normalized.

- **Timestamps are `Int64` nanoseconds since the UNIX epoch (UTC).**
  `Dates.DateTime` is millisecond-resolution and would silently truncate the
  nanosecond RFC 3339 timestamps Alpaca sends; a custom parser
  (`rfc3339_to_ns`) round-trips losslessly (tested at every fractional
  width). This representation maps directly onto Arrow `Timestamp(ns)`.
- Two clocks per tick: `time_ns` (exchange event time) and `recv_ns` (local
  receipt). Their difference measures pipeline latency; replay paces on its
  clock, receipt time when every record has one and exchange time otherwise,
  either forced by `clock`; `recv_ns = 0` marks backfilled rows.
- The venue's own resolution is recorded, not assumed: the SIP delivers
  nanoseconds; Binance delivers milliseconds unless asked for microseconds,
  which the adapter asks for. Every session sidecar carries
  `provenance.timestamp_unit`.

## 3. Persistence: two layers

1. **Raw** — append-only NDJSON, one `Trade` per line, session-stamped
   filenames with size-based part rolling, never reopened or overwritten.
   Chosen over binary formats for crash-tolerance (a torn final line loses
   one tick, and `read_raw` skips it with a warning — tested) and
   human-inspectability. Writes are batched (`flush_max_ticks` /
   `flush_interval_s`) to keep I/O off the hot path.
2. **Processed** — per-symbol, per-trading-day (the provider's calendar: New
   York for Alpaca, UTC for Binance) CSV or Arrow, time-sorted, produced by
   `compact_raw`. CSV is the default; Arrow is one config switch away when
   volume demands it. A rewrite never destroys a result: the new file takes the
   canonical name and the file it displaces is kept as a numbered backup,
   `_#1`, `_#2`, …, so the canonical path is always the newest data. These
   are the semantics of DrWatson's `safesave`, implemented in a dozen lines
   instead of taken as a dependency, because DrWatson would bring JLD2 and
   FileIO's loader stack into a library whose processed formats are CSV and
   Arrow. The write goes to a `.partial` file first and is renamed into
   place, so a reader never sees a half-written day.

The processed layer is read back through three functions. `corpus_inventory`
lists what the tree holds per symbol from file metadata alone; `eachday`
iterates a selection one trading day at a time, the symbols of a day merged
on exchange time, with an exchange-local time-of-day window and the
price-forming population as options; `read_processed` materializes a
selection and refuses one that would exceed `limits.read_mem_fraction` of
the free memory. Every day is loaded in full before its filters apply, so
the memory bound is the busiest selected date — the bound replay has.

A venue's daily bulk files are a third input to the raw layer. With
`backfill.source = "bulk"` the backfill fetches Binance's dump of each day,
verifies the venue's checksum, keeps the file under `data/bulk/` as the
vendor's record, and streams its rows through the same per-day raw sink and
compaction as a REST page walk, so the raw layer remains the one source of
truth every reader understands, at the cost of disk space.

If the raw layer has to scale, it can move to `Arrow.append` on
stream-format files queried with DuckDB.jl; the `Trade`-channel interface
confines that change to `sinks.jl`.

## 4. Concurrency model

Plain Julia tasks, no locks. The producer (WebSocket read loop) `put!`s into
the bounded channel; the sink task drains it with a poll/batch loop
(0.05 s idle poll, negligible against a 30 s flush interval). **Closing the
channel is the only shutdown signal**: producer exit (deadline, fatal error,
retry exhaustion, `stop!`) closes it, the sink drains what remains, flushes,
and returns its count. Ctrl-C is converted to `InterruptException`
(`Base.exit_on_sigint(false)`) and takes the same path — no data is lost on
interrupt (tested).

## 5. Failure handling

- **Reconnection**: jittered exponential backoff
  (`base·2^attempt`, capped), attempt counter reset after any connection
  that delivered data; bounded by `reconnect_max_retries`.
- **Session deadline**: `limits.max_session_hours` is enforced by a guard
  task, so it also ends a connection that never drops; the reconnect loop
  alone would only test it between connections.
- **Fatal vs retryable**: Alpaca error codes 401–411 (bad auth, connection
  limit, bad subscription) abort immediately, since retrying them risks a
  ban. The Alpaca adapter carries the fatal signal out of the WebSocket
  handler by value and throws it after the connection returns; the Binance
  adapter throws inside the handler. Both paths are tested against the mocks.
- **Stale connections**: HTTP.jl 1.x websockets have no read-idle timeout, so
  a watchdog task force-closes the socket after `stream.stale_timeout_s`
  without a frame, triggering the normal reconnect path.
- **Market-hours guards**: `/v2/clock` gates connection
  (`stream.require_market_open`); `stream.wait_for_open` sleeps until the
  next open instead of exiting; `stream.stop_at_market_close` schedules a
  graceful stop at `next_close` so an unattended session does not idle on a
  dead overnight connection. Half-days and holidays need no special case,
  because the clock endpoint, not a local calendar, is authoritative
  (early-close behavior is tested end-to-end against a mock clock).
  Delayed feeds (`delayed_sip`, 15 min) shift both guards by the feed
  delay via `feed_delay_ns`: the open-wait extends past the silent
  post-open window and the close guard runs to `next_close + delay + 60 s`
  so the delayed tail of the tape is captured.
- **Shutdown**: `close(ws)` is a handshake; a peer that never
  acks the CLOSE frame would hang the read loop, so `stop!` severs the raw
  transport after a 5 s grace if the graceful close has not completed.
- **Resource guards**: free-disk check before and during a session
  (`limits.min_free_disk_gb`, mid-session breach stops the stream
  gracefully); compaction reroutes raw inputs whose estimated footprint
  exceeds `limits.compact_mem_fraction` of free RAM through bounded-memory
  spill compaction, which keeps at most `limits.max_open_spill_files` spill
  files open and so stays inside the file-descriptor limit however many
  symbol-days the input spans; a channel-occupancy warning fires when the
  sink lags the stream (>80% capacity).
- **REST requests are bounded**: every request carries a read and a
  connection timeout (`[rest]`), so a server that accepts a connection and
  goes silent cannot block a backfill for good. Timeouts, refused or dropped
  connections and 429/5xx are retried with exponential backoff, honoring
  `Retry-After`; anything else fails at once.
- **Nothing is process-global**: a session runs under its own logger
  (`with_logger`), which it closes when it ends, and its stop flag is an
  atomic owned by the session and shared with that logger to mute transport
  teardown noise. The caller's global logger is untouched, and two sessions
  in one process neither share a log nor mute each other.
- **A dead analysis tap cannot end persistence**: `tee` drops an output its
  consumer closed, with a warning, and keeps feeding the others; the fan-out
  task is monitored, so its own failure is logged rather than lost.
- **Data integrity**: compaction removes exact duplicate prints
  (reconnection double-delivery, overlapping backfill/live captures);
  `session_report` audits every capture for duplicates, exchange-time gaps,
  out-of-order delivery, and clock skew (negative receive latency) before
  the data is used for science.
- **Provenance, crash-only**: every stream/backfill session writes its
  `<session_id>.meta.toml` sidecar at START (`status = "running"`; git
  commit + dirty flag, versions, hostname, pid, hardware fingerprint —
  CPU model/cores, memory, thread and BLAS-thread counts, `versioninfo` —
  and the full config snapshot) and finalizes it at exit (`completed` /
  `interrupted` / `failed`, counts: prints received, and for a backfill the
  prints stored after compaction removed duplicates). A copy of the resolved
  `Manifest.toml` is written beside it as `<session>.manifest.toml` and
  named under `provenance.manifest`: no manifest is tracked in the
  repository, so this copy is what pins the dependency versions a session
  ran with. Startup reconciliation relabels sidecars of dead processes
  `aborted` and reports zero-byte raw stubs; provenance thus survives SIGKILL
  and power loss, which handling on the graceful path alone cannot ensure
  (Ctrl-C delivery to a threaded Julia process is best-effort).
- **Reproducible randomness**: the package's only random draws are the
  jitter of its backoff schedules, REST retries and stream reconnects. Both
  generators derive from one seed, `[rng] seed`, drawn from the OS at load
  when the configuration names none and recorded in the sidecar's config
  snapshot, so a session's retry and reconnect schedule can be replayed
  from its sidecar.
- **Backfill**: per-(symbol, trading-day) download loop with
  page-streamed sink writes — memory is bounded by one REST page plus the
  configured heap ceiling (`limits.max_live_heap_mb`, auto-GC then loud
  stop); `backfill.resume` skips pairs already processed, so a rerun after
  any abort resumes. Compaction spills to per-group files when the input
  exceeds the in-memory budget, bounding memory by the largest single
  (symbol, day) group. Those files go beside `out_dir`, never to `tempdir()`:
  the spill copies the whole input once, and `/tmp` is RAM on a systemd
  distribution, so the bounded-memory path would otherwise be the one that
  exhausts memory. The free space is checked before the first line is read.
- **Single-instance lock**: a PID-file lock per data tree prevents two
  sessions from interleaving on one raw directory; stale locks from dead
  processes break automatically.

## 6. Dependency decisions

| Choice | Rationale |
|---|---|
| `HTTP.jl` pinned to 1.x | 2.x is a rewrite on a new transport (Reseau.jl backend). The stale-connection watchdog exists because 1.x WebSockets have no read-idle timeout, and the chunked downloads because 1.x bounds a whole request by one timeout; both go with the migration, which otherwise renames keyword arguments (`readtimeout`→`read_idle_timeout` etc.). |
| `JSON.jl` 1.x (since 0.3.0; JSON3 before) | JSON3 is deprecated in General and caps Parsers at 2, which blocks CSV 1.x. Measured against JSON.jl 1.10, on one core: a raw line parses straight into `Trade` four times faster, with no `SubString` penalty, and a 10 000-trade REST page about 80 times faster, the JSON3 cost being quadratic in page length. |
| No `WebSockets.jl` | Abandoned (last release 2022). `HTTP.WebSockets` is the ecosystem standard. |
| No `AlpacaMarkets.jl` | REST-only, single maintainer, pins that conflict with a modern stack. Direct HTTP+JSON is a few hundred lines and fully under test. |
| `PrecompileTools.jl` | The consumer-facing paths are compiled with the package; the first `compact_raw` and the first processed-file replay cost seconds of compilation before. |
| `DotEnv.jl` 1.0 (`load!`) | Registered and stable, with a narrow scope. |
| Custom ns timestamps over `NanoDates.jl` | One regex + integer math, zero deps on the hot path; NanoDates remains an option as a display layer. |
| Hand-rolled backoff | `Base.retry`/Retry.jl do not fit a stateful reconnect loop with attempt-reset semantics. |
| Plotting as package extensions | CairoMakie and UnicodePlots are weak dependencies. A consumer that feeds an estimator from `Channel{Trade}` should not install or precompile a plotting stack to do it; the figures load with `using CairoMakie`, the monitor's plots with `using UnicodePlots`. |
| No `MathTeXEngine.jl` dependency | The Computer Modern faces come from Makie's `theme_latexfonts()`, which wraps the same fonts. |

## 7. Testing strategy

`test/mock_alpaca.jl` implements the Alpaca REST + WebSocket v2 protocol
in-process (scriptable per-connection behavior: batches, then fatal codes),
and `test/mock_binance.jl` the Binance REST and WebSocket APIs and the
daily bulk files with their checksums, so the full pipeline — connect,
auth, subscribe, stream, reconnect, fatal stop, batched persistence,
backfill, compaction, replay — runs end-to-end on both venues with no
credentials and no network. The vendor readers and the import run on
synthetic ITCH 5.0 and IEX HIST files built by the suite, and the resumable
downloader against a mock file host. Unit tests cover timestamp round-trips
at every fractional width, offset normalization, config validation, sink
rolling/never-reopen, corrupt-line recovery, safesave compaction, `tee`
fan-out, replay pacing, and following a running session through torn lines,
rolls and the sidecar's end. The three worked examples under `examples/`
are executed, and static QA runs with the suite: Aqua, JET, ExplicitImports
and AllocCheck.

## 8. Visualization

The figures are implemented in `ext/MarketTickStreamerMakieExt.jl`, a package
extension triggered by CairoMakie; the package declares and documents the
functions (`src/figures.jl`) and keeps the plotting-free numerics
(`src/diagnostics.jl`): survival functions, tail-preserving thinning, min–max
decimation, the Hill estimator.

Two figure families with identical styling (Computer Modern, boxed axes,
no titles, `HH:MM` exchange-time axes, log ticks at powers of ten with
2×/5× intermediates and `10⁰ → 1`, `10¹ → 10` collapse, in-axis count and fitted
tail-exponent annotations):

- **Session figures** — one 2×2 panel per (symbol, trading day) from raw
  captures: price path (min–max decimated), trades per minute, and
  survival functions (CCDFs) of inter-arrival times and trade sizes on
  log-log axes: the distributions whose heavy tails matter most to the
  methods the package serves, and which linear histograms hide.
- **Overview figures** — one panel per symbol across many days from the
  processed tree: price on a concatenated *trading-time* axis (overnight
  gaps removed, session boundaries marked), a day × session-minute
  activity heatmap, and pooled **intra-session** waiting-time and size
  CCDFs — overnight gaps are excluded by construction because they would
  contaminate the waiting-time tail.

CCDF point clouds are deterministically thinned (tail-preserving) and
price paths min–max decimated, so vector exports of million-tick figures
stay small. PDF + PNG (`px_per_unit = 4`) via `scripts/visualize.jl`
(raw-file mode prints the `session_report` QA table; `--overview` sweeps the
processed tree with symbol/date filters).

## 9. Live monitoring

`monitor.jl` implements attach-mode monitoring: a separate read-only
process tails the session's raw NDJSON part files (incremental offsets,
torn-line carries, roll-aware) and renders an in-terminal dashboard: tick
totals and rate, tape-head timestamp and lag, per-symbol counts. The
dashboard is text; with UnicodePlots loaded a second package extension adds
a rate-history plot and draws the counts as a bar chart (headless-safe).
An attached process was chosen over an in-process panel: it cannot interfere
with the capture, its logging or its progress output; it works identically
for live capture and backfill; and it can attach to or detach from any
session at any point, including one started by another process. Refresh
cadence and panel sizing come from the `[monitor]` config table;
`scripts/monitor.jl` is the entry point.

