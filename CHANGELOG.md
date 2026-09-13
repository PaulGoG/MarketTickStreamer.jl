# Changelog

Notable changes to MarketTickStreamer. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- `activate.jl` at the repository root, activating and instantiating the
  package environment without output, so `julia -i activate.jl` opens a REPL
  in it. The entry points under `scripts/` already did this on start-up; this
  file serves interactive work.
- `CHANGELOG.md` and `CITATION.cff`.
- Sentinel backfill dates: `backfill.start_date` and `backfill.end_date`
  accept `"today"` and `"today-<N>d"` alongside ISO calendar dates, resolved
  against the current exchange date in America/New_York, so a committed
  configuration does not go stale. The sentinels count calendar days rather
  than trading days; a window may therefore resolve onto a weekend or a
  holiday and return no prints.
- Static analysis in the test suite beside Aqua: `ExplicitImports.jl` asserts
  that no name enters the module namespace implicitly, and `JET.jl` analyses
  the package with its reports restricted to this module.
- A `Documenter.jl` manual under `docs/`, in its own environment consuming
  the package by path: an overview, the architecture record (promoted from
  the standalone `docs/design.md`), a configuration reference covering every
  key, the analysis-interface contract — ordering, completion, backpressure
  and determinism guarantees for consumers of the tick channel — and an API
  reference generated per source file.
- A `bench/` environment with its own `Project.toml`, activation script and
  `BenchmarkTools.jl` suite, consuming the package by path: timestamp
  parsing, per-line serialization (including the `SubString` path that once
  cost 2000x), sink batch writes, compaction, unpaced replay and the session
  quality report.
- `AllocCheck.jl` in the test suite, asserting that `ns_to_datetime`,
  `now_ns` and `trading_date` allocate nothing. The two RFC 3339 string
  functions are excluded by measurement rather than by oversight: they
  allocate by construction, and at a few hundred nanoseconds against
  network-bound ingestion, hand-rolling them is not justified.

- Resampling onto activity clocks: `tick_bars`, `volume_bars` and
  `dollar_bars` turn a capture into `Bar`s sampled once per fixed count of
  prints, of shares, or of traded value, rather than per interval of the
  calendar. Prints are ordered by exchange time, the print that crosses a
  threshold closes the bar it completes, and a trailing incomplete bar is
  dropped unless asked for, since its threshold is not met and it is
  therefore not comparable to the rest.
- Sale-condition handling: `price_forming` and `filter_price_forming` decide
  whether a print may set a price, per tape and with the tape plans'
  precedence rule that any one disqualifying condition wins;
  `NON_PRICE_CONDITIONS` holds the provider's published lists and
  `[quality.non_price_conditions]` overrides them, so the rule a result used
  lands in the session sidecar. `session_report` gains `n_price_forming`
  beside `n_trades`. Capture is never filtered — a price path wants
  price-forming prints, an arrival process wants every execution, and
  discarding at capture would answer the first question by making the second
  unanswerable.
- `Quote` and `Bar` structs, with the same value semantics and two-clock
  convention as `Trade`, and parsers for the corresponding stream frames;
  `live_source` gained `on_quote` and `on_bar` callbacks. Neither channel is
  subscribed to by default and neither is persisted — a quote stream carries
  an order of magnitude more messages than the trade stream, and storing it
  is a separate decision. Bar frames reuse the `c` key for the closing price
  rather than for a condition list, which the parser reads explicitly, since
  confusing the two would be silent and would produce plausible numbers.
- `condition_map` fetches the provider's own code-to-description glossary
  from `/v2/stocks/meta/conditions`, per tape and tick type, rather than
  transcribing plan tables that describe a feed this package does not consume.
- A worked consumer example, `examples/waiting_times.jl`: replay a capture,
  fan out to two consumers, and estimate the inter-arrival distribution on
  each. The test suite executes it, so it cannot drift from the interfaces it
  documents. Both `tee` outputs are lossless — reading an arrival process off
  a lossy output does not thin the sample evenly, it discards precisely the
  bursts the distribution is about.
- `compact_raw` accepts `scratch_dir`, placing the spill copy explicitly.

### Changed
- The Julia floor rises to 1.12: `[sources]` in the auxiliary environments
  needs 1.11 and the JET release used for static analysis needs 1.12. The
  Manifest is re-resolved on 1.13.
- `Project.toml` opens the development version `0.2.0-DEV`.
- The shipped configuration carries `start_date = "today-2d"` /
  `end_date = "today-1d"` in place of the fixed August 2026 window.

### Fixed
- Backfill request windows are built from local midnight in the exchange's
  time zone rather than from UTC midnight. The two agree only while New York
  is at UTC-4, so between the November and March transitions every request
  returned the previous date's last post-market hour and stopped an hour short
  of its own. Because the day loop skips weekends, a winter Friday's closing
  hour fell in the unrequested UTC Saturday window and was lost outright, not
  merely misfiled. `trading_date` now floors in integer arithmetic as well: it
  routed through `ns_to_datetime`, which divides by 1e9 in floating point and
  rounds to the millisecond, filing the last half-millisecond of a date under
  the next one. A query window and the key it is bucketed by must come from
  the same clock.
- Backfill resume granularity: each symbol-day is compacted as its download
  finishes rather than after the whole run, so the presence of a processed
  file is an accurate resume marker. Previously a multi-hour download killed
  midway left raw NDJSON that resume could not use, and the rerun started
  over.
- Spill compaction places its scratch copy beside `out_dir` instead of in
  `tempdir()`, and verifies the free space before reading a line. The spill
  pass writes every input line back out once, and on systemd distributions
  `/tmp` is a tmpfs sized at half of RAM — so the path that exists to bound
  memory was in fact writing the copy into memory. Recompacting a 26 GB corpus
  exhausted 30 GiB of RAM and took the machine's shell down with it; measured
  on the same input, scratch use of tmpfs falls from the full input size to
  zero.
- Inference on three paths that JET flagged: the session lock was typed
  `Union{LockMonitor,Bool}`, so releasing it dispatched dynamically over a
  type with no `close` method; the backfill per-page closure captured a sink
  bound in two scopes and was therefore boxed; and JSON3's value unions
  reached the per-line `json_to_trade` conversions untyped.

## [0.1.0] - 2026-08-06

First tagged version, validated against the live consolidated tape: a
three-hour capture of 3.17 M prints across 24 symbols with no duplicate,
out-of-order or missing records, and per-symbol coverage cross-checked
against the historical SIP tape.

### Added
- Live acquisition over the Alpaca v2 WebSocket protocol with authenticated
  reconnection, exponential backoff, a stale-stream watchdog, and a
  market-close guard; the `iex`, `sip` and `delayed_sip` feeds, the last
  empirically print-complete against the consolidated tape.
- Historical acquisition over the REST trades endpoint: per-day iteration,
  page streaming, rate-limit pacing, and resumption of interrupted downloads.
- A crash-only raw layer — append-only NDJSON, one normalized `Trade` per
  line, size-rolled parts, never overwritten — with per-session
  `.meta.toml` provenance sidecars recording the session summary, the git
  commit, package versions, and a hardware fingerprint.
- Compaction of raw sessions into per-symbol per-day CSV or Arrow, with
  spill compaction for inputs that exceed the live-heap ceiling.
- Paced replay of recorded sessions as a `Channel{Trade}` indistinguishable
  from the live source, and `tee` fan-out to concurrent consumers.
- A data-quality layer: duplicate detection, ordering and gap analysis,
  session and coverage reports.
- CairoMakie session diagnostics and a multi-day overview family; an
  attach-mode terminal dashboard for running sessions.
- An offline test suite driven by an in-process mock of the Alpaca REST and
  WebSocket APIs, with Aqua static quality assurance.

[Unreleased]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/releases/tag/v0.1.0
