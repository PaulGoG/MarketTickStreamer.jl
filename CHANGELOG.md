# Changelog

Notable changes to MarketTickStreamer. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- A Binance backfill with `backfill.page_limit` above 1000 kept only the first
  page of each day. The server clamps an over-limit request to 1000 rows
  without an error, and the adapter ends its walk on a short page. The shipped
  configuration had `page_limit = 10000`. The limit is now bounded per
  provider (`ProviderSpec.max_page_limit`), in the configuration and in both
  adapters. **Binance days backfilled with a larger limit are truncated and
  should be downloaded again.**
- `limits.max_session_hours` did not stop a connection that never dropped: it
  was tested only between reconnections. A guard task now enforces it.
- Recorded-pace replay of a backfilled capture ran at full speed, because it
  paced on `recv_ns`, which is 0 for backfilled records. Replay also slept
  each gap separately, so timer overshoot accumulated (1.65 s for a nominal
  1.00 s at 2.5 ms gaps). Pacing now follows an absolute schedule on a
  selectable clock: `replay_source(...; clock)` and `replay.clock`, one of
  `"auto"`, `"recv"`, `"exchange"`.
- A backfill that died on an error finalized its sidecar as `completed`. It is
  now `failed`.
- An interrupt raised while a raw line was being parsed was counted as a
  corrupt line and compaction continued.
- `docs/make_readme_assets.jl` relied on CairoMakie being installed in the
  global environment.
- Log axes of up to four decades outside the plain-decimal range labelled
  their 2× and 5× ticks with the rounded decade, so a tick at 0.2 read
  `10⁻¹` and one at 0.5 read `1`. Intermediate ticks now carry their mantissa.

### Changed
- `load_config` checks every key for type and documented bounds and rejects
  unknown tables and keys, so a misspelt key no longer falls back to its
  default. A configuration that loaded before may now be rejected.
- No `Manifest.toml` is tracked. Each session writes a copy of the manifest it
  ran with next to its sidecar (`provenance.manifest`).
- The test suite has its own environment (`test/Project.toml`); scripts
  activate through `activate.jl`, and `scripts/startup.jl` is removed.
- `ProviderSpec` has a fifth field, `max_page_limit`.
- The tail exponent annotated on size distributions is the Hill estimator
  over the top decile, with its asymptotic standard error. It was a
  least-squares slope of the log-log survival function, whose standard error
  is not meaningful because the points of a cumulative curve are correlated.
  Annotated values change.
- `session_figure` and `overview_figure` draw the price path from
  price-forming prints and annotate their share; activity and the
  distributions still count every print. Both take `non_price`.
- Figures follow one layout standard: 26 pt labels over 22 pt ticks, 3-wide
  data lines, 1600 × 1100 canvas, a single exponent per axis, survival
  functions anchored on a labelled decade, a logarithmic activity color scale,
  and overview panels aligned on one grid.

### Added
- `observed_round_lot`, and a `round_lot` column in `session_report`: the
  venue's round lot read off the tape as one share more than the largest
  print still flagged an odd lot.

  It is reported because it is neither 100 shares nor constant. Under the SEC
  Market Data Infrastructure rules the round lot is tiered by share price and
  reassigned semiannually per symbol, effective the first business day of May
  and of November. The odd-lot flag follows it, so the price-forming
  population silently changes membership on those dates. On a 251-day AAPL
  capture taken with this package the lot fell from 100 shares to 40 on
  2026-05-01,
  and the median inter-arrival time of the price-forming population dropped by
  a factor of eight across the boundary with no change in market behaviour;
  ERIE went 100, 40 and back to 100 inside eight months. A redefinition that
  moves a population is otherwise invisible on the tape — no new code, no
  flag, no gap — so it is now in every session report and every sidecar.

## [0.2.0] - 2026-09-13

The release that made the package provider-agnostic in fact rather than in
description, and that put a year of real tape through it. A second adapter
(Binance spot: public, 24-hour, UTC) forced provider selection, feed
vocabulary, exchange calendar and trading days onto dispatch, each of which
had silently been Alpaca's. Three defects found by running the pipeline at
scale are fixed — two of them data-losing. The static-QA layer, a benchmark
suite, a Documenter manual and two executed consumer examples arrive with it.

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
- A second worked consumer, `examples/live_diagnostics.jl`, and an `examples/`
  environment of its own so a consumer's dependencies stay out of the
  package's. It is the mirror image of `waiting_times.jl`: the lossy tap
  rather than the lossless one, and constant-memory `OnlineStats`
  accumulators rather than a retained sample — the right trade for a
  dashboard beside a live capture, the wrong one for an estimator.
  It also records a result worth knowing before trusting a sketch: on one
  AAPL session `P2Quantile` misses the ninth decile of the waiting-time
  distribution by a factor of 31, and taking logarithms first rescues the
  median but not the shoulder. The gaps span six decades with most of the
  mass in the first millisecond, which is not the smooth unimodal density P²
  assumes. A histogram binned in log₁₀ holds every quantile to within a few
  percent for 100 kB of state, and that is what the example uses.
- A second provider, `BinanceProvider`: crypto spot market data over the
  public REST and WebSocket APIs, with no credentials. It exists as much to
  test the provider abstraction as to supply data, and it disagrees with the
  Alpaca adapter on every axis the abstraction covers — public rather than
  credentialed, UTC rather than New York, never closed rather than closed
  overnight and at weekends, milliseconds and string-typed numbers on the wire
  rather than RFC 3339 and JSON numbers.

  Backfill paginates by trade id, not by time window: Binance rejects a
  `startTime`/`endTime` pair spanning an hour or more, so the range is seeded
  with one timed request and walked forward on `fromId`. Hour-wide windows
  would issue 24 requests a day and still truncate, silently, any hour holding
  more than a page of trades. Only `aggTrade` has a time-seekable public
  endpoint, and an aggregate trade is one taker order's fill rather than an
  execution — a population choice like odd lots on an equity tape, and left to
  the analysis.

  The aggressor side is carried in `conditions` as `"buy"` or `"sell"`,
  spelled out so it cannot be mistaken for a CTA/UTP condition code. It is the
  one per-print classification a crypto venue reports, and the trade sign that
  order-flow work is built on. The tape is `"SPOT"`, which has no entry in
  `NON_PRICE_CONDITIONS`, so every crypto print is price-forming — correctly,
  there being no odd lots or late prints.
- Provider selection, feed vocabulary, exchange calendar and trading days are
  dispatched on the configured provider rather than assumed. `cfg.provider`
  existed but the pipeline always constructed `AlpacaProvider`; `provider.feed`
  and `backfill.feed` were validated against Alpaca's feed names whichever
  provider was selected; the backfill day loop skipped weekends
  unconditionally; and compaction filed rows under New York regardless. Each
  is now a method on the provider — `provider_spec`, `make_provider`,
  `exchange_tz`, `always_open`, `session_days` — with `Config` carrying the
  resolved `exchange_tz`. A venue that rests must now opt out of trading every
  day, never the reverse: an unnecessary request costs one empty response,
  whereas a skipped day loses that day's tape in silence.
- `run_entrypoint` is exported from the package. It was defined in
  `scripts/startup.jl`, which put it out of reach of anything that does not
  activate the package environment — the examples, now that they have their
  own. Every entry point already did `using MarketTickStreamer`, so the call
  sites are unchanged.

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

[Unreleased]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/releases/tag/v0.1.0
