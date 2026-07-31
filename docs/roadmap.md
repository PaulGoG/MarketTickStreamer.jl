# Action plan

Working plan as of 2026-07-31. Ordered within each section by
priority; strike items here (or move to "Done") as they land, so this file
stays the single view of project direction. Architecture rationale lives in
`design.md`; provider economics in `providers.md`.

## Phase 0 — hardening (before trusting captured data)

- [x] Duplicate-print removal in compaction (`dedup_trades`; reconnection
      double-delivery, overlapping backfill/live).
- [x] Session QA audit (`session_report`): dupes, gaps, out-of-order counts,
      latency distribution, clock-skew detection (negative latency).
- [x] Market-hours railings: wait-for-open option, graceful auto-stop at
      `next_close` (previously a session started at 15:55 would sit on a dead
      overnight connection until the 8 h deadline).
- [x] Disk-space guard (start refusal + mid-session stop), compaction RAM
      guard, channel-occupancy lag warning, REST 429/5xx backoff honoring
      `Retry-After`, `.env` permission warning.
- [ ] **Live validation** against the real Alpaca IEX feed during market
      hours (needs keys): one full session, then `session_report` +
      `scripts/visualize.jl` over the capture. First real-world checkpoint.
- [ ] Gap-aware capture QA: compare per-day live-captured trade counts
      against the free historical SIP REST for the same symbols/day —
      quantifies both IEX-feed coverage and any stream drops.
- [ ] Half-day/holiday handling relies entirely on `/v2/clock` — add a unit
      test with a mocked half-day and confirm the close guard fires at 13:00.

## Phase 1 — data foundation

- [ ] Quote (`q`) normalization: `QuoteTick` struct (bid/ask price+size, ns
      timestamps), second channel or tagged union stream; quotes dominate
      volume, so revisit `channel_capacity` and flush sizing.
- [ ] Streaming (chunked) compaction to lift the in-RAM limitation for
      multi-GB raw files.
- [ ] Run-metadata sidecars: per session, write `<session_id>.meta.toml`
      (config snapshot, git commit, package versions, QA summary) next to the
      raw files — DrWatson-style provenance without changing formats.
- [ ] Second free source adapter — `BinanceProvider` (free unauthenticated
      trade WS + free bulk dumps; Kraken/Bybit dumps as siblings) for 24/7
      methodology testing; validates the `AbstractProvider` seam beyond one
      vendor.
- [ ] Try Alpaca's `v2/delayed_sip` WS on the free tier (consolidated tape,
      15-min delay — delay is irrelevant for science): one-line config
      change, but free-tier availability needs an empirical auth test.
- [ ] Storage escalation trigger: when a day's processed CSV exceeds ~100 MB
      per symbol, flip `processed_format = "arrow"` and add a DuckDB query
      layer over the processed tree.

## Phase 2 — analysis pipeline (the science)

- [ ] Consumer harness: `run_analysis(ch::Channel{Trade}, estimators...)`
      driving OnlineStats-style online estimators off a `tee` tap, with
      periodic state snapshots (CSV) for later comparison against offline
      recomputation — the live-vs-batch consistency check is the core
      scientific safeguard.
- [ ] First estimator set (baseline, before anything exotic): realized
      variance at multiple sampling frequencies, inter-arrival Hawkes-style
      intensity fit, size/return tail exponents — establishes the reference
      the exotic methods must beat.
- [ ] Event-time vs wall-time resampling utilities (tick time, volume time,
      dollar time) — most exotic methods care about the clock choice.
- [ ] Replay determinism: seedable jitter-free replay is already
      deterministic in `pace = "max"`; document/verify determinism guarantees
      under `pace = "recorded"` (sleep granularity) for reproducibility
      claims.

## Phase 3 — scale & breadth

- [ ] Free-archive ingesters (see `providers.md` free-first stack): a
      Nasdaq ITCH 5.0 trade extractor and/or an IEX HIST pcap/TOPS parser —
      each a self-contained binary-format reader emitting `Trade`s, giving
      whole-market research data at $0. ITCH first (richer, LOBSTER-grade).
- [ ] Databento historical adapter (plain HTTPS + CSV) for cheap bulk
      cross-validation tapes ($125 free credit, ~$10/symbol-year, see
      `providers.md`).
- [ ] Overnight/multi-day orchestration: systemd unit or cron wrapper that
      starts before open with `wait_for_open = true` (close guard handles the
      other end).
- [ ] Benchmarks (`bench/`, BenchmarkTools): JSON parse → Trade throughput,
      sink throughput, replay pacing accuracy; establishes headroom before
      adding quotes or more symbols.

## Known limitations (accepted for now)

- Multi-day groups in one figure call are split by trading day (by design);
  intraday figures assume a single session — pre/post-market prints land on
  the same day's panel.
- `tee` propagates backpressure: one stalled analysis consumer eventually
  stalls persistence. Acceptable while consumers are trivial; Phase 2
  harness must add a drop-counting overflow policy for slow estimators.
- Alpaca does not replay missed data after reconnect: stream gaps are
  permanent for the live capture (detectable via `session_report` +
  the Phase 0 REST cross-check; recoverable via backfill).
- HTTP.jl pinned to 1.x; revisit 2.x once it has months of field use
  (watchdog becomes redundant).
