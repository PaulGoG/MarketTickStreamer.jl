# Usage and configuration

Everything tunable lives in `config/config.toml`. Nothing operational is
hardcoded, and the parser enforces exactly what the comments promise —
types, enumerated choices, bounds — failing with a message that names the
offending key, so a session cannot start from a configuration it could not
honor.

## Credentials

Keys come from the file named by `credentials.env_file` — `.env` beside the
repository root in the shipped configuration — or from the process
environment, never from the configuration file itself. Copy the template and
fill it in:

```bash
cp .env.example .env      # then set ALPACA_API_KEY_ID and ALPACA_SECRET_KEY
chmod 600 .env
```

Paper-account keys work for market data; point `[alpaca] trading_base` at
the paper endpoint, as the shipped configuration does.
[`load_credentials!`](@ref) warns if the file is group- or world-readable.

## Entry points

Every script activates and instantiates the scripts environment itself
(`scripts/Project.toml`: the package by path, plus CairoMakie and UnicodePlots
for the figures and the dashboard plots), so a fresh clone needs no
preparation, and each accepts an alternative config path as its first
argument.

```bash
julia --threads=auto scripts/stream.jl   # live capture
julia scripts/backfill.jl                # historical download over the configured window
julia scripts/compact.jl  data/raw/<session>_part001.jsonl
julia scripts/replay.jl   data/raw/<session>_part001.jsonl
julia scripts/visualize.jl data/raw/<session>_part001.jsonl
julia scripts/monitor.jl                 # read-only dashboard for a running session
```

From the REPL the same operations are [`run_stream`](@ref),
[`run_backfill`](@ref), [`compact_raw`](@ref) and [`replay_source`](@ref).
The figure functions need `using CairoMakie` beside the package, and
[`monitor_raw`](@ref) draws its plots once `using UnicodePlots` has run; the
package itself depends on neither.

## `[provider]`

| Key | Meaning |
| --- | --- |
| `name` | Provider adapter. One of: `"alpaca"` (US equities, credentialed), `"binance"` (crypto spot, public). |
| `feed` | Live feed. Binance: `"trade"` or `"aggTrade"`. Alpaca: one of `"iex"` (real-time, single venue, 30 symbols on the free tier), `"delayed_sip"` (consolidated tape, 15 minutes late, free), `"sip"` (real-time consolidated, paid). |

On the free tier `delayed_sip` is the scientifically stronger choice: it is
the complete consolidated tape, and a fixed 15-minute offset is irrelevant
to any analysis that is not trading on it.

## `[credentials]`

| Key | Meaning |
| --- | --- |
| `env_file` | `KEY=value` file holding the API credentials, relative to the configuration file unless absolute. |

Relative paths are resolved against the directory of the configuration file,
so a configuration can be copied into any project and keeps working.
`load_config()` without an argument reads the repository's own
`config/config.toml`, and is refused in an installed copy of the package,
where that file is a read-only template to copy
(`MarketTickStreamer.DEFAULT_CONFIG`).

## `[stream]`

| Key | Meaning |
| --- | --- |
| `symbols` | Subscription list; at most `limits.max_symbols` entries. |
| `channels` | Subset of `"trades"`, `"quotes"`, `"bars"`. Trades only by default: quote and bar frames are parsed into [`Quote`](@ref) and [`Bar`](@ref) and delivered to the `on_quote` / `on_bar` callbacks of [`live_source`](@ref), but neither is persisted. A quote stream carries an order of magnitude more messages than the trade stream, so storing it is a separate decision. |
| `require_market_open` | Query the market clock before connecting and exit if closed. |
| `wait_for_open` | When closed, sleep until the next open instead of exiting. The wait is shifted by the feed's intrinsic delay. |
| `stop_at_market_close` | Schedule a graceful stop at the session's next close, likewise shifted. |
| `reconnect_max_retries` | Reconnection attempts per disconnect before giving up. The counter resets after any connection that delivered data. |
| `reconnect_base_delay_s` | Backoff base; the delay is `base * 2^attempt`, jittered. |
| `reconnect_max_delay_s` | Backoff cap. |
| `stale_timeout_s` | Watchdog interval: a connection that delivers no frame for this long is severed and retried. |

The watchdog exists because HTTP.jl 1.x WebSockets have no read idle
timeout, so a silently dead TCP connection would otherwise block the read
loop indefinitely.

## `[storage]`

| Key | Meaning |
| --- | --- |
| `data_dir` | Root of the data tree, relative to the configuration file, or absolute. |
| `raw_subdir` | Append-only NDJSON session files. |
| `processed_subdir` | Compacted per-symbol per-day files. |
| `flush_interval_s` | Sink flush interval. |
| `flush_max_ticks` | Sink flush batch-size threshold; whichever trigger comes first. |
| `processed_format` | `"csv"` or `"arrow"`. CSV for interoperability and inspection; Arrow when size or read time demands it. |
| `processed_compression` | Compression of Arrow record batches: `"none"`, `"zstd"` or `"lz4"`; Arrow only. Uncompressed files are memory-mapped and read lazily. `"zstd"` is about five times smaller (8 against 44 bytes per print on a liquid US equity day) and is decompressed on load. |

## `[limits]`

| Key | Meaning |
| --- | --- |
| `max_session_hours` | Hard stop for a live session. |
| `max_raw_file_mb` | Roll to a new raw part file beyond this size. |
| `channel_capacity` | In-flight tick buffer; the producer blocks when full. |
| `max_symbols` | Subscription cap. |
| `min_free_disk_gb` | Refuse to start, and stop an active session, below this free space. |
| `max_live_heap_mb` | Live-heap ceiling for backfill and compaction; the streaming paths check it and spill rather than exhaust memory. |
| `max_open_spill_files` | File handles a spilling compaction keeps open at once. |
| `compact_mem_fraction` | Share of the free RAM an in-memory compaction may claim; beyond it the run spills. |
| `spill_headroom` | Scratch space a spilling compaction requires, as a multiple of the input size. |
| `compact_footprint_factor` | Estimated in-memory footprint per byte of input, which decides whether a file fits in memory. |

These are the safety margins. They are the sole source of truth for the
cutoffs — no threshold is hardcoded elsewhere in the pipeline.

## `[replay]`

| Key | Meaning |
| --- | --- |
| `pace` | `"recorded"` honors the original inter-arrival times; `"max"` emits as fast as the consumer takes them. |
| `speed` | Time-compression factor when pacing is honored; `60.0` replays an hour in a minute. |
| `clock` | Timestamp that orders and paces the replay: `"recv"` (local receipt), `"exchange"` (the venue's own), or `"auto"` — receipt time when every record has it, exchange time otherwise. A backfilled recording has no receipt clock. |

## `[backfill]`

| Key | Meaning |
| --- | --- |
| `start_date`, `end_date` | Inclusive exchange dates (America/New_York). Either an ISO date `"YYYY-MM-DD"` or a sentinel: `"today"`, or `"today-<N>d"` for N calendar days back. |
| `feed` | Historical feed, `"iex"` or `"sip"`. Free accounts have full SIP history back to 2016, minus the trailing 15 minutes. |
| `page_limit` | Rows per REST page, from 1 to the provider maximum (Alpaca 10000, Binance 1000), which is also the default. A larger value is rejected: Binance clamps it without an error, and a clamped page reads as the end of the tape. |
| `rate_limit_sleep_s` | Pause between pages. The free tier allows 200 requests per minute. |
| `resume` | Skip `(symbol, day)` pairs already present under `processed/`, so an aborted download resumes instead of restarting. |

The sentinels keep a committed configuration from going stale. They count
calendar days, not trading days: a window may land on a weekend or a holiday
and return nothing, which the session report will show as an empty capture
rather than an error.

## `[rest]`

| Key | Meaning |
| --- | --- |
| `request_timeout_s` | Read timeout of a REST request. |
| `connect_timeout_s` | Connection timeout of a REST request. |
| `max_retries` | Retries on transient failures: 429, 5xx, timeouts and dropped connections. |

## `[quality]`

`non_price_conditions` lists, per tape, the sale-condition codes that
disqualify a print from a price path. Tapes `"A"` and `"B"` are
CTA-processed, `"C"` is UTP-processed, `"O"` is the OTC tape; the same
character means different things across them, which is why the lists are
separate. The shipped defaults are the provider's own published lists.

This is used only by [`price_forming`](@ref), [`filter_price_forming`](@ref)
and the `n_price_forming` column of [`session_report`](@ref). **Capture is
never filtered** — see [Replay & Analysis Interfaces](replay.md) for why the
distinction belongs to the analysis rather than to the recording.

A print carrying several conditions is disqualified by any one of them, and a
print on a tape with no list is kept. The set lands in the session sidecar
with the rest of the configuration, so a result stays attributable to the
eligibility rule that produced it.

To decode the codes themselves, fetch the provider's own glossary with
[`condition_map`](@ref) rather than assuming a mapping.

## `[monitor]`

Attach-mode terminal dashboard, read-only and opt-in.

| Key | Meaning |
| --- | --- |
| `refresh_s` | Refresh interval. |
| `top_symbols` | Rows in the per-symbol bar plot. |
| `rate_window_s` | Rate-history window; at least `refresh_s`. |

## `[logging]`

| Key | Meaning |
| --- | --- |
| `level` | `"debug"`, `"info"`, `"warn"` or `"error"`. |
| `log_to_file` | Tee the log to a per-session file under `log_dir`. |
| `log_dir` | Log directory, relative to the configuration file, or absolute. |

File logs are always flushed and are sanitized of terminal control
sequences, so a killed session still leaves a readable tail.

## `[alpaca]`

Endpoint roots, overridable so the suite can point the whole pipeline at an
in-process mock server.

| Key | Default |
| --- | --- |
| `trading_base` | `https://paper-api.alpaca.markets` (paper keys) or `https://api.alpaca.markets` (live keys). |
| `data_base` | `https://data.alpaca.markets` |
| `ws_base` | `wss://stream.data.alpaca.markets/v2` |
