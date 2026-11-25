# Changelog

Notable changes to MarketTickStreamer.jl. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- Three producers starting on one stale lock could still both take a data
  tree: the settling check of 2.2.1 separates two, and a third that arrived
  between their unlinks found no lock file and did not wait. Producers now
  take the lock one at a time under a gate, `<data_dir>/.session.lock.take`,
  held from their look at the lock to the creation of their own. A producer
  that finds the gate taken waits up to 30 s and is then refused with a
  `SessionError` naming it; a gate left by a process that died is broken
  after 5 s. The settling check remains and separates one producer of 2.2.1.
  Like the lock, the gate is the producers' own, and the file contract lists
  it as such.
- A capture interrupted while its sink then failed in the final drain
  returned `status = "failed"` and recorded `interrupted` in its sidecar. A
  failure recorded before the interrupt is kept, and `run_stream` reports the
  status, the tick count and the raw files that the sidecar records, read
  from one place, so the two agree however the session ended.
- `backfill.start_date` and `end_date` outside 1678-01-01 to 2261-12-31, the
  whole years inside the span of an `Int64` nanosecond timestamp, are
  refused, as strings and as TOML dates. The request window of a day in 1500
  wrapped around to a later century, and `today-<N>d` reaching that far back
  either loaded or was refused under the other key.
- Raw parts beyond `_part999` were taken in name order, which puts
  `_part1000` before `_part101`: a follower, the monitor, the `raw_files` of
  every session's sidecar, and `scripts/compact.jl` and `scripts/visualize.jl`
  without arguments took them out of sequence. Parts are ordered by their
  number.
- Overview figure: the inter-arrival and size panels pooled every print of
  each day file, extended hours included, under an axis labeled as
  intra-session. Both are taken over the prints inside `session`, like the
  price path and the heatmap; with `session = (0, 24)` the figure is as
  before.

### Changed
- `market_clock` and `historical_trades` are documented on the provider
  interface, where an adapter author looks for them; the Alpaca and Binance
  docstrings keep what is specific to each venue.

## [2.2.1] - 2026-11-23

Corrections from a review of every module of 2.2.0. An interrupt inside a
request or a WebSocket read was retried as a transport failure; two producers
starting on a stale lock could both take a data tree; a day held in both
processed formats was read twice; a replay with capped gaps ran early after a
late print. The file contract and the configuration schema are those of 2.2.0:
the configuration refuses only a date written as a run of digits and intervals
no timer can count, which loaded and then failed, and it warns about a date
without its day.

### Fixed
- An interrupt (Ctrl-C, SIGINT) that arrived inside an HTTP request or a
  WebSocket read came out of HTTP.jl wrapped as a transport error and was
  retried: a backfill sent the request again, and a capture reconnected until
  its attempts ran out and ended `failed`. The retry schedule and
  `stream_websocket!` now rethrow it as an `InterruptException`; an interrupt
  that lands in the producer task ends the capture as `interrupted`
  (`LiveSession.outcome` reads `:interrupted`), and a session recognizes one
  that ended a task it waited on.
- Paced replay with a finite `max_gap_s`: a print stamped behind the latest
  one moved the schedule back, the next gap was then measured too long, and
  every later print came early by up to that step. It arises on the receive
  clock where two processed day files meet.
- A day present in the processed tree both as `.csv` and as `.arrow`, after a
  compaction under the other `storage.processed_format`, was read twice by
  `eachday`, `read_processed`, `replay_source` and `corpus_inventory`. A
  compaction now moves the day's file in the other format to that format's
  `_#N` backups, and `processed_files` raises an `IntegrityError` on a tree
  that holds such a pair.
- `tee`: an output closed with an exception while the fan-out waited on it (a
  channel bound to a consumer task that failed) ended the fan-out, the
  persistence output included. It is now dropped like any closed output. A
  `lossy` vector of the wrong length is a `DimensionMismatch` at the call.
- Two producers started within about a second of each other on a data tree
  whose lock was stale could both take it: a stale lock is broken by
  unlinking its name, after a warning that takes a fresh process about a
  second to compile. The warning is made after the lock is taken, and a
  producer that found a lock file waits a second and gives way if the name
  then holds another file than its own.
- `load_config` took for `backfill.start_date` and `end_date` any string
  Julia's date parser takes, a run of digits included: `"20260812"` loaded as
  the year 20260812. It is refused. `backfill.rate_limit_sleep_s`,
  `monitor.refresh_s` and `monitor.rate_window_s` are bounded at 10¹⁶ s:
  `inf`, and any interval a timer cannot count, loaded and failed at the
  first sleep or render. A channel named twice in `stream.channels` is
  subscribed once; an unknown `provider.name` is reported under that key.
- A provider whose spec declares no historical feed, as a streaming-only
  adapter in another package would, could not load a configuration; it loads
  now, and `run_backfill` refuses it before it takes the data tree. Under
  `backfill.source = "bulk"` the default feed is the last of the provider's
  REST feeds that is also published in bulk, else its first bulk feed.
- A window of `eachday` or `read_processed` with a bound inside the hour a
  daylight-saving transition skips or repeats threw a time-zone error. The
  bound is the instant of the jump, or the first occurrence of the repeated
  hour. `exchange_day_start_ns` threw likewise for a calendar that sets its
  clock forward at midnight; the date then starts at the jump.
- A copy of the package installed in a depot that lies inside another git
  work tree recorded that repository's commit in its sidecars. An installed
  copy records `git_available = false`.
- A backfill marks a day incomplete when the day was still open as its fetch
  began; the test ran after the fetch, so a fetch that crossed midnight on a
  venue that never closes left the day unmarked. `resume` recognizes the days
  of a symbol configured in lower case, which the Binance adapter files in
  upper case.
- `run_import` checks `broken`, `tape`, the file's date and the collision
  with existing days before it takes the data tree and hashes the vendor
  file.
- A spilling compaction removes each group's scratch copy once the group is
  loaded, so the copy and the day files beside it stay within the input's
  size, which the free-space check assumed (an Arrow file of a few prints
  exceeds its lines by a fixed few kilobytes); the heap ceiling is tested on
  the loaded group, before it is sorted and written.
- Vendor downloads: a `404` or `416` read on a connection that then broke
  decided the outcome of the retry as well, so a file whose retry succeeded
  was reported absent and its partial download removed; each attempt now
  decides for itself. A `206` without a `Content-Range` is refused like a
  range that starts elsewhere, and a retry after a chunk's last byte no
  longer asks for an empty range.
- A market clock whose close does not parse fails the session before its
  producer starts.
- Figures: a median inter-arrival time that rounds to 1000 of its unit, or
  exceeds 1000 s, was annotated in exponent notation. The price panel of the
  overview figure piled the extended-hours prints of each day onto the
  session's edges, and both day axes labeled every day; the panel now draws
  the session, about a dozen days are labeled whatever the span, a day
  without a print inside the session keeps its place, and session boundaries
  are drawn up to 31 days.
- `follow_source` attached with `from_start = false` while a backfill with
  `keep_raw = false` deleted a part; `scripts/compact.jl` without arguments
  on a tree without a raw directory; a trailing `--config` in
  `examples/follow_session.jl`.

### Changed
- `load_config` warns about a `backfill.start_date` or `end_date` given as a
  year, or a year and a month (`"2026"`, `"2026-08"`): it is read as the
  first day of that period, as before, which is hardly what was meant.
- `acquire_session_lock` takes `settle_s`, the pause after a lock file was
  found; `LiveSession.outcome` can read `:interrupted`.
- `monitor_raw` bounds `refresh_s` and `rate_window_s` at 10¹⁶ s.
- An import writes its sidecar before it hashes the vendor file, so one that
  fails or is interrupted while hashing leaves a `failed` or `interrupted`
  sidecar where it left none; it logs one closing line.
- `RawSink` loses the field `n_written`, which nothing read.
- The Documenter site is named `MarketTickStreamer.jl`.

## [2.2.0] - 2026-11-17

Both adapters run on one WebSocket loop, public as `stream_websocket!`, whose
upgrade and closing handshake are bounded and which a stop reaches at every
stage after the connection is made; a day file known to hold a partial day is
marked by a file beside it; the CTA sale-condition lists gain the reserved
`8`. The file contract gains that marker and states what a canonical file
holds; the configuration bounds `stream.stale_timeout_s` at a day.

### Added
- `stream_websocket!`, the WebSocket loop of both adapters, is part of the
  provider interface. An adapter built on it hands it a frame handler and
  gets the session's side of a connection: the stop handshake, the
  watchdog, the bound on the upgrade and on the closing handshake, and the
  record of the connection as opened.
- A day file known to hold a partial day when it is compacted gets the
  marker `<file>.incomplete.toml` beside it (keys `reason`, `written`): a
  day with a print from a session that did not complete (`running`,
  `interrupted`, `failed`, `aborted`, an unreadable sidecar), from a vendor
  file that ends early or an IEX capture with sequence gaps, a day a
  backfill fetched before it ended, or every day with
  `compact_raw(...; incomplete = true)`. The file keeps its canonical name,
  and `processed_files(...; complete = true)` leaves marked files out. A
  compaction that writes the file with nothing known against it removes the
  marker. Nothing else changes: resume and the import's collision check
  treat a marked day as any other.

### Changed
- An upgrade request the server leaves unanswered for
  `stream.stale_timeout_s` is cut and retried as an attempt that never
  opened; before, the producer waited for the answer until the session's
  deadline. The connection is registered with the session as soon as the
  request is sent, so `stop!` also severs one that has not been answered.
- A connection that ends other than by `stop!` or the watchdog sends a
  CLOSE and waits two seconds for the answer, then severs the transport;
  HTTP.jl's own handshake waited on a silent peer until TCP gave up.
- A session stopped during its first upgrade, before any print, ends
  `failed`, as one whose upgrades were never answered does; it ended
  `completed` when the server answered after the stop.
- A severed connection closes at once even when the peer has stopped
  reading and the client's send buffer is full: the socket is shut down
  before it is closed, since Julia's close first waits for queued writes to
  drain.
- `stream.stale_timeout_s` must be at most 86 400 s (a day); `inf`, which
  loaded before, left no timer that could be armed.
- New dependencies: the standard library `Sockets`, and `ExceptionUnwrapping`,
  already in the dependency tree through HTTP.jl.
- `compact_raw` reads the session sidecars beside the raw parts to mark
  days, and raises when such a directory exists but cannot be listed.

### Fixed
- The file contract stated that a day file under its canonical name is
  complete, which a compaction of a session still writing the day, or the
  import of a vendor file that ends early, contradicted with nothing on disk
  to tell. It now says that a canonical file holds every print its sources
  recorded for the day, and that the marker of the entry under Added tells
  a day known to be partial; an unmarked file is not thereby whole.
- The CTA lists of `NON_PRICE_CONDITIONS` (tapes `A` and `B`) and of the
  shipped configuration hold `8`, which the CTS sale-condition matrix
  reserves and bars from the consolidated last; the lists follow that rule
  for every other code. No print carries it today. The lists are now tested
  against both plans' matrices, transcribed into the suite.

## [2.1.0] - 2026-11-16

A Binance bulk backfill counts breaks in the trade-id sequence; corrections
to session shutdown, reconnection and exit status, to the tail and
survival-function diagnostics, and to what the documentation states about
market-data rules and their literature. The file contract gains a sidecar
key and a rule for identical rewrites; the configuration schema is
unchanged.

### Added
- A bulk backfill counts the prints whose trade id does not follow the
  previous one, within a day and from one day of the run to the next, logs
  a warning per day that has any, and records the total as the sidecar's
  `session.id_breaks`. The checksum proves a dump is the one the venue
  published; the ids show a gap inside it or at a day boundary of the run
  (#12). A day without rows, or without a dump, passes the last id before
  it on to the next. A count of zero is necessary for a complete span, not
  sufficient: the start of the first day, the end of the last and the edges
  of a day a resumed run skips are compared with nothing. The dump's zip
  entry is also checked against its own CRC-32.
  `bulk_trades` returns `id_breaks` and `last_id` and takes `previous_id`;
  `finalize_session_meta` takes `id_breaks`.
- `run_stream` returns `status` beside `ticks` and `raw_files`: the status
  written to the session's sidecar.
- `load_config` warns about an empty `stream.channels`, a `wait_for_open`
  without `require_market_open`, and storage subdirectories that are not
  distinct; all three still load.

### Changed
- A connection that opened and stayed open for `stream.stale_timeout_s`
  resets the reconnect counter, as one that delivered prints does. A quiet symbol cut by the watchdog every two minutes
  used up ten attempts in about 25 minutes and ended a healthy session as
  `failed`. An endpoint that never opens a connection still uses them up,
  however long each attempt takes to fail.
- An Alpaca connection limit (406) met after an earlier connection of the
  session opened is retried: the server most likely still counts that
  connection. Otherwise it stays fatal.
- `wait_for_open` waits before the session takes the data tree's lock, so a
  capture started before a weekend leaves the tree to backfills meanwhile.
- The data-tree lock is refreshed every minute and broken two minutes after
  its process died (was five and ten), so a restarted capture is not refused
  for ten minutes. A lock written on another host, whose process cannot be
  checked, and one whose holder is alive but starved for ten minutes, are
  broken after ten (was fifty).
- A rewrite of a processed file with the same bytes leaves the file as it
  was, without a `_#N` backup; every write goes through a temporary name of
  its own (`<day>.<tag>.partial.<ext>`), so two writers of one day cannot
  interleave in one file, and a backup claims its `_#N` with a hard link, or
  without hard links by creating the name exclusively, either of which fails
  when another writer took the name. Of two overlapping writes the earlier
  result can still end up under no name; no file is ever damaged.
- A raw part the sink opened and never wrote to is removed when it closes,
  so a capture without prints leaves no part and lists none in `raw_files`.
  Such parts were left as zero-byte files and reported at every later start
  as the stubs of an aborted session.
- A `Retry-After` of up to ten minutes is honored as given; it was cut to
  one minute, so the request went out again before the venue allowed it. A
  longer one fails the request. A chunk's timeout adds the policy's timeout and a
  quarter of margin to the paced transfer time.
- A bulk dump is read from the archive as a stream instead of being
  decompressed whole.

### Fixed
- `scripts/stream.jl` exits with status 1 when the session ended `failed`
  (a fatal stream error, exhausted reconnections). It exited 0, so a
  supervisor with `Restart=on-failure` did not restart it, contrary to the
  unattended-operation page.
- `stop!` and the stale-connection watchdog sever the connection's
  transport instead of performing the CLOSE handshake from their own task.
  The handshake waited for the peer's CLOSE frame, until TCP gave up on a
  silent peer, and read the socket concurrently with the protocol loop,
  which could leave one of the two readers waiting for good. A producer
  still waiting for an upgrade the server never answers is out of reach of
  both; five seconds after `stop!` its channel is closed, so the session
  ends at its deadline whatever state the connection is in, as `failed`
  when none of its connections opened and no print arrived. A connection that opens while
  `stop!` runs is severed by one side or the other.
- A producer that ended on its own left its deadline and close guards
  running; they end with it. A reconnect backoff ends at a stop request or
  at the session deadline.
- `follow_source` and `monitor_raw` without `session` attach to the whole
  session of the newest raw part. On a backfill they followed one
  (symbol, day) part set and then waited for the sidecar. Attached with
  `from_start = false` in the middle of a line, they read that line once it
  is complete instead of counting a corrupt fragment; a far-behind reader
  reads in blocks of 64 MiB instead of a part's whole tail, through one
  handle per part, so a part deleted meanwhile is still read to its end. The
  monitor's rate is measured over the time between reads.
- An error that escapes the live producer marks the session `failed` and is
  logged to the session's log; it ended the session as `completed`.
- A print put into the channel just before the producer closed it could be
  left unwritten by the sink, and a channel closed while a print waited to
  enter it made the sink throw.
- An ITCH file that ends before the End of Messages event, which closes
  every trading day, is reported `truncated` even when it was cut between
  two messages. A symbol whose Stock Directory message is repeated is
  counted once.
- An IEX capture of a protocol the reader does not know (DEEP+) is reported
  instead of read as empty, and a segment whose payload ends before its
  message count counts as a gap.
- Volume and value bars complete at a threshold the running sum reaches
  only up to rounding (a hundred prints of 10.01 against 1001).
- The tail exponent of the diagnostic figures leaves out the sizes tied with
  its threshold, which were counted while adding nothing to the Hill sum: on
  a liquid US equity day 19 % of the top decile sat on the 100-share
  threshold and the estimate read 1.43 where the exceedances give 1.16. Its
  uncertainty adds the dependence on the threshold (the larger shift of the
  estimate when the tail fraction is halved or doubled) to the statistical
  error, which alone understated it thirtyfold on that day. The
  survival-function panels are labeled `P(X ≥ x)`, carry one point per
  distinct value, and the inter-arrival panels state the share of zero gaps
  they leave out.
- The worked examples measure the gap after a late print from the latest
  timestamp seen, not from the late one; `print_rate` counts the prints whose
  gap was not positive, over a span that starts at the earliest timestamp.
- `run_import` applies its collision check to symbols as the readers select
  them (case-insensitively) and refuses a configuration that is not on the
  New York calendar.
- `snapshots(...; clock = :recv)` throws an `ArgumentError` on a print without
  a receive timestamp instead of stepping its boundary from 1970.
- Session log files render an `exception = (e, backtrace)` value as the error
  message; they held the backtrace as raw pointers.
- `limits.max_session_hours` must be finite: `inf` loaded and then failed at
  the start of the stream. A `quality.non_price_conditions` that is not a
  table is an `ArgumentError` naming the key.
- ITCH Market Category `M` (NYSE Texas, in the specification since June
  2025) is tape B; it was filed without a tape. A truncated Broken Trade
  message is reported as an `IntegrityError`. Under `broken = :keep` both
  vendor readers count the breaks of the selected symbols only, as the other
  modes do.
- A `Retry-After` header that is not a non-negative number of seconds falls
  back to the backoff schedule; a negative value raised an `ArgumentError`.
- An IEX HIST download resumed from a complete partial file is checked
  against the CRC-32C the host announces; it was checked by size only.
- `run_stream` interrupted before the session started returns its documented
  result, and the data-tree lock is released when the sidecar cannot be
  finalized.
- Documentation: the transaction-clock literature is cited with the work that
  contests it; the round-lot rule is cited as 17 CFR 242.600(b)(93).

## [2.0.3] - 2026-10-31

Dependent projects get their installation entry, and the TLS stack
distrusts the certificate keys compromised in October 2026.

### Added
- The setup sections of the README and the manual give the `[sources]` entry
  a dependent project writes for this unregistered package, with `rev` on a
  release tag.
- The suite checks that the version cited in the README and `CITATION.cff`
  is the latest release, has a dated CHANGELOG section matching
  `date-released`, and is the tag the installation recipes pin; on a release
  commit it must equal the `Project.toml` version.

### Security
- HTTP.jl 1.11.1 is the floor. It requires OpenSSL.jl 1.7.1 and MbedTLS.jl
  1.1.11, which reject certificate chains through the eight eMudhra
  intermediate CA keys compromised in October 2026, and it drops credential
  headers on cross-origin redirects. Every venue connection is TLS, so an
  environment resolved before this release should be updated.

## [2.0.2] - 2026-10-29

Sessions started within one second of each other on one data tree get
distinct identifiers.

### Fixed
- Two sessions started within one second on one data tree took the same
  identifier, which is stamped to the second: the second sidecar displaced
  the first into a numbered backup and listed both sessions' raw parts. A
  session now waits for the next second when its identifier is held by a
  sidecar, raw part or log of an earlier session. The identifier format is
  unchanged.

## [2.0.1] - 2026-10-28

The two capture scripts take their configuration the way the others do.

### Added
- `scripts/stream.jl` and `scripts/backfill.jl` take `--config <path>`, as
  the other scripts do; the configuration path as their only argument still
  works. Giving it both ways, or passing a second argument, is refused.

## [2.0.0] - 2026-10-28

The release that puts the package in maintenance mode. It settles the four
places where the 1.x Julia API was inconsistent or closed: modes become
`Symbol`s, every trade reader returns one shape, refusals throw package
exception types, and a provider adapter can live in another package. The
file contract, the scripts and the configuration file stay as they were,
apart from rejecting unknown keys in a provider table, so data trees and
configurations written under 1.x carry over. The manual's "Migrating to
2.0" page maps every 1.x call to its replacement, and the README states
what maintenance covers.

### Added
- `MarketTickStreamer.registered_providers()` lists the provider names a
  configuration may select. Defining `provider_spec(::Val{:name})` in
  another package registers an adapter: `load_config` accepts the name and
  its `[name]` table, and the session runners construct it through
  `make_provider`, with no change to this package. The providers page of the
  manual shows a skeleton adapter.
- A manual page, "Migrating to 2.0", and a "Maintenance" section in the
  README: what is maintained, on which release line, and where new venues
  and analysis methods belong.

### Changed
- **Breaking: the provider interface is open.** `ProviderSpec` is built with
  keywords and gains `credential_keys`, the environment variables the
  adapter reads, and `endpoint_keys`, the keys of its configuration table;
  `needs_credentials` is gone, being `!isempty(credential_keys)`.
  `make_provider(::Val{name}, cfg)` drops the `key` and `secret` arguments:
  an adapter that needs credentials reads them with `load_credentials!`,
  which now returns a `Dict{String,String}` keyed by variable name
  (`load_credentials!(names; env_path)` for an explicit list).
  `KNOWN_PROVIDERS` is replaced by `registered_providers()`. A key in
  `[alpaca]` or `[binance]` other than the adapter's endpoint roots is
  rejected, as an unknown key in any other table is.
- **Breaking: one return shape for the trade readers.** Every reader returns
  a `NamedTuple` whose `trades` holds the prints, empty when `each_page`
  consumed them, and whose `prints` counts them, followed by the reader's
  own statistics. `historical_trades` returns `(; trades, prints)` and
  `bulk_trades` `(; trades, prints, file)`, where they returned a
  `Vector{Trade}` or, under `each_page`, an `Int`; `itch50_trades` and
  `iex_hist_trades` keep their fields, with `prints` now second. The
  `on_page` progress callback of `historical_trades`, which nothing called,
  is removed; `each_page` sees every page.
- **Breaking: modes are `Symbol`s.** Keywords that choose among a fixed set
  of behaviors take a `Symbol`: `replay_source(...; pace = :recorded | :max,
  clock = :auto | :recv | :exchange)`, `compact_raw(...; format = :csv |
  :arrow, compression = :none | :zstd | :lz4)` (`:none` replaces `nothing`)
  and `run_import(...; format = :itch50 | :iex)`. The matching `Config`
  fields hold the `Symbol` (`processed_format`, `processed_compression`,
  `replay_pace`, `replay_clock`, `backfill_source`, `log_level`, and
  `channels` as a `Vector{Symbol}`), as does the `format` column of
  `corpus_inventory`. The configuration file and the sidecar's config
  snapshot keep the strings, so both read as before. Names from outside the
  package (providers, feeds, symbols, conditions, paths) stay strings.
- **Breaking: package exception types.** An operation refused or failed for
  a reason outside its arguments throws a subtype of
  `MarketTickStreamerError` instead of `ErrorException`: `ResourceError`
  (`resource` is `:disk`, `:scratch`, `:heap` or `:memory`),
  `IntegrityError` (checksum or size mismatch, malformed vendor file),
  `VendorError` (a vendor lists no file, answers a Range request from the
  wrong offset, or stops sending), `SessionError` (the data tree is locked,
  or a followed session stalls) and `CredentialError`. `FatalStreamError` is
  now a subtype as well. `ArgumentError` still marks an invalid argument or
  configuration value.

## [1.1.0] - 2026-10-28

Corrections to how a session records its own end, and to which vendor
requests are retried. A failed live session now reads `failed` in its
sidecar, and the sidecar says whether its git commit is real. This is the
last release of the 1.x series; 2.0 changes the Julia API and keeps the files
and the configuration.

### Added
- `save_session_figures` and `save_overview_figures` take `non_price`, the
  sale-condition table of the price-forming population, and
  `scripts/visualize.jl` passes the configured
  `[quality.non_price_conditions]` to the session report and the figures;
  the figures used the built-in lists before.
- The provider implementer interface beyond the exported names,
  `stream_protocol!`, `feed_delay_ns`, `exchange_day_start_ns`,
  `spawn_watchdog` and `bump!`, is declared `public`: documented, versioned
  with the package and reached qualified.
- The sidecar's `[provenance]` table records `git_available`: `false` when
  the package did not run from a git checkout (an installed copy) or git is
  missing, in which case `git_commit = "unknown"` and `git_dirty = false`
  are placeholders rather than a clean tree.

### Changed
- A live session that ends on a fatal stream error or on exhausted
  reconnection attempts is recorded `failed` in its sidecar, and a session
  whose sink fails is finalized `failed`; the first two read `completed` and
  the last stayed `running` before.
- The Binance adapter carries a stream error frame out of the WebSocket
  handler and aborts the session at once; the error was retried as a
  disconnect until the reconnection attempts ran out.
- A retryable status (429, 5xx) on the HIST index request and on the HEAD
  requests for a vendor file's announced size or checksum is retried on the
  policy's schedule; a transient 5xx on the checksum request no longer lets a
  cached file pass unverified. A status error on a Range request is raised
  outside the handler, so it is no longer retried as a transport failure.
- Sidecar rewrites at finalization and reconciliation are atomic: written
  beside the file and renamed into place.
- `isequal` and `hash` on `Trade`, `Quote` and `Bar` follow field-wise
  `isequal`, so a `Bar` with a NaN `vwap` is equal to itself in a `Set` or a
  `Dict`.
- The session envelope (lock on the data tree, reconciliation of stale
  sidecars, free-disk gate, sidecar start and finalization) is one helper
  shared by `run_stream`, `run_backfill` and `run_import`, so the three
  cannot diverge on status again.

### Removed
- The backfill progress bar and the ProgressMeter dependency: the bar was
  enabled only in an interactive session, where it interleaved with the
  per-day log lines.

### Fixed
- `reconcile_sessions!` warns about a sidecar it cannot parse instead of
  skipping it silently.

## [1.0.0] - 2026-10-22

The first stable release. It fixes what is public and versioned from here
on: the exported API, the file contract a capture leaves on disk, the
configuration schema and the entry-point scripts. A change to any of them
that alters what a consumer reads now waits for a major release. It also adds
a third way to consume a capture, beside the live channel and the replay of
a recording: a process of its own can follow a running session through its
files.

### Added
- `follow_source(dir; session, from_start, poll_s, idle_timeout_s,
  capacity)` returns a `Channel{Trade}` that tails a session's raw NDJSON
  parts as they are flushed: it carries a torn final line until its newline
  arrives, follows the roll to a new part, and closes once the sidecar's
  `session.status` leaves `running`, after everything flushed until then has
  been delivered. A finished session is read whole. A consumer in another
  process is coupled to the capture through the files and the sidecar only,
  so the capture never waits on it. `idle_timeout_s` ends the wait on a
  session whose process died unreconciled.
- `examples/follow_session.jl` applies `snapshots` to a followed stream, one
  row per minute of exchange time and a final row when the session ends; the
  suite executes it.
- A manual page, "Files and provenance", that specifies the on-disk contract
  for consumers in any language: the raw line, the sidecar, the processed
  day files, the vendor files, the lock, the two consumer classes and what
  counts as a breaking change.
- `CONTRIBUTING.md`.
- A PrecompileTools workload: the first `compact_raw` and the first replay
  of processed files no longer cost seconds of compilation.
- `run_stream`'s closing log line reports the frames received and the
  reconnections.
- The monitor reports the lines it could not parse; it shares its tailing
  with `follow_source`.

### Changed
- **Breaking.** Fifteen names of the session plumbing are no longer
  exported: `RawSink`, `open_raw_sink`, `write_batch!`, `close_sink!`,
  `run_sink!`, `free_disk_gb`, `check_live_heap`, `schedule_close_stop!`,
  `session_id`, `setup_logging`, `write_session_meta`, `start_session_meta`,
  `finalize_session_meta`, `reconcile_sessions!` and `acquire_session_lock`.
  They are declared `public`, documented and versioned as before, and are
  reached as `MarketTickStreamer.<name>`.
- `historical_trade_count` on a provider without a countable tape throws an
  `ArgumentError` with a message that names the check. `coverage_report` is
  therefore an Alpaca check; a Binance day is verified by its vendor
  checksum; its trade ids are consecutive, so completeness can be checked on
  them.
- Session log files are stripped of terminal control sequences.

### Fixed
- `scripts/visualize.jl --from/--to` failed: `Date` was not imported.
- `docs/make_readme_assets.jl` reads the annotation font size from the
  extension again.

## [0.8.0] - 2026-10-20

Vendor downloads that suit a shared link and a lossy path. A cap on the
download rate leaves room for other traffic, several connections per file
lift a transfer that a long path with packet loss limits per connection,
and a progress line every five minutes makes a transfer of hours visible.
Nothing changes by default: no cap, one connection.

### Added
- `[rest] max_download_mb_s` caps the download rate of vendor files —
  Binance dumps, ITCH and IEX day files — in MB/s, so a multi-gigabyte
  transfer leaves room on a shared link; `0`, the default, leaves it
  uncapped. It reaches the downloader as `RestPolicy(...; max_download_rate)`
  in bytes per second. Each Range request is paced from its own start, so a
  stall or a retry earns no burst afterwards, and the per-chunk timeout
  stretches to whatever the cap needs.
- `[rest] download_connections` (1 to 8, default 1) fetches a vendor file on
  that many Range requests at once, for hosts whose path limits each TCP
  connection: across a 150 ms path the ITCH host gave 0.6–0.75 MB/s on one
  connection and 1.6–2.1 MB/s on four. Chunks are appended in order, so the
  partial file stays a prefix of the remote one and resumes as before; the
  rate cap applies to the sum. It reaches the downloader as
  `RestPolicy(...; download_connections)`.
- Vendor-file downloads log their progress — bytes received, total, rate over
  the interval — every five minutes, so a transfer of hours is visible in its
  log.

## [0.7.0] - 2026-10-16

Safeguards and fixes found while importing six vendor days for every listed
symbol. An import no longer displaces a (symbol, day) already in the
processed tree unless asked to with `replace = true`, which is the one
change that can stop a script that re-runs imports over days it already
holds. NYSE American listings read from ITCH carry tape B, as the
consolidated tape does. Every vendor download, Binance dumps included, runs
in bounded, resumable Range requests that survive slow links and dropped
connections without splicing a byte twice.

### Added
- `run_import` refuses to write a (symbol, day) that already exists in the
  processed tree unless `replace = true`: the tree has no source dimension,
  and an imported day would otherwise displace a capture of that day into a
  safesave backup. `iex_hist_date` reads the session date off a HIST file
  name, as `itch50_date` does for Nasdaq's.

### Fixed
- A chunked download resumes from the bytes actually on disk when a request
  fails mid-body, and refuses a 206 whose range does not start at the
  requested byte. A retry used to re-request from the offset captured before
  the failure, splicing duplicate bytes into a file whose size still matched
  the announced length; a 3.5 GB ITCH day downloaded that way failed to
  inflate after 157 M of its 269 M messages.
- Binance bulk dumps are fetched in bounded Range requests through the same
  resumable downloader as the ITCH and IEX files. A dump used to arrive in a
  single request, which HTTP.jl cuts off at `rest.request_timeout_s` however
  far the transfer has got, so on a link slower than about 2 MB/s a busy
  BTCUSDT day (65 MB) failed on every retry; an interrupted dump now resumes
  from its partial file.
- `itch50_trades` files NYSE American listings (Market Category `A`) under
  tape B, as the CTA plan and the SIP's own tape field do; they were filed
  under tape A, which belongs to NYSE listings alone. Checked on 2019-12-30
  and 2026-06-12 against the consolidated tape: every sampled `A` symbol
  prints on B, and every other category already matched.

## [0.6.0] - 2026-10-15

Opens the corpus to two archives the package did not produce. A full-day
Nasdaq TotalView-ITCH 5.0 file and an IEX HIST packet capture are read into
the processed tree through the same raw sink, compaction and sidecar as a
backfill, every displayed Nasdaq execution priced off the order book the
reader keeps, every IEX trade report filed with the tape's own flag letters;
the vendor files are fetched in resumable, verified transfers and recorded
in the sidecar. Beside them, the backoff jitter is seeded from the
configuration, a consumer's state is snapshotted on the exchange clock under
one convention, and a capture host runs unattended under systemd.

### Added
- `[rng] seed` seeds the jitter of the backoff schedules, REST retries and
  stream reconnects, the package's only random draws. Left out, the seed is
  drawn from the OS when the configuration loads; either way it is in the
  sidecar's config snapshot as `rng_seed`, so a session's retry schedule is
  reproducible from its provenance. `RestPolicy` carries the generator and
  compares equal on its bounds.
- `snapshots` drains a stream through a consumer's fold and reports the
  consumer's state at every multiple of a period on the exchange clock and
  once at close, each snapshot stamped with its boundary and print count: the
  snapshot convention of the analysis interface, a function of the sequence
  alone, so replay reproduces it and consumers on one `tee` align.
- `run_import` reads a vendor day file into the processed tree through the
  raw sink, compaction and sidecar of a backfill: Nasdaq TotalView-ITCH 5.0
  full-day files (`itch50_trades`: displayed executions priced off the
  order book the reader keeps, printable executions with price, hidden
  trades, crosses; odd lots marked `I` against the stock directory's round
  lot; breaks dropped, marked or kept) and IEX HIST captures
  (`iex_hist_trades`: pcap and pcapng, IEX-TP sessions with sequence
  checks, TOPS 1.5/1.6 and DEEP 1.0 trade reports with the tape's flag
  letters, breaks). The sidecar's `session.source` names the file, its
  size and SHA-256, and the reader's counts. `scripts/import.jl` drives it.
- A manual page on unattended capture under systemd: a user service and a
  weekday timer that start the session before the open and let the
  package's market-hours guards wait for the bell and stop at the close,
  with SIGINT as the stop signal so the sidecar is finalized; the units are
  verified with `systemd-analyze`.

### Fixed
- A vendor-file download is fetched in bounded Range requests of 8 MiB,
  each retried on the REST policy's schedule, because HTTP.jl's read
  timeout bounds a whole request and cut a multi-hour streaming transfer
  at the policy's 30 s on every attempt. A dropped connection now costs one
  chunk, resumed from the partial file.

## [0.5.0] - 2026-10-15

Closes the accounting of a bulk backfill. A sidecar now records what a
backfill stored beside what it received, a duplicate-carrying vendor file
can be named from the log alone, and the NDJSON copy of a bulk backfill,
some twenty times the size of its vendor file, may be dropped once each day
is compacted, because the checksum-verified vendor file is the record and
rebuilds the day without the network.

### Added
- A backfill's sidecar records the prints it stored as
  `session.ticks_processed`, beside `session.ticks`, the prints it received;
  the per-day and closing log lines carry both. They differ by the duplicates
  compaction drops, and a vendor file can contain them: Binance's
  `ETHUSDT-trades-2026-07-24.zip`, checksum valid, repeats its first
  259 000 rows. `finalize_session_meta` takes the stored count as an
  optional keyword.
- `[backfill] keep_raw` (default `true`). With `source = "bulk"` it may be
  set to `false`: each day's raw NDJSON is deleted once the day is
  compacted, and the checksum-verified vendor file stays as its record; a
  rerun with `resume = false` rebuilds the day from that file without the
  network. On Binance `trades` the NDJSON copy is about 21 times the vendor
  file, 79.6 GB against 3.7 GB for three pairs over a quarter. The
  configuration refuses `false` for a REST backfill, whose raw files are its
  only record.

### Changed
- Compaction removes duplicates per symbol and trading day and logs the
  count with both, so a duplicate-carrying file can be named from the log
  alone. The prints kept are the same as before.

## [0.4.1] - 2026-10-13

### Added
- `event_bars` collapses the prints that share an exchange timestamp into
  one `Bar` per instant, the matching event, with the fills' open, high,
  low, close, summed volume, count and volume-weighted price. A taker order
  filled against several resting orders is one print per fill at a single
  timestamp, so on a liquid crypto pair 88 % of consecutive `trade` prints
  are a gap of zero and neither Binance feed is the arrival process of
  orders; the event series is. Measured on BTCUSDT 2026-07-01: 4 166 849
  prints, 503 061 events.

## [0.4.0] - 2026-10-13

Makes the corpus a thing to be queried and Binance a venue to be captured at
scale. The processed tree is read back directly — an inventory from file
metadata, one trading day at a time with an exchange-local window and the
price-forming population, or a whole selection behind a memory guard — so a
downstream pipeline consumes what the package produces without going through
replay. Binance's daily dump files are a backfill source, which brings the
`trade` feed, every execution, within reach of a backfill for the first
time, with the venue's checksums verified and its files kept as the record;
and the adapter captures at microsecond resolution on REST and on the
stream, the clock those files carry, with the unit of every capture recorded
in its sidecar. Quote persistence is settled as out of scope through 1.0.

### Added
- A query path over the processed tree, for pipelines that consume the
  corpus rather than a stream. `corpus_inventory` lists what the tree holds
  per symbol (days, span, prints, bytes) from file metadata alone. `eachday`
  iterates a selection one trading day at a time, the symbols of a day
  merged on exchange time, with an exchange-local time-of-day window and the
  price-forming population as options. `read_processed` materializes a
  selection and refuses one that would exceed `limits.read_mem_fraction` of
  the free memory, pointing at `eachday`. All three take a `Config`, which
  supplies the tree, the calendar, the condition lists and
  `window = :regular`. `trades_table` turns prints into a `DataFrame` in the
  processed schema.
- `scripts/inventory.jl` prints the inventory of the configured tree.
- `limits.read_mem_fraction`, default 0.5.
- A bulk backfill source. `backfill.source = "bulk"` fetches Binance's
  daily dump files (`aggTrades`, or `trades` for every execution, which no
  public REST endpoint can backfill), verifies the venue's checksum, keeps
  the file under `storage.bulk_subdir` as the vendor's record, and streams
  its rows through the same per-day raw sink, compaction and resume as a
  REST backfill. `bulk_trades` is the provider interface behind it;
  `ProviderSpec.bulk_feeds` declares which feeds a venue publishes this
  way. Timestamps in milliseconds (before 2025-01-01) and microseconds are
  both read.
- `[binance] bulk_base`, `[storage] bulk_subdir`, `[backfill] source`.

### Changed
- The Binance adapter requests microsecond timestamps on REST
  (`X-MBX-TIME-UNIT`) and on the stream (`timeUnit`), the clock its bulk
  files already carry; rows and frames in either unit are read. Captures
  made before this release are millisecond-quantized: two prints in the same
  millisecond were a waiting time of zero. Every session sidecar now records
  `provenance.timestamp_unit` (`"nanosecond"` on the SIP, `"microsecond"`
  on Binance); a Binance sidecar without the key predates this change.

## [0.3.0] - 2026-10-06

Settles which prints form a price and makes the package usable as a
dependency. The price-forming population is now derived from the CTS and
UTDF condition matrices, which excludes two thirds of the prints of a
high-priced name, and the round lot that defines an odd lot is observed from
the tape instead of assumed. Configuration paths, logging and shutdown
belong to a session and its configuration file rather than to the process
and the package directory, and plotting moved into extensions, so a consumer
of the trade channel loads in 0.9 s. A corpus replays streamed a day at a
time, with an optional cap on gaps. Defects found by running at scale are
fixed, one of them data-losing: a Binance backfill above 1000 rows per page
kept only the first page of each day. JSON3, deprecated in General, gives
way to JSON.jl, which in turn opens the way to CSV.jl 1.1.

### Fixed
- A processed CSV whose `conditions` or `exchange` column held only numeric
  codes read back with an integer column. The string columns are declared on
  read.
- A Binance backfill with `backfill.page_limit` above 1000 kept only the first
  page of each day. The server clamps an over-limit request to 1000 rows
  without an error, and the adapter ends its walk on a short page. The shipped
  configuration had `page_limit = 10000`. The limit is now bounded per
  provider (`ProviderSpec.max_page_limit`), in the configuration and in both
  adapters. Binance days backfilled with a larger limit are truncated and
  should be downloaded again.
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
- Log axes spanning up to four orders of magnitude outside the plain-decimal
  range labeled their 2× and 5× ticks with the rounded power of ten, so a
  tick at 0.2 read `10⁻¹` and one at 0.5 read `1`. Intermediate ticks now
  carry their mantissa.
- Parsing an Alpaca REST page cost time quadratic in its length: converting
  each trade's condition list from a JSON3 array scanned the whole page, so a
  10 000-trade page (the default `page_limit`) took 1 to 2 s of CPU, about 80
  times what it takes now.
- A processed CSV with an empty `exchange` or `tape` field could not be read:
  the writer left empty text unquoted, it read back as `missing`, and only the
  `conditions` column allowed for that.

### Changed
- **Breaking: relative paths in a configuration are relative to the
  configuration file**, no longer to the package directory. For a package
  installed with `Pkg.add(url = ...)` that directory is a read-only depot
  path which Pkg replaces on every update, so the defaults could not work for
  exactly the projects that consume the package. The shipped configuration
  now reads `data_dir = "../data"`, `log_dir = "../logs"`, and a new
  `[credentials] env_file = "../.env"` replaces the hard-wired `.env`
  location; `load_credentials!` takes the configuration (or an explicit
  `env_path`). `load_config()` without an argument is for a clone of the
  repository and is refused in an installed copy, where the shipped file is a
  template (`MarketTickStreamer.DEFAULT_CONFIG`) to copy.
- **Breaking: logging is scoped to the session.** `run_stream` and
  `run_backfill` run under their own logger and close its file when they end;
  they no longer replace the global logger. `setup_logging` returns
  `(logger, io)`.
- **Breaking: no process-global shutdown flag.** `LiveSession.stop` and the
  watchdog flags are `Threads.Atomic`; the session's stop flag is what mutes
  HTTP teardown noise in its own logger, so stopping one session no longer
  silences another's records. `live_source` takes `stop`.
- **Breaking: the price-forming population is redefined.**
  `NON_PRICE_CONDITIONS` now follows the sale-condition matrices of the tape
  plans (CTS Pillar output specification 2.11b, UTP data feed services
  specification 4.1): a code is excluded when the plan does not let it update
  the consolidated last sale, or lets it only as the sole qualifying trade of
  the day. Tapes A and B gain `N`, `P`, `Z`, `4`; tape C gains `G`, `N`, `P`,
  `W`, `Z`, `4`. `T` stays in and `9` stays out, both on purpose, and the
  docstring says why. The previous lists were the provider's bar-exclusion
  lists, which on tape C omit `W`, the UTP average-price trade, although they
  exclude its CTA counterpart `B`. On 40 symbol-days captured with this
  package the newly excluded classes lie one to two orders of magnitude
  further from the last regular print than regular prints do (tape C, 90th
  percentile: 15.7 bp for `4`+`W`, 21.8 bp for `P`, 41.3 bp for `4`, against
  0.63 bp). One AAPL session loses 12 175 of 420 969 price-forming prints
  (2.9 %), and its price-forming minimum moves from 304.35 to 316.00.
  Statistics computed on the earlier population are not comparable with
  those computed on this one. The earlier lists remain available through
  `[quality.non_price_conditions]` and the `non_price` keyword.
- **Breaking: `Bar` has a closing instant.** A new third field `close_ns`
  follows `time_ns`. For `tick_bars`, `volume_bars` and `dollar_bars` it is
  the timestamp of the print that completed the bar, so
  `close_ns - time_ns` is the time the market took to transact the bar's
  fixed quantity; for a streamed one-minute bar it is `time_ns` plus 60 s.
  Positional construction of `Bar` takes eleven arguments.
- **Breaking: rewriting a result keeps the newest data at the canonical
  path.** Compacted day files, session sidecars, manifest snapshots and
  figures used to be written beside an existing file as `name #2.ext`, which
  left the stale file under the name every reader opens. The new file now
  takes the canonical name and the one it displaces is kept as `name_#1.ext`,
  `name_#2.ext`, … (oldest first), the semantics of DrWatson's `safesave`.
  Writes go to `name.partial.ext` and are renamed into place, so the
  canonical path never holds a half-written file, and a failed write leaves
  it untouched. Files named ` #N` by earlier versions are left alone and are
  still ignored by the readers.
- **Breaking: plotting is a package extension.** CairoMakie and UnicodePlots
  are weak dependencies. `session_figure`, `overview_figure`,
  `save_session_figures`, `save_overview_figures` and `tick_theme` are
  declared and documented in the package and implemented by
  `MarketTickStreamerMakieExt`, which loads with `using CairoMakie`; called
  without it they throw a `MethodError` that names the package to load.
  `monitor_raw` prints a text dashboard, and draws its rate history and bar
  chart through `MarketTickStreamerUnicodePlotsExt` once UnicodePlots is
  loaded. A consumer that only reads the `Channel{Trade}` no longer installs
  or precompiles a plotting stack: `using MarketTickStreamer` takes 0.9 s
  where it took 5.9 s (Julia 1.13, precompiled). MathTeXEngine is no longer a
  dependency, the Computer Modern faces come from Makie's
  `theme_latexfonts()`. The scripts have their own environment
  (`scripts/Project.toml`) with both plotting packages.
- `ProviderSpec` carries what the figures need to know about a venue:
  `regular_session` (exchange-local hours), `price_unit` and `size_unit`.
  `overview_figure` takes `session`, all four figure functions take the two
  units, and `scripts/visualize.jl` passes them with the provider's time zone.
  A Binance capture is now drawn on the UTC day, labeled every four hours,
  with prices in the quote asset and sizes in the base asset, and the wait
  across midnight counts as a waiting time when the next day is present; it
  was drawn on the New York clock and the US equity session, in dollars and
  shares, with "overnight gaps excluded".
- `save_session_figures` and `save_overview_figures` take `out_dir` as a
  required argument. The default pointed into the package directory, which is
  read-only for an installed package.
- The price and activity panels of `session_figure` share their time range.
- `load_config` checks every key for type and documented bounds and rejects
  unknown tables and keys, so a misspelled key no longer falls back to its
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
  functions anchored on a labeled power of ten, a logarithmic activity color scale,
  and overview panels aligned on one grid.
- JSON is read and written with JSON.jl 1.x in place of JSON3, which is
  deprecated in the General registry and held CSV below 1.0 through its
  Parsers bound. Raw lines parse straight into `Trade`, about four times
  faster than before, with no penalty for a `SubString` of a large parent.
  The raw format is unchanged: lines written by earlier versions read back
  field for field and are written again byte for byte.
- CSV.jl 1.1 is required (was 0.10). Processed CSV files quote empty text
  (`""`) where 0.10 left the field empty; files written by earlier versions
  read back as before.

### Added
- REST timeouts and transport retry. Every request carries a read and a
  connection timeout, and timeouts, refused or dropped connections are
  retried like 429/5xx. A request against a server that accepted the
  connection and went silent used to block a backfill for good.
  `RestPolicy`, a new `[rest]` table (`request_timeout_s`,
  `connect_timeout_s`, `max_retries`), and a `rest` field on both providers.
- Spill compaction holds at most `limits.max_open_spill_files` files open
  (least recently written closed first, reopened in append mode), where it
  held one per symbol-day and ran into the file-descriptor limit on a large
  input. Its thresholds are configuration: `limits.compact_mem_fraction`,
  `limits.spill_headroom`, `limits.compact_footprint_factor`.
- `compact_raw(cfg, raw_paths)` takes the output directory, format, heap
  ceiling, spill thresholds and the provider's calendar from the
  configuration. `scripts/compact.jl` uses it; it filed a Binance capture
  under the New York calendar before.
- `tee` drops an output its consumer closed and keeps feeding the others,
  and its fan-out task is monitored.
- `replay_source` streams a corpus. Given processed day files it loads one
  trading day at a time, merges that day's symbols on the replay clock and
  emits them before opening the next, on one pacing schedule across the days;
  raw session files are still read whole. Six AAPL days (5.6 M prints) replay
  at about 1 M prints/s with the resident set flat at 1.2 GiB.
  `processed_files(dir, symbol; from, to)` lists a symbol's day files in date
  order, without safesave backups.
- `replay_source(...; max_gap_s)` and `replay.max_gap_s` cap every gap on the
  replay clock, in recorded seconds: closures replay as `max_gap_s` while
  timing within a session is kept. The default `Inf` replays every gap.
- Arrow output is smaller. The `symbol`, `exchange`, `conditions` and `tape`
  columns are dictionary-encoded, which takes a liquid US equity day
  (1.2 M prints) from 65 to 44 bytes per print at unchanged read cost, and
  `storage.processed_compression` (`"zstd"`, `"lz4"`; `compression` in
  `compact_raw`) compresses the record batches: 8 bytes per print with
  `"zstd"`, read in 64 ms where the memory-mapped uncompressed file takes
  under 1 ms. Uncompressed stays the default. Files written earlier read as
  before.
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
  a factor of eight across the boundary with no change in market behavior;
  ERIE went 100, 40 and back to 100 inside eight months. Nothing on the tape
  marks such a redefinition, so the round lot is now in every session report
  and every sidecar.

## [0.2.0] - 2026-09-13

Makes the package provider-agnostic and puts a year of real tape through it.
A second adapter (Binance spot: public, 24-hour, UTC) forced provider
selection, feed vocabulary, exchange calendar and trading days onto
dispatch, each of which had silently been Alpaca's.
Three defects found by running the pipeline at scale are fixed — two of them
data-losing. The static-QA layer, a benchmark suite, a Documenter manual and
two executed consumer examples arrive with it.

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
  that no name enters the module namespace implicitly, and `JET.jl` analyzes
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
  beside `n_trades`. Capture is never filtered: a price path wants
  price-forming prints, an arrival process wants every execution.
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
  each. The test suite executes it, so an interface change that breaks it
  fails the suite. Both `tee` outputs are lossless, because a lossy output
  drops ticks mostly in the bursts, which hold the short waiting times.
- `compact_raw` accepts `scratch_dir`, placing the spill copy explicitly.
- A second worked consumer, `examples/live_diagnostics.jl`, and an `examples/`
  environment of its own so a consumer's dependencies stay out of the
  package's. It is the mirror image of `waiting_times.jl`: the lossy tap
  rather than the lossless one, and constant-memory `OnlineStats`
  accumulators rather than a retained sample, suited to a dashboard beside a
  live capture and not to an estimator.
  It also records a result worth knowing before trusting a sketch: on one
  AAPL session `P2Quantile` misses the ninth decile of the waiting-time
  distribution by a factor of 31, and taking logarithms first rescues the
  median but not the shoulder. The gaps span six orders of magnitude with
  most of the mass in the first millisecond, which is not the smooth unimodal density P²
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
  pass writes every input line back out once, and where `/tmp` is a tmpfs
  (the default on several systemd distributions, sized at half of RAM) that
  copy went into memory. Recompacting a 26 GB corpus exhausted 30 GiB of RAM;
  measured on the same input, scratch use of tmpfs falls from the full input
  size to zero.
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
  checked for coverage against the consolidated tape.
- Historical acquisition over the REST trades endpoint: per-day iteration,
  page streaming, rate-limit pacing, and resumption of interrupted downloads.
- A crash-only raw layer — append-only NDJSON, one normalized `Trade` per
  line, size-rolled parts, never overwritten — with per-session
  `.meta.toml` provenance sidecars recording the session summary, the git
  commit, package versions, and a hardware fingerprint.
- Compaction of raw sessions into per-symbol per-day CSV or Arrow, with
  spill compaction for inputs that exceed the live-heap ceiling.
- Paced replay of recorded sessions as a `Channel{Trade}` of the same type as
  the live source, and `tee` fan-out to concurrent consumers.
- A data-quality layer: duplicate detection, ordering and gap analysis,
  session and coverage reports.
- CairoMakie session diagnostics and a multi-day overview family; an
  attach-mode terminal dashboard for running sessions.
- An offline test suite driven by an in-process mock of the Alpaca REST and
  WebSocket APIs, with Aqua static quality assurance.

[Unreleased]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v2.2.1...HEAD
[2.2.1]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v2.2.0...v2.2.1
[2.2.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v2.0.3...v2.1.0
[2.0.3]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v2.0.2...v2.0.3
[2.0.2]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.8.0...v1.0.0
[0.8.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/PaulGoG/MarketTickStreamer.jl/releases/tag/v0.1.0
