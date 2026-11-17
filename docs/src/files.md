# Files and provenance

A run is coupled to an external analysis through its files: the raw parts a
session appends to, the sidecar that records its lifecycle and provenance,
and the processed day files compaction produces. This page specifies them,
for a consumer written in any language; the contract of the
`Channel{Trade}` a Julia consumer receives is on the
[replay page](replay.md). Besides the exported Julia API, this file
contract, the configuration schema and the entry-point scripts are public
and versioned with the package.

## Rules

1. A consumer reads or copies, and never writes under the data tree.
2. A raw part is append-only. It is never reopened once the sink has rolled
   past it or the session has ended, and while the session runs its last
   line may be torn.
3. A processed file appears atomically and is never modified. A rewrite
   with other content displaces the old file to a `_#N` backup, so the
   canonical name always holds the newest data. A day file known to hold
   an incomplete day when it was compacted has the marker
   `<file>.incomplete.toml` beside it.
4. The sidecar is the session's lifecycle, and its configuration snapshot is
   the source of every derived quantity: the calendar a print is filed under,
   the time zone of a window, the sale-condition lists.
5. A change that alters what a consumer reads is a major release.

## Two consumer classes

A streaming consumer follows the raw parts of a session while it runs. A
stationary consumer reads the processed tree of sessions that have finished.

| | Streaming | Stationary |
| --- | --- | --- |
| Attaches to | The raw parts of one session, running or finished. | The processed tree. |
| Lifecycle signal | The sidecar's `session.status`: follow while it is `running`, stop once it leaves. | None: a file under its canonical name holds every print its sources recorded for the day. The marker `<file>.incomplete.toml` beside it says the compaction knew the day to be partial; its absence does not prove the day whole. |
| Order of prints | Arrival order, as the sink received them. | Ascending `time_ns` within each (symbol, day) file. |
| Mutability | Append-only; the last line may be torn while the session runs. | Immutable; a rewrite takes the canonical name and moves the old file to `_#N`. |
| Julia entry point | [`follow_source`](@ref) | [`eachday`](@ref), [`read_processed`](@ref), [`replay_source`](@ref) |
| Other languages | Tail the parts by byte offset, carry a torn line until its newline arrives, poll the sidecar. | Any Arrow or CSV reader. |

The two meet at the sidecar: a stationary consumer starts once the status
has left `running`, and a streaming one stops there. The order of the two
reads matters to a streaming consumer. It reads the status first and drains
the parts second; when the status it read was not `running`, the producer had
flushed everything before finalizing, so that drain delivered the session's
last line and the consumer stops. Read the other way round, a line flushed
between the drain and the status read would be lost.

## The data tree

Paths are relative to `[storage] data_dir`; `raw/`, `processed/` and `bulk/`
are the defaults of `raw_subdir`, `processed_subdir` and `bulk_subdir`.

| Path | Written by | Consumer access |
| --- | --- | --- |
| `raw/<id>_partNNN.jsonl` | A capture (`run_stream`) or an import (`run_import`) | Tail while the session runs; read whole once it has ended. |
| `raw/<id>_<SYMBOL>_<YYYY-MM-DD>_partNNN.jsonl` | A backfill (`run_backfill`), one part set per (symbol, day) | As above. Deleted once the day is compacted when `keep_raw = false`. |
| `raw/<id>.meta.toml` | Every session | Read; re-read when it does not parse. |
| `raw/<id>.manifest.toml` | Every session | Read. |
| `processed/<SYMBOL>/<YYYY-MM-DD>.csv` or `.arrow` | Compaction: `compact_raw`, and each day of a backfill or import | Read. |
| `processed/<SYMBOL>/<YYYY-MM-DD>_#N.csv` or `.arrow` | The rewrite that displaced it | Ignore, or read as history; `_#1` is the oldest. |
| `processed/<SYMBOL>/<YYYY-MM-DD>.csv.incomplete.toml` or `.arrow.incomplete.toml` | A compaction that knew the day partial; removed by one that writes the file with nothing known against it | Read to tell a partial day; `processed_files(...; complete = true)` leaves marked files out. Keys `reason` and `written`. |
| `*.partial.csv`, `*.partial.arrow`, `*.partial.toml`, `raw/<id>.meta.toml.partial`, `bulk/**/*.partial` | A write or a download in progress | Never read. |
| `bulk/binance/<SYMBOL>/<SYMBOL>-<kind>-<YYYY-MM-DD>.zip` and `.zip.CHECKSUM` | A bulk backfill; `kind` is `aggTrades` or `trades` | Read only as the record of a day. |
| `bulk/itch50/<file>`, `bulk/iex/<file>` | `fetch_itch50`, `fetch_iex_hist` | Read only as the record of a day. |
| `.session.lock` | The producer holding the tree | Never touched. |

Session logs are not part of the tree: they go to `[logging] log_dir` as
`<id>.log`, stripped of terminal control sequences.

## Session identifiers

Every file a session produces carries its identifier as a prefix.

| Session | Identifier |
| --- | --- |
| Capture | `<provider>_<feed>_<yyyymmdd-HHMMSS>`, the UTC start, e.g. `alpaca_delayed_sip_20260806-133012` |
| Backfill | `backfill_` before the same form with the historical feed, `backfill_bulk_` for `source = "bulk"`; its day parts append `_<SYMBOL>_<YYYY-MM-DD>` |
| Import | `import_<format>_<yyyymmdd-HHMMSS>`, `format` being `itch50` or `iex` |

The identifier names the sidecar, `<id>.meta.toml`; the longest sidecar
identifier that prefixes a part's name is that part's session. Identifiers
are unique within a data tree: a session that starts in the same second as
an earlier one there waits for the next second before taking its identifier.

## The raw line

One print per line, a JSON object with its keys in this order:

| Key | JSON type | Meaning |
| --- | --- | --- |
| `symbol` | string | Ticker as the venue reports it. |
| `time_ns` | integer | Exchange timestamp, nanoseconds since the UNIX epoch (UTC). |
| `recv_ns` | integer | Local receipt timestamp, same units; `0` for a print that never crossed the wire (backfill, import). |
| `price` | number | Trade price. |
| `size` | number | Trade size; may be fractional. |
| `exchange` | string | Reporting venue code. |
| `conditions` | array of strings | Sale-condition codes as reported; may be empty. |
| `tape` | string | Tape identifier; may be empty. |
| `id` | integer | Venue trade identifier. |

```json
{"symbol":"AAPL","time_ns":1789027200063125269,"recv_ns":0,"price":317.5,"size":192.0,"exchange":"P","conditions":["@","T"],"tape":"C","id":1}
```

A timestamp exceeds 2⁵³, so a reader that parses JSON numbers as doubles
loses the low digits; read `time_ns`, `recv_ns` and `id` as 64-bit integers.

Lines are in the order the sink received them, which for a capture is the
arrival order. It is not chronological: a consolidated tape interleaves
venues and late prints are normal. A part rolls to the next one once it
reaches `limits.max_raw_file_mb` MiB, checked after each batch, so a part may
exceed the limit by one batch; parts are numbered from `001` and read in name
order. A part the sink opened and never wrote to (after a roll that ended the
session, or in a session without prints) is removed when the sink closes.
Batches are flushed on `flush_interval_s` or `flush_max_ticks`, and a
write can reach the file in pieces, so the last line of a running session's
newest part may be torn. A reader carries it until its newline arrives.
Every reader in the package skips a line that does not parse and counts it;
a hard kill leaves at most one, at the end of the part being written.

## The sidecar

`<id>.meta.toml` sits beside the raw parts and has four tables.

`[session]`: `id`; `status`; `pid` of the producer; `started_utc` and, once
finalized, `finished_utc` (ISO 8601, UTC); `ticks`, the prints received and
written to `raw/`; `ticks_processed`, the prints written to `processed/`, for
a backfill or an import, which differ from `ticks` by the duplicates
compaction dropped; `raw_files`, the base names of the session's parts;
`reconciled_utc` when the status was set by reconciliation; `id_breaks`, for
a bulk backfill, the prints whose trade id does not follow the previous one,
within a day or from one day of the run to the next (zero on a complete
span; see [Vendor and bulk files](#Vendor-and-bulk-files)). An import adds
`[session.source]`: the vendor file's `file` name, `bytes`, `sha256` and
`format`, and the reader's statistics (`messages`, `prints`, `broken`,
`truncated`, and per format `unresolved` and `symbols` for ITCH, `feed`,
`packets`, `segments`, `gaps` and `duplicates` for IEX).

`[provenance]`: `git_commit`, `git_dirty`, `git_available` (false when the
package was not run from a git checkout, an installed copy for instance, or
git is missing; `git_commit` then reads `"unknown"` and `git_dirty` `false`,
and `package_version` with the manifest copy identifies the code; sidecars
written before 1.1.0 lack the key), `julia_version`,
`package_version`, `hostname`, `timestamp_unit` (the venue's resolution:
`"nanosecond"` on the SIP, `"microsecond"` on Binance; a Binance sidecar
without it was recorded in milliseconds), and `manifest`, the base name of
`<id>.manifest.toml`, a copy of the resolved `Manifest.toml` the session ran
with (empty when the process had none).

`[hardware]`: `cpu_model`, `cpu_threads`, `total_memory_gib`,
`julia_threads`, `blas_threads`, `versioninfo`.

`[config]`: every field of the effective `Config`, keyed by field name, with
paths resolved absolute, the provider's calendar as `exchange_tz` (an IANA
name), the condition lists as `non_price_conditions`, and the `rng_seed` of
the backoff jitter. A derived quantity is taken from here, never from the
configuration file the session was started with, which may have changed
since.

The status is `running` when the sidecar is first written, at the start;
that write is atomic. At the end it is rewritten as `completed`,
`interrupted` or `failed`. A sidecar left `running` by a process that no
longer exists is relabeled `aborted` by the next session that starts on the
tree; its raw parts stay valid to the last flushed line. Every rewrite of
the sidecar is atomic as well: the new content is written beside it and
renamed into place, so a reader never finds it empty or truncated.

## Processed day files

`processed/<SYMBOL>/<YYYY-MM-DD>.csv` or `.arrow`, one file per symbol and
exchange date on the provider's calendar (New York for Alpaca, which hosts
the ITCH and IEX imports as well; UTC for Binance):

| Column | Type | Meaning |
| --- | --- | --- |
| `symbol` | String | As in the raw line. |
| `time_ns` | Int64 | Exchange timestamp, nanoseconds since the UNIX epoch (UTC). |
| `recv_ns` | Int64 | Receipt timestamp; `0` for a print that never crossed the wire. |
| `price` | Float64 | Trade price. |
| `size` | Float64 | Trade size. |
| `exchange` | String | Reporting venue code. |
| `conditions` | String | Sale-condition codes in one string, separator below; empty for none. |
| `tape` | String | Tape identifier; may be empty. |
| `id` | Int64 | Venue trade identifier. |

The codes in `conditions` are joined with `|`, so `["@","T"]` is stored as
`@|T`. Rows are sorted by `time_ns`. Exact duplicates, prints equal in symbol,
exchange timestamp, id, price, size and exchange, are removed and the first
occurrence kept. Arrow files dictionary-encode the four string columns, and
their record batches are uncompressed or compressed with zstd or lz4
(`storage.processed_compression`). CSV files quote empty text, and the four
string columns should be read as strings: a column of numeric codes would
otherwise be typed as integers.

A file is written to `<YYYY-MM-DD>.<tag>.partial.<ext>`, `tag` unique to the
write, and renamed into place, so the canonical name is either the previous
complete file or the new one; a write killed before its rename leaves its
partial file, which may be deleted. A rewrite with other content keeps the
displaced file as `<YYYY-MM-DD>_#N.<ext>`, `N` counting up from 1; a rewrite
with the same bytes leaves the file as it was. The tree has no source dimension: an ITCH or IEX day is filed where a
capture of that day would be, so each source (capture, ITCH, IEX) has a data
tree of its own.

A day file is marked `<file>.incomplete.toml` when its compaction knew the day
to be partial: a print written for it comes from a session that did not
complete (its sidecar says `running`, `interrupted`, `failed` or `aborted`,
has no status, or cannot be read), or from a vendor file that ends early
(`source.truncated`) or an IEX capture with sequence gaps; a backfill marks a
day it fetched before the day ended on the exchange calendar. The file keeps
its canonical name. The marker is written before the file, so a partial day
from such a source is never on disk unmarked, and a later compaction that
writes the file with nothing known against it removes the marker, which so
describes the latest write. Every day of a session that did not complete is
marked, those it had finished and those another source covers whole
included: the sidecar records how a session ended, not which of its days
were whole. An unmarked file is not thereby a whole day: a capture stopped
by its deadline or by a guard completes, and a bulk day with trade-id breaks
is counted in its sidecar, not marked; a session's coverage is its own
record, its sidecar times and [`coverage_report`](@ref) against the
historical tape. Markers change nothing else: a backfill's resume skips a
marked day and an import refuses one without `replace = true`, as for any
day in the tree. Compactions of one day are expected one at a time; a manual
compaction does not take the data tree's lock.

## Vendor and bulk files

Files fetched from a venue or a vendor are kept as downloaded, under
`bulk/`. A Binance dump is verified against the SHA-256 in the venue's
`.CHECKSUM` file beside it; an ITCH day against the size the server
announces, there being no checksum; an IEX HIST day against the CRC-32C the
file host announces and the size in IEX's index. A download in progress is
`<file>.partial` and is resumed from there; a file that fails verification
is deleted. The checksum proves the file is the one the venue published;
whether that file holds the whole day shows in its trade ids, which are
consecutive per symbol within a day and across days. A bulk backfill counts
the rows that break the sequence as the sidecar's `session.id_breaks`, the
first row of a day measured against the last id known the day before
(across a day without rows or without a file) when that day is in the run;
a missing run of prints counts once, and so does a run the vendor repeated,
which compaction then drops. A count of zero is necessary for a complete
span, not sufficient: the start of the run's first day, the end of its last
day and the edges of a day that a resumed run skips are compared with
nothing. A consumer never reads these files except as the record of a
day: the prints they hold reach it through the raw and processed layers, and
an import's sidecar names the file and its SHA-256.

## The lock

`<data_dir>/.session.lock` is a PID file held by the one producer a data
tree admits at a time: a capture, a backfill or an import. A second producer
on the same tree refuses to start; a lock left by a dead process is broken
by the next one once it has gone two minutes without a refresh (a live
producer refreshes it every minute); one written on another host, whose
process cannot be checked, after ten. Consumers never take it, and their reads
never wait on it.

## Versioning

This contract follows the package's semantic versioning. A change that
removes or renames a file, a key or a column, changes a type, a unit, an
encoding or the order of rows, redefines a value or a status, changes how
identifiers or paths are formed, or relaxes a rule above, alters what a
consumer reads and waits for a major release. A minor release may add keys,
columns or files, but never removes or redefines one; a consumer ignores
what it does not know.
