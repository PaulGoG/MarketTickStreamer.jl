# Migrating from 1.x to 2.0

2.0 changes the Julia API in four places and leaves the rest of the public
surface as it was: the configuration file, the file contract (raw parts,
sidecars, processed day files, vendor files) and the scripts' arguments and
products are those of 1.1. A data tree written by 1.x is read by 2.0
unchanged, and a 1.x configuration file loads unchanged unless its
`[alpaca]` or `[binance]` table holds a key other than the adapter's
endpoint roots, which 2.0 rejects as it rejects an unknown key anywhere
else.

## Modes are `Symbol`s

Every keyword that selects among a fixed set of behaviors takes a `Symbol`,
and the matching [`Config`](@ref) fields hold one. The TOML spellings are
unchanged; [`load_config`](@ref) converts them.

| 1.x | 2.0 |
|:----|:----|
| `replay_source(files; pace = "max", clock = "exchange")` | `replay_source(files; pace = :max, clock = :exchange)` |
| `compact_raw(raw, out; format = "arrow", compression = :zstd)` | `compact_raw(raw, out; format = :arrow, compression = :zstd)` |
| `compact_raw(raw, out; compression = nothing)` | `compact_raw(raw, out; compression = :none)` |
| `run_import(cfg, path; format = "itch50")` | `run_import(cfg, path; format = :itch50)` |
| `cfg.processed_format == "arrow"` | `cfg.processed_format === :arrow` |
| `cfg.replay_pace`, `cfg.replay_clock`, `cfg.backfill_source`, `cfg.log_level`, `cfg.processed_compression` | the same fields, as `Symbol`s |
| `cfg.channels::Vector{String}` | `cfg.channels::Vector{Symbol}` |

Names from outside the package stay strings: provider and feed names,
symbols, sale conditions, tapes, paths, and the file formats of the figure
drivers. A `String` passed where a mode is expected is a `TypeError` naming
the keyword. The `format` column of [`corpus_inventory`](@ref) holds
`:csv`, `:arrow` or `:mixed`.

## Every reader returns `(; trades, prints, …)`

A reader of trades returns a `NamedTuple` whose `trades` holds the prints,
empty when `each_page` consumed them, and whose `prints` counts them, followed
by the reader's own statistics. [`itch50_trades`](@ref) and
[`iex_hist_trades`](@ref) already did, with `prints` further down their
field list; the REST and bulk readers now do too.

| 1.x | 2.0 |
|:----|:----|
| `trades = historical_trades(p, sym, d0, d1)` | `trades = historical_trades(p, sym, d0, d1).trades` |
| `n = historical_trades(p, sym, d0, d1; each_page = f)` | `n = historical_trades(p, sym, d0, d1; each_page = f).prints` |
| `bulk_trades(p, sym, day; dest_dir)` (`Vector{Trade}` or `Int`) | `(; trades, prints, file)` |
| `historical_trades(...; on_page = (n, total) -> …)` | removed; `each_page` receives every page |

## Package exceptions

Operational refusals and failures throw subtypes of
[`MarketTickStreamerError`](@ref) instead of `ErrorException`. Code that
matched on message text can match on the type.

| Situation | 1.x | 2.0 |
|:----------|:----|:----|
| low disk, no scratch space, heap ceiling, `read_processed` memory guard | `ErrorException` | [`ResourceError`](@ref) (`resource` = `:disk`, `:scratch`, `:heap`, `:memory`) |
| checksum or size mismatch, malformed vendor file | `ErrorException` | [`IntegrityError`](@ref) |
| vendor lists no file, Range answered wrongly, download stalls | `ErrorException` | [`VendorError`](@ref) |
| data tree locked, followed session stalls | `ErrorException` | [`SessionError`](@ref) |
| missing credentials | `ErrorException` | [`CredentialError`](@ref) |
| fatal stream error | `FatalStreamError <: Exception` | `FatalStreamError <: MarketTickStreamerError` |

`ArgumentError` still marks an invalid argument or configuration value.

## Providers register themselves

An adapter in another package is configured like a shipped one: defining
`provider_spec(::Val{:name})` registers it, `[provider] name = "name"` selects
it and a `[name]` table holds its endpoint roots.

| 1.x | 2.0 |
|:----|:----|
| `ProviderSpec(feeds, backfill_feeds, bulk_feeds, tz, needs_credentials, max_page_limit, regular_session, price_unit, size_unit, timestamp_unit)` | `ProviderSpec(; feeds, tz, max_page_limit, …)`, keyword construction; `needs_credentials` is replaced by `credential_keys`, and `endpoint_keys` lists the keys of the provider's table |
| `make_provider(::Val{name}, cfg, key, secret)` | `make_provider(::Val{name}, cfg)`; an adapter that needs credentials calls `load_credentials!(cfg)` |
| `load_credentials!(cfg) -> (key, secret)` | `load_credentials!(cfg) -> Dict{String,String}`, keyed by the variable names of `credential_keys` |
| `load_credentials!(; env_path)` (Alpaca's two variables) | `load_credentials!(names; env_path)` |
| `MarketTickStreamer.KNOWN_PROVIDERS` | [`MarketTickStreamer.registered_providers()`](@ref MarketTickStreamer.registered_providers) |
| a key in `[alpaca]` or `[binance]` other than an endpoint root was accepted | rejected, as an unknown key in any other table is |
