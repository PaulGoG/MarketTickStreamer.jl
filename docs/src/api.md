# API reference: acquisition and storage

The public surface, grouped by pipeline stage. Each group is generated from
one source file, so everything carrying a docstring appears here. The
provider interface and the adapters are on the
[providers page](api_providers.md); the analysis side — data quality,
activity clocks, replay, corpus access and the diagnostic figures — on the
[analysis page](api_analysis.md).

Names with a leading underscore are internal. They are documented for the
reader of the code and are not covered by the versioning policy of the
README.

```@docs
MarketTickStreamer
```

## Errors

An invalid argument or configuration value throws `ArgumentError`. Every
other refusal or failure of the package throws one of the types below, so a
caller can tell a full disk from a corrupt vendor file without reading the
message.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["errors.jl"]
```

## Configuration and credentials

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["config.jl"]
```

## Schema and timestamps

Timestamps are `Int64` nanoseconds since the UNIX epoch everywhere, which
holds the full range of exchange precision without rounding.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["schema.jl"]
```

## Persistence and compaction

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["sinks.jl"]
```

## Session orchestration

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["pipeline.jl"]
```

## Vendor day files

Readers for vendor files, each holding one trading day of every symbol, and
the import stage built on them.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["itch50.jl", "iex_hist.jl", "import.jl"]
```

## Live monitoring

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["monitor.jl"]
```
