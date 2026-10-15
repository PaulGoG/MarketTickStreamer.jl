# API reference: acquisition and storage

The public surface, grouped by pipeline stage. Each group is generated from
one source file, so everything carrying a docstring appears here. The
provider interface and the two adapters are on the
[providers page](api_providers.md); the analysis side — data quality,
activity clocks, replay, corpus access and the diagnostic figures — on the
[analysis page](api_analysis.md).

```@docs
MarketTickStreamer
```

## Configuration and credentials

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["config.jl"]
```

## Schema and timestamps

Timestamps are `Int64` nanoseconds since the UNIX epoch everywhere — the
only representation that survives the full range of exchange precision
without rounding.

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

One file, one trading day, every symbol: the readers and the import stage.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["itch50.jl", "iex_hist.jl", "import.jl"]
```

## Live monitoring

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["monitor.jl"]
```
