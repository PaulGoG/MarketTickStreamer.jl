# API reference

The public surface, grouped by pipeline stage. Each group is generated from
one source file, so everything carrying a docstring appears here.

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

## Provider interface

A provider adapter implements this interface; the acquisition layer is
written against it and never against a vendor.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["live.jl"]
```

## Alpaca adapter

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["alpaca.jl"]
```

## Persistence and compaction

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["sinks.jl"]
```

## Data quality and resource guards

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["quality.jl"]
```

## Resampling onto activity clocks

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["resample.jl"]
```

## Replay

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["replay.jl"]
```

## Session orchestration

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["pipeline.jl"]
```

## Live monitoring

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["monitor.jl"]
```

## Visualization

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["visualization.jl"]
```
