# API reference: acquisition and storage

The public surface, grouped by pipeline stage. Each group is generated from
one source file, so everything carrying a docstring appears here. The
analysis side — data quality, activity clocks, replay and the diagnostic
figures — is on the [next page](api_analysis.md).

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
written against it and never against a vendor. Two adapters ship, and they
disagree on every axis the interface abstracts: Alpaca is credentialed, files
its tape under the New York calendar, and closes overnight and at weekends;
Binance is public, files under UTC, and never closes. A configuration is
validated against the selected provider's own [`ProviderSpec`](@ref) rather
than against one vendor's vocabulary.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["live.jl"]
```

## Alpaca adapter

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["alpaca.jl"]
```

## Binance adapter

Crypto spot, public market data, no credentials. The venue never closes, so
the market-hours railings do not apply and the calendar is UTC.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["binance.jl"]
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

## Live monitoring

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["monitor.jl"]
```
