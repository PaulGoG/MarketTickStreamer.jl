# API reference: providers

The acquisition layer is written against the provider interface below and
never against a vendor. Each adapter declares what the configuration layer
must know about it in a [`ProviderSpec`](@ref), and implements the clock,
the historical and bulk sources and the stream protocol.

## Provider interface

A provider adapter implements this interface. Two adapters ship, and they
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
the market-hours guards do not apply and the calendar is UTC.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["binance.jl"]
```
