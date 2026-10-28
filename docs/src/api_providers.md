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
Pages = ["live.jl", "rest.jl"]
```

## Adding a provider

An adapter for a third venue lives in a package of its own and needs no
change to this one. Defining `provider_spec` for a new name registers it:
[`load_config`](@ref) then accepts `[provider] name = "<name>"`, validates
`provider.feed` against the spec's feeds and the `[<name>]` table against its
`endpoint_keys`, and [`run_stream`](@ref) and [`run_backfill`](@ref)
construct it through `make_provider`.

```julia
using MarketTickStreamer
import MarketTickStreamer: provider_spec, make_provider, exchange_tz, market_clock,
    historical_trades, stream_protocol!
using TimeZones: @tz_str

struct VenueProvider <: AbstractProvider
    rest_base::String
    token::String
end

provider_spec(::Val{:venue}) = ProviderSpec(;
    feeds = ["trades"],
    backfill_feeds = ["trades"],
    tz = tz"UTC",
    credential_keys = ["VENUE_TOKEN"],
    endpoint_keys = ["rest_base"],
    max_page_limit = 1000,
    regular_session = (0.0, 24.0),
    price_unit = "USD",
    size_unit = "units",
    timestamp_unit = "microsecond",
)

make_provider(::Val{:venue}, cfg::Config) = VenueProvider(
    get(cfg.endpoints, "rest_base", "https://api.venue.example"),
    load_credentials!(cfg)["VENUE_TOKEN"],
)

exchange_tz(::VenueProvider) = tz"UTC"
# market_clock, historical_trades and stream_protocol! complete the adapter;
# always_open, session_days and feed_delay_ns where the venue departs from
# the defaults.
```

The shipped adapters, `src/providers/alpaca.jl` (credentialed, market
hours) and `src/providers/binance.jl` (public, around the clock, bulk
files), are complete examples of the rest of the interface.

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
