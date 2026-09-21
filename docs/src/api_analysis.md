# API reference: analysis interfaces

Continues the [acquisition and storage reference](api.md): what operates on
a loaded or replayed capture.

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

## Diagnostic figures

Declared and documented in the package, implemented by a package extension
that loads with CairoMakie: `using MarketTickStreamer, CairoMakie`. Without it
a call ends in a `MethodError` that names the package to load.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["figures.jl"]
```

## Numerics behind the figures

Plotting-free, and usable on their own.

```@autodocs
Modules = [MarketTickStreamer]
Pages = ["diagnostics.jl"]
```
