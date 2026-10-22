# Contributing

## Environment

Julia ≥ 1.12, installed with `juliaup`. Every environment — the package, and
`scripts/`, `test/`, `docs/`, `bench/` and `examples/` — has an
`activate.jl` that activates and instantiates it silently, and every script
includes its own as its first statement, so `--project` is never passed. The
side environments consume the package by path, so they always run against
the working tree. Manifests are not tracked. A dependency change updates
`[deps]` and `[compat]` together.

## Tests and static analysis

```bash
julia -e 'include("activate.jl"); using Pkg; Pkg.test()'
```

The suite includes Aqua, JET, ExplicitImports and AllocCheck, and its
mock servers need no network or credentials. Every change lands with a
green suite, and new behaviour comes with a test. Floating-point
comparisons use `isapprox` with explicit tolerances.

## Formatting

JuliaFormatter, against the committed `.JuliaFormatter.toml`. CI fails on
unformatted files.

## Documentation

Every public function and struct carries a docstring. A change that alters a
claim made on a manual page updates that page in the same change.
`julia docs/make.jl` builds the manual into `docs/build/`.

## Configuration changes

A new key gets validation in `src/config.jl`, a one-line comment in
`config/config.toml` — what it is, its allowed values, its bounds and units,
no rationale — and a row on the usage page of the manual.

## Commits and pull requests

Conventional Commits prefixes (`feat:`, `fix:`, `refactor:`, `docs:`,
`test:`, `perf:`, `chore:`), one logical change per commit, and a body that
states what changed and why. No attribution trailers, generated footers or
tool identifiers in commits or pull requests. A pull request states the
change, how it was verified, and what is still open.

## Versioning

User-visible changes go under `[Unreleased]` in `CHANGELOG.md`. A change to
the public surface, as the README defines it under "Public surface and
versioning", is marked **Breaking** and waits for a major release.
