@testset "static QA (Aqua)" begin
    Aqua.test_all(MarketTickStreamer)
end

@testset "static QA (ExplicitImports)" begin
    # The owner and public-access checks are not asserted:
    # a few stable non-public names are load-bearing here
    # (`HTTP.StatusError`, `Arrow.Table`, `Base.gc_live_bytes`).
    @test check_no_implicit_imports(MarketTickStreamer) === nothing
    @test check_no_stale_explicit_imports(MarketTickStreamer) === nothing
    @test check_no_self_qualified_accesses(MarketTickStreamer) === nothing
end

@testset "static QA (JET)" begin
    # Restricted to this module: the dependency tree reports a hundred-odd
    # findings of its own, none of them actionable here. JET follows the
    # compiler, so a Julia upgrade may surface new reports — which is the
    # reason to run it.
    if JET_ENABLED
        JET.test_package(MarketTickStreamer; target_modules = (MarketTickStreamer,))
    else
        @test_skip "JET package analysis (JET unavailable)"
    end
end

@testset "static QA (allocation-free hot paths)" begin
    # These three run once per print on the ingest and compaction paths.
    # Zero allocations is the measured state, asserted here so an
    # accidental boxing or a dynamic dispatch shows up as a test failure.
    if ALLOCCHECK_AVAILABLE
        @test isempty(check_allocs(MarketTickStreamer.ns_to_datetime, (Int64,)))
        @test isempty(check_allocs(MarketTickStreamer.now_ns, ()))
        @test isempty(check_allocs(MarketTickStreamer.trading_date, (Int64,)))
    else
        @test_skip "allocation-free hot paths (AllocCheck unavailable)"
    end
    # `rfc3339_to_ns` and `ns_to_rfc3339` are not asserted: the first
    # matches a regular expression and validates through `DateTime`, the
    # second builds a String. At a few hundred nanoseconds each against
    # network-bound ingestion, hand-rolling either to reach zero is not
    # justified by measurement — see bench/benchmarks.jl.
end

@testset "release metadata: README, CITATION.cff, CHANGELOG and Project.toml agree" begin
    # The cited version (README BibTeX, CITATION.cff) is the latest
    # release. Project.toml carries it on a release commit and the next
    # -DEV version between releases. Checked here so a tag whose README,
    # CITATION.cff or CHANGELOG lags fails in CI rather than after
    # publication.
    root = pkgdir(MarketTickStreamer)
    project = pkgversion(MarketTickStreamer)
    readme = read(joinpath(root, "README.md"), String)
    cff = read(joinpath(root, "CITATION.cff"), String)
    changelog = read(joinpath(root, "CHANGELOG.md"), String)
    index = read(joinpath(root, "docs", "src", "index.md"), String)

    bibtex = match(r"version\s*=\s*\{([0-9][^}]*)\}", readme)
    @test bibtex !== nothing
    cited = VersionNumber(bibtex[1])
    cff_version = match(r"^version:\s*\"([^\"]+)\""m, cff)
    @test cff_version !== nothing
    @test VersionNumber(cff_version[1]) == cited

    if isempty(project.prerelease)
        @test cited == project
        @test occursin("## [$project] - ", changelog)
    else
        @test cited < project
    end

    section =
        match(Regex("^## \\[\\Q$cited\\E\\] - (\\d{4}-\\d{2}-\\d{2})\$", "m"), changelog)
    @test section !== nothing
    released = match(r"^date-released:\s*\"(\d{4}-\d{2}-\d{2})\""m, cff)
    @test released !== nothing
    @test section !== nothing && released !== nothing && section[1] == released[1]

    # The installation recipes pin the cited release tag.
    for text in (readme, index)
        revs = [m[1] for m in eachmatch(r"rev = \"(v[^\"]+)\"", text)]
        @test !isempty(revs)
        @test all(==("v$cited"), revs)
    end
end

@testset "exceptions: one supertype, typed fields, plain messages" begin
    for T in (
        ResourceError,
        IntegrityError,
        VendorError,
        SessionError,
        CredentialError,
        FatalStreamError,
    )
        @test T <: MarketTickStreamerError <: Exception
    end
    @test sprint(showerror, ResourceError(:disk, "only 1.2 GiB free")) ==
          "ResourceError: only 1.2 GiB free"
    @test sprint(showerror, FatalStreamError("auth failed")) ==
          "FatalStreamError: auth failed"
    # A refusal names its resource: a handler branches on a field, not on text.
    err = try
        check_live_heap(0; context = "test")
    catch e
        e
    end
    @test err isa ResourceError && err.resource === :heap
    @test occursin("during test", err.msg)
    # An interrupt ends an entry point without an error.
    @test run_entrypoint(() -> throw(InterruptException())) === nothing
end
