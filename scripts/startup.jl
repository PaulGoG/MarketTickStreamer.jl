# Shared script prelude: silent environment activation plus the common
# entry-point wrapper — `include` this from any script.
using Pkg
Pkg.activate(joinpath(@__DIR__, ".."); io = devnull)
Pkg.instantiate(; io = devnull)

"""
    run_entrypoint(main)

Run a script's `main` with clean Ctrl-C shutdown: SIGINT arrives as
`InterruptException` (best-effort under threads; the crash-only session
lifecycle is the real guarantee) and exits without a stack trace.
"""
function run_entrypoint(main::Function)
    Base.exit_on_sigint(false)
    try
        main()
    catch e
        e isa InterruptException || rethrow()
        println("\nInterrupted — exiting cleanly.")
    end
end
