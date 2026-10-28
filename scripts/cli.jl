# Argument parsing shared by the entry-point scripts. Their syntax is public
# and versioned with the package: `--name value` or `--name=value` for a
# valued option, `--name` for a flag, anything else positional, in any order.

"""
    parse_cli(args; valued = (), flags = ()) -> (; opts, flags, positional)

Walk `args`: `--name value` and `--name=value` for every `name` in `valued`
(`opts::Dict{String,String}`), `--name` for every `name` in `flags`
(`flags::Set{String}`), everything else positional (`positional::Vector{String}`).
A valued option without a value and an option of neither kind throw
`ArgumentError`.
"""
function parse_cli(args; valued = (), flags = ())
    opts = Dict{String,String}()
    seen = Set{String}()
    positional = String[]
    i = 1
    while i <= length(args)
        a = args[i]
        if startswith(a, "--")
            name, value =
                occursin('=', a) ? split(a[3:end], '='; limit = 2) : (a[3:end], nothing)
            name = String(name)
            if name in valued
                if value === nothing
                    i < length(args) || throw(ArgumentError("--$name needs a value"))
                    value = args[i+1]
                    i += 1
                end
                opts[name] = String(value)
            elseif name in flags
                value === nothing || throw(ArgumentError("--$name takes no value"))
                push!(seen, name)
            else
                throw(ArgumentError("unknown option --$name"))
            end
        else
            push!(positional, String(a))
        end
        i += 1
    end
    return (; opts, flags = seen, positional)
end

"""
    config_path(opts) -> String
    config_path(opts, positional) -> String

The `--config` value, or the repository's `config/config.toml`. The second
form, used by `stream.jl` and `backfill.jl`, also takes the path as the only
positional argument; a path given both ways, or a second positional
argument, throws `ArgumentError`.
"""
config_path(opts) = get(opts, "config", joinpath(@__DIR__, "..", "config", "config.toml"))

function config_path(opts, positional)
    length(positional) <= 1 || throw(
        ArgumentError(
            "expected at most one configuration path, got $(length(positional)) arguments",
        ),
    )
    isempty(positional) && return config_path(opts)
    haskey(opts, "config") && throw(
        ArgumentError(
            "configuration given twice: --config $(opts["config"]) and $(only(positional))",
        ),
    )
    return only(positional)
end
