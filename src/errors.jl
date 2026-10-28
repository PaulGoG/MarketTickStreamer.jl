# Exceptions of the package. An invalid argument or configuration value throws
# `ArgumentError`; an operation refused or failed for a reason outside its
# arguments throws a subtype of `MarketTickStreamerError`, so a caller tells
# the cases apart by type rather than by message text.

"""
    MarketTickStreamerError <: Exception

Supertype of the exceptions thrown when an operation is refused or fails for
a reason outside its arguments. Invalid arguments and configuration values
throw `ArgumentError`; HTTP statuses and transport failures that outlast the
retry policy ([`RestPolicy`](@ref)) propagate as HTTP.jl's own exceptions.

| Type | Thrown when |
|:-----|:------------|
| [`ResourceError`](@ref) | free disk, scratch space, heap or memory is below what the operation needs |
| [`IntegrityError`](@ref) | data fails verification or lacks the structure its format specifies |
| [`VendorError`](@ref) | a vendor's service does not deliver what it lists or was asked for |
| [`SessionError`](@ref) | the data tree is locked by another session, or a followed session stalls |
| [`CredentialError`](@ref) | credentials the provider needs are missing |
| [`FatalStreamError`](@ref) | the venue ends a live stream for a reason a reconnect cannot fix |

Every subtype has a field `msg`, the account `showerror` prints, beside the
fields a handler branches on.
"""
abstract type MarketTickStreamerError <: Exception end

Base.showerror(io::IO, e::MarketTickStreamerError) =
    print(io, nameof(typeof(e)), ": ", e.msg)

"""
    ResourceError(resource, msg)

A resource is short of what the operation needs. `resource` is `:disk` (free
space on the data tree below `limits.min_free_disk_gb`), `:scratch` (no room
for a spill compaction's scratch copy), `:heap` (live heap above
`limits.max_live_heap_mb`) or `:memory` (a read that would exceed its share
of free memory).
"""
struct ResourceError <: MarketTickStreamerError
    resource::Symbol
    msg::String
end

"""
    IntegrityError(source, msg)

Data failed verification (checksum, announced size) or does not have the
structure its format specifies (a truncated message, a malformed capture
block, a row filed under the wrong day). `source` names the file. A download
that failed verification has been discarded; a malformed file is left as it
is.
"""
struct IntegrityError <: MarketTickStreamerError
    source::String
    msg::String
end

"""
    VendorError(source, msg)

A vendor's service did not deliver what it lists or what was asked of it: no
file listed for the date, a listed file missing, a Range request answered
from the wrong offset, a download that stopped making progress. `source` is
the URL. Nothing was written in its place; a later attempt may succeed.
"""
struct VendorError <: MarketTickStreamerError
    source::String
    msg::String
end

"""
    SessionError(path, msg)

The session lifecycle refused or ended an operation: another live session
holds the data tree's lock (`path` is the lock file), or a followed session
stopped growing while its sidecar still reads `running` (`path` is the
sidecar).
"""
struct SessionError <: MarketTickStreamerError
    path::String
    msg::String
end

"""
    CredentialError(names, msg)

Credentials the provider declares (`ProviderSpec.credential_keys`) are absent
from both the environment file and the process environment; `names` lists
the missing variables.
"""
struct CredentialError <: MarketTickStreamerError
    names::Vector{String}
    msg::String
end

"""
    FatalStreamError(msg)

A stream error that must not trigger reconnection (bad credentials,
connection-limit exceeded, invalid subscription). Aborts the session.
"""
struct FatalStreamError <: MarketTickStreamerError
    msg::String
end
