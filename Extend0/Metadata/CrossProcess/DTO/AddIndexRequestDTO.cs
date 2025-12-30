using System.Text.Json;

namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Serializable request used to add a new index to a table, or replace an existing one.
/// </summary>
/// <remarks>
/// <para>
/// The request supports two layers of extensibility:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///     A kind-specific JSON payload (<see cref="IndexInputPayload"/>) that describes the index configuration
///     (targets, options, schema version, etc.).
///     </description>
///   </item>
///   <item>
///     <description>
///     An optional binary "program" (<see cref="ProgramBytes"/>) that can be interpreted by the host as a safe
///     DSL/bytecode for custom indexing without loading arbitrary .NET assemblies.
///     </description>
///   </item>
/// </list>
/// <para>
/// When <see cref="ProgramBytes"/> is provided, <see cref="ProgramHashSha256"/> can be used by the receiver
/// to validate integrity (and optionally as part of a trust/signature policy).
/// </para>
/// </remarks>
/// <param name="Name">
/// Logical index name (unique within the target registry). Used to reference the index for later mutations.
/// </param>
/// <param name="Kind">
/// The kind of index to create, which determines how <paramref name="IndexInputPayload"/> and
/// <paramref name="ProgramBytes"/> are interpreted.
/// </param>
/// <param name="IndexInputPayload">
/// Opaque, kind-specific JSON payload describing how to build the index (e.g., target columns, options,
/// ABI/schema version). The receiver is responsible for validating its schema for the given <paramref name="Kind"/>.
/// </param>
/// <param name="ProgramBytes">
/// Optional binary program payload (e.g., bytecode/DSL) executed or interpreted by the host to implement
/// custom indexing logic. When <see langword="null"/>, the receiver should rely solely on
/// <paramref name="IndexInputPayload"/> and built-in capabilities.
/// </param>
/// <param name="ProgramHashSha256">
/// Optional SHA-256 hash of <paramref name="ProgramBytes"/> (typically hex-encoded) for integrity verification.
/// When provided, receivers may reject the request if the computed hash does not match.
/// </param>
/// <param name="ReplaceIfExists">
/// When <see langword="true"/>, replaces an existing index with the same <paramref name="Name"/> if present.
/// When <see langword="false"/>, the operation returns <see cref="IndexMutationStatusDTO.AlreadyExists"/> if the index exists.
/// </param>
/// <param name="Notes">
/// Optional free-form notes (diagnostics, caller context, or human-readable hints) carried along with the request.
/// </param>
public readonly record struct AddIndexRequestDTO(
    string Name,
    IndexKindDTO Kind,
    JsonElement IndexInputPayload,
    byte[]? ProgramBytes = null,
    string? ProgramHashSha256 = null,
    bool ReplaceIfExists = false,
    string? Notes = null
);