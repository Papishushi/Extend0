using System.Text.Json;

namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Serializable request used to add a new index to a table, or replace an existing one.
/// </summary>
/// <param name="Name">
/// Logical index name (unique within the table). Used to reference the index for later mutations.
/// </param>
/// <param name="Kind">
/// The kind of index to create (implementation-defined), which determines how <paramref name="IndexInputPayload"/> is interpreted.
/// </param>
/// <param name="IndexInputPayload">
/// Opaque, kind-specific JSON payload describing how to build the index (e.g., target columns, options).
/// The receiver is responsible for validating its schema for the given <paramref name="Kind"/>.
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
    bool ReplaceIfExists = false,
    string? Notes = null
);
