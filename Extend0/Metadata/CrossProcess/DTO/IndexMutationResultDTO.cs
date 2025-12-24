namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Serializable result returned by index mutation operations (add/remove/replace).
/// </summary>
/// <param name="Status">
/// Outcome of the operation. See <see cref="IndexMutationStatusDTO"/> for possible values.
/// </param>
/// <param name="Index">
/// When <paramref name="Status"/> is <see cref="IndexMutationStatusDTO.Ok"/>, may contain the resulting index metadata.
/// For non-success outcomes, this is typically <see langword="null"/>.
/// </param>
/// <param name="Notes">
/// Optional human-readable details about the outcome (validation errors, warnings, or diagnostics).
/// </param>
public readonly record struct IndexMutationResultDTO(
    IndexMutationStatusDTO Status,
    IndexInfoDTO? Index = null,
    string? Notes = null
);