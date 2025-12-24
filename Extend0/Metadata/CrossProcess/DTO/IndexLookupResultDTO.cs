namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Serializable result for an index lookup, including optional diagnostics.
/// </summary>
/// <remarks>
/// <para>
/// This DTO is designed for RPC/IPC boundaries where exceptions are expensive/noisy and
/// clients benefit from structured outcomes.
/// </para>
/// <para>
/// When <see cref="Status"/> is <see cref="IndexLookupStatusDTO.Ok"/>, <see cref="Hit"/> should have
/// <see cref="IndexHitDTO.Found"/> = <see langword="true"/>.
/// When <see cref="Status"/> is <see cref="IndexLookupStatusDTO.NotFound"/>, <see cref="Hit"/> should have
/// <see cref="IndexHitDTO.Found"/> = <see langword="false"/>.
/// For operational failures, <see cref="Hit"/> is typically default and <see cref="Notes"/> may contain
/// a best-effort explanation suitable for logs/UI.
/// </para>
/// </remarks>
public readonly record struct IndexLookupResultDTO(
    /// <summary>
    /// Structured outcome of the lookup request.
    /// </summary>
    IndexLookupStatusDTO Status,

    /// <summary>
    /// Hit coordinates when the lookup found an entry; otherwise default.
    /// </summary>
    IndexHitDTO Hit,

    /// <summary>
    /// Optional best-effort diagnostic notes (not intended for programmatic parsing).
    /// </summary>
    string? Notes = null
);