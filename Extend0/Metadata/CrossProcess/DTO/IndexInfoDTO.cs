namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Serializable, cross-process safe description of an index registered in a table.
/// </summary>
/// <remarks>
/// <para>
/// This DTO is intended for discovery and diagnostics (e.g., UI tooling, remote inspection).
/// It describes index identity and capabilities without exposing live index instances.
/// </para>
/// </remarks>
/// <param name="Name">Logical name of the index in the table registry.</param>
/// <param name="Kind">Kind/category of the index, used for client-side routing and display.</param>
/// <param name="IsRebuildable">Indicates whether the index supports rebuilding by scanning the table.</param>
/// <param name="IsBuiltIn">Indicates whether this index is a built-in/system index.</param>
/// <param name="Notes">Optional free-form notes intended for diagnostics or UI display.</param>
public readonly record struct IndexInfoDTO(
    string Name,
    IndexKindDTO Kind,
    bool IsRebuildable,
    bool IsBuiltIn,
    string? Notes = null
);
