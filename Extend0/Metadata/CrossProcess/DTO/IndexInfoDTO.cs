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
public readonly record struct IndexInfoDTO(
    /// <summary>
    /// Logical name of the index in the table registry.
    /// </summary>
    string Name,

    /// <summary>
    /// Kind/category of the index, used for client-side routing and display.
    /// </summary>
    IndexKindDTO Kind,

    /// <summary>
    /// Indicates whether the index supports rebuilding by scanning the table.
    /// </summary>
    /// <remarks>
    /// Typically corresponds to implementing an internal rebuild contract (e.g. <c>IRebuildableIndex</c>).
    /// </remarks>
    bool IsRebuildable,

    /// <summary>
    /// Indicates whether this index is a built-in/system index.
    /// </summary>
    bool IsBuiltIn,

    /// <summary>
    /// Optional free-form notes intended for diagnostics or UI display.
    /// </summary>
    string? Notes = null
);
