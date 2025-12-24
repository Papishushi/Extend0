namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Status code for an index lookup request performed across RPC/IPC boundaries.
/// </summary>
/// <remarks>
/// <para>
/// Values are grouped so clients can distinguish "normal misses" (<see cref="NotFound"/>) from
/// operational issues (e.g. table not open, index not built).
/// </para>
/// </remarks>
public enum IndexLookupStatusDTO : byte
{
    /// <summary>
    /// The lookup completed successfully and returned a hit.
    /// </summary>
    Ok = 0,

    /// <summary>
    /// The lookup completed successfully but no matching entry was found.
    /// </summary>
    NotFound = 1,

    /// <summary>
    /// The table is not open/available in the server process for the requested operation.
    /// </summary>
    TableNotOpen = 10,

    /// <summary>
    /// The index exists but is not populated/built (e.g. rebuild has not been performed).
    /// </summary>
    IndexNotBuilt = 11,

    /// <summary>
    /// The target column is value-only (no key segment) and cannot participate in key lookups.
    /// </summary>
    ValueOnlyColumn = 12,

    /// <summary>
    /// The provided column index is out of range or otherwise invalid.
    /// </summary>
    InvalidColumn = 13,

    /// <summary>
    /// The provided key is invalid for the requested lookup (e.g. empty, null, wrong encoding, or rejected by policy).
    /// </summary>
    InvalidKey = 14
}
