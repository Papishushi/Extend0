namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Identifies the kind/category of an index for cross-process inspection and routing.
/// </summary>
/// <remarks>
/// <para>
/// This enum is intentionally stable and serializer-friendly (byte-backed) so RPC clients
/// can switch on the kind without relying on string conventions.
/// </para>
/// </remarks>
public enum IndexKindDTO : byte
{
    /// <summary>
    /// Unknown or implementation-defined index kind.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Built-in per-column key index (maps: column + key → row).
    /// </summary>
    BuiltIn_ColumnKey = 1,

    /// <summary>
    /// Built-in global key index (maps: key → (column,row)).
    /// </summary>
    BuiltIn_GlobalKey = 2,

    /// <summary>
    /// Built-in global multi table key index (maps: key → (tableName,column,row)).
    /// </summary>
    BuiltIn_GlobalMultiTableKey = 3,

    /// <summary>
    /// Custom / user-registered table index kind.
    /// </summary>
    Custom_InTable = 100,

    /// <summary>
    /// Custom / user-registered manager index kind.
    /// </summary>
    Custom_CrossTable = 101
}
