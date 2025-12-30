namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Serializable, cross-process safe representation of a key lookup hit.
/// </summary>
/// <remarks>
/// <para>
/// This DTO is used across RPC/IPC boundaries to report where a key was found
/// without returning process-dependent objects (tables, cells, spans, pointers).
/// </para>
/// <para>
/// When <see cref="Found"/> is <see langword="false"/>, <see cref="Col"/> and <see cref="Row"/>
/// should be treated as default values.
/// </para>
/// </remarks>
public readonly record struct IndexHitDTO(
    /// <summary>
    /// Indicates whether the lookup found a matching entry.
    /// </summary>
    bool Found,

    /// <summary>
    /// Zero-based column index where the key was found.
    /// </summary>
    /// <remarks>
    /// Only meaningful when <see cref="Found"/> is <see langword="true"/>.
    /// </remarks>
    uint Col,

    /// <summary>
    /// Zero-based row index where the key was found.
    /// </summary>
    /// <remarks>
    /// Only meaningful when <see cref="Found"/> is <see langword="true"/>.
    /// </remarks>
    uint Row,

    /// <summary>
    /// The name of the table where the key was found.
    /// </summary>
    /// <remarks>
    /// Only meaningful when <see cref="Found"/> is <see langword="true"/>.
    /// </remarks>
    string TableName
);
