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
/// <param name="Found">Indicates whether the lookup found a matching entry.</param>
/// <param name="Col">Zero-based column index where the key was found; meaningful only when <paramref name="Found"/> is <see langword="true"/>.</param>
/// <param name="Row">Zero-based row index where the key was found; meaningful only when <paramref name="Found"/> is <see langword="true"/>.</param>
/// <param name="TableName">The name of the table where the key was found.</param>
public readonly record struct IndexHitDTO(
    bool Found,
    uint Col,
    uint Row,
    string TableName
);
