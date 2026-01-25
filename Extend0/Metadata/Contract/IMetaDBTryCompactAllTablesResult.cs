namespace Extend0.Metadata.Contract;

/// <summary>
/// RPC/serialization-friendly result for bulk table compaction operations.
/// </summary>
/// <param name="Success">
/// <see langword="true"/> when all eligible tables were compacted successfully; otherwise, <see langword="false"/>.
/// </param>
/// <param name="FailedTableIds">
/// Identifiers of tables that failed to compact.
/// When <paramref name="Success"/> is <see langword="true"/>, this value is expected to be <see langword="null"/>.
/// When <paramref name="Success"/> is <see langword="false"/>, this value is expected to be a non-empty sequence.
/// </param>
/// <remarks>
/// <para>
/// This DTO exists to avoid relying on <see cref="ValueTuple"/> serialization details across RPC boundaries.
/// </para>
/// <para>
/// For convenience, implicit conversions are provided to and from the tuple shape
/// <c>(bool Success, IEnumerable&lt;Guid&gt;? FailedTableIds)</c>.
/// Consumers should treat <paramref name="FailedTableIds"/> as read-only and may materialize it (e.g., to an array)
/// if they need a stable snapshot.
/// </para>
/// </remarks>
public record struct TryCompactAllTablesResult(bool Success, IEnumerable<Guid>? FailedTableIds)
{
    /// <summary>
    /// Converts this DTO to its tuple equivalent.
    /// </summary>
    /// <param name="value">The DTO value to convert.</param>
    /// <returns>
    /// A tuple containing <c>Success</c> and <c>FailedTableIds</c> with the same semantics as this DTO.
    /// </returns>
    public static implicit operator (bool Success, IEnumerable<Guid>? FailedTableIds)(TryCompactAllTablesResult value)
        => (value.Success, value.FailedTableIds);

    /// <summary>
    /// Converts a tuple result into a <see cref="TryCompactAllTablesResult"/>.
    /// </summary>
    /// <param name="value">The tuple value to convert.</param>
    /// <returns>
    /// A DTO containing the tuple's <c>Success</c> and <c>FailedTableIds</c> values.
    /// </returns>
    public static implicit operator TryCompactAllTablesResult((bool Success, IEnumerable<Guid>? FailedTableIds) value)
        => new(value.Success, value.FailedTableIds);
}
