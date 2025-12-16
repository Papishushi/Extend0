using Extend0.Metadata.Schema;

namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Optional extension interface for <see cref="ICellStore"/> implementations
    /// that support growing the logical row capacity of a column.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Stores that implement this interface can participate in capacity policies like
    /// <see cref="CapacityPolicy.AutoGrowZeroInit"/>, allowing callers to request that
    /// a column is expanded up to a given minimum number of rows.
    /// </para>
    /// <para>
    /// Implementations are responsible for updating any internal metadata
    /// (row capacities, offsets, etc.) and, if requested, zero-initializing
    /// newly added value regions.
    /// </para>
    /// </remarks>
    internal interface ITryGrowableStore : ICellStore
    {
        /// <summary>
        /// Attempts to grow the specified column so that it can hold at least
        /// <paramref name="minRows"/> rows.
        /// </summary>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="minRows">
        /// Minimum logical row count required for this column (max row index + 1).
        /// </param>
        /// <param name="meta">
        /// Column configuration describing key/value sizes and other metadata
        /// for the target column.
        /// </param>
        /// <param name="zeroInit">
        /// When <see langword="true"/>, newly allocated value regions must be
        /// zero-initialized by the implementation.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the column can satisfy the requested capacity
        /// (either because it was already large enough or growth succeeded);
        /// otherwise, <see langword="false"/>.
        /// </returns>
        bool TryGrowColumnTo(uint column, uint minRows, in ColumnConfiguration meta, bool zeroInit);

        /// <summary>
        /// Attempts to retrieve the current physical row capacity of the specified column.
        /// </summary>
        /// <param name="column">
        /// Zero-based column index whose capacity is being queried.
        /// </param>
        /// <param name="rowCapacity">
        /// When this method returns <see langword="true"/>, contains the number of rows
        /// physically allocated for the column (i.e. the maximum valid row index is
        /// <c>rowCapacity - 1</c>). When it returns <see langword="false"/>, the value is undefined.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the column index is valid and the capacity could be
        /// determined; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The returned capacity represents the <em>physical</em> size of the column as
        /// allocated by the underlying store, not the logical row count of the table.
        /// </para>
        /// <para>
        /// This value is stable until the column is grown via
        /// <see cref="TryGrowColumnTo(uint, uint, in ColumnConfiguration, bool)"/>.
        /// </para>
        /// <para>
        /// For stores that do not support dynamic growth, this method typically reports
        /// the initial fixed capacity configured at creation time.
        /// </para>
        /// </remarks>
        bool TryGetColumnCapacity(uint column, out uint rowCapacity);

    }
}
