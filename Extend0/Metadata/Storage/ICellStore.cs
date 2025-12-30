using Extend0.Metadata.Schema;

namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Abstraction over the underlying storage mechanism for metadata cells.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implementations of <see cref="ICellStore"/> provide a uniform way for
    /// <see cref="IMetadataTable"/> to access and materialize cells, regardless of whether
    /// the data is backed by an in-memory buffer (<c>InMemoryStore</c>) or a memory-mapped
    /// file (<c>MappedStore</c>).
    /// </para>
    /// <para>
    /// The store is responsible for:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Exposing the total number of cells via <see cref="Count"/>.</description></item>
    ///   <item><description>Returning existing cells or creating them on demand.</description></item>
    ///   <item><description>Providing column-wide views via <see cref="TryGetColumnBlock"/> for fast, unsafe access.</description></item>
    ///   <item><description>Enumerating all cells in a table via <see cref="EnumerateCells"/>.</description></item>
    /// </list>
    /// <para>
    /// <see cref="ICellStore"/> instances are <see cref="IDisposable"/> and typically own
    /// unmanaged resources (e.g., memory-mapped views), so callers must dispose them when
    /// the associated <see cref="IMetadataTable"/> is no longer needed.
    /// </para>
    /// </remarks>
    internal interface ICellStore : IDisposable, IEnumerable<CellRowColumnValueEntry>
    {
        /// <summary>
        /// Attempts to retrieve a low-level view over all values in a column.
        /// </summary>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="block">
        /// When this method returns <see langword="true"/>, contains a <see cref="ColumnBlock"/>
        /// describing the base pointer, stride, and value-region layout for the column.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the column exists and the block could be obtained;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This is a low-level, unsafe API intended for bulk operations (e.g. scanning or
        /// vectorized processing) where the caller knows the exact binary layout of the
        /// values stored in the column.
        /// </remarks>
        bool TryGetColumnBlock(uint column, out ColumnBlock block);

        /// <summary>
        /// Attempts to retrieve an existing cell at the specified column and row.
        /// </summary>
        /// <param name="col">Zero-based column index.</param>
        /// <param name="row">Zero-based row index.</param>
        /// <param name="cell">
        /// When this method returns <see langword="true"/>, contains a <see cref="MetadataCell"/>
        /// representing the requested location.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the cell exists; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This method never creates new storage; it only exposes an already existing cell,
        /// if present. To allocate a new cell (or ensure it exists), use
        /// <see cref="GetOrCreateCell(uint,uint,in ColumnConfiguration)"/>.
        /// </remarks>
        bool TryGetCell(uint col, uint row, out CodeGen.MetadataCell cell);

        /// <summary>
        /// Gets a cell at the specified column and row, creating it if necessary.
        /// </summary>
        /// <param name="col">Zero-based column index.</param>
        /// <param name="row">Zero-based row index.</param>
        /// <param name="meta">
        /// Column metadata describing the key/value sizes and initial capacity for the column.
        /// </param>
        /// <returns>
        /// A <see cref="MetadataCell"/> bound to the requested location.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Implementations may allocate backing storage lazily when this method is called.
        /// For memory-mapped stores, the underlying region is assumed to exist and this
        /// typically just wraps the appropriate pointer.
        /// </para>
        /// <para>
        /// Callers are responsible for populating keys and values inside the returned cell.
        /// </para>
        /// </remarks>
        CodeGen.MetadataCell GetOrCreateCell(uint col, uint row, in ColumnConfiguration meta);

        /// <summary>
        /// Returns an enumerable that iterates over all cells in the store.
        /// </summary>
        /// <returns>
        /// A <see cref="CellEnumerable"/> that yields tuples of column, row and cell.
        /// </returns>
        /// <remarks>
        /// The enumeration order is implementation-defined but typically iterates
        /// column-by-column and row-by-row. Cells may be materialized lazily.
        /// </remarks>
        CellEnumerable EnumerateCells();

        /// <summary>
        /// Gets the total number of addressable cells in the store.
        /// </summary>
        /// <remarks>
        /// This is usually the product of the number of columns and each column's
        /// configured row capacity, but implementations are free to compute it in a way
        /// that makes sense for their internal layout.
        /// </remarks>
        int Count { get; }
    }
}
