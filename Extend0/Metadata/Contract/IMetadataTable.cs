using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Indexing.Registries.Contract;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;

namespace Extend0.Metadata.Contract
{
    /// <summary>
    /// Represents a metadata table backed by a cell store and optionally accelerated by one or more indexes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A metadata table exposes a column/row addressing model. Cells can be accessed by numeric coordinates
    /// (<c>column,row</c>) or by column name, and can be discovered via key-based indexes when available.
    /// </para>
    /// <para>
    /// <b>Storage:</b> the table is backed by an <see cref="ICellStore"/> which provides the underlying persistence and
    /// capacity management for cells. Implementations may allow swapping the store via <see cref="CellStore"/> for
    /// testing or advanced scenarios.
    /// </para>
    /// <para>
    /// <b>Schema:</b> <see cref="Spec"/> defines the table layout (columns, sizes, and key/value behavior) and is used by
    /// helpers such as row counting and index rebuilds.
    /// </para>
    /// <para>
    /// <b>Indexes:</b> <see cref="Indexes"/> exposes the index registry for this table. Indexes may enable fast key lookups
    /// (per-column or global). Implementations may treat indexes as optional accelerators and fall back to scans depending
    /// on configuration and rebuild policy.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> not guaranteed. Unless an implementation explicitly documents otherwise, assume callers must
    /// synchronize concurrent reads/writes at a higher level.
    /// </para>
    /// </remarks>
    public interface IMetadataTable : IDisposable
    {
        /// <summary>
        /// Gets the number of columns in this table.
        /// </summary>
        int ColumnCount { get; }

        /// <summary>
        /// Gets the registry that holds the table indexes (built-in and custom).
        /// </summary>
        ITableIndexesRegistry Indexes { get; }

        /// <summary>
        /// Gets the schema specification that defines the table columns and sizing rules.
        /// </summary>
        TableSpec Spec { get; }

        /// <summary>
        /// Gets or sets the underlying cell store used by this table.
        /// </summary>
        /// <remarks>
        /// Changing the store may invalidate previously materialized cells and/or indexes depending on the implementation.
        /// If you replace the store, you typically should call <see cref="RebuildIndexes(bool)"/> afterwards.
        /// </remarks>
        ICellStore CellStore { get; set; }

        /// <summary>
        /// Enumerates existing cells in the table.
        /// </summary>
        /// <remarks>
        /// Enumeration order is implementation-defined. Cells returned by the enumerator reflect the current state of the store.
        /// </remarks>
        CellEnumerable EnumerateCells();

        /// <summary>
        /// Gets the column names defined by the table schema.
        /// </summary>
        IEnumerable<string> GetColumnNames();

        /// <summary>
        /// Gets the logical row count of the table.
        /// </summary>
        /// <remarks>
        /// The logical row count is derived from the schema/store and represents the rows that are considered addressable.
        /// It may differ from physical capacity (see <see cref="TryGetColumnCapacity(uint, out uint)"/>).
        /// </remarks>
        uint GetLogicalRowCount();

        /// <summary>
        /// Gets an existing cell or creates it if missing.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <param name="row">The row id.</param>
        /// <returns>The existing or newly created <see cref="MetadataCell"/>.</returns>
        MetadataCell GetOrCreateCell(string columnName, uint row);

        /// <summary>
        /// Gets an existing cell or creates it if missing.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="row">The row id.</param>
        /// <returns>The existing or newly created <see cref="MetadataCell"/>.</returns>
        MetadataCell GetOrCreateCell(uint column, uint row);

        /// <summary>
        /// Opens the table, ensuring that the underlying store is ready for use.
        /// </summary>
        /// <remarks>
        /// Implementations may lazily materialize resources (files, memory maps, caches) on first open.
        /// </remarks>
        /// <returns>The table instance (typically <see langword="this"/>).</returns>
        IMetadataTable Open();

        /// <summary>
        /// Rebuilds all registered indexes using the table as the source of truth.
        /// </summary>
        /// <param name="strict">
        /// If <see langword="true"/>, failures during index rebuild are surfaced (e.g., thrown or reported) according to the implementation.
        /// If <see langword="false"/>, the implementation may skip or tolerate rebuild failures for best-effort indexing.
        /// </param>
        void RebuildIndexes(bool strict = false);

        /// <summary>
        /// Returns a human-readable representation of the table.
        /// </summary>
        string ToString();

        /// <summary>
        /// Returns a human-readable representation of the table limited to a maximum number of rows.
        /// </summary>
        /// <param name="maxRows">Maximum number of rows to include in the output.</param>
        string ToString(uint maxRows);

        /// <summary>
        /// Attempts to locate a cell by key within a specific column using a UTF-8 key buffer.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="keyUtf8">UTF-8 key bytes.</param>
        /// <param name="cell">When this method returns <see langword="true"/>, receives the matching cell.</param>
        /// <returns><see langword="true"/> if a matching cell was found; otherwise <see langword="false"/>.</returns>
        bool TryFindCellByKey(uint column, byte[] keyUtf8, out MetadataCell cell);

        /// <summary>
        /// Attempts to locate a cell by key within a specific column using a UTF-8 key span without allocating.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="keyUtf8">UTF-8 key bytes.</param>
        /// <param name="cell">When this method returns <see langword="true"/>, receives the matching cell.</param>
        /// <returns><see langword="true"/> if a matching cell was found; otherwise <see langword="false"/>.</returns>
        bool TryFindCellByKey(uint column, ReadOnlySpan<byte> keyUtf8, out MetadataCell cell);

        /// <summary>
        /// Attempts to locate the first-class location of a key across all key-producing columns.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 key bytes.</param>
        /// <param name="hit">When this method returns <see langword="true"/>, receives the <c>(col,row)</c> location.</param>
        /// <returns><see langword="true"/> if the key was found; otherwise <see langword="false"/>.</returns>
        bool TryFindGlobal(byte[] keyUtf8, out (uint col, uint row) hit);

        /// <summary>
        /// Attempts to locate the first-class location of a key across all key-producing columns without allocating.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 key bytes.</param>
        /// <param name="hit">When this method returns <see langword="true"/>, receives the <c>(col,row)</c> location.</param>
        /// <returns><see langword="true"/> if the key was found; otherwise <see langword="false"/>.</returns>
        bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out (uint col, uint row) hit);

        /// <summary>
        /// Attempts to locate the row id of a key within a specific column using a UTF-8 key buffer.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="keyUtf8">UTF-8 key bytes.</param>
        /// <param name="row">When this method returns <see langword="true"/>, receives the row id.</param>
        /// <returns><see langword="true"/> if a matching key exists; otherwise <see langword="false"/>.</returns>
        bool TryFindRowByKey(uint column, byte[] keyUtf8, out uint row);

        /// <summary>
        /// Attempts to locate the row id of a key within a specific column using a UTF-8 key span without allocating.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="keyUtf8">UTF-8 key bytes.</param>
        /// <param name="row">When this method returns <see langword="true"/>, receives the row id.</param>
        /// <returns><see langword="true"/> if a matching key exists; otherwise <see langword="false"/>.</returns>
        bool TryFindRowByKey(uint column, ReadOnlySpan<byte> keyUtf8, out uint row);

        /// <summary>
        /// Attempts to retrieve a cell by column name and row id.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <param name="row">The row id.</param>
        /// <param name="cell">When this method returns <see langword="true"/>, receives the cell.</param>
        /// <returns><see langword="true"/> if the cell exists; otherwise <see langword="false"/>.</returns>
        bool TryGetCell(string columnName, uint row, out MetadataCell cell);

        /// <summary>
        /// Attempts to retrieve a cell by column id and row id.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="row">The row id.</param>
        /// <param name="cell">When this method returns <see langword="true"/>, receives the cell.</param>
        /// <returns><see langword="true"/> if the cell exists; otherwise <see langword="false"/>.</returns>
        bool TryGetCell(uint column, uint row, out MetadataCell cell);

        /// <summary>
        /// Attempts to retrieve the physical row capacity for a column.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="rowCapacity">When this method returns <see langword="true"/>, receives the row capacity.</param>
        /// <returns><see langword="true"/> if capacity information is available; otherwise <see langword="false"/>.</returns>
        bool TryGetColumnCapacity(uint column, out uint rowCapacity);

        /// <summary>
        /// Attempts to grow the storage for a column to at least <paramref name="minRows"/> rows.
        /// </summary>
        /// <param name="column">The column id.</param>
        /// <param name="minRows">Minimum number of rows required.</param>
        /// <param name="zeroInit">
        /// If <see langword="true"/>, newly allocated storage is zero-initialized when applicable.
        /// </param>
        /// <returns><see langword="true"/> if the column was grown (or already satisfied the requirement); otherwise <see langword="false"/>.</returns>
        bool TryGrowColumnTo(uint column, uint minRows, bool zeroInit = true);
    }
}
