using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata
{
    /// <summary>
    /// Represents a logical metadata table composed of fixed-size columns and rows,
    /// backed by an <see cref="ICellStore"/> (in-memory or memory-mapped built-in, you can implement your own
    /// custom <see cref="ICellStore"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="MetadataTable"/> is defined by a <see cref="TableSpec"/> and a set of
    /// <see cref="ColumnConfiguration"/> instances describing each column's layout and capacity.
    /// The actual storage is delegated to an <see cref="ICellStore"/> implementation:
    /// <list type="bullet">
    ///   <item><description><see cref="InMemoryStore"/> for purely in-memory usage.</description></item>
    ///   <item><description><see cref="MappedStore"/> for memory-mapped file persistence.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The table exposes helpers to:
    /// <list type="bullet">
    ///   <item><description>Lookup cells by column index or name and row index.</description></item>
    ///   <item><description>Build and query per-column and global key indexes.</description></item>
    ///   <item><description>Grow column capacity when supported by the underlying store.</description></item>
    ///   <item><description>Render a human-readable preview of the table contents.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Instances of this class own the underlying store and must be disposed via
    /// <see cref="Dispose"/> when no longer needed.
    /// </para>
    /// </remarks>
    public sealed class MetadataTable : IDisposable
    {
        private readonly List<ColumnConfiguration> _columns = [];
        private readonly Dictionary<string, uint> _colIndexByName = new(StringComparer.Ordinal);

        // col -> (keyBytes -> row)
        private readonly Dictionary<uint, Dictionary<byte[], uint>> _indexByKeyPerColumn = [];

        // global key -> (column, row)
        private readonly Dictionary<byte[], (uint col, uint row)> _globalKeyIndex = new(ByteArrayComparer.Ordinal);

        private readonly ICellStore _store;
        private readonly TableSpec _spec;


        /// <summary>
        /// Gets the total number of columns defined in this table.
        /// </summary>
        public int ColumnCount => _columns.Count;

        /// <summary>
        /// Retrieves the <see cref="TableSpec"/> that defines this table.
        /// </summary>
        public TableSpec Spec => _spec;

        /// <summary>
        /// Opens an existing metadata table from the memory-mapped file described by
        /// the specified <see cref="TableSpec"/>.
        /// </summary>
        /// <param name="spec">
        /// The table specification containing the name, path and column definitions.
        /// </param>
        /// <returns>
        /// A new <see cref="MetadataTable"/> instance backed by a <see cref="MappedStore"/>
        /// using the columns discovered in the on-disk file.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the file at <see cref="TableSpec.MapPath"/> does not contain a valid
        /// metadata table header.
        /// </exception>
        public static MetadataTable Open(TableSpec spec)
        {
            if (!MappedStore.TryLoadColumns(spec.MapPath, out var columns))
                throw new InvalidOperationException("File is not a valid metadata table.");
            return new MetadataTable(new(spec.Name, spec.MapPath, columns));
        }

        /// <summary>
        /// Opens a new <see cref="MetadataTable"/> instance using the
        /// <see cref="TableSpec"/> associated with this table.
        /// </summary>
        /// <remarks>
        /// This is a convenience method that reloads the table from its persistent
        /// <see cref="TableSpec.MapPath"/> using the column layout found on disk.
        /// </remarks>
        /// <returns>
        /// A new <see cref="MetadataTable"/> that reflects the current on-disk column
        /// configuration.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the file at the stored <see cref="_spec"/> path does not contain
        /// a valid metadata table header.
        /// </exception>
        public MetadataTable Open()
        {
            if (!MappedStore.TryLoadColumns(_spec.MapPath, out var columns))
                throw new InvalidOperationException("File is not a valid metadata table.");

            return new MetadataTable(new(_spec.Name, _spec.MapPath, columns));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataTable"/> class using the
        /// provided <see cref="TableSpec"/>.
        /// </summary>
        /// <param name="spec">
        /// The specification describing the table name, backing file path (if any) and columns.
        /// </param>
        /// <exception cref="ArgumentException">
        /// Thrown when <see cref="TableSpec.Columns"/> is empty.
        /// </exception>
        /// <remarks>
        /// <para>
        /// If <see cref="TableSpec.MapPath"/> is <see langword="null"/>, the table is backed by
        /// an <see cref="InMemoryStore"/>. Otherwise, a <see cref="MappedStore"/> is created.
        /// </para>
        /// <para>
        /// For in-memory tables, initial cells for each column are materialized up to
        /// <see cref="ColumnConfiguration.InitialCapacity"/>. For mapped tables, the space
        /// already exists in the underlying file.
        /// </para>
        /// </remarks>
        public MetadataTable(TableSpec spec)
        {
            _spec = spec;
            var columns = spec.Columns;
            if (columns.Length == 0) throw new ArgumentException("At least one column.", nameof(spec));

            _columns = new List<ColumnConfiguration>(columns.Length);
            _colIndexByName = new Dictionary<string, uint>(columns.Length, StringComparer.Ordinal);

            for (int i = 0; i < columns.Length; i++)
            {
                ref readonly var c = ref columns[i];
                _columns.Add(c);
                _colIndexByName[c.Name] = (uint)i;
            }

            _store = spec.MapPath is null
                ? new InMemoryStore(_columns)
                : new MappedStore(spec);

            // Prematerialize initial cells (solo relevante para InMemoryStore en MappedStore ya existe el espacio)
            if (_store is InMemoryStore)
                for (uint col = 0; col < _columns.Count; col++)
                {
                    var meta = _columns[(int)col];
                    for (uint row = 0; row < meta.InitialCapacity; row++)
                        _ = _store.GetOrCreateCell(col, row, meta);
                }
        }

        /// <summary>
        /// Rebuilds the per-column and, optionally, the global key indexes by scanning
        /// all cells in the table.
        /// </summary>
        /// <param name="includeGlobal">
        /// <see langword="true"/> to rebuild the global index that maps keys to
        /// <c>(column, row)</c> pairs; <see langword="false"/> to rebuild only
        /// per-column indexes.
        /// </param>
        /// <remarks>
        /// <para>
        /// This method performs a full scan over the underlying store via
        /// <see cref="EnumerateCells"/>. For each <see cref="CellRowColumnValueEntry"/> it:
        /// </para>
        /// <list type="bullet">
        ///   <item><description>
        ///   Extracts the cell key (when present) using <see cref="MetadataCell.TryGetKey(out ReadOnlySpan{byte})"/>.
        ///   </description></item>
        ///   <item><description>
        ///   Populates <see cref="_indexByKeyPerColumn"/> so that, for each column,
        ///   keys map to their row index.
        ///   </description></item>
        ///   <item><description>
        ///   Optionally populates <see cref="_globalKeyIndex"/> so that keys map to a
        ///   <c>(column, row)</c> pair across all columns.
        ///   </description></item>
        /// </list>
        /// <para>
        /// Keys are stored as defensive copies (<see cref="byte"/> arrays) to ensure
        /// they remain valid beyond the lifetime of any temporary spans used while
        /// scanning the store.
        /// </para>
        /// <para>
        /// When duplicate keys are found within the same column, the last occurrence
        /// overwrites previous entries. If you need to track multiple rows for the
        /// same key, replace the value type with a multi-map or a list.
        /// </para>
        /// </remarks>
        public void RebuildIndexes(bool includeGlobal = true)
        {
            _indexByKeyPerColumn.Clear();
            if (includeGlobal) _globalKeyIndex.Clear();

            foreach (var entry in EnumerateCells())
            {
                if (!entry.Cell.TryGetKey(out ReadOnlySpan<byte> k) || k.Length == 0)
                    continue;

                // Defensive copy for small keys
                var keyCopy = k.ToArray();

                if (!_indexByKeyPerColumn.TryGetValue(entry.Col, out var dict))
                {
                    dict = new Dictionary<byte[], uint>(ByteArrayComparer.Ordinal);
                    _indexByKeyPerColumn[entry.Col] = dict;
                }

                // If there are duplicates in the same column, the last one wins.
                dict[keyCopy] = entry.Row;

                if (includeGlobal)
                    _globalKeyIndex[keyCopy] = (entry.Col, entry.Row);
            }
        }

        /// <summary>
        /// Tries to find the row index within a column that matches the specified UTF-8 key.
        /// </summary>
        /// <param name="column">The zero-based column index to search.</param>
        /// <param name="keyUtf8">The key bytes, encoded as UTF-8, provided as a read-only span.</param>
        /// <param name="row">
        /// When this method returns <see langword="true"/>, contains the row index of
        /// the cell with the given key; otherwise, 0.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a row with the specified key was found in the given
        /// column; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The lookup uses the per-column index built by <see cref="RebuildIndexes(bool)"/>.
        /// If the index is stale or has not been built, the result may be incomplete.
        /// </para>
        /// <para>
        /// This overload allocates a new <see cref="byte"/> array per call to match the
        /// dictionary key type. For hot paths, prefer the <see cref="TryFindRowByKey(uint, byte[], out uint)"/>
        /// overload, which avoids this allocation.
        /// </para>
        /// </remarks>
        public bool TryFindRowByKey(uint column, ReadOnlySpan<byte> keyUtf8, out uint row)
        {
            row = 0;
            if (!_indexByKeyPerColumn.TryGetValue(column, out var dict)) return false;

            // We need a byte[] because the dictionary is keyed on byte[].
            // This allocates once per lookup; if this is hot, consider using the overload.
            var key = keyUtf8.ToArray();
            return dict.TryGetValue(key, out row);
        }

        /// <summary>
        /// Tries to find the row index within a column that matches the specified UTF-8 key.
        /// </summary>
        /// <param name="column">The zero-based column index to search.</param>
        /// <param name="keyUtf8">The key bytes, encoded as UTF-8, provided as a <see cref="byte"/> array.</param>
        /// <param name="row">
        /// When this method returns <see langword="true"/>, contains the row index of
        /// the cell with the given key; otherwise, 0.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a row with the specified key was found in the given
        /// column; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The lookup uses the per-column index built by <see cref="RebuildIndexes(bool)"/>.
        /// If the index is stale or has not been built, the result may be incomplete.
        /// </para>
        /// <para>
        /// This overload avoids allocations by using the provided <paramref name="keyUtf8"/>
        /// array directly as the dictionary key, and is preferred for hot paths where the
        /// caller already has a <see cref="byte"/> array.
        /// </para>
        /// </remarks>
        public bool TryFindRowByKey(uint column, byte[] keyUtf8, out uint row)
        {
            row = 0;
            return _indexByKeyPerColumn.TryGetValue(column, out var dict) && dict.TryGetValue(keyUtf8, out row);
        }

        /// <summary>
        /// Tries to locate a cell by its UTF-8 key within a specific column.
        /// </summary>
        /// <param name="column">The zero-based column index.</param>
        /// <param name="keyUtf8">The key bytes, encoded as UTF-8.</param>
        /// <param name="cell">
        /// When this method returns <see langword="true"/>, contains the located
        /// <see cref="MetadataCell"/>; otherwise, the default value.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a cell with the specified key was found;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        public bool TryFindCellByKey(uint column, ReadOnlySpan<byte> keyUtf8, out MetadataCell cell)
        {
            if (TryFindRowByKey(column, keyUtf8, out var row))
                return TryGetCell(column, row, out cell);
            cell = default;
            return false;
        }

        /// <summary>
        /// Tries to find a cell by its UTF-8 key across all columns using the global index.
        /// </summary>
        /// <param name="keyUtf8">The key bytes, encoded as UTF-8.</param>
        /// <param name="hit">
        /// When this method returns <see langword="true"/>, contains the column and row
        /// indices where the key was found; otherwise, the default tuple.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the key exists in the global index; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This method relies on the global index created by <see cref="RebuildIndexes(bool)"/>
        /// with <paramref name="includeGlobal"/> set to <see langword="true"/>.
        /// </remarks>
        public bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out (uint col, uint row) hit)
        {
            var tmp = keyUtf8.ToArray();
            if (_globalKeyIndex.TryGetValue(tmp, out hit)) return true;
            hit = default;
            return false;
        }

        /// <summary>
        /// Attempts to retrieve a cell at the specified column and row indices.
        /// </summary>
        /// <param name="column">The zero-based column index.</param>
        /// <param name="row">The zero-based row index.</param>
        /// <param name="cell">
        /// When this method returns <see langword="true"/>, contains the retrieved
        /// <see cref="MetadataCell"/>; otherwise, the default value.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the cell exists in the underlying store;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        public bool TryGetCell(uint column, uint row, out MetadataCell cell)
            => _store.TryGetCell(column, row, out cell!);

        /// <summary>
        /// Attempts to retrieve a cell by column name and row index.
        /// </summary>
        /// <param name="columnName">The logical name of the column.</param>
        /// <param name="row">The zero-based row index.</param>
        /// <param name="cell">
        /// When this method returns <see langword="true"/>, contains the retrieved
        /// <see cref="MetadataCell"/>; otherwise, the default value.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if both the column name is found and the cell exists;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        public bool TryGetCell(string columnName, uint row, out MetadataCell cell)
        {
            if (!_colIndexByName.TryGetValue(columnName, out var col))
            {
                cell = default;
                return false;
            }

            return _store.TryGetCell(col, row, out cell!);
        }

        /// <summary>
        /// Attempts to retrieve a contiguous block descriptor for a given column.
        /// </summary>
        /// <param name="column">The zero-based column index.</param>
        /// <param name="block">
        /// When this method returns <see langword="true"/>, contains a <see cref="ColumnBlock"/>
        /// describing the base pointer, stride and value layout for the column.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the underlying store can provide a block descriptor;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        internal bool TryGetColumnBlock(uint column, out ColumnBlock block) => _store.TryGetColumnBlock(column, out block);

        /// <summary>
        /// Gets an existing cell or creates a new one at the specified column and row indices.
        /// </summary>
        /// <param name="column">The zero-based column index.</param>
        /// <param name="row">The zero-based row index.</param>
        /// <returns>
        /// A <see cref="MetadataCell"/> instance corresponding to the requested location.
        /// </returns>
        /// <remarks>
        /// The creation semantics depend on the <see cref="ICellStore"/> implementation;
        /// some stores may automatically grow capacity when accessing high row indices.
        /// </remarks>
        public MetadataCell GetOrCreateCell(uint column, uint row)
            => _store.GetOrCreateCell(column, row, _columns[(int)column]);

        /// <summary>
        /// Gets an existing cell or creates a new one in the column identified by the
        /// specified name and at the given row index.
        /// </summary>
        /// <param name="columnName">The logical name of the column.</param>
        /// <param name="row">The zero-based row index.</param>
        /// <returns>
        /// A <see cref="MetadataCell"/> instance corresponding to the requested location.
        /// </returns>
        /// <exception cref="KeyNotFoundException">
        /// Thrown if the column name does not exist in this table.
        /// </exception>
        public MetadataCell GetOrCreateCell(string columnName, uint row)
        {
            if (!_colIndexByName.TryGetValue(columnName, out var col))
                throw new KeyNotFoundException($"Column '{columnName}' not found.");
            return GetOrCreateCell(col, row);
        }

        /// <summary>
        /// Computes the logical row count of the table based on non-empty cells,
        /// instead of the raw capacity configured for each column.
        /// </summary>
        /// <remarks>
        /// <para>
        /// A row is considered <em>logically present</em> if at least one column in that row
        /// contains data:
        /// </para>
        /// <list type="bullet">
        ///   <item><description>
        ///   For <c>value-only</c> columns (<c>KeySize == 0</c>), the row is non-empty if
        ///   any byte in the VALUE segment is non-zero.
        ///   </description></item>
        ///   <item><description>
        ///   For <c>key/value</c> columns (<c>KeySize &gt; 0</c>), the row is non-empty if
        ///   the stored key is not empty.
        ///   </description></item>
        /// </list>
        /// <para>
        /// The logical row count is defined as <c>lastNonEmptyRowIndex + 1</c>, where
        /// <c>lastNonEmptyRowIndex</c> is the highest zero-based row index that has data
        /// in at least one column. If all rows are empty, the method returns 0.
        /// </para>
        /// <para>
        /// This method scans up to the maximum configured capacity among all columns.
        /// It is intended for diagnostics and metadata inspection rather than hot paths.
        /// </para>
        /// </remarks>
        /// <returns>
        /// The number of logical rows that contain data in at least one column.
        /// </returns>
        public unsafe int GetLogicalRowCount()
        {
            if (_columns.Count == 0)
                return 0;

            int colCount = _columns.Count;
            int maxCapacity = (int)_columns.Max(c => (long)c.InitialCapacity);
            int logical = 0;

            for (int r = 0; r < maxCapacity; r++)
            {
                bool anyNonEmpty = RowHasAnyData(colCount, r);
                if (anyNonEmpty)
                    logical = r + 1;  // last non-empty row (0-based) + 1
            }

            return logical;
        }

        /// <summary>
        /// Determines whether the specified row has any non-empty cell
        /// across all columns of the table.
        /// </summary>
        /// <param name="colCount">
        /// Total number of columns defined in the table.
        /// </param>
        /// <param name="r">
        /// Zero-based row index to inspect.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if at least one column in the row contains data;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The notion of “non-empty” depends on the column layout:
        /// </para>
        /// <list type="bullet">
        ///   <item><description>
        ///     For value-only columns:
        ///     <list type="number">
        ///       <item><description>
        ///         If valueSize is 1, 2, 4 or 8, the VALUE segment is interpreted as a
        ///         primitive-like payload (bool/char/int/float/double/long, etc.) and the
        ///         row is non-empty when the raw value != 0.
        ///       </description></item>
        ///       <item><description>
        ///         For larger payloads, the row is non-empty when any byte in the VALUE
        ///         segment is non-zero.
        ///       </description></item>
        ///     </list>
        ///   </description></item>
        ///   <item><description>
        ///   For <c>key/value</c> columns (<c>KeySize &gt; 0</c>), the row is considered
        ///   non-empty if the stored key is not empty.
        ///   </description></item>
        /// </list>
        /// <para>
        /// This method is used as a building block for computing the logical row count
        /// of the table and is not intended for ultra hot paths.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private unsafe bool RowHasAnyData(int colCount, int r)
        {
            for (int c = 0; c < colCount; c++)
            {
                if (!_store.TryGetCell((uint)c, (uint)r, out var cell))
                    continue;

                var meta = _columns[c];
                int keySize = meta.Size.GetKeySize();
                int valueSize = meta.Size.GetValueSize();

                if (keySize == 0)
                {
                    // VALUE-ONLY: interpret common primitive sizes directly, fallback to byte scan.
                    byte* valuePtr = cell.GetValuePointer();

                    switch (valueSize)
                    {
                        case 1:
                            if (*(byte*)valuePtr != 0)
                                return true;
                            break;

                        case 2:
                            if (*(ushort*)valuePtr != 0)
                                return true;
                            break;

                        case 4:
                            if (*(uint*)valuePtr != 0)
                                return true;
                            break;

                        case 8:
                            if (*(ulong*)valuePtr != 0)
                                return true;
                            break;

                        default:
                            {
                                var raw = new ReadOnlySpan<byte>(valuePtr, valueSize);
                                foreach (var b in raw)
                                {
                                    if (b != 0)
                                        return true;
                                }
                                break;
                            }
                    }
                }
                else
                {
                    // KEY-VALUE: non-empty if key is non-empty
                    if (cell.TryGetKey(out ReadOnlySpan<byte> k) && k.Length != 0)
                        return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Returns the logical names of all columns in this table.
        /// </summary>
        /// <returns>
        /// An enumerable sequence of column names in their declared order.
        /// </returns>
        public IEnumerable<string> GetColumnNames()
        {
            foreach (var m in _columns) yield return m.Name;
        }

        /// <summary>
        /// Enumerates all cells in the table along with their coordinates.
        /// </summary>
        /// <returns>
        /// A <see cref="CellEnumerable"/> that yields <see cref="CellRowColumnValueEntry"/> values,
        /// each combining a <see cref="MetadataCellPointer"/> (row and column) with the
        /// corresponding <see cref="MetadataCell"/> instance.
        /// </returns>
        /// <remarks>
        /// The enumeration walks the underlying <see cref="ICellStore"/> in an implementation-defined
        /// order, typically column by column and row by row. Cells may be materialized lazily.
        /// </remarks>
        public CellEnumerable EnumerateCells() => _store.EnumerateCells();

        /// <summary>
        /// Returns a formatted string representation of the table contents, limited
        /// to a default maximum number of rows.
        /// </summary>
        /// <returns>
        /// A human-readable preview of the table, showing column headers and sampled rows.
        /// </returns>
        public override string ToString() => ToString(byte.MaxValue);

        /// <summary>
        /// Returns a formatted string representation of the table contents, limited
        /// to the specified maximum number of rows.
        /// </summary>
        /// <param name="maxRows">
        /// The maximum number of rows to include in the textual preview.
        /// </param>
        /// <returns>
        /// A human-readable preview of the table, showing column headers and up to
        /// <paramref name="maxRows"/> rows, with values rendered as UTF-8 when possible
        /// or hexadecimal otherwise.
        /// </returns>
        /// <remarks>
        /// This method performs a best-effort preview and is intended primarily for
        /// debugging and diagnostics. It samples the data to size column widths and
        /// truncates long values with an ellipsis.
        /// </remarks>
        public string ToString(int maxRows)
        {
            var sb = new System.Text.StringBuilder();

            int colCount = _columns.Count;
            if (colCount == 0) return "MetadataTable { Columns=0 }\n";

            int totalRows = _columns.Min(c => GetLogicalRowCount());
            int rowsToShow = Math.Clamp(maxRows, 0, totalRows);

            const int MAX_COL_WIDTH = 48;
            var widths = new int[colCount + 1];
            widths[0] = Math.Max(3, rowsToShow.ToString().Length); // "Row"

            for (int c = 0; c < colCount; c++)
                widths[c + 1] = Math.Min(MAX_COL_WIDTH, Math.Max(3, _columns[c].Name.Length));

            // Sample to compute column widths (uses Preview with MAX_COL_WIDTH)
            ObtainWidths(colCount, rowsToShow, MAX_COL_WIDTH, widths);

            // Header
            sb.Append("MetadataTable { Columns=").Append(_columns.Count)
              .Append(", Rows≈").Append(totalRows).AppendLine(" }");

            sb.Append(Border(colCount, widths));
            sb.Append("| ").Append(Pad("Row", widths[0])).Append(' ');
            for (int c = 0; c < colCount; c++)
                sb.Append("| ").Append(Pad(_columns[c].Name, widths[c + 1])).Append(' ');
            sb.Append("|\n");
            sb.Append(Border(colCount, widths));

            // Rows
            WriteRows(sb, colCount, rowsToShow, widths);

            if (rowsToShow < totalRows)
            {
                sb.Append(Border(colCount, widths));
                sb.Append("… ").Append(totalRows - rowsToShow).Append(" more rows\n");
            }
            sb.Append(Border(colCount, widths));

            return sb.ToString();
        }

        /// <summary>
        /// Writes the table body rows into the provided <see cref="System.Text.StringBuilder"/>.
        /// </summary>
        /// <param name="sb">The string builder that accumulates the textual table representation.</param>
        /// <param name="colCount">Total number of columns in the table.</param>
        /// <param name="rowsToShow">Number of rows to render in the preview.</param>
        /// <param name="widths">Precomputed column widths (including the row index column at index 0).</param>
        /// <remarks>
        /// Each cell is retrieved from the underlying store and rendered using <see cref="Preview(ReadOnlySpan{byte}, int)"/>.
        /// Missing or unreadable cells are rendered as empty strings.
        /// </remarks>
        private unsafe void WriteRows(System.Text.StringBuilder sb, int colCount, int rowsToShow, int[] widths)
        {
            for (int r = 0; r < rowsToShow; r++)
            {
                sb.Append("| ").Append(Pad(r.ToString(), widths[0])).Append(' ');
                for (int c = 0; c < colCount; c++)
                {
                    string cellText = "";

                    if (_store.TryGetCell((uint)c, (uint)r, out var cell))
                    {
                        var meta = _columns[c];
                        int keySize = meta.Size.GetKeySize();
                        int valueSize = meta.Size.GetValueSize();

                        ReadOnlySpan<byte> v = default;

                        if (keySize == 0)
                        {
                            // VALUE-ONLY: leer directamente el segmento de valor
                            var raw = new ReadOnlySpan<byte>(cell.GetValuePointer(), valueSize);

                            bool allZero = true;
                            foreach (var b in raw)
                            {
                                if (b != 0) { allZero = false; break; }
                            }

                            if (!allZero)
                                v = raw;
                        }
                        else if (cell.TryGetKey(out ReadOnlySpan<byte> k) &&
                                 cell.TryGetValue(k, out ReadOnlySpan<byte> vv))
                        {
                            // KEY-VALUE: camino clásico
                            v = vv;
                        }

                        if (!v.IsEmpty)
                        {
                            // Usar ancho final de columna para el preview
                            cellText = Preview(v, widths[c + 1]);
                        }
                    }

                    sb.Append("| ").Append(Pad(cellText, widths[c + 1])).Append(' ');
                }
                sb.Append("|\n");
            }
        }

        /// <summary>
        /// Computes the effective width for each column by sampling up to the requested number of rows.
        /// </summary>
        /// <param name="colCount">Total number of columns in the table.</param>
        /// <param name="rowsToShow">Number of rows to sample for width calculation.</param>
        /// <param name="MAX_COL_WIDTH">Maximum allowed width for any column.</param>
        /// <param name="widths">
        /// Array of column widths to be updated in place. Index 0 holds the row-index column width;
        /// indices 1..colCount hold the widths for each data column.
        /// </param>
        /// <remarks>
        /// Uses <see cref="Preview(ReadOnlySpan{byte}, int)"/> with <paramref name="MAX_COL_WIDTH"/>
        /// to estimate a reasonable width for each column, capped at the specified maximum.
        /// </remarks>
        private unsafe void ObtainWidths(int colCount, int rowsToShow, int MAX_COL_WIDTH, int[] widths)
        {
            for (int r = 0; r < rowsToShow; r++)
            {
                for (int c = 0; c < colCount; c++)
                {
                    if (!_store.TryGetCell((uint)c, (uint)r, out var cell))
                        continue;

                    var meta = _columns[c];
                    int keySize = meta.Size.GetKeySize();
                    int valueSize = meta.Size.GetValueSize();

                    ReadOnlySpan<byte> v = default;

                    if (keySize == 0)
                    {
                        // VALUE-ONLY: leer directamente el segmento de valor
                        var raw = new ReadOnlySpan<byte>(cell.GetValuePointer(), valueSize);

                        bool allZero = true;
                        foreach (var b in raw)
                        {
                            if (b != 0) { allZero = false; break; }
                        }

                        if (!allZero)
                            v = raw;
                    }
                    else if (cell.TryGetKey(out ReadOnlySpan<byte> k) &&
                             cell.TryGetValue(k, out ReadOnlySpan<byte> vv))
                    {
                        // KEY-VALUE
                        v = vv;
                    }

                    if (!v.IsEmpty)
                    {
                        var prev = Preview(v, MAX_COL_WIDTH);
                        widths[c + 1] = Math.Min(MAX_COL_WIDTH, Math.Max(widths[c + 1], prev.Length));
                    }
                }
            }
        }

        /// <summary>
        /// Produces a short textual preview for a VALUE payload given the maximum width.
        /// </summary>
        /// <param name="v">The raw VALUE bytes to render.</param>
        /// <param name="maxChars">Maximum number of characters to include in the preview.</param>
        /// <returns>
        /// An empty string when <paramref name="v"/> is empty; otherwise a UTF-8 or hexadecimal
        /// preview string truncated with an ellipsis if necessary.
        /// </returns>
        /// <remarks>
        /// Delegates to <see cref="Utf8Preview(ReadOnlySpan{byte}, int)"/> which prefers UTF-8 decoding
        /// when the bytes form a printable string, or to a hexadecimal preview otherwise.
        /// </remarks>
        private static string Preview(ReadOnlySpan<byte> v, int maxChars) => v.Length == 0 ? "" : Utf8Preview(v, maxChars);

        /// <summary>
        /// Attempts to render the given VALUE bytes as a printable UTF-8 string, falling back
        /// to a hexadecimal preview when decoding fails or produces non-printable characters.
        /// </summary>
        /// <param name="v">The raw VALUE bytes to render.</param>
        /// <param name="maxChars">Maximum number of characters allowed in the resulting string.</param>
        /// <returns>
        /// A UTF-8 decoded preview string (possibly truncated with an ellipsis) when the data is
        /// printable, or a hexadecimal preview produced by <see cref="HexPreview(ReadOnlySpan{byte}, int)"/>
        /// otherwise.
        /// </returns>
        private static string Utf8Preview(ReadOnlySpan<byte> v, int maxChars)
        {
            if (!TryDecodePrintableUtf8(v, out var s)) return HexPreview(v, maxChars);
            return SafeEllipsis(s, maxChars);
        }

        /// <summary>
        /// Renders VALUE bytes as an uppercase hexadecimal string, truncated with an ellipsis if needed.
        /// </summary>
        /// <param name="v">The raw VALUE bytes to render.</param>
        /// <param name="maxChars">
        /// Maximum number of characters in the output, including the ellipsis character if truncation occurs.
        /// </param>
        /// <returns>
        /// A hexadecimal representation of <paramref name="v"/>. If the full representation would exceed
        /// <paramref name="maxChars"/>, the output is truncated to fit and suffixed with <c>'…'</c>.
        /// </returns>
        private static string HexPreview(ReadOnlySpan<byte> v, int maxChars)
        {
            if (maxChars <= 0) return string.Empty;

            int fullChars = v.Length * 2;
            if (fullChars <= maxChars)
            {
                var chars = new char[fullChars];
                FillHex(v, chars);
                return new string(chars);
            }
            else
            {
                int maxBytes = Math.Max(0, (maxChars - 1) / 2); // leave 1 char for ellipsis
                var chars = new char[maxBytes * 2 + 1];
                FillHex(v[..maxBytes], chars);
                chars[maxBytes * 2] = '…';
                return new string(chars);
            }
        }

        /// <summary>
        /// Tries to decode a UTF-8 byte span into a printable string.
        /// </summary>
        /// <param name="v">The UTF-8 encoded bytes to decode.</param>
        /// <param name="text">
        /// When this method returns <see langword="true"/>, contains the decoded string.
        /// When it returns <see langword="false"/>, the value is undefined.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the bytes decode cleanly to UTF-8 without replacement characters
        /// and do not contain control characters other than TAB, CR, or LF; otherwise <see langword="false"/>.
        /// </returns>
        private static bool TryDecodePrintableUtf8(ReadOnlySpan<byte> v, out string text)
        {
            text = System.Text.Encoding.UTF8.GetString(v);
            if (text.Contains('\uFFFD')) return false;
            foreach (var ch in text)
                if (char.IsControl(ch) && ch != '\t' && ch != '\r' && ch != '\n')
                    return false;
            return true;
        }

        /// <summary>
        /// Converts a sequence of bytes to their uppercase hexadecimal representation
        /// and writes the result into a caller-provided character span.
        /// </summary>
        /// <param name="v">The input bytes to convert.</param>
        /// <param name="outChars">
        /// Destination span that receives the hexadecimal characters. Its length must be
        /// at least <c>2 * v.Length</c>.
        /// </param>
        private static void FillHex(ReadOnlySpan<byte> v, Span<char> outChars)
        {
            static char Hex(byte x) => (char)(x < 10 ? '0' + x : 'A' + (x - 10));
            int ci = 0;
            foreach (var b in v)
            {
                outChars[ci++] = Hex((byte)(b >> 4));
                outChars[ci++] = Hex((byte)(b & 0xF));
            }
        }

        /// <summary>
        /// Safely truncates a string to a maximum length and appends an ellipsis when needed.
        /// </summary>
        /// <param name="s">The input string to truncate.</param>
        /// <param name="maxChars">Maximum allowed number of characters in the result.</param>
        /// <returns>
        /// The original string if it fits within <paramref name="maxChars"/>;
        /// otherwise, a truncated version with a trailing <c>'…'</c>. Surrogate pairs at the cut
        /// boundary are preserved by backing off one character when necessary.
        /// </returns>
        private static string SafeEllipsis(string s, int maxChars)
        {
            if (maxChars <= 0) return string.Empty;
            if (s.Length <= maxChars) return s;
            int cut = Math.Max(0, maxChars - 1);
            if (cut > 0 && char.IsHighSurrogate(s[cut - 1])) cut--;
            return s.AsSpan(0, cut).ToString() + "…";
        }

        /// <summary>
        /// Pads a string on the right with spaces so that its length is at least the specified width.
        /// </summary>
        /// <param name="s">The string to pad.</param>
        /// <param name="w">The desired minimum width.</param>
        /// <returns>
        /// The original string if its length is greater than or equal to <paramref name="w"/>;
        /// otherwise, the string padded with spaces on the right to reach the given width.
        /// </returns>
        private static string Pad(string s, int w) => s.Length >= w ? s : s + new string(' ', w - s.Length);

        /// <summary>
        /// Builds a horizontal border line for the textual table representation.
        /// </summary>
        /// <param name="colCount">Number of data columns in the table.</param>
        /// <param name="widths">
        /// Column widths array, where index 0 corresponds to the row index column and indices 1..colCount
        /// correspond to the data columns.
        /// </param>
        /// <returns>
        /// A string containing a border line composed of <c>'+'</c> and <c>'-'</c> characters,
        /// followed by a newline.
        /// </returns>
        private static string Border(int colCount, int[] widths)
        {
            var b = new System.Text.StringBuilder();
            b.Append('+').Append(new string('-', widths[0] + 2));
            for (int c = 0; c < colCount; c++)
                b.Append('+').Append(new string('-', widths[c + 1] + 2));
            b.Append("+\n");
            return b.ToString();
        }

        /// <summary>
        /// Attempts to grow the capacity (number of rows) of the specified column
        /// to at least <paramref name="minRows"/>.
        /// </summary>
        /// <param name="column">The zero-based column index.</param>
        /// <param name="minRows">The desired minimum row capacity.</param>
        /// <param name="zeroInit">
        /// <see langword="true"/> to request that newly allocated cells are zero-initialized
        /// (when supported by the underlying store); otherwise, <see langword="false"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the column already had sufficient capacity or the
        /// operation completed successfully; otherwise, <see langword="false"/>.
        /// </returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="column"/> is greater than or equal to
        /// <see cref="ColumnCount"/>.
        /// </exception>
        /// <remarks>
        /// <para>
        /// If the <see cref="ICellStore"/> implements <see cref="ITryGrowableStore"/>, the
        /// request is delegated to <see cref="ITryGrowableStore.TryGrowColumnTo"/>. Otherwise,
        /// a fallback strategy is used that touches the last requested row via
        /// <see cref="ICellStore.GetOrCreateCell"/>, which may or may not grow the underlying
        /// storage depending on the implementation.
        /// </para>
        /// </remarks>
        public bool TryGrowColumnTo(uint column, uint minRows, bool zeroInit = true)
        {
            if (minRows == 0) return true;
            if (column >= _columns.Count) throw new ArgumentOutOfRangeException(nameof(column));

            // Delegate to store if it supports growth; otherwise try the “touch last row” fallback.
            if (_store is ITryGrowableStore growable)
            {
                var meta = _columns[(int)column];
                return growable.TryGrowColumnTo(column, minRows, meta, zeroInit);
            }

            // Fallback: touch last row; works for stores that auto-expand in GetOrCreateCell.
            try
            {
                _ = _store.GetOrCreateCell(column, minRows - 1, _columns[(int)column]);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Tries to retrieve the row capacity for the given column from the underlying store,
        /// when the store supports capacity reporting (e.g. <see cref="ITryGrowableStore"/>).
        /// </summary>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="rowCapacity">When true, receives the current row capacity.</param>
        /// <returns>
        /// <see langword="true"/> if capacity is available and was returned; otherwise <see langword="false"/>.
        /// </returns>
        /// <exception cref="ObjectDisposedException">Propagated if the underlying store is disposed.</exception>
        public bool TryGetColumnCapacity(uint column, out uint rowCapacity)
        {
            if (_store is ITryGrowableStore growable)
                return growable.TryGetColumnCapacity(column, out rowCapacity);

            rowCapacity = 0;
            return false;
        }

        /// <summary>
        /// Releases all resources used by this <see cref="MetadataTable"/>, including
        /// the underlying <see cref="ICellStore"/>.
        /// </summary>
        public void Dispose() => _store.Dispose();
    }
}
