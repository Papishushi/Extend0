using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.CodeGen;

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
        /// <see cref="EnumerateCells"/>. It extracts the key of each cell (when present)
        /// and populates:
        /// </para>
        /// <list type="bullet">
        ///   <item><description><see cref="_indexByKeyPerColumn"/>: key → row within one column.</description></item>
        ///   <item><description><see cref="_globalKeyIndex"/>: key → (column, row) across all columns.</description></item>
        /// </list>
        /// <para>
        /// When duplicate keys are found within the same column, the last occurrence
        /// overwrites previous entries.
        /// </para>
        /// </remarks>
        public void RebuildIndexes(bool includeGlobal = true)
        {
            _indexByKeyPerColumn.Clear();
            if (includeGlobal) _globalKeyIndex.Clear();

            foreach (var (ptr, cell) in EnumerateCells())
            {
                if (!cell.TryGetKey(out ReadOnlySpan<byte> k) || k.Length == 0)
                    continue;

                // copia defensiva (clave pequeña)
                var keyCopy = k.ToArray();

                if (!_indexByKeyPerColumn.TryGetValue(ptr.Column, out var dict))
                {
                    dict = new Dictionary<byte[], uint>(ByteArrayComparer.Ordinal);
                    _indexByKeyPerColumn[ptr.Column] = dict;
                }
                // si hay duplicados en la misma col, el último gana; cambia si quieres multi-map
                dict[keyCopy] = ptr.Row;

                if (includeGlobal) _globalKeyIndex[keyCopy] = (ptr.Column, ptr.Row);
            }
        }

        /// <summary>
        /// Tries to find the row index within a column that matches the specified UTF-8 key.
        /// </summary>
        /// <param name="column">The zero-based column index.</param>
        /// <param name="keyUtf8">The key bytes, encoded as UTF-8.</param>
        /// <param name="row">
        /// When this method returns <see langword="true"/>, contains the row index of
        /// the cell with the given key; otherwise, 0.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a row with the specified key was found in the given
        /// column; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// The lookup uses the per-column index built by <see cref="RebuildIndexes(bool)"/>.
        /// If the index is stale or has not been built, the result may be incomplete.
        /// </remarks>
        public bool TryFindRowByKey(uint column, ReadOnlySpan<byte> keyUtf8, out uint row)
        {
            row = 0;
            if (!_indexByKeyPerColumn.TryGetValue(column, out var dict)) return false;
            // para buscar en dict necesitamos byte[]: evita alloc si puedes mantener clave cacheada
            // aquí hacemos una copia temporal (puedes usar ArrayPool si es muy frecuente)
            var tmp = keyUtf8.ToArray();
            if (dict.TryGetValue(tmp, out row)) return true;
            return false;
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
            var a = _colIndexByName.TryGetValue(columnName, out var col);
            var b = _store.TryGetCell(col, row, out cell!);
            return a && b;
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
        /// An enumerable sequence of tuples containing a <see cref="MetadataCellPointer"/>
        /// (column and row) and the corresponding <see cref="MetadataCell"/>.
        /// </returns>
        public IEnumerable<(MetadataCellPointer Ptr, MetadataCell Cell)> EnumerateCells()
        {
            foreach (var (c, r, cell) in _store.EnumerateCells())
                yield return (new MetadataCellPointer(r, c), cell);
        }

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

            int totalRows = (int)_columns.Min(c => (long)c.InitialCapacity);
            int rowsToShow = Math.Clamp(maxRows, 0, totalRows);

            const int MAX_COL_WIDTH = 48;
            var widths = new int[colCount + 1];
            widths[0] = Math.Max(3, rowsToShow.ToString().Length); // "Row"

            for (int c = 0; c < colCount; c++)
                widths[c + 1] = Math.Min(MAX_COL_WIDTH, Math.Max(3, _columns[c].Name.Length));

            // ================ Helpers ================
            string Border()
            {
                var b = new System.Text.StringBuilder();
                b.Append('+').Append(new string('-', widths[0] + 2));
                for (int c = 0; c < colCount; c++)
                    b.Append('+').Append(new string('-', widths[c + 1] + 2));
                b.Append("+\n");
                return b.ToString();
            }
            static string Pad(string s, int w) => s.Length >= w ? s : s + new string(' ', w - s.Length);

            // Evita cortar UTF-16 en medio de un surrogate
            static string SafeEllipsis(string s, int maxChars)
            {
                if (maxChars <= 0) return string.Empty;
                if (s.Length <= maxChars) return s;
                int cut = Math.Max(0, maxChars - 1);
                if (cut > 0 && char.IsHighSurrogate(s[cut - 1])) cut--;
                return s.AsSpan(0, cut).ToString() + "…";
            }

            static void FillHex(ReadOnlySpan<byte> v, Span<char> outChars)
            {
                static char Hex(byte x) => (char)(x < 10 ? '0' + x : 'A' + (x - 10));
                int ci = 0;
                foreach (var b in v)
                {
                    outChars[ci++] = Hex((byte)(b >> 4));
                    outChars[ci++] = Hex((byte)(b & 0xF));
                }
            }

            // UTF-8 válido "imprimible": sin U+FFFD ni controles (salvo \t\r\n)
            static bool TryDecodePrintableUtf8(ReadOnlySpan<byte> v, out string text)
            {
                text = System.Text.Encoding.UTF8.GetString(v);
                if (text.Contains('\uFFFD')) return false;
                foreach (var ch in text)
                    if (char.IsControl(ch) && ch != '\t' && ch != '\r' && ch != '\n')
                        return false;
                return true;
            }

            static string HexPreview(ReadOnlySpan<byte> v, int maxChars)
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
                    int maxBytes = Math.Max(0, (maxChars - 1) / 2); // deja 1 para la elipsis
                    var chars = new char[maxBytes * 2 + 1];
                    FillHex(v[..maxBytes], chars);
                    chars[maxBytes * 2] = '…';
                    return new string(chars);
                }
            }

            static string Utf8Preview(ReadOnlySpan<byte> v, int maxChars)
            {
                if (!TryDecodePrintableUtf8(v, out var s)) return HexPreview(v, maxChars);
                return SafeEllipsis(s, maxChars);
            }

            static string Preview(ReadOnlySpan<byte> v, int maxChars)
                => v.Length == 0 ? "" : Utf8Preview(v, maxChars);
            // =========================================

            // Muestreo para calcular anchos (usa Preview con MAX_COL_WIDTH)
            for (int r = 0; r < rowsToShow; r++)
            {
                for (int c = 0; c < colCount; c++)
                {
                    if (_store.TryGetCell((uint)c, (uint)r, out var cell) &&
                        cell.TryGetKey(out ReadOnlySpan<byte> k) &&
                        cell.TryGetValue(k, out ReadOnlySpan<byte> v))
                    {
                        var prev = Preview(v, MAX_COL_WIDTH);
                        widths[c + 1] = Math.Min(MAX_COL_WIDTH, Math.Max(widths[c + 1], prev.Length));
                    }
                }
            }

            // Cabecera
            sb.Append("MetadataTable { Columns=").Append(_columns.Count)
              .Append(", Rows≈").Append(totalRows).AppendLine(" }");

            sb.Append(Border());
            sb.Append("| ").Append(Pad("Row", widths[0])).Append(' ');
            for (int c = 0; c < colCount; c++)
                sb.Append("| ").Append(Pad(_columns[c].Name, widths[c + 1])).Append(' ');
            sb.Append("|\n");
            sb.Append(Border());

            // Filas
            for (int r = 0; r < rowsToShow; r++)
            {
                sb.Append("| ").Append(Pad(r.ToString(), widths[0])).Append(' ');
                for (int c = 0; c < colCount; c++)
                {
                    string cellText = "";
                    if (_store.TryGetCell((uint)c, (uint)r, out var cell) &&
                        cell.TryGetKey(out ReadOnlySpan<byte> k) &&
                        cell.TryGetValue(k, out ReadOnlySpan<byte> v))
                    {
                        // Usa el ancho final de la columna
                        cellText = Preview(v, widths[c + 1]);
                    }
                    sb.Append("| ").Append(Pad(cellText, widths[c + 1])).Append(' ');
                }
                sb.Append("|\n");
            }

            if (rowsToShow < totalRows)
            {
                sb.Append(Border());
                sb.Append("… ").Append(totalRows - rowsToShow).Append(" more rows\n");
            }
            sb.Append(Border());

            return sb.ToString();
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
        /// Releases all resources used by this <see cref="MetadataTable"/>, including
        /// the underlying <see cref="ICellStore"/>.
        /// </summary>
        public void Dispose() => _store.Dispose();
    }

}
