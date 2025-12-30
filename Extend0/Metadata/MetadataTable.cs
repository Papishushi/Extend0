using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Internal.BuiltIn;
using Extend0.Metadata.Indexing.Registries;
using Extend0.Metadata.Indexing.Registries.Contract;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Text;

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
    public sealed class MetadataTable : IMetadataTable
    {
        private static readonly UTF8Encoding Utf8Strict = new(false, true);

        private readonly List<ColumnConfiguration> _columns = [];
        // Schema-level lookup (not a rebuildable table index). Built once from TableSpec.Columns.
        private readonly FrozenDictionary<string, uint> _colIndexByName;

        private readonly ICellStore _store;
        private readonly TableSpec _spec;

        private const string COL_KEY_INDEX = "__builtIn:colKey";
        private const string GLOBAL_KEY_INDEX = "__builtIn:globalKey";

        /// <summary>
        /// Gets the registry of indexes associated with this table.
        /// </summary>
        public ITableIndexesRegistry Indexes { get; } = new TableIndexesRegistry();

        /// <summary>
        /// Gets the total number of columns defined in this table.
        /// </summary>
        public int ColumnCount => _columns.Count;

        /// <summary>
        /// Retrieves the <see cref="TableSpec"/> that defines this table.
        /// </summary>
        public TableSpec Spec => _spec;

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
            var tempColIndexByName = new Dictionary<string, uint>(columns.Length, StringComparer.Ordinal);

            for (int i = 0; i < columns.Length; i++)
            {
                ref readonly var c = ref columns[i];
                _columns.Add(c);
                tempColIndexByName[c.Name] = (uint)i;
            }

            _colIndexByName = tempColIndexByName.ToFrozenDictionary();

            _store = spec.MapPath is null
                ? new InMemoryStore(_columns)
                : new MappedStore(spec);

            // Prematerialize initial cells (only relevant for InMemoryStore and similars for example on MappedStore already exists the space)
            if (_store is InMemoryStore)
                for (uint col = 0; col < _columns.Count; col++)
                {
                    var meta = _columns[(int)col];
                    for (uint row = 0; row < meta.InitialCapacity; row++)
                        _ = _store.GetOrCreateCell(col, row, meta);
                }

            GetOrCreateColKeyIndex();
            GetOrCreateGlobalKeyIndex();
        }

        /// <summary>
        /// Opens an existing metadata table from the memory-mapped file described by
        /// the specified <see cref="TableSpec"/>.
        /// </summary>
        /// <param name="spec">
        /// The table specification containing the name, path and column definitions.
        /// </param>
        /// <returns>
        /// A new <see cref="IMetadataTable"/> instance backed by a <see cref="MappedStore"/>
        /// using the columns discovered in the on-disk file.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the file at <see cref="TableSpec.MapPath"/> does not contain a valid
        /// metadata table header.
        /// </exception>
        public static IMetadataTable Open(TableSpec spec)
        {
            if (!MappedStore.TryLoadColumns(spec.MapPath, out var columns))
                throw new InvalidOperationException("File is not a valid metadata table.");
            return new MetadataTable(new(spec.Name, spec.MapPath, columns));
        }

        /// <summary>
        /// Opens a new <see cref="IMetadataTable"/> instance using the
        /// <see cref="TableSpec"/> associated with this table.
        /// </summary>
        /// <remarks>
        /// This is a convenience method that reloads the table from its persistent
        /// <see cref="TableSpec.MapPath"/> using the column layout found on disk.
        /// </remarks>
        /// <returns>
        /// A new <see cref="IMetadataTable"/> that reflects the current on-disk column
        /// configuration.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the file at the stored <see cref="_spec"/> path does not contain
        /// a valid metadata table header.
        /// </exception>
        public IMetadataTable Open()
        {
            if (!MappedStore.TryLoadColumns(_spec.MapPath, out var columns))
                throw new InvalidOperationException("File is not a valid metadata table.");

            return new MetadataTable(new(_spec.Name, _spec.MapPath, columns));
        }

        /// <summary>
        /// Rebuilds all registered rebuildable indexes for this table by scanning the underlying storage.
        /// </summary>
        /// <param name="strict">
        /// When <see langword="true"/>, the method enforces strict rebuild rules:
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///     Any index registered in <see cref="Indexes"/> that does not implement
        ///     <see cref="IRebuildableIndex"/> causes an exception.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     Any enumerated cell that belongs to a key/value column and does not expose a non-empty key
        ///     causes an exception.
        ///     </description>
        ///   </item>
        /// </list>
        /// When <see langword="false"/>, non-rebuildable indexes are ignored and keyless cells are skipped.
        /// </param>
        /// <remarks>
        /// <para>
        /// Indexes are treated as ephemeral caches. The method starts by clearing all current index state via
        /// <see cref="TableIndexesRegistry.ClearAll"/> and then repopulates indexes from the table storage.
        /// </para>
        /// <para>
        /// Rebuild flow:
        /// </para>
        /// <list type="number">
        ///   <item>
        ///     <description>
        ///     <b>Optional strict validation</b>: when <paramref name="strict"/> is enabled, the table is scanned
        ///     and every cell in a key/value column must expose a non-empty key.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <b>Index rebuild</b>: all indexes in <see cref="Indexes"/> that implement
        ///     <see cref="IRebuildableIndex"/> are rebuilt by calling
        ///     <see cref="IRebuildableIndex.Rebuild(MetadataTable)"/>.
        ///     </description>
        ///   </item>
        /// </list>
        /// <para>
        /// Key semantics:
        /// </para>
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///     Value-only columns (<c>KeySize == 0</c>) do not produce keys and are always allowed to be keyless.
        ///     They are ignored by key-based indexes.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     In strict mode, only key/value columns are validated; value-only columns are explicitly exempt.
        ///     </description>
        ///   </item>
        /// </list>
        /// <para>
        /// Concurrency: this method does not take a global table lock. Individual indexes are expected to handle
        /// their own synchronization (e.g., reader/writer locks) during rebuild.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when <paramref name="strict"/> is <see langword="true"/> and either:
        /// <list type="bullet">
        ///   <item><description>An index in <see cref="Indexes"/> is not rebuildable.</description></item>
        ///   <item><description>A scanned cell in a key/value column does not expose a non-empty key.</description></item>
        /// </list>
        /// </exception>
        public void RebuildIndexes(bool strict = false)
        {
            // Clear everything (ephemeral by design)
            Indexes.ClearAll();

            if (strict)
            {
                foreach (var entry in EnumerateCells())
                {
                    // Value-only columns are allowed to have no key
                    var meta = _columns[(int)entry.Col];
                    if (meta.Size.GetKeySize() == 0)
                        continue;

                    if (!entry.Cell.TryGetKey(out ReadOnlySpan<byte> k) || k.Length == 0)
                        throw new InvalidOperationException($"Cell at ({entry.Col}, {entry.Row}) has no key.");
                }
            }

            // Rebuild all indexes that know how to rebuild
            foreach (var idx in Indexes.Enumerate())
            {
                if (idx is IRebuildableIndex r) r.Rebuild(this);
                else if (strict) throw new InvalidOperationException($"Index '{idx.Name}' is not rebuildable.");
            }
        }

        /// <summary>
        /// Gets the built-in column-key index or creates and registers it on demand.
        /// </summary>
        /// <returns>The singleton <see cref="ColumnKeyIndex"/> instance for this table.</returns>
        /// <remarks>
        /// <para>
        /// The column-key index is a built-in index stored in <see cref="Indexes"/> under the fixed name
        /// <see cref="COL_KEY_INDEX"/> (<c>"__builtIn:colKey"</c>).
        /// </para>
        /// <para>
        /// This method is a fast-path helper: it first attempts to retrieve the index from the registry and,
        /// if missing, constructs it and registers it via <c>Indexes.Add(created)</c>.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ColumnKeyIndex GetOrCreateColKeyIndex()
        {
            if (Indexes.TryGet(COL_KEY_INDEX, out var idx) && idx is ColumnKeyIndex ck)
                return ck;

            var created = new ColumnKeyIndex(COL_KEY_INDEX);
            Indexes.Add(created);
            return created;
        }

        /// <summary>
        /// Gets the built-in global-key index or creates and registers it on demand.
        /// </summary>
        /// <returns>The singleton <see cref="GlobalKeyIndex"/> instance for this table.</returns>
        /// <remarks>
        /// <para>
        /// The global-key index is a built-in index stored in <see cref="Indexes"/> under the fixed name
        /// <see cref="GLOBAL_KEY_INDEX"/> (<c>"__builtIn:globalKey"</c>).
        /// </para>
        /// <para>
        /// This index typically maps a key to a global hit (for example, <c>(column,row)</c>) across the table,
        /// enabling cross-column key lookups without scanning every column.
        /// </para>
        /// <para>
        /// This helper is implemented as a lazy accessor with a fast registry lookup first, then creation +
        /// registration if missing.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private GlobalKeyIndex GetOrCreateGlobalKeyIndex()
        {
            if (Indexes.TryGet(GLOBAL_KEY_INDEX, out var idx) && idx is GlobalKeyIndex gk)
                return gk;

            var created = new GlobalKeyIndex(GLOBAL_KEY_INDEX);
            Indexes.Add(created);
            return created;
        }

        /// <summary>
        /// Tries to find the row index within a column that matches the specified UTF-8 key.
        /// </summary>
        /// <param name="column">The zero-based column index to search.</param>
        /// <param name="keyUtf8">
        /// The key bytes, encoded as UTF-8, provided as a read-only span.
        /// </param>
        /// <param name="row">
        /// When this method returns <see langword="true"/>, contains the row index of the cell
        /// with the given key; otherwise, 0.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a row with the specified key was found in the given column;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method performs the lookup using the built-in per-column key index
        /// (<see cref="ColumnKeyIndex"/>) registered under
        /// <see cref="COL_KEY_INDEX"/>. The index is created on demand via
        /// <see cref="GetOrCreateColKeyIndex"/>; creation does not populate it.
        /// </para>
        /// <para>
        /// The index must have been populated beforehand by <see cref="RebuildIndexes(bool)"/>.
        /// If the index is empty or stale, the lookup may return <see langword="false"/>
        /// even if the key exists in the underlying storage.
        /// </para>
        /// <para>
        /// This overload allocates a new <see cref="byte"/> array on each call in order to
        /// match the index key type (<see cref="byte"/>[]). For allocation-sensitive or hot
        /// paths, prefer the overload that accepts a <see cref="byte"/>[] directly.
        /// </para>
        /// <para>
        /// Note: value-only columns (<c>KeySize == 0</c>) do not produce keys and therefore
        /// cannot be queried through this API. Only key/value columns participate in the
        /// key index.
        /// </para>
        /// </remarks>
        public bool TryFindRowByKey(uint column, ReadOnlySpan<byte> keyUtf8, out uint row)
        {
            var keySize = _columns[(int)column].Size.GetKeySize();
            if (keySize == 0) { row = 0; return false; } // value-only

            var index = GetOrCreateColKeyIndex();
            return index.TryGetRow(column, keyUtf8, out row);
        }

        /// <summary>
        /// Tries to find the row index within a column that matches the specified UTF-8 key.
        /// </summary>
        /// <param name="column">The zero-based column index to search.</param>
        /// <param name="keyUtf8">
        /// The key bytes, encoded as UTF-8, provided as a <see cref="byte"/> array.
        /// </param>
        /// <param name="row">
        /// When this method returns <see langword="true"/>, contains the row index of the cell
        /// with the given key; otherwise, 0.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a row with the specified key was found in the given column;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method performs the lookup using the built-in per-column key index
        /// (<see cref="ColumnKeyIndex"/>) registered under
        /// <see cref="COL_KEY_INDEX"/>. The index is created on demand via
        /// <see cref="GetOrCreateColKeyIndex"/>; creation does not populate it.
        /// </para>
        /// <para>
        /// The index must have been populated beforehand by <see cref="RebuildIndexes(bool)"/>.
        /// If the index is empty or stale, the lookup may return <see langword="false"/>
        /// even if the key exists in the underlying storage.
        /// </para>
        /// <para>
        /// This overload avoids allocations by using the provided <paramref name="keyUtf8"/> array
        /// directly as the lookup key. It is preferred for hot paths where the caller already
        /// has a <see cref="byte"/>[].
        /// </para>
        /// <para>
        /// Note: value-only columns (<c>KeySize == 0</c>) do not produce keys and therefore
        /// cannot be queried through this API. Only key/value columns participate in the
        /// key index.
        /// </para>
        /// </remarks>
        public bool TryFindRowByKey(uint column, byte[] keyUtf8, out uint row)
        {
            var indexByKeyPerColumn = GetOrCreateColKeyIndex();
            return indexByKeyPerColumn.TryGetRow(column, keyUtf8, out row);
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
        public bool TryFindCellByKey(uint column, byte[] keyUtf8, out MetadataCell cell)
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
        /// <para>
        /// This method performs the lookup using the built-in global key index
        /// (<see cref="GlobalKeyIndex"/>) registered under
        /// <see cref="GLOBAL_KEY_INDEX"/>. The index is created on demand via
        /// <see cref="GetOrCreateGlobalKeyIndex"/>; creation does not populate it.
        /// </para>
        /// <para>
        /// The index must have been populated beforehand by <see cref="RebuildIndexes(bool)"/>.
        /// If the index is empty or stale, the lookup may return <see langword="false"/>
        /// even if the key exists in the underlying storage.
        /// </para>
        /// </remarks>
        public bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out (uint col, uint row) hit)
        {
            var _globalKeyIndex = GetOrCreateGlobalKeyIndex();
            if (_globalKeyIndex.TryGetHit(keyUtf8, out hit)) return true;
            hit = default;
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
        /// <para>
        /// This method performs the lookup using the built-in global key index
        /// (<see cref="GlobalKeyIndex"/>) registered under
        /// <see cref="GLOBAL_KEY_INDEX"/>. The index is created on demand via
        /// <see cref="GetOrCreateGlobalKeyIndex"/>; creation does not populate it.
        /// </para>
        /// <para>
        /// The index must have been populated beforehand by <see cref="RebuildIndexes(bool)"/>.
        /// If the index is empty or stale, the lookup may return <see langword="false"/>
        /// even if the key exists in the underlying storage.
        /// </para>
        /// </remarks>
        public bool TryFindGlobal(byte[] keyUtf8, out (uint col, uint row) hit)
        {
            var _globalKeyIndex = GetOrCreateGlobalKeyIndex();
            if (_globalKeyIndex.TryGetHit(keyUtf8, out hit)) return true;
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
        public unsafe uint GetLogicalRowCount()
        {
            if (_columns.Count == 0)
                return 0;

            int colCount = _columns.Count;

            uint maxCapacity = 0;
            if (_store is ITryGrowableStore growableStore)
                for (uint i = 0; i < (uint)colCount; i++)
                    if (growableStore.TryGetColumnCapacity(i, out uint cap) && cap > maxCapacity)
                        maxCapacity = cap;

            if (maxCapacity == 0) maxCapacity = _columns.Max(c => c.InitialCapacity);

            uint logical = 0;
            for (uint r = 0; r < maxCapacity; r++)
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
        private unsafe bool RowHasAnyData(int colCount, uint r)
        {
            for (int c = 0; c < colCount; c++)
            {
                if (!_store.TryGetCell((uint)c, r, out var cell))
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
                            if (*valuePtr != 0)
                                return true;
                            break;

                        case 2:
                            if (Unsafe.ReadUnaligned<ushort>(valuePtr) != 0)
                                return true;
                            break;

                        case 4:
                            if (Unsafe.ReadUnaligned<uint>(valuePtr) != 0)
                                return true;
                            break;

                        case 8:
                            if (Unsafe.ReadUnaligned<ulong>(valuePtr) != 0)
                                return true;
                            break;

                        default:
                            {
                                var raw = new ReadOnlySpan<byte>(valuePtr, valueSize);
                                for (int i = 0; i < raw.Length; i++)
                                    if (raw[i] != 0)
                                        return true;
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
        public string ToString(uint maxRows)
        {
            var sb = new System.Text.StringBuilder();

            int colCount = _columns.Count;
            if (colCount == 0) return "MetadataTable { Columns=0 }\n";

            uint totalRows = GetLogicalRowCount();
            uint rowsToShow = Math.Clamp(maxRows, 0, totalRows);

            const int MAX_COL_WIDTH = 48;

            var widths = new int[colCount + 1];
            widths[0] = Math.Max(3, rowsToShow.ToString().Length); // "Row"

            for (int c = 0; c < colCount; c++)
                widths[c + 1] = Math.Min(MAX_COL_WIDTH, Math.Max(3, _columns[c].Name.Length));

            var colCountUnsigned = (uint)colCount;
            // Sample to compute column widths (uses Preview with MAX_COL_WIDTH)
            ObtainWidths(colCountUnsigned, rowsToShow, MAX_COL_WIDTH, widths);

            // Header
            sb.Append("MetadataTable { Columns=").Append(_columns.Count)
              .Append(", Rows≈").Append(totalRows).AppendLine(" }");

            sb.Append(Border(colCountUnsigned, widths));
            sb.Append("| ").Append(Pad("Row", widths[0])).Append(' ');
            for (int c = 0; c < colCount; c++)
                sb.Append("| ").Append(Pad(_columns[c].Name, widths[c + 1])).Append(' ');
            sb.Append("|\n");
            sb.Append(Border(colCountUnsigned, widths));

            // Rows
            WriteRows(sb, colCountUnsigned, rowsToShow, widths);

            if (rowsToShow < totalRows)
            {
                sb.Append(Border(colCountUnsigned, widths));
                sb.Append("… ").Append(totalRows - rowsToShow).Append(" more rows\n");
            }
            sb.Append(Border(colCountUnsigned, widths));

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
        private unsafe void WriteRows(System.Text.StringBuilder sb, uint colCount, uint rowsToShow, int[] widths)
        {
            for (uint r = 0; r < rowsToShow; r++)
            {
                sb.Append("| ").Append(Pad(r.ToString(), widths[0])).Append(' ');
                for (uint c = 0; c < colCount; c++)
                {
                    string cellText = "";

                    if (_store.TryGetCell(c, r, out var cell))
                    {
                        var meta = _columns[(int)c];
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
        private unsafe void ObtainWidths(uint colCount, uint rowsToShow, int MAX_COL_WIDTH, int[] widths)
        {
            for (uint r = 0; r < rowsToShow; r++)
            {
                for (uint c = 0; c < colCount; c++)
                {
                    if (!_store.TryGetCell(c, r, out var cell))
                        continue;

                    var meta = _columns[(int)c];
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
            try
            {
                text = Utf8Strict.GetString(v);
            }
            catch
            {
                text = string.Empty;
                return false;
            }

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
        private static string Border(uint colCount, int[] widths)
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
