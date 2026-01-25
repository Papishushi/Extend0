using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Internal.BuiltIn;
using Extend0.Metadata.Indexing.Registries;
using Extend0.Metadata.Indexing.Registries.Contract;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Metadata.Storage.Internal;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Text;

namespace Extend0.Metadata.Internal
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
    internal sealed class MetadataTable : IMetadataTable
    {
        private readonly List<ColumnConfiguration> _columns = [];
        // Schema-level lookup (not a rebuildable table index). Built once from TableSpec.Columns.
        private readonly FrozenDictionary<string, uint> _colIndexByName;

        private ICellStore _store;
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
        /// Gets or sets the underlying cell store used by this table.
        /// </summary>
        /// <remarks>
        /// Changing the store may invalidate previously materialized cells and/or indexes depending on the implementation.
        /// If you replace the store, you typically should call <see cref="RebuildIndexes(bool)"/> afterwards.
        /// </remarks>
        public ICellStore CellStore { get => _store; set => _store = value; }

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
        /// Rebuilds index state for this table by clearing all registered indexes and invoking rebuild on those that support it.
        /// </summary>
        /// <param name="strict">
        /// When <see langword="true"/>, the method enforces stricter validation:
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///     Every index returned by <see cref="Indexes"/> must implement <see cref="IRebuildableIndex"/>; otherwise an
        ///     <see cref="InvalidOperationException"/> is thrown before rebuild starts.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     The cell scan validates that entries returned by <see cref="EnumerateCells"/> for key/value columns expose a non-empty key.
        ///     Violations are collected and reported as an <see cref="AggregateException"/> after the scan completes.
        ///     Value-only columns (<c>KeySize == 0</c>) are exempt.
        ///     </description>
        ///   </item>
        /// </list>
        /// When <see langword="false"/>, non-rebuildable indexes are ignored and key validation is skipped.
        /// </param>
        /// <param name="cancellationToken">Token used to cancel scanning and rebuild work.</param>
        /// <returns>A task that completes when all applicable indexes have finished rebuilding.</returns>
        /// <remarks>
        /// <para>
        /// Indexes are treated as ephemeral caches. The method first clears current index state via
        /// <see cref="TableIndexesRegistry.ClearAll"/> and then rebuilds indexes implementing <see cref="IRebuildableIndex"/>.
        /// </para>
        /// <para>
        /// Concurrency: rebuild is executed in parallel using
        /// <see cref="Parallel.ForEachAsync(IEnumerable{TSource}, CancellationToken, Func{TSource, CancellationToken, ValueTask})"/>.
        /// Multiple indexes may rebuild simultaneously; each index implementation must be thread-safe with respect to its own state.
        /// </para>
        /// <para>
        /// Exceptions thrown by individual <see cref="IRebuildableIndex.Rebuild"/> calls propagate from the parallel loop.
        /// In addition, in strict mode, collected validation errors are thrown as an <see cref="AggregateException"/> at the end.
        /// </para>
        /// <para>
        /// Note: compaction/remapping operations may invalidate previously obtained spans/pointers into the store. Do not hold
        /// unmanaged views across calls that may rebuild indexes or otherwise mutate/remap the underlying storage.
        /// </para>
        /// </remarks>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when <paramref name="strict"/> is <see langword="true"/> and an index in <see cref="Indexes"/> does not implement
        /// <see cref="IRebuildableIndex"/>.
        /// </exception>
        /// <exception cref="AggregateException">
        /// Thrown when <paramref name="strict"/> is <see langword="true"/> and one or more validation or rebuild errors are accumulated.
        /// </exception>
        public async Task RebuildIndexes(bool strict = false, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Clear everything (ephemeral by design)
            Indexes.ClearAll();

            ConcurrentQueue<InvalidOperationException>? errors = null;

            if (strict)
            {
                // 1) Strict rule: all indexes must be rebuildable
                foreach (var idx in Indexes.Enumerate())
                    if (idx is not IRebuildableIndex)
                        throw new InvalidOperationException($"Index '{idx.Name}' is not rebuildable.");

                // 2) Strict rule: any CREATED cell in a key/value column must have a key
                await foreach (var entry in EnumerateCells().AsAsync().WithCancellation(cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var meta = _columns[(int)entry.Col];
                    if (meta.Size.GetKeySize() == 0)
                        continue; // value-only exempt

                    if (!entry.Cell.HasKeyRaw())
                    {
                        // It's empty, nothing to index. This assumes the store is zero-init or has been byte marked and processed by the user.
                        // If a legitimate value to index is an all-zero value, then this will fail to find the value as it will be treated as ASCII NULL \0
                        if (!entry.Cell.HasAnyValueRaw()) continue;
                        errors ??= [];
                        errors.Enqueue(new InvalidOperationException(
                            $"Cell at ({entry.Col}, {entry.Row}) is created but has no key. KeySize={meta.Size.GetKeySize()}, ValueSize={meta.Size.GetValueSize()}"));
                    }
                }
            }

            // Rebuild all indexes that know how to rebuild
            await Parallel.ForEachAsync(
                Indexes.Enumerate(),
                cancellationToken,
                async (idx, ct) =>
                {
                    ct.ThrowIfCancellationRequested();

                    // In strict mode we already validated rebuildability above,
                    // but keep it safe for non-strict mode.
                    if (idx is IRebuildableIndex r)
                        try { await r.Rebuild(this).ConfigureAwait(false); }
                        catch (Exception ex)
                        {
                            if (strict)
                            {
                                errors ??= [];
                                errors.Enqueue(new InvalidOperationException($"Index '{idx.Name}' rebuild failed.", ex));
                            }
                        }
                    else if (strict)
                    {
                        errors ??= [];
                        errors.Enqueue(new InvalidOperationException($"Index '{idx.Name}' does not support cross-table rebuilds."));
                    }
                });

            if (errors is not null)
                throw new AggregateException("Errors occurred during index rebuild.", errors);
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
        public bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out TryFindGlobalHit hit)
        {
            var _globalKeyIndex = GetOrCreateGlobalKeyIndex();
            if (_globalKeyIndex.TryGetHit(keyUtf8: keyUtf8, out var resultHit))
            {
                hit = new(resultHit.Col, resultHit.Row);
                return true;
            }
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
        public bool TryFindGlobal(byte[] keyUtf8, out TryFindGlobalHit hit)
        {
            var _globalKeyIndex = GetOrCreateGlobalKeyIndex();
            if (_globalKeyIndex.TryGetHit(key: keyUtf8, out var resultHit))
            {
                hit = new(resultHit.Col, resultHit.Row);
                return true;
            }
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
                    if (cell.HasKeyRaw() && cell.TryGetKeyRaw(out ReadOnlySpan<byte> k))
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CellEnumerable EnumerateCells() => _store.EnumerateCells();

        /// <summary>
        /// Returns a formatted string representation of the table contents, limited
        /// to a default maximum number of rows.
        /// </summary>
        /// <returns>
        /// A human-readable preview of the table, showing column headers and sampled rows.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            var sb = new StringBuilder();

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

            sb.Append(MetadataTableHelpers.Border(colCountUnsigned, widths));
            sb.Append("| ").Append(MetadataTableHelpers.Pad("Row", widths[0])).Append(' ');
            for (int c = 0; c < colCount; c++)
                sb.Append("| ").Append(MetadataTableHelpers.Pad(_columns[c].Name, widths[c + 1])).Append(' ');
            sb.Append("|\n");
            sb.Append(MetadataTableHelpers.Border(colCountUnsigned, widths));

            // Rows
            WriteRows(sb, colCountUnsigned, rowsToShow, widths);

            if (rowsToShow < totalRows)
            {
                sb.Append(MetadataTableHelpers.Border(colCountUnsigned, widths));
                sb.Append("… ").Append(totalRows - rowsToShow).Append(" more rows\n");
            }
            sb.Append(MetadataTableHelpers.Border(colCountUnsigned, widths));

            return sb.ToString();
        }

        /// <summary>
        /// Writes the table body rows into the provided <see cref="StringBuilder"/>.
        /// </summary>
        /// <param name="sb">The string builder that accumulates the textual table representation.</param>
        /// <param name="colCount">Total number of columns in the table.</param>
        /// <param name="rowsToShow">Number of rows to render in the preview.</param>
        /// <param name="widths">Precomputed column widths (including the row index column at index 0).</param>
        /// <remarks>
        /// Each cell is retrieved from the underlying store and rendered using <see cref="Preview(ReadOnlySpan{byte}, int)"/>.
        /// Missing or unreadable cells are rendered as empty strings.
        /// </remarks>
        private unsafe void WriteRows(StringBuilder sb, uint colCount, uint rowsToShow, int[] widths)
        {
            for (uint r = 0; r < rowsToShow; r++)
            {
                sb.Append("| ").Append(MetadataTableHelpers.Pad(r.ToString(), widths[0])).Append(' ');
                for (uint c = 0; c < colCount; c++)
                {
                    string cellText = "";

                    if (_store.TryGetCell(c, r, out var cell))
                    {
                        var meta = _columns[(int)c];
                        int keySize = meta.Size.GetKeySize();

                        ReadOnlySpan<byte> v = default;

                        if (keySize == 0 && cell.HasAnyValueRaw())   // VALUE-ONLY
                            v = cell.GetValueRaw();
                        else if (cell.HasAnyValueRaw() && cell.TryGetValueRaw(out ReadOnlySpan<byte> raw))   // KEY-VALUE
                            v = raw;

                        if (!v.IsEmpty)
                            cellText = MetadataTableHelpers.Preview(v, widths[c + 1]);
                    }

                    sb.Append("| ").Append(MetadataTableHelpers.Pad(cellText, widths[c + 1])).Append(' ');
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

                    ReadOnlySpan<byte> v = default;

                    if (keySize == 0 && cell.HasAnyValueRaw())  // VALUE-ONLY
                        v = cell.GetValueRaw();
                    else if (cell.HasAnyValueRaw() && cell.TryGetValueRaw(out ReadOnlySpan<byte> raw))   // KEY-VALUE
                        v = raw;

                    if (!v.IsEmpty)
                    {
                        var prev = MetadataTableHelpers.Preview(v, MAX_COL_WIDTH);
                        widths[c + 1] = Math.Min(MAX_COL_WIDTH, Math.Max(widths[c + 1], prev.Length));
                    }
                }
            }
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
        /// Attempts to compact the table's backing store and then rebuild indexes, if supported.
        /// </summary>
        /// <param name="strict">
        /// When <see langword="true"/>, any exception thrown during compaction or index rebuild is propagated to the caller.
        /// When <see langword="false"/>, failures are swallowed and the method returns <see langword="false"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the store implements <see cref="ICompactableStore"/>, compaction completed successfully,
        /// and indexes were rebuilt; otherwise, <see langword="false"/> (either because the store is not compactable or because
        /// compaction/rebuild failed in non-strict mode).
        /// </returns>
        /// <remarks>
        /// <para>
        /// Compaction is an implementation-defined operation performed by the underlying store (for example, rewriting a mapped
        /// file, shrinking capacity, or relocating slabs). It may be expensive and can involve I/O and temporary extra space.
        /// </para>
        /// <para>
        /// After a successful compaction, this method rebuilds all registered indexes via <see cref="RebuildIndexes(bool)"/>.
        /// Indexes are treated as ephemeral caches and may be cleared and repopulated.
        /// </para>
        /// <para>
        /// Any previously obtained spans/pointers into the store's backing memory may become invalid after compaction
        /// (e.g., due to remapping). Consumers must not hold on to <see cref="MetadataCell"/>-backed spans across this call.
        /// </para>
        /// </remarks>
        /// <exception cref="Exception">
        /// When <paramref name="strict"/> is <see langword="true"/>, rethrows any exception produced by
        /// <see cref="ICompactableStore.Compact"/> or by <see cref="RebuildIndexes(bool)"/>.
        /// </exception>
        public async Task<bool> TryCompactStore(bool strict, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_store is not ICompactableStore compactableStore) return false;
            try
            {
                await compactableStore.Compact(strict, cancellationToken);
                await RebuildIndexes(strict, cancellationToken);
                return true;
            }
            catch
            {
                if (strict) throw;
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
