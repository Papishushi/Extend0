using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.CrossProcess.Contract;
using Extend0.Metadata.CrossProcess.DTO;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Schema;
using Microsoft.Extensions.Logging;
using System.Buffers;

namespace Extend0.Metadata.CrossProcess
{
    /// <summary>
    /// RPC/IPC compatible façade for <see cref="MetaDBManager"/> using <see cref="CrossProcessServiceBase{TContract}"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This type exists to expose MetaDB operations across process boundaries without leaking any pointer-backed
    /// state (memory-mapped views, spans over unmanaged memory, store ownership, etc.).
    /// </para>
    /// <para>
    /// Read operations return <see cref="CellResultDTO"/> snapshots, which are cross-process safe and can include:
    /// UTF-8 decoded payloads, raw byte copies, or both, depending on <see cref="CellPayloadModeDTO"/>.
    /// </para>
    /// <para>
    /// Column layout rules are derived from the column entry size:
    /// <list type="bullet">
    ///   <item><description><b>Value-only</b>: <c>KeySize == 0</c>. The cell stores only VALUE bytes.</description></item>
    ///   <item><description><b>Key/Value</b>: <c>KeySize &gt; 0</c>. The entry layout is <c>[KEY][VALUE]</c>; presence is typically signaled by a non-empty KEY.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// All table-bound operations are executed via <c>_innerManager.WithTable(...)</c> to ensure consistent resolution,
    /// lifecycle behavior, and centralized error handling in the in-proc manager.
    /// </para>
    /// </remarks>
    public class MetaDBManagerRPCCompatible(
        /// <summary>
        /// Logger used by the underlying <see cref="MetaDBManager"/> for structured logging and diagnostics.
        /// </summary>
        ILogger? logger,
        /// <summary>
        /// Optional table factory. When null, the underlying manager uses its default creation logic.
        /// </summary>
        Func<TableSpec?, IMetadataTable>? factory = null,
        /// <summary>
        /// Default capacity behavior when per-call policies are unspecified (<see cref="CapacityPolicy.None"/>).
        /// </summary>
        CapacityPolicy capacityPolicy = CapacityPolicy.Throw,
        /// <summary>
        /// Optional persisted delete queue path for the delete worker. When null, the default path is used.
        /// </summary>
        string? deleteQueuePath = null)
        : CrossProcessServiceBase<IMetaDBManagerRPCCompatible>, IMetaDBManagerRPCCompatible
    {
        /// <summary>
        /// Named pipe identifier used by the cross-process host/client pair for MetaDB IPC.
        /// </summary>
        protected override string? PipeName => "Extend0.MetaDB";

        /// <summary>
        /// Enumerates the ids of all tables currently registered in the underlying manager.
        /// </summary>
        /// <remarks>
        /// This is a live view of the manager registry; it may change as tables are registered/closed.
        /// </remarks>
        public IEnumerable<Guid> TableIds => _innerManager.TableIds;

        /// <summary>
        /// In-process manager that executes the actual MetaDB operations.
        /// </summary>
        private readonly MetaDBManager _innerManager = new(logger, factory, capacityPolicy, deleteQueuePath);

        /// <summary>
        /// Indicates whether this instance has been disposed.
        /// </summary>
        private bool _disposed;

        // -----------------------------
        // Shared methods (already wired)
        // -----------------------------

        /// <summary>
        /// Registers a table by name/path and column definitions.
        /// </summary>
        /// <param name="name">Unique logical name for the table.</param>
        /// <param name="mapPath">Backing file path for mapped storage.</param>
        /// <param name="columns">Column layout definitions.</param>
        /// <returns>The table identifier.</returns>
        public Guid RegisterTable(string name, string mapPath, params ColumnConfiguration[] columns)
            => _innerManager.RegisterTable(name, mapPath, columns);

        /// <summary>
        /// Registers a table from a <see cref="TableSpec"/> definition.
        /// </summary>
        /// <param name="spec">Table specification (name/path/columns).</param>
        /// <param name="createNow">
        /// When <see langword="true"/>, forces immediate creation/opening; otherwise the table may be materialized lazily.
        /// </param>
        /// <returns>The table identifier.</returns>
        public Guid RegisterTable(TableSpec spec, bool createNow = false)
            => _innerManager.RegisterTable(spec, createNow);

        /// <summary>
        /// Tries to resolve a table id by its registered name.
        /// </summary>
        /// <param name="name">Registered table name.</param>
        /// <param name="id">Receives the resolved table id when found.</param>
        /// <returns><see langword="true"/> if found; otherwise <see langword="false"/>.</returns>
        public bool TryGetIdByName(string name, out Guid id)
            => _innerManager.TryGetIdByName(name, out id);

        /// <summary>
        /// Closes (and disposes) a managed table instance using strict semantics.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <returns><see langword="true"/> if the table was closed; otherwise <see langword="false"/>.</returns>
        public bool CloseStrict(Guid tableId)
            => _innerManager.CloseStrict(tableId);

        /// <summary>
        /// Closes (and disposes) a managed table instance by name using strict semantics.
        /// </summary>
        /// <param name="name">Registered table name.</param>
        /// <returns><see langword="true"/> if the table was closed; otherwise <see langword="false"/>.</returns>
        public bool CloseStrict(string name)
            => _innerManager.CloseStrict(name);

        /// <summary>
        /// Closes all managed tables using best-effort semantics.
        /// </summary>
        public void CloseAll()
            => _innerManager.CloseAll();

        /// <summary>
        /// Closes all managed tables using strict semantics (fail-fast on inconsistencies).
        /// </summary>
        public void CloseAllStrict()
            => _innerManager.CloseAllStrict();

        /// <summary>
        /// Rebuilds indexes for a single table.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="includeGlobal">Whether to rebuild the global key index.</param>
        public void RebuildIndexes(Guid tableId, bool includeGlobal = true)
            => _innerManager.RebuildIndexes(tableId, includeGlobal);

        /// <summary>
        /// Rebuilds indexes for all managed tables.
        /// </summary>
        /// <param name="includeGlobal">Whether to rebuild the global key index.</param>
        public void RebuildAllIndexes(bool includeGlobal = true)
            => _innerManager.RebuildAllIndexes(includeGlobal);

        /// <summary>
        /// Restarts the background delete worker responsible for deleting queued table files.
        /// </summary>
        /// <param name="deleteQueuePath">Optional override queue path; when null, uses the configured default.</param>
        public Task RestartDeleteWorker(string? deleteQueuePath = null)
            => _innerManager.RestartDeleteWorker(deleteQueuePath);

        /// <summary>
        /// Copies a contiguous range of rows from one column to another column.
        /// </summary>
        /// <param name="srcTableId">Source table id.</param>
        /// <param name="srcCol">Source column index.</param>
        /// <param name="dstTableId">Destination table id.</param>
        /// <param name="dstCol">Destination column index.</param>
        /// <param name="rows">Number of rows to copy.</param>
        /// <param name="dstPolicy">Capacity policy for the destination column.</param>
        public void CopyColumn(Guid srcTableId, uint srcCol, Guid dstTableId, uint dstCol, uint rows,
            CapacityPolicy dstPolicy = CapacityPolicy.None)
            => _innerManager.CopyColumn(srcTableId, srcCol, dstTableId, dstCol, rows, dstPolicy);

        /// <summary>
        /// Ensures the reference vector entry exists for a given parent row.
        /// </summary>
        /// <param name="parentTableId">Parent table id.</param>
        /// <param name="refsCol">Reference vector column index.</param>
        /// <param name="parentRow">Parent row index.</param>
        /// <param name="policy">Capacity policy.</param>
        public void EnsureRefVec(Guid parentTableId, uint refsCol, uint parentRow,
            CapacityPolicy policy = CapacityPolicy.None)
            => _innerManager.EnsureRefVec(parentTableId, refsCol, parentRow, policy);

        /// <summary>
        /// Links a parent row to a child entry by writing a reference into the parent's reference vector.
        /// </summary>
        /// <param name="parentTableId">Parent table id.</param>
        /// <param name="refsCol">Reference vector column index.</param>
        /// <param name="parentRow">Parent row index.</param>
        /// <param name="childTableId">Child table id.</param>
        /// <param name="childCol">Child column index.</param>
        /// <param name="childRow">Child row index.</param>
        /// <param name="policy">Capacity policy.</param>
        public void LinkRef(Guid parentTableId, uint refsCol, uint parentRow, Guid childTableId,
            uint childCol = 0, uint childRow = 0, CapacityPolicy policy = CapacityPolicy.None)
            => _innerManager.LinkRef(parentTableId, refsCol, parentRow, childTableId, childCol, childRow, policy);

        // -----------------------------
        // RPC-only methods (implemented)
        // -----------------------------

        /// <summary>
        /// Returns the logical row count of the table (best-effort).
        /// </summary>
        /// <remarks>
        /// Uses <see cref="IMetadataTable.GetLogicalRowCount"/> which scans the table and considers a row present when:
        /// a value-only column has any non-zero VALUE byte, or a key/value column stores a non-empty KEY.
        /// Intended for diagnostics and UI inspection rather than hot paths.
        /// </remarks>
        public uint GetRowCount(Guid tableId)
            => _innerManager.WithTable(tableId, t => t.GetLogicalRowCount());

        /// <summary>
        /// Returns the number of columns defined in the table spec.
        /// </summary>
        public int GetColumnCount(Guid tableId)
            => _innerManager.WithTable(tableId, t => t.ColumnCount);

        /// <summary>
        /// Returns column names in their declared order.
        /// </summary>
        public string[] GetColumnNames(Guid tableId)
            => _innerManager.WithTable(tableId, t => t.Spec.Columns.Select(c => c.Name).ToArray());

        /// <summary>
        /// Produces a human-readable preview of the table content (best-effort).
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="maxRows">Maximum number of rows to render in the preview.</param>
        public string PreviewTable(Guid tableId, uint maxRows = 32)
            => _innerManager.WithTable(tableId, t => t.ToString(maxRows));

        /// <summary>
        /// Reads a single cell snapshot as a <see cref="CellResultDTO"/> suitable for IPC boundaries.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index.</param>
        /// <param name="row">Row index.</param>
        /// <param name="cellPayloadMode">Payload selection strategy (UTF-8, raw, or both).</param>
        /// <returns>
        /// A DTO snapshot. When the cell is missing/unreadable, <see cref="CellResultDTO.HasCell"/> is <see langword="false"/>.
        /// </returns>
        public CellResultDTO? ReadCell(Guid tableId, uint column, uint row, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => _innerManager.WithTable(tableId, t => MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, column, row, cellPayloadMode));

        /// <summary>
        /// Reads the VALUE payload of a cell as a raw byte copy.
        /// </summary>
        /// <remarks>
        /// Convenience wrapper over <see cref="ReadCell"/> that requests <see cref="CellPayloadModeDTO.RawOnly"/>
        /// and returns <see cref="CellResultDTO.ValueRaw"/>.
        /// </remarks>
        public byte[]? ReadCellRaw(Guid tableId, uint column, uint row)
            => _innerManager.WithTable(tableId, t => MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, column, row, CellPayloadModeDTO.RawOnly)?.ValueRaw);

        /// <summary>
        /// Reads a row and returns a dictionary keyed by column name with per-cell DTO snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="row">Row index.</param>
        /// <param name="cellPayloadMode">Payload selection strategy.</param>
        public Dictionary<string, CellResultDTO?> ReadRow(Guid tableId, uint row, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => _innerManager.WithTable(tableId, t =>
            {
                var cols = t.Spec.Columns;
                var dict = new Dictionary<string, CellResultDTO?>(cols.Length, StringComparer.Ordinal);
                for (uint c = 0; c < (uint)cols.Length; c++)
                    dict[cols[c].Name] = MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, c, row, cellPayloadMode);
                return dict;
            });

        /// <summary>
        /// Reads a row and returns a dictionary keyed by column name with raw VALUE payloads.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="row">Row index.</param>
        public Dictionary<string, byte[]?> ReadRowRaw(Guid tableId, uint row)
            => _innerManager.WithTable(tableId, t =>
            {
                var cols = t.Spec.Columns;
                var dict = new Dictionary<string, byte[]?>(cols.Length, StringComparer.Ordinal);
                for (uint c = 0; c < (uint)cols.Length; c++)
                    dict[cols[c].Name] = MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, c, row, CellPayloadModeDTO.RawOnly)?.ValueRaw;
                return dict;
            });

        /// <summary>
        /// Reads a contiguous slice of a column as DTO snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        /// <param name="cellPayloadMode">Payload selection strategy.</param>
        public CellResultDTO?[] ReadColumn(Guid tableId, uint column, uint startRow, uint rowCount, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => _innerManager.WithTable(tableId, t =>
            {
                var arr = new CellResultDTO?[rowCount];
                for (uint i = 0; i < rowCount; i++)
                    arr[i] = MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, column, startRow + i, cellPayloadMode);
                return arr;
            });

        /// <summary>
        /// Reads a contiguous slice of a column as raw VALUE payload copies.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        public byte[]?[] ReadColumnRaw(Guid tableId, uint column, uint startRow, uint rowCount)
            => _innerManager.WithTable(tableId, t =>
            {
                var arr = new byte[]?[rowCount];
                for (uint i = 0; i < rowCount; i++)
                    arr[i] = MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, column, startRow + i, CellPayloadModeDTO.RawOnly)?.ValueRaw;
                return arr;
            });

        /// <summary>
        /// Reads a rectangular block defined by a column set and a contiguous row range, returning DTO snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="columns">Column indices to read.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        /// <param name="cellPayloadMode">Payload selection strategy.</param>
        public CellResultDTO?[][] ReadBlock(Guid tableId, uint[] columns, uint startRow, uint rowCount, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => _innerManager.WithTable(tableId, t =>
            {
                var rows = new CellResultDTO?[rowCount][];
                for (uint r = 0; r < rowCount; r++)
                {
                    var line = new CellResultDTO?[columns.Length];
                    for (int ci = 0; ci < columns.Length; ci++)
                        line[ci] = MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, columns[ci], startRow + r, cellPayloadMode);
                    rows[r] = line;
                }
                return rows;
            });

        /// <summary>
        /// Reads a rectangular block as raw VALUE payload copies.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="columns">Column indices to read.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        public byte[]?[][] ReadBlockRaw(Guid tableId, uint[] columns, uint startRow, uint rowCount)
            => _innerManager.WithTable(tableId, t =>
            {
                var rows = new byte[]?[rowCount][];
                for (uint r = 0; r < rowCount; r++)
                {
                    var line = new byte[]?[columns.Length];
                    for (int ci = 0; ci < columns.Length; ci++)
                        line[ci] = MetaDBManagerRPCCompatibleHelpers.BuildCellDto(t, columns[ci], startRow + r, CellPayloadModeDTO.RawOnly)?.ValueRaw;
                    rows[r] = line;
                }
                return rows;
            });

        /// <summary>
        /// Creates (or opens) a child table from <paramref name="childSpec"/>, links it under the parent reference vector,
        /// and returns the child table id.
        /// </summary>
        public Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, TableSpec childSpec, uint childCol = 0, uint childRow = 0)
            => _innerManager.GetOrCreateAndLinkChild(parentTableId, refsCol, parentRow, _ => childSpec, childCol, childRow);

        /// <summary>
        /// Creates (or opens) a child table from <paramref name="childSpec"/> using an explicit child key, links it under the parent,
        /// and returns the child table id.
        /// </summary>
        public Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, uint childKey, TableSpec childSpec, uint childCol = 0, uint childRow = 0)
            => _innerManager.GetOrCreateAndLinkChild(parentTableId, refsCol, parentRow, childKey, _ => childSpec, childCol, childRow);

        /// <summary>
        /// Writes a contiguous sequence of cells into a column using <see cref="CellResultDTO"/> snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index.</param>
        /// <param name="startRow">First row to write.</param>
        /// <param name="values">Cell snapshots to write; null entries clear the corresponding cell.</param>
        /// <param name="policy">Capacity policy governing growth behavior when addressing high row indices.</param>
        /// <param name="cellPayloadMode">
        /// Determines which DTO fields are used as the source of the write (raw bytes, UTF-8 strings, or both).
        /// When both are available, raw bytes are preferred.
        /// </param>
        /// <remarks>
        /// <para>
        /// Null elements in <paramref name="values"/> are treated as "clear cell":
        /// <list type="bullet">
        ///   <item><description>Value-only columns: clears VALUE bytes.</description></item>
        ///   <item><description>Key/value columns: clears both KEY and VALUE bytes.</description></item>
        /// </list>
        /// </para>
        /// <para>
        /// For key/value columns, KEY and VALUE are written directly into the fixed layout <c>[KEY][VALUE]</c>.
        /// If no key payload is provided, the KEY segment remains empty (which usually means "not present").
        /// </para>
        /// </remarks>
        public void FillColumn(Guid tableId, uint column, uint startRow, CellResultDTO?[] values,
            CapacityPolicy policy = CapacityPolicy.None, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => _innerManager.WithTable(tableId, t =>
            {
                var meta = t.Spec.Columns[(int)column];
                int keyCap = meta.Size.GetKeySize();
                int valCap = meta.Size.GetValueSize();

                for (int i = 0; i < values.Length; i++)
                {
                    uint row = startRow + (uint)i;

                    // best-effort growth depending on policy
                    if (!MetaDBManagerRPCCompatibleHelpers.EnsureCapacityBestEffort(t, column, row, policy))
                        continue;

                    var cell = t.GetOrCreateCell(column, row);
                    unsafe
                    {
                        byte* valuePtr = cell.GetValuePointer();
                        byte* keyPtr = keyCap == 0 ? valuePtr : valuePtr - keyCap;

                        var dtoNullable = values[i];

                        if (dtoNullable is null)
                        {
                            // Treat null as "clear cell"
                            MetaDBManagerRPCCompatibleHelpers.ZeroFill(keyPtr, keyCap + valCap);
                            continue;
                        }

                        CellResultDTO dto = dtoNullable.Value;

                        if (keyCap == 0)
                            // VALUE-ONLY: write VALUE only
                            MetaDBManagerRPCCompatibleHelpers.WriteValueSegment(valuePtr, valCap, dto.ValueRaw, dto.ValueUtf8, cellPayloadMode);
                        else
                        {
                            // KEY/VALUE: write KEY + VALUE
                            MetaDBManagerRPCCompatibleHelpers.WriteKeySegment(keyPtr, keyCap, dto.KeyRaw, dto.KeyUtf8, cellPayloadMode);
                            MetaDBManagerRPCCompatibleHelpers.WriteValueSegment(valuePtr, valCap, dto.ValueRaw, dto.ValueUtf8, cellPayloadMode);
                        }
                    }
                }
            });

        /// <summary>
        /// Writes a contiguous sequence of raw VALUE payloads into a column.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index.</param>
        /// <param name="startRow">First row to write.</param>
        /// <param name="valuesRaw">
        /// Raw VALUE payloads. Null entries clear VALUE bytes for the corresponding row.
        /// </param>
        /// <param name="policy">Capacity policy governing growth behavior when addressing high row indices.</param>
        /// <remarks>
        /// This method writes only the VALUE segment. For key/value columns it does not synthesize or modify the KEY segment.
        /// Use <see cref="FillColumn"/> when you need to write keys.
        /// </remarks>
        public void FillColumnRaw(Guid tableId, uint column, uint startRow, byte[]?[] valuesRaw,
            CapacityPolicy policy = CapacityPolicy.None)
            => _innerManager.WithTable(tableId, t =>
            {
                var meta = t.Spec.Columns[(int)column];
                int valCap = meta.Size.GetValueSize();

                for (int i = 0; i < valuesRaw.Length; i++)
                {
                    uint row = startRow + (uint)i;

                    if (!MetaDBManagerRPCCompatibleHelpers.EnsureCapacityBestEffort(t, column, row, policy))
                        continue;

                    var cell = t.GetOrCreateCell(column, row);
                    unsafe
                    {
                        byte* valuePtr = cell.GetValuePointer();
                        var temp = valuesRaw[i];
                        if (temp is null)
                        {
                            MetaDBManagerRPCCompatibleHelpers.ZeroFill(valuePtr, valCap); // only VALUE cleared
                            continue;
                        }

                        MetaDBManagerRPCCompatibleHelpers.WriteFixed(valuePtr, valCap, temp);
                    }
                }
            });

        /// <summary>
        /// Returns a snapshot of the indexes currently registered for the specified table.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <returns>
        /// An array of <see cref="IndexInfoDTO"/> describing each registered index.  
        /// Returns an empty array when <paramref name="tableId"/> is <see cref="Guid.Empty"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method enumerates <c>IMetadataTable.Indexes</c> and projects each <see cref="ITableIndex"/> into a DTO
        /// suitable for cross-process transport.
        /// </para>
        /// <para>
        /// The returned list is a point-in-time snapshot; it does not reflect subsequent registry mutations.
        /// </para>
        /// </remarks>
        public IndexInfoDTO[] GetIndexes(Guid tableId)
        {
            if (tableId == Guid.Empty) return [];

            return _innerManager.WithTable(tableId, t =>
            {
                return t.Indexes
                    .Enumerate()
                    .Select(idx => new IndexInfoDTO(
                        Name: idx.Name,
                        Kind: idx switch
                        {
                            Indexing.Internal.BuiltIn.ColumnKeyIndex => IndexKindDTO.BuiltIn_ColumnKey,
                            Indexing.Internal.BuiltIn.GlobalKeyIndex => IndexKindDTO.BuiltIn_GlobalKey,
                            _ => IndexKindDTO.Custom_InTable
                        },
                        IsRebuildable: idx is IRebuildableIndex,
                        IsBuiltIn: idx is Indexing.Internal.BuiltIn.ColumnKeyIndex
                                     || idx is Indexing.Internal.BuiltIn.GlobalKeyIndex,
                        Notes: null
                    ))
                    .ToArray();
            });
        }

        /// <summary>
        /// Returns a snapshot of the indexes registered at the manager (cross-table) level.
        /// </summary>
        /// <returns>
        /// An array of <see cref="IndexInfoDTO"/> describing each registered manager-level index.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method enumerates <c>_innerManager.Indexes</c> (cross-table registry) and projects each
        /// <see cref="ICrossTableIndex"/> into a DTO suitable for cross-process transport.
        /// </para>
        /// <para>
        /// The returned list is a point-in-time snapshot; it does not reflect subsequent registry mutations.
        /// </para>
        /// </remarks>
        public IndexInfoDTO[] GetIndexes()
        {
            return [.. _innerManager.Indexes
                    .Enumerate()
                    .Select(idx => new IndexInfoDTO(
                        Name: idx.Name,
                        Kind: idx switch
                        {
                            Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex => IndexKindDTO.BuiltIn_GlobalMultiTableKey,
                            _ => IndexKindDTO.Custom_CrossTable
                        },
                        IsRebuildable: idx is ICrossTableRebuildableIndex,
                        IsBuiltIn: idx is Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex,
                        Notes: null
                    ))];
        }

        /// <summary>
        /// Registers a new index in the specified table’s index registry, optionally replacing an existing index
        /// with the same name.
        /// </summary>
        /// <param name="tableId">The target table identifier.</param>
        /// <param name="request">
        /// Index definition request (name, kind, and optional configuration payload). When
        /// <see cref="AddIndexRequestDTO.ReplaceIfExists"/> is <see langword="true"/>, an existing index with the same
        /// name may be replaced (subject to built-in protections).
        /// </param>
        /// <returns>
        /// A <see cref="IndexMutationResultDTO"/> describing the mutation outcome:
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.Ok"/> when the index was added (or replaced) successfully, with
        ///     <see cref="IndexMutationResultDTO.Info"/> populated for the created index.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.TableNotOpen"/> when <paramref name="tableId"/> is empty or the table
        ///     cannot be resolved/opened.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.InvalidName"/> when <see cref="AddIndexRequestDTO.Name"/> is null/empty/whitespace.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.InvalidKind"/> when <see cref="AddIndexRequestDTO.Kind"/> is unknown/invalid.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.BuiltInProtected"/> when the request targets a built-in kind, or when an
        ///     existing built-in index would be mutated.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.AlreadyExists"/> when an index with the same name already exists and
        ///     <see cref="AddIndexRequestDTO.ReplaceIfExists"/> is <see langword="false"/>.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.NotSupported"/> when the backend cannot create the requested custom index kind/payload.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="IndexMutationStatusDTO.Error"/> on unexpected failures (creation/add/remove/rollback errors).
        ///     </description>
        ///   </item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// <para>
        /// Built-in indexes are protected and cannot be added, removed, or replaced through this API.
        /// </para>
        /// <para>
        /// Replacement is performed as a safe swap: the existing index is not removed until a new instance is created successfully.
        /// If removal occurs and adding the new index fails, the method attempts a best-effort rollback by re-adding the previous instance.
        /// </para>
        /// <para>
        /// Index instantiation is delegated to <c>MetaDBManagerRPCCompatibleHelpers.TryCreateCustomIndex(...)</c>, which is backend-specific.
        /// </para>
        /// </remarks>
        public IndexMutationResultDTO AddIndex(Guid tableId, AddIndexRequestDTO request)
        {
            if (tableId == Guid.Empty)
                return new(IndexMutationStatusDTO.TableNotOpen, null, "Empty tableId.");

            if (string.IsNullOrWhiteSpace(request.Name))
                return new(IndexMutationStatusDTO.InvalidName, null, "Invalid index name.");

            if (request.Kind == IndexKindDTO.Unknown)
                return new(IndexMutationStatusDTO.InvalidKind, null, "Invalid/unknown index kind.");

            // Built-ins are protected (cannot be created/replaced manually).
            if (request.Kind is IndexKindDTO.BuiltIn_ColumnKey or IndexKindDTO.BuiltIn_GlobalKey or IndexKindDTO.BuiltIn_GlobalMultiTableKey)
                return new(IndexMutationStatusDTO.BuiltInProtected, null, "Built-in indexes cannot be added manually.");

            return _innerManager.WithTable(tableId, t =>
            {
                // 1) Detect existing index (if any)
                bool exists = t.Indexes.TryGet(request.Name, out ITableIndex? existing) && existing is not null;

                if (exists)
                {
                    // Never allow mutating built-ins even if someone named-collides.
                    if (MetaDBManagerRPCCompatibleHelpers.IsBuiltIn(existing!))
                        return new IndexMutationResultDTO(IndexMutationStatusDTO.BuiltInProtected, null, "Built-in indexes cannot be replaced/removed.");

                    if (!request.ReplaceIfExists)
                        return new(IndexMutationStatusDTO.AlreadyExists, null, request.Notes);
                }

                // 2) Create the new index instance (backend-specific)
                //    IMPORTANT: do NOT remove the old one until we have a valid new instance.
                var create = MetaDBManagerRPCCompatibleHelpers.TryCreateCustomIndex(request, out ITableIndex? created, out var createNotes);

                if (create != IndexMutationStatusDTO.Ok || created is null)
                    return new(create, null, createNotes ?? request.Notes);

                // 3) Swap (safe replace)
                try
                {
                    if (exists)
                        t.Indexes.Remove(request.Name);

                    t.Indexes.Add(created);

                    // Optional: if the index is rebuildable and you want it "ready" immediately, uncomment:
                    // if (created is IRebuildableIndex r) r.Rebuild(t);

                    var info = MetaDBManagerRPCCompatibleHelpers.ToIndexInfoDTO(created);
                    return new(IndexMutationStatusDTO.Ok, info, request.Notes);
                }
                catch (Exception ex)
                {
                    // Best-effort rollback if we removed the previous one
                    try
                    {
                        if (exists) t.Indexes.Add(existing!);
                    }
                    catch
                    {
                        // swallow: we already have an error to report
                    }

                    return new(IndexMutationStatusDTO.Error, null, ex.Message);
                }
            });
        }

        /// <summary>
        /// Registers a new cross-table index in the manager-level index registry, optionally replacing an existing
        /// index with the same name.
        /// </summary>
        /// <param name="request">
        /// Index definition request (name, kind, and optional configuration payload). When
        /// <see cref="AddIndexRequestDTO.ReplaceIfExists"/> is <see langword="true"/>, an existing index with the same
        /// name may be replaced (subject to built-in protections).
        /// </param>
        /// <returns>
        /// A <see cref="IndexMutationResultDTO"/> describing the mutation outcome:
        /// <list type="bullet">
        ///   <item><description><see cref="IndexMutationStatusDTO.Ok"/> when the index was added (or replaced) successfully.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.InvalidName"/> when <see cref="AddIndexRequestDTO.Name"/> is invalid.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.InvalidKind"/> when <see cref="AddIndexRequestDTO.Kind"/> is unknown/invalid.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.BuiltInProtected"/> when the request targets a built-in kind or would mutate a built-in.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.AlreadyExists"/> when the name already exists and replacement was not requested.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.NotSupported"/> when the backend cannot create the requested custom index kind/payload.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.Error"/> on unexpected failures (creation/add/remove/rollback errors).</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// <para>
        /// This overload mutates the manager-level (cross-table) index registry (<c>_innerManager.Indexes</c>).
        /// Built-in indexes are protected and cannot be added, removed, or replaced through this API.
        /// </para>
        /// <para>
        /// Replacement uses a safe swap with best-effort rollback: the old index is removed only after a new instance
        /// is successfully created; if adding the new one fails, the method attempts to restore the previous instance.
        /// </para>
        /// </remarks>
        public IndexMutationResultDTO AddIndex(AddIndexRequestDTO request)
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new(IndexMutationStatusDTO.InvalidName, null, "Invalid index name.");

            if (request.Kind == IndexKindDTO.Unknown)
                return new(IndexMutationStatusDTO.InvalidKind, null, "Invalid/unknown index kind.");

            // Built-ins are protected (cannot be created/replaced manually).
            if (request.Kind is IndexKindDTO.BuiltIn_ColumnKey or IndexKindDTO.BuiltIn_GlobalKey or IndexKindDTO.BuiltIn_GlobalMultiTableKey)
                return new(IndexMutationStatusDTO.BuiltInProtected, null, "Built-in indexes cannot be added manually.");

            // 1) Detect existing index (if any)
            bool exists = _innerManager.Indexes.TryGet(request.Name, out ICrossTableIndex? existing) && existing is not null;

            if (exists)
            {
                // Never allow mutating built-ins even if someone named-collides.
                if (MetaDBManagerRPCCompatibleHelpers.IsBuiltIn(existing!))
                    return new IndexMutationResultDTO(IndexMutationStatusDTO.BuiltInProtected, null, "Built-in indexes cannot be replaced/removed.");

                if (!request.ReplaceIfExists)
                    return new(IndexMutationStatusDTO.AlreadyExists, null, request.Notes);
            }

            // 2) Create the new index instance (backend-specific)
            //    IMPORTANT: do NOT remove the old one until we have a valid new instance.
            var create = MetaDBManagerRPCCompatibleHelpers.TryCreateCustomIndex(request, out ICrossTableIndex? created, out var createNotes);

            if (create != IndexMutationStatusDTO.Ok || created is null)
                return new(create, null, createNotes ?? request.Notes);

            // 3) Swap (safe replace)
            try
            {
                if (exists)
                    _innerManager.Indexes.Remove(request.Name);

                _innerManager.Indexes.Add(created);

                var info = MetaDBManagerRPCCompatibleHelpers.ToIndexInfoDTO(created);
                return new(IndexMutationStatusDTO.Ok, info, request.Notes);
            }
            catch (Exception ex)
            {
                // Best-effort rollback if we removed the previous one
                try
                {
                    if (exists) _innerManager.Indexes.Add(existing!);
                }
                catch
                {
                    // swallow: we already have an error to report
                }

                return new(IndexMutationStatusDTO.Error, null, ex.Message);
            }
        }

        /// <summary>
        /// Removes an index from the table registry by name.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="name">Index name.</param>
        /// <returns>
        /// A <see cref="IndexMutationResultDTO"/> describing the outcome of the mutation.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Built-in indexes are protected and cannot be removed.
        /// </para>
        /// <para>
        /// If the index does not exist, the method returns <see cref="IndexMutationStatusDTO.NotFound"/>.
        /// </para>
        /// </remarks>
        public IndexMutationResultDTO RemoveIndex(Guid tableId, string name)
        {
            if (tableId == Guid.Empty)
                return new(IndexMutationStatusDTO.TableNotOpen, null);

            if (string.IsNullOrWhiteSpace(name))
                return new(IndexMutationStatusDTO.InvalidName, null);

            return _innerManager.WithTable(tableId, t =>
            {
                if (!t.Indexes.TryGet(name, out var idx))
                    return new IndexMutationResultDTO(IndexMutationStatusDTO.NotFound, null);

                if (idx is Indexing.Internal.BuiltIn.ColumnKeyIndex
                    || idx is Indexing.Internal.BuiltIn.GlobalKeyIndex)
                    return new IndexMutationResultDTO(IndexMutationStatusDTO.BuiltInProtected, null);

                t.Indexes.Remove(name);
                return new IndexMutationResultDTO(IndexMutationStatusDTO.Ok, null);
            });
        }

        /// <summary>
        /// Removes a manager-level (cross-table) index from the registry by name.
        /// </summary>
        /// <param name="name">
        /// Index name (unique within the manager-level registry).
        /// </param>
        /// <returns>
        /// A <see cref="IndexMutationResultDTO"/> describing the outcome:
        /// <list type="bullet">
        ///   <item><description><see cref="IndexMutationStatusDTO.Ok"/> when the index was removed.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.InvalidName"/> when <paramref name="name"/> is null/empty/whitespace.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.NotFound"/> when no index exists with the specified name.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.BuiltInProtected"/> when the target index is built-in/protected and cannot be removed.</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// This method mutates the manager-level (cross-table) index registry (<c>_innerManager.Indexes</c>).
        /// Built-in indexes are protected and cannot be removed via this API.
        /// </remarks>
        public IndexMutationResultDTO RemoveIndex(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return new(IndexMutationStatusDTO.InvalidName, null);

            if (!_innerManager.Indexes.TryGet(name, out var idx))
                return new IndexMutationResultDTO(IndexMutationStatusDTO.NotFound, null);

            if (idx is Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex)
                return new IndexMutationResultDTO(IndexMutationStatusDTO.BuiltInProtected, null);

            _innerManager.Indexes.Remove(name);
            return new IndexMutationResultDTO(IndexMutationStatusDTO.Ok, null);
        }

        /// <summary>
        /// Attempts to locate the row index for a given key in a specific column using the built-in column-key index.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index to search.</param>
        /// <param name="keyUtf8">Key as a managed string (encoded as UTF-8 before lookup).</param>
        /// <returns>
        /// A <see cref="IndexLookupResultDTO"/> containing the lookup status and, when found, the hit information.
        /// </returns>
        /// <remarks>
        /// This overload encodes <paramref name="keyUtf8"/> to UTF-8 and forwards the lookup to the byte[] overload.
        /// </remarks>
        public IndexLookupResultDTO FindRowByKey(Guid tableId, uint column, string keyUtf8)
        {
            if (string.IsNullOrEmpty(keyUtf8))
                return new(IndexLookupStatusDTO.InvalidKey, default);

            return MetaDBManagerRPCCompatibleHelpers.WithUtf8(keyUtf8, bytes => FindRowByKey(tableId, column, bytes));
        }

        /// <summary>
        /// Attempts to locate the row index for a given UTF-8 key in a specific column using the built-in column-key index.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index to search.</param>
        /// <param name="keyUtf8">Key bytes in UTF-8 encoding.</param>
        /// <returns>
        /// A <see cref="IndexLookupResultDTO"/> containing:
        /// <list type="bullet">
        ///   <item><description><see cref="IndexLookupStatusDTO.Ok"/> and a populated <see cref="IndexHitDTO"/> when found.</description></item>
        ///   <item><description><see cref="IndexLookupStatusDTO.NotFound"/> when the key is not present in the index.</description></item>
        ///   <item><description><see cref="IndexLookupStatusDTO.InvalidColumn"/> when <paramref name="column"/> is out of range.</description></item>
        ///   <item><description><see cref="IndexLookupStatusDTO.ValueOnlyColumn"/> when the column has no key segment (<c>KeySize == 0</c>).</description></item>
        ///   <item><description><see cref="IndexLookupStatusDTO.TableNotOpen"/> when the table cannot be resolved/opened.</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method relies on the built-in "column key index" which maps (column, key) -> row.
        /// </para>
        /// <para>
        /// The returned <see cref="IndexHitDTO"/> includes the resolved <paramref name="column"/> and the matching row index.
        /// </para>
        /// </remarks>
        public IndexLookupResultDTO FindRowByKey(Guid tableId, uint column, byte[] keyUtf8)
        {
            if (tableId == Guid.Empty)
                return new(IndexLookupStatusDTO.TableNotOpen, default);

            return _innerManager.WithTable(tableId, t =>
            {
                if (column >= t.ColumnCount)
                    return new IndexLookupResultDTO(IndexLookupStatusDTO.InvalidColumn, default);

                var keySize = t.Spec.Columns[(int)column].Size.GetKeySize();
                if (keySize == 0)
                    return new(IndexLookupStatusDTO.ValueOnlyColumn, default);

                if (!t.TryFindRowByKey(column, keyUtf8, out var row))
                    return new(IndexLookupStatusDTO.NotFound, new(false, 0, 0, t.Spec.Name));

                return new(
                    IndexLookupStatusDTO.Ok,
                    new IndexHitDTO(true, column, row, t.Spec.Name)
                );
            });
        }

        /// <summary>
        /// Attempts to locate a (column,row) hit for a given global key using the built-in global key index.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="keyUtf8">Global key as a managed string (encoded as UTF-8 before lookup).</param>
        /// <returns>
        /// A <see cref="IndexLookupResultDTO"/> containing the lookup status and, when found, the hit information.
        /// </returns>
        /// <remarks>
        /// This overload encodes <paramref name="keyUtf8"/> to UTF-8 and forwards the lookup to the byte[] overload.
        /// </remarks>
        public IndexLookupResultDTO FindGlobal(Guid tableId, string keyUtf8)
        {
            if (string.IsNullOrEmpty(keyUtf8))
                return new(IndexLookupStatusDTO.InvalidKey, default);

            return MetaDBManagerRPCCompatibleHelpers.WithUtf8(keyUtf8, bytes => FindGlobal(tableId, bytes));
        }

        /// <summary>
        /// Attempts to locate a (column,row) hit for a given global UTF-8 key using the built-in global key index.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="keyUtf8">Global key bytes in UTF-8 encoding.</param>
        /// <returns>
        /// A <see cref="IndexLookupResultDTO"/> containing:
        /// <list type="bullet">
        ///   <item><description><see cref="IndexLookupStatusDTO.Ok"/> and a populated <see cref="IndexHitDTO"/> when found.</description></item>
        ///   <item><description><see cref="IndexLookupStatusDTO.NotFound"/> when the key is not present in the index.</description></item>
        ///   <item><description><see cref="IndexLookupStatusDTO.TableNotOpen"/> when the table cannot be resolved/opened.</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// The global index typically maps a unique key to the corresponding (column,row) location within the table.
        /// </remarks>
        public IndexLookupResultDTO FindGlobal(Guid tableId, byte[] keyUtf8)
        {
            if (tableId == Guid.Empty)
                return new(IndexLookupStatusDTO.TableNotOpen, default);

            return _innerManager.WithTable(tableId, t =>
            {
                if (!t.TryFindGlobal(keyUtf8, out var hit))
                    return new IndexLookupResultDTO(IndexLookupStatusDTO.NotFound, new(false, 0, 0, t.Spec.Name));

                return new(
                    IndexLookupStatusDTO.Ok,
                    new IndexHitDTO(true, hit.col, hit.row, t.Spec.Name)
                );
            });
        }

        /// <summary>
        /// Attempts to locate a (column,row) hit for a given global key using the built-in global key index.
        /// </summary>
        /// <param name="keyUtf8">Global key as a managed string (encoded as UTF-8 before lookup).</param>
        /// <returns>
        /// A <see cref="IndexLookupResultDTO"/> containing the lookup status and, when found, the hit information.
        /// </returns>
        /// <remarks>
        /// This overload encodes <paramref name="keyUtf8"/> to UTF-8 and forwards the lookup to the byte[] overload.
        /// </remarks>
        public IndexLookupResultDTO FindGlobal(string keyUtf8)
        {
            if (string.IsNullOrEmpty(keyUtf8))
                return new(IndexLookupStatusDTO.InvalidKey, default);

            return MetaDBManagerRPCCompatibleHelpers.WithUtf8(keyUtf8, FindGlobal);
        }

        /// <summary>
        /// Attempts to locate a (column,row) hit for a given global UTF-8 key across all tables using the global multi-table index.
        /// </summary>
        /// <param name="keyUtf8">Global key bytes in UTF-8 encoding.</param>
        /// <returns>
        /// A <see cref="IndexLookupResultDTO"/> containing:
        /// <list type="bullet">
        ///   <item><description><see cref="IndexLookupStatusDTO.Ok"/> and a populated <see cref="IndexHitDTO"/> when found.</description></item>
        ///   <item><description><see cref="IndexLookupStatusDTO.NotFound"/> when the key is not present in the index.</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method performs the lookup using the built-in cross-table global key index
        /// (<see cref="Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex"/>) registered under the manager-level registry.
        /// </para>
        /// <para>
        /// The index is created on demand (if missing) but <b>creation does not populate it</b>.
        /// You must populate/rebuild it beforehand via <see cref="RebuildAllIndexes(bool)"/> or <see cref="RebuildIndexes(Guid, bool)"/>
        /// with global indexing enabled. If the index is empty or stale, this method may return NotFound
        /// even if the key exists in the underlying storage.
        /// </para>
        /// </remarks>
        public IndexLookupResultDTO FindGlobal(byte[] keyUtf8)
        {
            if (!_innerManager.TryFindGlobal(keyUtf8, out var hit))
                return new IndexLookupResultDTO(IndexLookupStatusDTO.NotFound, new(false, 0, 0, string.Empty));

            return new(
                IndexLookupStatusDTO.Ok,
                new IndexHitDTO(true, hit.col, hit.row, hit.tableName)
            );
        }

        // -----------------------------
        // Dispose
        // -----------------------------

        /// <summary>
        /// Disposes this wrapper and the underlying <see cref="MetaDBManager"/>.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try { _innerManager.Dispose(); }
            finally { GC.SuppressFinalize(this); }
        }

        /// <summary>
        /// Asynchronously disposes this wrapper and the underlying <see cref="MetaDBManager"/>.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            try { await _innerManager.DisposeAsync().ConfigureAwait(false); }
            finally { GC.SuppressFinalize(this); }
        }
    }
}
