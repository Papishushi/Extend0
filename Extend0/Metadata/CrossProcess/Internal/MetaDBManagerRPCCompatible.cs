using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.Contract;
using Extend0.Metadata.CrossProcess.DTO;
using Extend0.Metadata.CrossProcess.HResult;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Microsoft.Extensions.Logging;
using System.Buffers;
using static Extend0.Metadata.CrossProcess.Internal.MetaDBManagerRPCCompatibleHelpers;

namespace Extend0.Metadata.CrossProcess.Internal
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
    internal class MetaDBManagerRPCCompatible(
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

        /// <summary>
        /// Throws if this RPC-compatible facade (or its underlying in-proc manager) has already been disposed.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method checks both disposal sources:
        /// </para>
        /// <list type="bullet">
        ///   <item><description>
        ///   The wrapper itself (<c>_disposed</c>) which represents the lifetime of this RPC service instance.
        ///   </description></item>
        ///   <item><description>
        ///   The underlying <c>MetaDBManager</c> (<c>_innerManager.Disposed</c>) which may be disposed independently
        ///   (e.g., shutdown, explicit disposal, or external lifetime management).
        ///   </description></item>
        /// </list>
        /// <para>
        /// <c>_innerManager.Disposed</c> is read using <see cref="Volatile.Read(ref int)"/> to ensure the latest value is observed
        /// across threads without requiring a lock.
        /// </para>
        /// <para>
        /// If the inner manager is found disposed, this wrapper proactively calls <see cref="Dispose"/> to synchronize lifetimes,
        /// then throws <see cref="ObjectDisposedException"/> using <see cref="ObjectDisposedException.ThrowIf(bool, string?)"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">
        /// Thrown when the facade instance has been disposed or when the underlying manager has been disposed.
        /// </exception>
        private void ThrowIfDisposed()
        {
            var innerDisposed = false;
            if (Volatile.Read(ref _innerManager.Disposed) != 0) innerDisposed = true;
            if (innerDisposed) Dispose();
            ObjectDisposedException.ThrowIf(_disposed || innerDisposed, nameof(MetaDBManagerRPCCompatible));
        }

        // -----------------------------
        // Shared methods (RPC-wrapped)
        // -----------------------------

        /// <summary>
        /// Registers a table by name/path and column definitions.
        /// </summary>
        /// <param name="name">Unique logical name for the table.</param>
        /// <param name="mapPath">Backing file path for mapped storage.</param>
        /// <param name="columns">Column layout definitions.</param>
        /// <returns>The table identifier.</returns>
        public Guid RegisterTable(string name, string mapPath, params ColumnConfiguration[] columns)
            => Rpc(RpcOp.RegisterTable_NamePathColumns, () => _innerManager.RegisterTable(name, mapPath, columns), ThrowIfDisposed);

        /// <summary>
        /// Registers a table from a <see cref="TableSpec"/> definition.
        /// </summary>
        /// <param name="spec">Table specification (name/path/columns).</param>
        /// <param name="createNow">
        /// When <see langword="true"/>, forces immediate creation/opening; otherwise the table may be materialized lazily.
        /// </param>
        /// <returns>The table identifier.</returns>
        public Guid RegisterTable(TableSpec spec, bool createNow = false)
            => Rpc(RpcOp.RegisterTable_Spec, () => _innerManager.RegisterTable(spec, createNow), ThrowIfDisposed);

        /// <summary>
        /// Attempts to resolve a table id from a registered <paramref name="name"/>.
        /// </summary>
        /// <param name="name">Registered table name.</param>
        /// <returns>When this method returns, contains the resolved id if found.</returns>
        public Guid? TryGetIdByName(string name)
            => Rpc<Guid?>(RpcOp.TryGetIdByName, () => !_innerManager.TryGetIdByName(name, out var id) ? null : id, ThrowIfDisposed);

        /// <summary>
        /// Closes (and disposes) a managed table instance using strict semantics.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <returns><see langword="true"/> if the table was closed; otherwise <see langword="false"/>.</returns>
        public bool CloseStrict(Guid tableId)
            => Rpc(RpcOp.CloseStrict_ById, () => _innerManager.CloseStrict(tableId), ThrowIfDisposed);

        /// <summary>
        /// Closes (and disposes) a managed table instance by name using strict semantics.
        /// </summary>
        /// <param name="name">Registered table name.</param>
        /// <returns><see langword="true"/> if the table was closed; otherwise <see langword="false"/>.</returns>
        public bool CloseStrict(string name)
            => Rpc(RpcOp.CloseStrict_ByName, () => _innerManager.CloseStrict(name), ThrowIfDisposed);

        /// <summary>
        /// Closes all managed tables using best-effort semantics.
        /// </summary>
        public void CloseAll()
            => RpcVoid(RpcOp.CloseAll, _innerManager.CloseAll, ThrowIfDisposed);

        /// <summary>
        /// Closes all managed tables using strict semantics (fail-fast on inconsistencies).
        /// </summary>
        public void CloseAllStrict()
            => RpcVoid(RpcOp.CloseAllStrict, _innerManager.CloseAllStrict, ThrowIfDisposed);

        /// <summary>
        /// Rebuilds indexes for a single table.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="includeGlobal">Whether to rebuild the global key index.</param>
        public void RebuildIndexes(Guid tableId, bool includeGlobal = true)
            => RpcVoid(RpcOp.RebuildIndexes, () => _innerManager.RebuildIndexes(tableId, includeGlobal), ThrowIfDisposed);

        /// <summary>
        /// Rebuilds indexes for all managed tables.
        /// </summary>
        /// <param name="includeGlobal">Whether to rebuild the global key index.</param>
        public void RebuildAllIndexes(bool includeGlobal = true)
            => RpcVoid(RpcOp.RebuildAllIndexes, () => _innerManager.RebuildAllIndexes(includeGlobal), ThrowIfDisposed);

        /// <summary>
        /// Restarts the background delete worker responsible for deleting queued table files.
        /// </summary>
        /// <param name="deleteQueuePath">Optional override queue path; when null, uses the configured default.</param>
        public Task RestartDeleteWorker(string? deleteQueuePath = null)
            => RpcAsync(RpcOp.RestartDeleteWorker, () => _innerManager.RestartDeleteWorker(deleteQueuePath), ThrowIfDisposed);

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
            => RpcVoid(RpcOp.CopyColumn, () => _innerManager.CopyColumn(srcTableId, srcCol, dstTableId, dstCol, rows, dstPolicy), ThrowIfDisposed);

        /// <summary>
        /// Ensures the reference vector entry exists for a given parent row.
        /// </summary>
        /// <param name="parentTableId">Parent table id.</param>
        /// <param name="refsCol">Reference vector column index.</param>
        /// <param name="parentRow">Parent row index.</param>
        /// <param name="policy">Capacity policy.</param>
        public void EnsureRefVec(Guid parentTableId, uint refsCol, uint parentRow,
            CapacityPolicy policy = CapacityPolicy.None)
            => RpcVoid(RpcOp.EnsureRefVec, () => _innerManager.EnsureRefVec(parentTableId, refsCol, parentRow, policy), ThrowIfDisposed);

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
            => RpcVoid(RpcOp.LinkRef, () => _innerManager.LinkRef(parentTableId, refsCol, parentRow, childTableId, childCol, childRow, policy), ThrowIfDisposed);

        // -----------------------------
        // RPC-only methods (RPC-wrapped)
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
            => Rpc(RpcOp.GetRowCount, () => _innerManager.WithTable(tableId, t => t.GetLogicalRowCount()), ThrowIfDisposed);

        /// <summary>
        /// Returns the number of columns defined in the table spec.
        /// </summary>
        public int GetColumnCount(Guid tableId)
            => Rpc(RpcOp.GetColumnCount, () => _innerManager.WithTable(tableId, t => t.ColumnCount), ThrowIfDisposed);

        /// <summary>
        /// Returns column names in their declared order.
        /// </summary>
        public string[] GetColumnNames(Guid tableId)
            => Rpc(RpcOp.GetColumnNames, () => _innerManager.WithTable(tableId, t => t.Spec.Columns.Select(c => c.Name).ToArray()), ThrowIfDisposed);

        /// <summary>
        /// Produces a human-readable preview of the table content (best-effort).
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="maxRows">Maximum number of rows to render in the preview.</param>
        public string PreviewTable(Guid tableId, uint maxRows = 32)
            => Rpc(RpcOp.PreviewTable, () => _innerManager.WithTable(tableId, t => t.ToString(maxRows)), ThrowIfDisposed);

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
            => Rpc(RpcOp.ReadCell, () => _innerManager.WithTable(tableId, t => BuildCellDto(t, column, row, cellPayloadMode)), ThrowIfDisposed);

        /// <summary>
        /// Reads the VALUE payload of a cell as a raw byte copy.
        /// </summary>
        /// <remarks>
        /// Convenience wrapper over <see cref="ReadCell"/> that requests <see cref="CellPayloadModeDTO.RawOnly"/>
        /// and returns <see cref="CellResultDTO.ValueRaw"/>.
        /// </remarks>
        public byte[]? ReadCellRaw(Guid tableId, uint column, uint row)
            => Rpc(RpcOp.ReadCellRaw, () => _innerManager.WithTable(tableId, t => BuildCellDto(t, column, row, CellPayloadModeDTO.RawOnly)?.ValueRaw), ThrowIfDisposed);

        /// <summary>
        /// Reads a row and returns a dictionary keyed by column name with per-cell DTO snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="row">Row index.</param>
        /// <param name="cellPayloadMode">Payload selection strategy.</param>
        public Dictionary<string, CellResultDTO?> ReadRow(Guid tableId, uint row, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => Rpc(RpcOp.ReadRow, () => _innerManager.WithTable(tableId, t =>
            {
                var cols = t.Spec.Columns;
                var dict = new Dictionary<string, CellResultDTO?>(cols.Length, StringComparer.Ordinal);
                for (uint c = 0; c < (uint)cols.Length; c++)
                    dict[cols[c].Name] = BuildCellDto(t, c, row, cellPayloadMode);
                return dict;
            }), ThrowIfDisposed);

        /// <summary>
        /// Reads a row and returns a dictionary keyed by column name with raw VALUE payloads.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="row">Row index.</param>
        public Dictionary<string, byte[]?> ReadRowRaw(Guid tableId, uint row)
            => Rpc(RpcOp.ReadRowRaw, () => _innerManager.WithTable(tableId, t =>
            {
                var cols = t.Spec.Columns;
                var dict = new Dictionary<string, byte[]?>(cols.Length, StringComparer.Ordinal);
                for (uint c = 0; c < (uint)cols.Length; c++)
                    dict[cols[c].Name] = BuildCellDto(t, c, row, CellPayloadModeDTO.RawOnly)?.ValueRaw;
                return dict;
            }), ThrowIfDisposed);

        /// <summary>
        /// Reads a contiguous slice of a column as DTO snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        /// <param name="cellPayloadMode">Payload selection strategy.</param>
        public CellResultDTO?[] ReadColumn(Guid tableId, uint column, uint startRow, uint rowCount, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => Rpc(RpcOp.ReadColumn, () => _innerManager.WithTable(tableId, t =>
            {
                var arr = new CellResultDTO?[rowCount];
                for (uint i = 0; i < rowCount; i++)
                    arr[i] = BuildCellDto(t, column, startRow + i, cellPayloadMode);
                return arr;
            }), ThrowIfDisposed);

        /// <summary>
        /// Reads a contiguous slice of a column as raw VALUE payload copies.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="column">Column index.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        public byte[]?[] ReadColumnRaw(Guid tableId, uint column, uint startRow, uint rowCount)
            => Rpc(RpcOp.ReadColumnRaw, () => _innerManager.WithTable(tableId, t =>
            {
                var arr = new byte[]?[rowCount];
                for (uint i = 0; i < rowCount; i++)
                    arr[i] = BuildCellDto(t, column, startRow + i, CellPayloadModeDTO.RawOnly)?.ValueRaw;
                return arr;
            }), ThrowIfDisposed);

        /// <summary>
        /// Reads a rectangular block defined by a column set and a contiguous row range, returning DTO snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="columns">Column indices to read.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        /// <param name="cellPayloadMode">Payload selection strategy.</param>
        public CellResultDTO?[][] ReadBlock(Guid tableId, uint[] columns, uint startRow, uint rowCount, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => Rpc(RpcOp.ReadBlock, () => _innerManager.WithTable(tableId, t =>
            {
                var rows = new CellResultDTO?[rowCount][];
                for (uint r = 0; r < rowCount; r++)
                {
                    var line = new CellResultDTO?[columns.Length];
                    for (int ci = 0; ci < columns.Length; ci++)
                        line[ci] = BuildCellDto(t, columns[ci], startRow + r, cellPayloadMode);
                    rows[r] = line;
                }
                return rows;
            }), ThrowIfDisposed);

        /// <summary>
        /// Reads a rectangular block as raw VALUE payload copies.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="columns">Column indices to read.</param>
        /// <param name="startRow">First row to read.</param>
        /// <param name="rowCount">Number of rows to read.</param>
        public byte[]?[][] ReadBlockRaw(Guid tableId, uint[] columns, uint startRow, uint rowCount)
            => Rpc(RpcOp.ReadBlockRaw, () => _innerManager.WithTable(tableId, t =>
            {
                var rows = new byte[]?[rowCount][];
                for (uint r = 0; r < rowCount; r++)
                {
                    var line = new byte[]?[columns.Length];
                    for (int ci = 0; ci < columns.Length; ci++)
                        line[ci] = BuildCellDto(t, columns[ci], startRow + r, CellPayloadModeDTO.RawOnly)?.ValueRaw;
                    rows[r] = line;
                }
                return rows;
            }), ThrowIfDisposed);

        /// <summary>
        /// Creates (or opens) a child table from <paramref name="childSpec"/>, links it under the parent reference vector,
        /// and returns the child table id.
        /// </summary>
        public Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, TableSpec childSpec, uint childCol = 0, uint childRow = 0)
            => Rpc(RpcOp.GetOrCreateAndLinkChild, () =>
                _innerManager.GetOrCreateAndLinkChild(parentTableId, refsCol, parentRow, _ => childSpec, childCol, childRow), ThrowIfDisposed);

        /// <summary>
        /// Creates (or opens) a child table from <paramref name="childSpec"/> using an explicit child key, links it under the parent,
        /// and returns the child table id.
        /// </summary>
        public Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, uint childKey, TableSpec childSpec, uint childCol = 0, uint childRow = 0)
            => Rpc(RpcOp.GetOrCreateAndLinkChild_WithKey, () =>
                _innerManager.GetOrCreateAndLinkChild(parentTableId, refsCol, parentRow, childKey, _ => childSpec, childCol, childRow), ThrowIfDisposed);

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
        public void FillColumn(
            Guid tableId,
            uint column,
            uint startRow,
            CellResultDTO?[] values,
            CapacityPolicy policy = CapacityPolicy.None,
            CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both)
            => RpcVoid(RpcOp.FillColumn, () =>
                _innerManager.WithTable(tableId, t =>
                {
                    var meta = t.Spec.Columns[(int)column];
                    int keyCap = meta.Size.GetKeySize();
                    int valCap = meta.Size.GetValueSize();

                    for (int i = 0; i < values.Length; i++)
                    {
                        uint row = startRow + (uint)i;

                        // best-effort growth depending on policy
                        if (!EnsureCapacityBestEffort(t, column, row, policy))
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
                                ZeroFill(keyPtr, keyCap + valCap);
                                continue;
                            }

                            CellResultDTO dto = dtoNullable.Value;

                            if (keyCap == 0)
                                // VALUE-ONLY: write VALUE only
                                WriteValueSegment(valuePtr, valCap, dto.ValueRaw, dto.ValueUtf8, cellPayloadMode);
                            else
                            {
                                // KEY/VALUE: write KEY + VALUE
                                WriteKeySegment(keyPtr, keyCap, dto.KeyRaw, dto.KeyUtf8, cellPayloadMode);
                                WriteValueSegment(valuePtr, valCap, dto.ValueRaw, dto.ValueUtf8, cellPayloadMode);
                            }
                        }
                    }
                }), ThrowIfDisposed);

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
        public void FillColumnRaw(
            Guid tableId,
            uint column,
            uint startRow,
            byte[]?[] valuesRaw,
            CapacityPolicy policy = CapacityPolicy.None)
            => RpcVoid(RpcOp.FillColumnRaw, () =>
                _innerManager.WithTable(tableId, t =>
                {
                    var meta = t.Spec.Columns[(int)column];
                    int valCap = meta.Size.GetValueSize();

                    for (int i = 0; i < valuesRaw.Length; i++)
                    {
                        uint row = startRow + (uint)i;

                        if (!EnsureCapacityBestEffort(t, column, row, policy))
                            continue;

                        var cell = t.GetOrCreateCell(column, row);
                        unsafe
                        {
                            byte* valuePtr = cell.GetValuePointer();
                            var temp = valuesRaw[i];

                            if (temp is null)
                            {
                                ZeroFill(valuePtr, valCap); // only VALUE cleared
                                continue;
                            }

                            WriteFixed(valuePtr, valCap, temp);
                        }
                    }
                }), ThrowIfDisposed);

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
            => Rpc(RpcOp.GetIndexes_Table, () =>
            {
                if (tableId == Guid.Empty) return [];

                return _innerManager.WithTable(tableId, t =>
                    t.Indexes
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
                     .ToArray()
                );
            }, ThrowIfDisposed);

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
            => Rpc<IndexInfoDTO[]>(RpcOp.GetIndexes_Manager, () =>
                [.. _innerManager.Indexes
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
            ))], ThrowIfDisposed);

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
        /// Index instantiation is delegated to <c>TryCreateCustomIndex(...)</c>, which is backend-specific.
        /// </para>
        /// </remarks>
        public IndexMutationResultDTO AddIndex(Guid tableId, AddIndexRequestDTO request)
            => Rpc(RpcOp.AddIndex_Table, () =>
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
                        if (IsBuiltIn(existing!))
                            return new IndexMutationResultDTO(IndexMutationStatusDTO.BuiltInProtected, null, "Built-in indexes cannot be replaced/removed.");

                        if (!request.ReplaceIfExists)
                            return new(IndexMutationStatusDTO.AlreadyExists, null, request.Notes);
                    }

                    // 2) Create the new index instance (backend-specific)
                    //    IMPORTANT: do NOT remove the old one until we have a valid new instance.
                    var create = TryCreateCustomIndex(request, out ITableIndex? created, out var createNotes);

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

                        var info = ToIndexInfoDTO(created);
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
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.AddIndex_Manager, () =>
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
                    if (IsBuiltIn(existing!))
                        return new IndexMutationResultDTO(IndexMutationStatusDTO.BuiltInProtected, null, "Built-in indexes cannot be replaced/removed.");

                    if (!request.ReplaceIfExists)
                        return new(IndexMutationStatusDTO.AlreadyExists, null, request.Notes);
                }

                // 2) Create the new index instance (backend-specific)
                //    IMPORTANT: do NOT remove the old one until we have a valid new instance.
                var create = TryCreateCustomIndex(request, out ICrossTableIndex? created, out var createNotes);

                if (create != IndexMutationStatusDTO.Ok || created is null)
                    return new(create, null, createNotes ?? request.Notes);

                // 3) Swap (safe replace)
                try
                {
                    if (exists)
                        _innerManager.Indexes.Remove(request.Name);

                    _innerManager.Indexes.Add(created);

                    var info = ToIndexInfoDTO(created);
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
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.RemoveIndex_Table, () =>
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
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.RemoveIndex_Manager, () =>
            {
                if (string.IsNullOrWhiteSpace(name))
                    return new(IndexMutationStatusDTO.InvalidName, null);

                if (!_innerManager.Indexes.TryGet(name, out var idx))
                    return new IndexMutationResultDTO(IndexMutationStatusDTO.NotFound, null);

                if (idx is Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex)
                    return new IndexMutationResultDTO(IndexMutationStatusDTO.BuiltInProtected, null);

                _innerManager.Indexes.Remove(name);
                return new IndexMutationResultDTO(IndexMutationStatusDTO.Ok, null);
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.FindRowByKey_String, () =>
            {
                if (string.IsNullOrEmpty(keyUtf8))
                    return new(IndexLookupStatusDTO.InvalidKey, default);

                return WithUtf8(keyUtf8, bytes => FindRowByKey(tableId, column, bytes));
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.FindRowByKey_Bytes, () =>
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
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.FindGlobal_Table_String, () =>
            {
                if (string.IsNullOrEmpty(keyUtf8))
                    return new(IndexLookupStatusDTO.InvalidKey, default);

                return WithUtf8(keyUtf8, bytes => FindGlobal(tableId, bytes));
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.FindGlobal_Table_Bytes, () =>
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
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.FindGlobal_Manager_String, () =>
            {
                if (string.IsNullOrEmpty(keyUtf8))
                    return new(IndexLookupStatusDTO.InvalidKey, default);

                return WithUtf8(keyUtf8, FindGlobal);
            }, ThrowIfDisposed);

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
            => Rpc(RpcOp.FindGlobal_Manager_Bytes, () =>
            {
                if (!_innerManager.TryFindGlobal(keyUtf8, out var hit))
                    return new IndexLookupResultDTO(IndexLookupStatusDTO.NotFound, new(false, 0, 0, string.Empty));

                return new(
                    IndexLookupStatusDTO.Ok,
                    new IndexHitDTO(true, hit.col, hit.row, hit.tableName)
                );
            }, ThrowIfDisposed);

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
