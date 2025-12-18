using System.Text;
using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;
using Microsoft.Extensions.Logging;

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
    /// UTF-8 decoded payloads, raw byte copies, or both, depending on <see cref="CellPayloadMode"/>.
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
    public class MetaDBManagerRCPCompatible(
        /// <summary>
        /// Logger used by the underlying <see cref="MetaDBManager"/> for structured logging and diagnostics.
        /// </summary>
        ILogger? logger,
        /// <summary>
        /// Optional table factory. When null, the underlying manager uses its default creation logic.
        /// </summary>
        Func<TableSpec?, MetadataTable>? factory = null,
        /// <summary>
        /// Default capacity behavior when per-call policies are unspecified (<see cref="CapacityPolicy.None"/>).
        /// </summary>
        CapacityPolicy capacityPolicy = CapacityPolicy.Throw,
        /// <summary>
        /// Optional persisted delete queue path for the delete worker. When null, the default path is used.
        /// </summary>
        string? deleteQueuePath = null)
        : CrossProcessServiceBase<IMetaDBManagerRCPCompatible>, IMetaDBManagerRCPCompatible
    {
        /// <summary>
        /// Named pipe identifier used by the cross-process host/client pair for MetaDB IPC.
        /// </summary>
        protected override string? PipeName => "Extend0.MetaDB";

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
        /// Uses <see cref="MetadataTable.GetLogicalRowCount"/> which scans the table and considers a row present when:
        /// a value-only column has any non-zero VALUE byte, or a key/value column stores a non-empty KEY.
        /// Intended for diagnostics and UI inspection rather than hot paths.
        /// </remarks>
        public int GetRowCount(Guid tableId)
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
        public string PreviewTable(Guid tableId, int maxRows = 32)
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
        public CellResultDTO? ReadCell(Guid tableId, uint column, uint row, CellPayloadMode cellPayloadMode = CellPayloadMode.Both)
            => _innerManager.WithTable(tableId, t => BuildCellDto(t, column, row, cellPayloadMode));

        /// <summary>
        /// Reads the VALUE payload of a cell as a raw byte copy.
        /// </summary>
        /// <remarks>
        /// Convenience wrapper over <see cref="ReadCell"/> that requests <see cref="CellPayloadMode.RawOnly"/>
        /// and returns <see cref="CellResultDTO.ValueRaw"/>.
        /// </remarks>
        public byte[]? ReadCellRaw(Guid tableId, uint column, uint row)
            => _innerManager.WithTable(tableId, t => BuildCellDto(t, column, row, CellPayloadMode.RawOnly)?.ValueRaw);

        /// <summary>
        /// Reads a row and returns a dictionary keyed by column name with per-cell DTO snapshots.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <param name="row">Row index.</param>
        /// <param name="cellPayloadMode">Payload selection strategy.</param>
        public Dictionary<string, CellResultDTO?> ReadRow(Guid tableId, uint row, CellPayloadMode cellPayloadMode = CellPayloadMode.Both)
            => _innerManager.WithTable(tableId, t =>
            {
                var cols = t.Spec.Columns;
                var dict = new Dictionary<string, CellResultDTO?>(cols.Length, StringComparer.Ordinal);
                for (uint c = 0; c < (uint)cols.Length; c++)
                    dict[cols[c].Name] = BuildCellDto(t, c, row, cellPayloadMode);
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
                    dict[cols[c].Name] = BuildCellDto(t, c, row, CellPayloadMode.RawOnly)?.ValueRaw;
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
        public CellResultDTO?[] ReadColumn(Guid tableId, uint column, uint startRow, uint rowCount, CellPayloadMode cellPayloadMode = CellPayloadMode.Both)
            => _innerManager.WithTable(tableId, t =>
            {
                var arr = new CellResultDTO?[rowCount];
                for (uint i = 0; i < rowCount; i++)
                    arr[i] = BuildCellDto(t, column, startRow + i, cellPayloadMode);
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
                    arr[i] = BuildCellDto(t, column, startRow + i, CellPayloadMode.RawOnly)?.ValueRaw;
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
        public CellResultDTO?[][] ReadBlock(Guid tableId, uint[] columns, uint startRow, uint rowCount, CellPayloadMode cellPayloadMode = CellPayloadMode.Both)
            => _innerManager.WithTable(tableId, t =>
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
                        line[ci] = BuildCellDto(t, columns[ci], startRow + r, CellPayloadMode.RawOnly)?.ValueRaw;
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
            CapacityPolicy policy = CapacityPolicy.None, CellPayloadMode cellPayloadMode = CellPayloadMode.Both)
            => _innerManager.WithTable(tableId, t =>
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
            });

        // -----------------------------
        // DTO builder + helpers
        // -----------------------------

        /// <summary>
        /// Builds a cross-process safe snapshot (<see cref="CellResultDTO"/>) of the specified cell.
        /// </summary>
        /// <param name="t">Resolved table instance.</param>
        /// <param name="column">Column index.</param>
        /// <param name="row">Row index.</param>
        /// <param name="mode">Payload selection strategy (UTF-8, raw, or both).</param>
        /// <returns>
        /// A populated DTO. When the cell is missing/unreadable, returns a DTO with <see cref="CellResultDTO.HasCell"/> = false.
        /// </returns>
        /// <remarks>
        /// <para>
        /// For value-only columns, VALUE bytes are read directly and <see cref="CellResultDTO.HasAnyValue"/> is computed by scanning for
        /// any non-zero byte.
        /// </para>
        /// <para>
        /// For key/value columns, KEY is read via <c>TryGetKey</c>; when present, VALUE is read via <c>TryGetValue(key,...)</c>.
        /// </para>
        /// <para>
        /// Payload copies are created only according to <paramref name="mode"/> to minimize allocations in bulk reads.
        /// </para>
        /// </remarks>
        private static CellResultDTO? BuildCellDto(MetadataTable t, uint column, uint row, CellPayloadMode mode)
        {
            var meta = t.Spec.Columns[(int)column];
            int keyCap = meta.Size.GetKeySize();
            int valCap = meta.Size.GetValueSize();
            bool isKeyValue = keyCap > 0;

            if (!t.TryGetCell(column, row, out var cell))
                return new CellResultDTO(
                    HasCell: false,
                    EntrySize: meta.Size,
                    KeyCapacity: keyCap,
                    ValueCapacity: valCap,
                    IsKeyValue: isKeyValue,
                    HasKey: false,
                    HasAnyValue: false,
                    KeyUtf8LengthHint: 0,
                    ValueUtf8LengthHint: 0,
                    Mode: mode,
                    KeyUtf8: null,
                    ValueUtf8: null,
                    KeyRaw: null,
                    ValueRaw: null,
                    Preview: null
                );

            ReadOnlySpan<byte> key = default;
            ReadOnlySpan<byte> value = default;
            bool hasKey = false;
            bool hasAnyValue = false;

            unsafe
            {
                if (!isKeyValue)
                {
                    // value-only: VALUE is direct segment
                    var raw = new ReadOnlySpan<byte>(cell.GetValuePointer(), valCap);
                    hasAnyValue = AnyNonZero(raw);
                    if (hasAnyValue) value = raw;
                }
                else
                {
                    if (cell.TryGetKey(out ReadOnlySpan<byte> k) && k.Length != 0)
                    {
                        hasKey = true;
                        key = k;

                        // classic: value obtained using key
                        if (cell.TryGetValue(k, out var v) && v.Length != 0)
                        {
                            value = v;
                            hasAnyValue = AnyNonZero(v); // optional signal
                        }
                    }
                }
            }

            int keyLenHint = hasKey && !key.IsEmpty ? CStrLenHint(key, keyCap) : 0;
            int valLenHint = !value.IsEmpty ? CStrLenHint(value, valCap) : 0;

            byte[]? keyRaw = null;
            byte[]? valRaw = null;
            string? keyUtf8 = null;
            string? valUtf8 = null;

            if (mode is CellPayloadMode.Both or CellPayloadMode.RawOnly)
            {
                if (hasKey && !key.IsEmpty) keyRaw = key.ToArray();
                if (!value.IsEmpty) valRaw = value.ToArray();
            }

            if (mode is CellPayloadMode.Both or CellPayloadMode.Utf8Only)
            {
                if (hasKey && !key.IsEmpty) keyUtf8 = TryDecodePrintableUtf8(key, keyLenHint);
                if (!value.IsEmpty) valUtf8 = TryDecodePrintableUtf8(value, valLenHint);
            }

            // preview prefers VALUE, fallback to KEY, else null
            string? preview = null;
            var prevSource = !value.IsEmpty ? value : hasKey ? key : default;
            if (!prevSource.IsEmpty)
                preview = MakePreview(prevSource, 48);

            return new CellResultDTO(
                HasCell: true,
                EntrySize: meta.Size,
                KeyCapacity: keyCap,
                ValueCapacity: valCap,
                IsKeyValue: isKeyValue,
                HasKey: hasKey,
                HasAnyValue: hasAnyValue,
                KeyUtf8LengthHint: keyLenHint,
                ValueUtf8LengthHint: valLenHint,
                Mode: mode,
                KeyUtf8: keyUtf8,
                ValueUtf8: valUtf8,
                KeyRaw: keyRaw,
                ValueRaw: valRaw,
                Preview: preview
            );
        }

        /// <summary>
        /// Ensures the table can address <paramref name="row"/> in <paramref name="column"/> using best-effort growth.
        /// </summary>
        /// <param name="t">Resolved table.</param>
        /// <param name="column">Column index.</param>
        /// <param name="row">Target row index that must be addressable.</param>
        /// <param name="policy">
        /// Capacity behavior:
        /// <list type="bullet">
        ///   <item><description><see cref="CapacityPolicy.None"/>: no growth attempt; returns false on failure.</description></item>
        ///   <item><description><see cref="CapacityPolicy.TryGrow"/>: attempts growth; returns false if still failing.</description></item>
        ///   <item><description><see cref="CapacityPolicy.Throw"/>: throws when capacity cannot be ensured.</description></item>
        /// </list>
        /// </param>
        /// <returns><see langword="true"/> if capacity is ensured; otherwise <see langword="false"/>.</returns>
        private static bool EnsureCapacityBestEffort(MetadataTable t, uint column, uint row, CapacityPolicy policy)
        {
            // Needs capacity >= row+1
            try
            {
                // If store auto-grows, this is enough.
                _ = t.GetOrCreateCell(column, row);
                return true;
            }
            catch
            {
                if (policy == CapacityPolicy.None) return false;

                // best-effort try grow + retry once
                if (t.TryGrowColumnTo(column, row + 1, zeroInit: true))
                {
                    try { _ = t.GetOrCreateCell(column, row); return true; }
                    catch { /* fallthrough */ }
                }

                if (policy == CapacityPolicy.Throw)
                    throw;

                return false;
            }
        }

        /// <summary>
        /// Clears <paramref name="bytes"/> bytes starting at <paramref name="ptr"/> by writing zeros.
        /// </summary>
        private static unsafe void ZeroFill(byte* ptr, int bytes)
        {
            if (bytes <= 0) return;
            new Span<byte>(ptr, bytes).Clear();
        }

        /// <summary>
        /// Writes <paramref name="src"/> into a fixed-size segment and zero-fills the remainder.
        /// </summary>
        /// <param name="dst">Destination pointer.</param>
        /// <param name="cap">Segment capacity in bytes.</param>
        /// <param name="src">Source bytes.</param>
        private static unsafe void WriteFixed(byte* dst, int cap, byte[] src)
        {
            var span = new Span<byte>(dst, cap);
            span.Clear();
            int n = Math.Min(cap, src.Length);
            src.AsSpan(0, n).CopyTo(span);
        }

        /// <summary>
        /// Writes the KEY segment for key/value columns.
        /// </summary>
        /// <remarks>
        /// Raw bytes are preferred when <paramref name="mode"/> allows it; otherwise UTF-8 encoding is used.
        /// When room exists, a trailing <c>0</c> is written to preserve "C-string-like" semantics for textual keys.
        /// </remarks>
        private static unsafe void WriteKeySegment(byte* keyPtr, int keyCap, byte[]? keyRaw, string? keyUtf8, CellPayloadMode mode)
        {
            var seg = new Span<byte>(keyPtr, keyCap);
            seg.Clear();

            ReadOnlySpan<byte> payload = default;

            if ((mode == CellPayloadMode.RawOnly || mode == CellPayloadMode.Both) && keyRaw is { Length: > 0 })
                payload = keyRaw;
            else if ((mode == CellPayloadMode.Utf8Only || mode == CellPayloadMode.Both) && !string.IsNullOrEmpty(keyUtf8))
                payload = Encoding.UTF8.GetBytes(keyUtf8);

            if (payload.IsEmpty) return;

            int n = Math.Min(keyCap, payload.Length);
            payload[..n].CopyTo(seg);

            // C-string-like: ensure terminator when room exists
            if (n < keyCap) seg[n] = 0;
        }

        /// <summary>
        /// Writes the VALUE segment.
        /// </summary>
        /// <remarks>
        /// Raw bytes are preferred when <paramref name="mode"/> allows it; otherwise UTF-8 encoding is used.
        /// When room exists, a trailing <c>0</c> is written to preserve "C-string-like" semantics for textual values.
        /// </remarks>
        private static unsafe void WriteValueSegment(byte* valuePtr, int valCap, byte[]? valueRaw, string? valueUtf8, CellPayloadMode mode)
        {
            var seg = new Span<byte>(valuePtr, valCap);
            seg.Clear();

            ReadOnlySpan<byte> payload = default;

            if ((mode == CellPayloadMode.RawOnly || mode == CellPayloadMode.Both) && valueRaw is { Length: > 0 })
                payload = valueRaw;
            else if ((mode == CellPayloadMode.Utf8Only || mode == CellPayloadMode.Both) && !string.IsNullOrEmpty(valueUtf8))
                payload = Encoding.UTF8.GetBytes(valueUtf8);

            if (payload.IsEmpty) return;

            int n = Math.Min(valCap, payload.Length);
            payload[..n].CopyTo(seg);

            if (n < valCap) seg[n] = 0;
        }

        /// <summary>
        /// Returns whether the provided byte span contains any non-zero byte.
        /// </summary>
        private static bool AnyNonZero(ReadOnlySpan<byte> data)
        {
            foreach (var b in data) if (b != 0) return true;
            return false;
        }

        /// <summary>
        /// Computes a best-effort length hint up to the first <c>0</c> terminator, capped to <paramref name="cap"/>.
        /// </summary>
        private static int CStrLenHint(ReadOnlySpan<byte> data, int cap)
        {
            int max = Math.Min(cap, data.Length);
            for (int i = 0; i < max; i++)
                if (data[i] == 0) return i;
            return max;
        }

        /// <summary>
        /// Attempts to decode printable UTF-8 from the payload.
        /// </summary>
        /// <param name="data">Raw bytes to decode.</param>
        /// <param name="lenHint">Length hint to slice before decoding (commonly derived from a terminator scan).</param>
        /// <returns>
        /// The decoded string when valid printable UTF-8; otherwise <see langword="null"/> (binary/non-printable data).
        /// </returns>
        private static string? TryDecodePrintableUtf8(ReadOnlySpan<byte> data, int lenHint)
        {
            var slice = data[..Math.Clamp(lenHint, 0, data.Length)];
            var s = Encoding.UTF8.GetString(slice);
            if (s.Contains('\uFFFD')) return null;
            foreach (var ch in s)
                if (char.IsControl(ch) && ch != '\t' && ch != '\r' && ch != '\n')
                    return null;
            return s;
        }

        /// <summary>
        /// Produces a compact preview for diagnostics: printable UTF-8 when possible, otherwise hex, truncated to <paramref name="maxChars"/>.
        /// </summary>
        private static string MakePreview(ReadOnlySpan<byte> data, int maxChars)
        {
            var s = TryDecodePrintableUtf8(data, data.Length);
            if (s is not null) return EllipsisSafe(s, maxChars);

            // hex preview
            if (maxChars <= 0) return string.Empty;
            int maxBytes = Math.Max(0, (maxChars - 1) / 2);
            int bytes = Math.Min(maxBytes, data.Length);

            Span<char> chars = stackalloc char[bytes * 2 + (bytes < data.Length ? 1 : 0)];
            int ci = 0;

            static char Hex(byte x) => (char)(x < 10 ? '0' + x : 'A' + (x - 10));

            for (int i = 0; i < bytes; i++)
            {
                byte b = data[i];
                chars[ci++] = Hex((byte)(b >> 4));
                chars[ci++] = Hex((byte)(b & 0xF));
            }

            if (bytes < data.Length)
                chars[ci++] = '…';

            return new string(chars[..ci]);
        }

        /// <summary>
        /// Truncates <paramref name="s"/> to <paramref name="maxChars"/> and appends an ellipsis when needed,
        /// preserving surrogate pairs at the cut boundary.
        /// </summary>
        private static string EllipsisSafe(string s, int maxChars)
        {
            if (maxChars <= 0) return string.Empty;
            if (s.Length <= maxChars) return s;
            int cut = Math.Max(0, maxChars - 1);
            if (cut > 0 && char.IsHighSurrogate(s[cut - 1])) cut--;
            return s.AsSpan(0, cut).ToString() + "…";
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
