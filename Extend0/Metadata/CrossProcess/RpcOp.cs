namespace Extend0.Metadata.CrossProcess
{
    /// <summary>
    /// Identifies the RPC operation that failed, encoded into the <c>HRESULT</c> code produced by <see cref="MakeRpcHResult"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Values are <see cref="ushort"/> to allow a reasonably sized operation namespace while still packing into a 16-bit code.
    /// </para>
    /// <para>
    /// The current encoding uses only the low 8 bits of the operation id when building the final 16-bit code
    /// (<c>(op &amp; 0xFF) &lt;&lt; 8 | err</c>). If you need more than 256 operations, update <see cref="MakeRpcHResult"/>
    /// and ensure older clients can still interpret the result.
    /// </para>
    /// </remarks>
    public enum RpcOp : ushort
    {
        /// <summary>Registers a table by name/path/columns.</summary>
        RegisterTable_NamePathColumns = 0x0001,

        /// <summary>Registers a table from a <see cref="TableSpec"/>.</summary>
        RegisterTable_Spec = 0x0002,

        /// <summary>Resolves a table id by its registered name.</summary>
        TryGetIdByName = 0x0003,

        /// <summary>Closes a table strictly by id.</summary>
        CloseStrict_ById = 0x0004,

        /// <summary>Closes a table strictly by name.</summary>
        CloseStrict_ByName = 0x0005,

        /// <summary>Closes all tables (best-effort).</summary>
        CloseAll = 0x0006,

        /// <summary>Closes all tables strictly (fail-fast).</summary>
        CloseAllStrict = 0x0007,

        /// <summary>Rebuilds indexes for a single table.</summary>
        RebuildIndexes = 0x0008,

        /// <summary>Rebuilds indexes for all tables.</summary>
        RebuildAllIndexes = 0x0009,

        /// <summary>Restarts the background delete worker.</summary>
        RestartDeleteWorker = 0x000A,

        /// <summary>Copies a contiguous row range from one column to another.</summary>
        CopyColumn = 0x000B,

        /// <summary>Ensures a reference-vector entry exists for a parent row.</summary>
        EnsureRefVec = 0x000C,

        /// <summary>Links a parent row to a child reference.</summary>
        LinkRef = 0x000D,

        /// <summary>Gets a best-effort logical row count.</summary>
        GetRowCount = 0x000E,

        /// <summary>Gets the number of columns.</summary>
        GetColumnCount = 0x000F,

        /// <summary>Gets column names in declared order.</summary>
        GetColumnNames = 0x0010,

        /// <summary>Returns a human-readable preview of the table.</summary>
        PreviewTable = 0x0011,

        /// <summary>Reads a single cell as a DTO snapshot.</summary>
        ReadCell = 0x0012,

        /// <summary>Reads the VALUE segment of a cell as raw bytes.</summary>
        ReadCellRaw = 0x0013,

        /// <summary>Reads a row into a dictionary keyed by column name (DTO snapshots).</summary>
        ReadRow = 0x0014,

        /// <summary>Reads a row into a dictionary keyed by column name (raw VALUE bytes).</summary>
        ReadRowRaw = 0x0015,

        /// <summary>Reads a contiguous column slice as DTO snapshots.</summary>
        ReadColumn = 0x0016,

        /// <summary>Reads a contiguous column slice as raw VALUE bytes.</summary>
        ReadColumnRaw = 0x0017,

        /// <summary>Reads a rectangular block as DTO snapshots.</summary>
        ReadBlock = 0x0018,

        /// <summary>Reads a rectangular block as raw VALUE bytes.</summary>
        ReadBlockRaw = 0x0019,

        /// <summary>Creates/opens a child table and links it under a parent reference vector.</summary>
        GetOrCreateAndLinkChild = 0x001A,

        /// <summary>Creates/opens a child table using an explicit child key and links it under a parent.</summary>
        GetOrCreateAndLinkChild_WithKey = 0x001B,

        /// <summary>Writes cells into a column from DTO snapshots.</summary>
        FillColumn = 0x001C,

        /// <summary>Writes raw VALUE payloads into a column.</summary>
        FillColumnRaw = 0x001D,

        /// <summary>Enumerates indexes for a specific table.</summary>
        GetIndexes_Table = 0x001E,

        /// <summary>Enumerates manager-level (cross-table) indexes.</summary>
        GetIndexes_Manager = 0x001F,

        /// <summary>Adds/replaces an index in a table registry.</summary>
        AddIndex_Table = 0x0020,

        /// <summary>Adds/replaces an index in the manager-level registry.</summary>
        AddIndex_Manager = 0x0021,

        /// <summary>Removes an index from a table registry.</summary>
        RemoveIndex_Table = 0x0022,

        /// <summary>Removes an index from the manager-level registry.</summary>
        RemoveIndex_Manager = 0x0023,

        /// <summary>Finds a row by key (string) using the built-in column-key index.</summary>
        FindRowByKey_String = 0x0024,

        /// <summary>Finds a row by key (bytes) using the built-in column-key index.</summary>
        FindRowByKey_Bytes = 0x0025,

        /// <summary>Finds a global hit in a specific table (string) using the built-in global key index.</summary>
        FindGlobal_Table_String = 0x0026,

        /// <summary>Finds a global hit in a specific table (bytes) using the built-in global key index.</summary>
        FindGlobal_Table_Bytes = 0x0027,

        /// <summary>Finds a global hit across all tables (string) using the global multi-table index.</summary>
        FindGlobal_Manager_String = 0x0028,

        /// <summary>Finds a global hit across all tables (bytes) using the global multi-table index.</summary>
        FindGlobal_Manager_Bytes = 0x0029,
    }
}