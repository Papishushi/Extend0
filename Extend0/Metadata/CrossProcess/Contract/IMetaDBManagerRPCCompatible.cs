using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.DTO;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;

namespace Extend0.Metadata.CrossProcess.Contract;

/// <summary>
/// High-level manager for cross-process (RPC) interaction with MetaDB tables.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IMetaDBManagerRPCCompatible"/> exposes an RPC-safe subset of MetaDB operations where all inputs and
/// outputs are serializable and do not leak process-dependent state (pointers, spans, mapped views, or live table objects).
/// </para>
/// <para>
/// Core responsibilities typically include:
/// </para>
/// <list type="bullet">
///   <item><description><b>Registration</b>: define tables and their schema (<see cref="IMetaDBManagerCommon.RegisterTable(string,string,ColumnConfiguration[])"/>, <see cref="IMetaDBManagerCommon.RegisterTable(TableSpec,bool)"/>).</description></item>
///   <item><description><b>Identity</b>: resolve table ids by name (<see cref="IMetaDBManagerCommon.TryGetIdByName(string,out Guid)"/>).</description></item>
///   <item><description><b>Open/Close</b>: close managed instances using strict or best-effort semantics (<see cref="IMetaDBManagerCommon.CloseStrict(Guid)"/>, <see cref="IMetaDBManagerCommon.CloseStrict(string)"/>, <see cref="IMetaDBManagerCommon.CloseAll"/>, <see cref="IMetaDBManagerCommon.CloseAllStrict"/>).</description></item>
///   <item><description><b>Reads</b>: read cell/row/column/block values as serializable payloads and produce human-readable previews (<see cref="ReadCell(Guid,uint,uint,CellPayloadModeDTO)"/>, <see cref="ReadBlock(Guid,uint[],uint,uint,CellPayloadModeDTO)"/>, <see cref="PreviewTable(Guid, uint)"/>).</description></item>
///   <item><description><b>Data movement</b>: copy and fill columns without delegates (<see cref="IMetaDBManagerCommon.CopyColumn"/>, <see cref="FillColumn"/>, <see cref="FillColumnRaw"/>).</description></item>
///   <item><description><b>Relationships</b>: maintain parent/child references and ref-vectors without factory callbacks (<see cref="IMetaDBManagerCommon.EnsureRefVec"/>, <see cref="IMetaDBManagerCommon.LinkRef"/>, <see cref="GetOrCreateAndLinkChild(Guid,uint,uint,TableSpec,uint,uint)"/>).</description></item>
///   <item><description><b>Maintenance</b>: rebuild indexes and manage background deletion workers (<see cref="IMetaDBManagerCommon.RebuildIndexes"/>, <see cref="IMetaDBManagerCommon.RebuildAllIndexes"/>, <see cref="IMetaDBManagerCommon.RestartDeleteWorker(string?)"/>).</description></item>
/// </list>
/// <para>
/// This interface intentionally excludes APIs that:
/// </para>
/// <list type="bullet">
///   <item><description>return <c>MetadataTable</c>/<c>MetadataCell</c> (they embed process-dependent state),</description></item>
///   <item><description>accept delegates/functions (not viable for RPC boundaries),</description></item>
///   <item><description>expose direct unmanaged access (pointers, spans, views).</description></item>
/// </list>
/// <para>
/// Implementations are expected to be thread-safe where appropriate, and to honor best-effort vs strict semantics
/// as exposed by the API (e.g., <see cref="IMetaDBManagerCommon.CloseAll"/> vs <see cref="IMetaDBManagerCommon.CloseAllStrict"/>).
/// </para>
/// </remarks>
public interface IMetaDBManagerRPCCompatible : IMetaDBManagerCommon, ICrossProcessService
{
    /// <summary>
    /// Attempts to resolve a table id from a registered <paramref name="name"/>.
    /// </summary>
    /// <param name="name">Registered table name.</param>
    /// <returns>When this method returns, contains the resolved id if found.</returns>
    Guid? TryGetIdByName(string name);

    /// <summary>
    /// Gets the logical row count for the table identified by <paramref name="tableId"/>.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <returns>The logical number of rows containing data (implementation-defined).</returns>
    uint GetRowCount(Guid tableId);

    /// <summary>
    /// Gets the number of columns declared for the table identified by <paramref name="tableId"/>.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <returns>The number of columns in the table schema.</returns>
    int GetColumnCount(Guid tableId);

    /// <summary>
    /// Returns the logical names of all columns in the table identified by <paramref name="tableId"/>.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <returns>An array of column names in declared order.</returns>
    string[] GetColumnNames(Guid tableId);

    /// <summary>
    /// Returns a human-readable preview of the table identified by <paramref name="tableId"/>,
    /// similar to calling <c>MetadataTable.ToString(maxRows)</c> locally.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="maxRows">Maximum number of rows to include in the preview.</param>
    /// <returns>A formatted textual preview of the table.</returns>
    string PreviewTable(Guid tableId, uint maxRows = 32);

    /// <summary>
    /// Reads a single cell from a table and returns a serializable snapshot of its VALUE payload.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Zero-based column index.</param>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="cellPayloadMode">Controls whether UTF-8 text, raw bytes, or both are included in the result.</param>
    /// <returns>
    /// A snapshot of the cell's VALUE payload, or <see langword="null"/> if the cell is missing or logically empty.
    /// </returns>
    /// <remarks>
    /// <para>
    /// The returned payload is detached from any underlying mapped view; callers may safely retain it beyond the RPC call.
    /// </para>
    /// <para>
    /// For text output (<see cref="CellResultDTO.Utf8"/>), implementations should decode bytes as UTF-8 on a best-effort basis,
    /// typically treating empty or all-zero payloads as missing. For non-printable or invalid UTF-8, implementations may return
    /// <see langword="null"/> (or an implementation-defined placeholder) while still returning raw bytes when requested.
    /// </para>
    /// </remarks>
    CellResultDTO? ReadCell(Guid tableId, uint column, uint row, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both);

    /// <summary>
    /// Reads a single cell from a table and returns a defensive copy of its raw VALUE payload bytes.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Zero-based column index.</param>
    /// <param name="row">Zero-based row index.</param>
    /// <returns>The raw VALUE bytes, or <see langword="null"/> if the cell is missing or logically empty.</returns>
    /// <remarks>
    /// This is a convenience wrapper over <see cref="ReadCell(Guid,uint,uint,CellPayloadModeDTO)"/> using <see cref="CellPayloadModeDTO.RawOnly"/>.
    /// </remarks>
    byte[]? ReadCellRaw(Guid tableId, uint column, uint row);

    /// <summary>
    /// Reads all column values for a given row and returns them as a name→value map.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="cellPayloadMode">Controls whether UTF-8 text, raw bytes, or both are included for each cell.</param>
    /// <returns>
    /// A dictionary where keys are column names and values are cell payload snapshots
    /// (or <see langword="null"/> when a cell is missing or logically empty).
    /// </returns>
    Dictionary<string, CellResultDTO?> ReadRow(Guid tableId, uint row, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both);

    /// <summary>
    /// Reads all column values for a given row and returns them as a name→raw VALUE payload map.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="row">Zero-based row index.</param>
    /// <returns>
    /// A dictionary where keys are column names and values are defensive copies of raw VALUE payload bytes
    /// (or <see langword="null"/> for missing/empty cells).
    /// </returns>
    Dictionary<string, byte[]?> ReadRowRaw(Guid tableId, uint row);

    /// <summary>
    /// Reads a contiguous range of values from a column and returns them as serializable payload snapshots.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Zero-based column index.</param>
    /// <param name="startRow">First row to read.</param>
    /// <param name="rowCount">Number of rows to read.</param>
    /// <param name="cellPayloadMode">Controls whether UTF-8 text, raw bytes, or both are included for each cell.</param>
    /// <returns>
    /// An array of length <paramref name="rowCount"/> containing payload snapshots
    /// (or <see langword="null"/> for missing/empty cells).
    /// </returns>
    CellResultDTO?[] ReadColumn(Guid tableId, uint column, uint startRow, uint rowCount, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both);

    /// <summary>
    /// Reads a contiguous range of raw VALUE payloads from a column.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Zero-based column index.</param>
    /// <param name="startRow">First row to read.</param>
    /// <param name="rowCount">Number of rows to read.</param>
    /// <returns>
    /// An array of length <paramref name="rowCount"/> where each entry is a defensive copy of the raw VALUE bytes
    /// (or <see langword="null"/> for missing/empty cells).
    /// </returns>
    byte[]?[] ReadColumnRaw(Guid tableId, uint column, uint startRow, uint rowCount);

    /// <summary>
    /// Reads a rectangular block of data for the specified columns, starting at <paramref name="startRow"/>.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="columns">Zero-based column indices to include in the block.</param>
    /// <param name="startRow">First row to read.</param>
    /// <param name="rowCount">Number of rows to read.</param>
    /// <param name="cellPayloadMode">Controls whether UTF-8 text, raw bytes, or both are included for each cell.</param>
    /// <returns>
    /// A jagged array of rows, where each row is an array of payload snapshots
    /// (or <see langword="null"/> for missing/empty cells).
    /// </returns>
    /// <remarks>
    /// Jagged arrays are used instead of multi-dimensional arrays for maximum serializer compatibility across RPC boundaries.
    /// </remarks>
    CellResultDTO?[][] ReadBlock(Guid tableId, uint[] columns, uint startRow, uint rowCount, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both);

    /// <summary>
    /// Reads a rectangular block of raw VALUE payloads for the specified columns, starting at <paramref name="startRow"/>.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="columns">Zero-based column indices to include in the block.</param>
    /// <param name="startRow">First row to read.</param>
    /// <param name="rowCount">Number of rows to read.</param>
    /// <returns>
    /// A jagged array of rows, where each row is an array of defensive copies of the raw VALUE bytes
    /// (or <see langword="null"/> for missing/empty cells).
    /// </returns>
    byte[]?[][] ReadBlockRaw(Guid tableId, uint[] columns, uint startRow, uint rowCount);

    /// <summary>
    /// Ensures a child table exists and links it into a parent ref-vector, using a fully specified child <see cref="TableSpec"/>.
    /// </summary>
    /// <param name="parentTableId">Parent table id.</param>
    /// <param name="refsCol">Parent ref-vector column index.</param>
    /// <param name="parentRow">Parent row index.</param>
    /// <param name="childSpec">Child table specification used when creation/materialization is required.</param>
    /// <param name="childCol">Child column index referenced by the parent entry.</param>
    /// <param name="childRow">Child row index referenced by the parent entry.</param>
    /// <returns>The child table id.</returns>
    Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, TableSpec childSpec, uint childCol = 0, uint childRow = 0);

    /// <summary>
    /// Ensures a child table exists for <paramref name="childKey"/> and links it into a parent ref-vector,
    /// using a fully specified child <see cref="TableSpec"/>.
    /// </summary>
    /// <param name="parentTableId">Parent table id.</param>
    /// <param name="refsCol">Parent ref-vector column index.</param>
    /// <param name="parentRow">Parent row index.</param>
    /// <param name="childKey">Key used to locate or derive the child table (implementation-defined).</param>
    /// <param name="childSpec">Child table specification used when creation/materialization is required.</param>
    /// <param name="childCol">Child column index referenced by the parent entry.</param>
    /// <param name="childRow">Child row index referenced by the parent entry.</param>
    /// <returns>The child table id.</returns>
    Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, uint childKey, TableSpec childSpec, uint childCol = 0, uint childRow = 0);

    /// <summary>
    /// Fills a column by writing VALUE payloads provided as UTF-8 strings (and/or raw bytes) into a contiguous range of rows.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Target column index.</param>
    /// <param name="startRow">First row to write.</param>
    /// <param name="values">Payloads to write (each entry represents one row; <see langword="null"/> clears the value).</param>
    /// <param name="policy">Capacity policy applied when growth is needed.</param>
    /// <param name="cellPayloadMode">
    /// Describes which parts of <see cref="CellResultDTO"/> should be considered for writes.
    /// For example, <see cref="CellPayloadModeDTO.Utf8Only"/> uses <see cref="CellResultDTO.Utf8"/>,
    /// <see cref="CellPayloadModeDTO.RawOnly"/> uses <see cref="CellResultDTO.Raw"/>, and <see cref="CellPayloadModeDTO.Both"/>
    /// prefers raw bytes when present and otherwise falls back to UTF-8 encoding (implementation-defined).
    /// </param>
    /// <remarks>
    /// This replaces delegate-based fill APIs for RPC usage. Implementations must apply the underlying column size rules:
    /// truncate oversize inputs, and zero-fill remaining capacity as required by the storage layout.
    /// </remarks>
    void FillColumn(Guid tableId, uint column, uint startRow, CellResultDTO?[] values, CapacityPolicy policy = CapacityPolicy.None, CellPayloadModeDTO cellPayloadMode = CellPayloadModeDTO.Both);

    /// <summary>
    /// Fills a column by writing raw VALUE payloads directly into a contiguous range of rows.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Target column index.</param>
    /// <param name="startRow">First row to write.</param>
    /// <param name="valuesRaw">Raw VALUE payloads to write (each entry represents one row; <see langword="null"/> clears the value).</param>
    /// <param name="policy">Capacity policy applied when growth is needed.</param>
    /// <remarks>
    /// This is the most general RPC-safe fill primitive: the caller supplies the VALUE bytes already encoded.
    /// Implementations must apply the underlying column size rules: truncate oversize inputs and zero-fill remaining capacity.
    /// </remarks>
    void FillColumnRaw(Guid tableId, uint column, uint startRow, byte[]?[] valuesRaw, CapacityPolicy policy = CapacityPolicy.None);

    /// <summary>
    /// Returns a point-in-time snapshot of the indexes currently registered for the specified table.
    /// </summary>
    /// <param name="tableId">The identifier of the target table.</param>
    /// <returns>
    /// An array of <see cref="IndexInfoDTO"/> describing each registered index. The returned snapshot is detached
    /// from future mutations (subsequent adds/removes won’t affect this array).
    /// </returns>
    IndexInfoDTO[] GetIndexes(Guid tableId);

    /// <summary>
    /// Returns a point-in-time snapshot of the indexes currently registered for the specified manager.
    /// </summary>
    /// <returns>
    /// An array of <see cref="IndexInfoDTO"/> describing each registered index. The returned snapshot is detached
    /// from future mutations (subsequent adds/removes won’t affect this array).
    /// </returns>
    IndexInfoDTO[] GetIndexes();

    /// <summary>
    /// Adds (or replaces) a per-table index in the specified table's index registry.
    /// </summary>
    /// <param name="tableId">The identifier of the target table.</param>
    /// <param name="request">
    /// Index creation request (name, kind, and kind-specific payload and/or program bytes).
    /// </param>
    /// <returns>
    /// A <see cref="IndexMutationResultDTO"/> describing the mutation outcome:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexMutationStatusDTO.Ok"/> when the index was added or replaced.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.TableNotOpen"/> when the table cannot be resolved/opened.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.InvalidName"/> when the requested name is invalid.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.InvalidKind"/> when the requested kind is invalid/unknown.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.BuiltInProtected"/> when the request targets a built-in/protected kind or collides with a built-in index.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.AlreadyExists"/> when an index with the same name exists and <see cref="AddIndexRequestDTO.ReplaceIfExists"/> is <see langword="false"/>.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.NotSupported"/> when the backend does not support the requested custom kind/payload.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.Error"/> for unexpected failures while swapping/adding.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method mutates the per-table registry (<c>IMetadataTable.Indexes</c>). Built-in indexes are protected
    /// and cannot be added/replaced/removed through this API.
    /// </para>
    /// <para>
    /// Replacement is best-effort: the implementation should avoid removing an existing index until a new instance
    /// has been successfully created. If a swap partially fails, it may attempt a rollback to the previous instance.
    /// </para>
    /// </remarks>
    IndexMutationResultDTO AddIndex(Guid tableId, AddIndexRequestDTO request);

    /// <summary>
    /// Adds (or replaces) a manager-level (cross-table) index in the manager index registry.
    /// </summary>
    /// <param name="request">
    /// Index creation request (name, kind, and kind-specific payload and/or program bytes).
    /// </param>
    /// <returns>
    /// A <see cref="IndexMutationResultDTO"/> describing the mutation outcome:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexMutationStatusDTO.Ok"/> when the index was added or replaced.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.InvalidName"/> when the requested name is invalid.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.InvalidKind"/> when the requested kind is invalid/unknown.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.BuiltInProtected"/> when the request targets a built-in/protected kind or collides with a built-in index.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.AlreadyExists"/> when an index with the same name exists and <see cref="AddIndexRequestDTO.ReplaceIfExists"/> is <see langword="false"/>.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.NotSupported"/> when the backend does not support the requested custom kind/payload.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.Error"/> for unexpected failures while swapping/adding.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method mutates the manager-level registry (<c>_innerManager.Indexes</c>) which stores cross-table indexes
    /// (e.g., a global multi-table key index).
    /// </para>
    /// <para>
    /// Replacement is best-effort: the implementation should avoid removing an existing index until a new instance
    /// has been successfully created. If a swap partially fails, it may attempt a rollback to the previous instance.
    /// </para>
    /// </remarks>
    IndexMutationResultDTO AddIndex(AddIndexRequestDTO request);

    /// <summary>
    /// Removes an index from the table registry by its logical name.
    /// </summary>
    /// <param name="tableId">The identifier of the target table.</param>
    /// <param name="name">The logical name of the index to remove.</param>
    /// <returns>
    /// A <see cref="IndexMutationResultDTO"/> describing the mutation outcome:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexMutationStatusDTO.Ok"/> when the index was removed.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.InvalidName"/> when <paramref name="name"/> is null/empty/whitespace.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.NotFound"/> when no index exists with the specified name.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.BuiltInProtected"/> when the target index is built-in/protected.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.Error"/> for unexpected failures.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// Built-in indexes are protected and cannot be removed through this API.
    /// </remarks>
    IndexMutationResultDTO RemoveIndex(Guid tableId, string name);

    /// <summary>
    /// Removes an index from the manager-level (cross-table) index registry by its logical name.
    /// </summary>
    /// <param name="name">The logical name of the index to remove.</param>
    /// <returns>
    /// A <see cref="IndexMutationResultDTO"/> describing the mutation outcome:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexMutationStatusDTO.Ok"/> when the index was removed.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.InvalidName"/> when <paramref name="name"/> is null/empty/whitespace.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.NotFound"/> when no index exists with the specified name.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.BuiltInProtected"/> when the target index is built-in/protected.</description></item>
    ///   <item><description><see cref="IndexMutationStatusDTO.Error"/> for unexpected failures.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// This method affects the manager-level index registry (cross-table indexes). Built-in indexes are protected and
    /// cannot be removed through this API.
    /// </remarks>
    IndexMutationResultDTO RemoveIndex(string name);


    /// <summary>
    /// Finds the first matching row using an index that targets a specific column and a UTF-8 key provided as text.
    /// </summary>
    /// <param name="tableId">The identifier of the target table.</param>
    /// <param name="column">The target column identifier used by the index lookup.</param>
    /// <param name="keyUtf8">
    /// The lookup key encoded as text; the receiver treats it as UTF-8 input for matching.
    /// </param>
    /// <returns>
    /// An <see cref="IndexLookupResultDTO"/> describing the lookup outcome (found/not found/error) and any row reference.
    /// </returns>
    IndexLookupResultDTO FindRowByKey(Guid tableId, uint column, string keyUtf8);

    /// <summary>
    /// Finds the first matching row using an index that targets a specific column and a UTF-8 key provided as raw bytes.
    /// </summary>
    /// <param name="tableId">The identifier of the target table.</param>
    /// <param name="column">The target column identifier used by the index lookup.</param>
    /// <param name="keyUtf8">The lookup key as a UTF-8 byte sequence.</param>
    /// <returns>
    /// An <see cref="IndexLookupResultDTO"/> describing the lookup outcome (found/not found/error) and any row reference.
    /// </returns>
    IndexLookupResultDTO FindRowByKey(Guid tableId, uint column, byte[] keyUtf8);

    /// <summary>
    /// Attempts to locate a (column,row) hit for a given global key within a specific table using the built-in
    /// global key index.
    /// </summary>
    /// <param name="tableId">The identifier of the target table.</param>
    /// <param name="keyUtf8">
    /// The lookup key as managed text. It is encoded as UTF-8 before performing the lookup.
    /// </param>
    /// <returns>
    /// An <see cref="IndexLookupResultDTO"/> describing the lookup outcome and, when found, the hit information.
    /// Typical results include:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexLookupStatusDTO.Ok"/> with a populated hit when found.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.NotFound"/> when the key is not present in the index.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.InvalidKey"/> when <paramref name="keyUtf8"/> is null/empty.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.TableNotOpen"/> when the table cannot be resolved/opened.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// This overload is a convenience wrapper that converts <paramref name="keyUtf8"/> to UTF-8 bytes and forwards
    /// the lookup to <see cref="FindGlobal(System.Guid,byte[])"/>.
    /// </remarks>
    IndexLookupResultDTO FindGlobal(Guid tableId, string keyUtf8);

    /// <summary>
    /// Attempts to locate a (column,row) hit for a given global UTF-8 key within a specific table using the built-in
    /// global key index.
    /// </summary>
    /// <param name="tableId">The identifier of the target table.</param>
    /// <param name="keyUtf8">The lookup key as a UTF-8 byte sequence.</param>
    /// <returns>
    /// An <see cref="IndexLookupResultDTO"/> describing the lookup outcome and, when found, the hit information.
    /// Typical results include:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexLookupStatusDTO.Ok"/> with a populated hit when found.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.NotFound"/> when the key is not present in the index.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.TableNotOpen"/> when the table cannot be resolved/opened.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// The global index typically maps a unique key to the corresponding (column,row) location within the table.
    /// </remarks>
    IndexLookupResultDTO FindGlobal(Guid tableId, byte[] keyUtf8);

    /// <summary>
    /// Attempts to locate a (column,row) hit for a given global key across all tables using the built-in
    /// manager-level global multi-table key index.
    /// </summary>
    /// <param name="keyUtf8">
    /// The lookup key as managed text. It is encoded as UTF-8 before performing the lookup.
    /// </param>
    /// <returns>
    /// An <see cref="IndexLookupResultDTO"/> describing the lookup outcome and, when found, the hit information.
    /// Typical results include:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexLookupStatusDTO.Ok"/> with a populated hit when found.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.NotFound"/> when the key is not present in the index.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.InvalidKey"/> when <paramref name="keyUtf8"/> is null/empty.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// This overload is a convenience wrapper that converts <paramref name="keyUtf8"/> to UTF-8 bytes and forwards
    /// the lookup to <see cref="FindGlobal(byte[])"/>.
    /// </remarks>
    IndexLookupResultDTO FindGlobal(string keyUtf8);

    /// <summary>
    /// Attempts to locate a (column,row) hit for a given global UTF-8 key across all tables using the built-in
    /// manager-level global multi-table key index.
    /// </summary>
    /// <param name="keyUtf8">The lookup key as a UTF-8 byte sequence.</param>
    /// <returns>
    /// An <see cref="IndexLookupResultDTO"/> describing the lookup outcome and, when found, the hit information.
    /// Typical results include:
    /// <list type="bullet">
    ///   <item><description><see cref="IndexLookupStatusDTO.Ok"/> with a populated hit when found.</description></item>
    ///   <item><description><see cref="IndexLookupStatusDTO.NotFound"/> when the key is not present in the index.</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// The global multi-table index is typically built/rebuilt out-of-band. If it is missing, empty, or stale, this
    /// method may return <see cref="IndexLookupStatusDTO.NotFound"/> even if the key exists in underlying storage.
    /// </remarks>
    IndexLookupResultDTO FindGlobal(byte[] keyUtf8);
}
