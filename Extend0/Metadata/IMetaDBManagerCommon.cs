using Extend0.Metadata.Schema;

namespace Extend0.Metadata;

/// <summary>
/// Common contract shared by local and RPC-compatible MetaDB managers.
/// </summary>
/// <remarks>
/// <para>
/// This interface contains only operations that make sense in both contexts:
/// registration/identity, lifecycle (close), maintenance, and relationship wiring.
/// </para>
/// <para>
/// It intentionally avoids anything that would leak process-dependent state
/// (tables, spans, pointers, mapped views) or require code execution across boundaries
/// (delegates/factories).
/// </para>
/// </remarks>
public interface IMetaDBManagerCommon : IDisposable, IAsyncDisposable
{
    IEnumerable<Guid> TableIds { get; }

    /// <summary>
    /// Registers a table by name and map path with the provided column configuration.
    /// </summary>
    /// <param name="name">Logical table name.</param>
    /// <param name="mapPath">Backing map file path.</param>
    /// <param name="columns">Column schema definitions.</param>
    /// <returns>The registered table id.</returns>
    Guid RegisterTable(string name, string mapPath, params ColumnConfiguration[] columns);

    /// <summary>
    /// Registers a table using a <see cref="TableSpec"/>.
    /// </summary>
    /// <param name="spec">Table specification.</param>
    /// <param name="createNow">When <see langword="true"/>, forces immediate creation/materialization.</param>
    /// <returns>The registered table id.</returns>
    Guid RegisterTable(TableSpec spec, bool createNow = false);

    /// <summary>
    /// Closes a managed table identified by <paramref name="tableId"/> using strict semantics.
    /// </summary>
    /// <param name="tableId">Identifier of the table to close.</param>
    /// <returns><see langword="true"/> if a table was found and closed; otherwise <see langword="false"/>.</returns>
    bool CloseStrict(Guid tableId);

    /// <summary>
    /// Closes a managed table identified by its registered <paramref name="name"/> using strict semantics.
    /// </summary>
    /// <param name="name">Registered table name.</param>
    /// <returns><see langword="true"/> if a table was found and closed; otherwise <see langword="false"/>.</returns>
    bool CloseStrict(string name);

    /// <summary>
    /// Closes all managed tables using best-effort semantics.
    /// </summary>
    void CloseAll();

    /// <summary>
    /// Closes all managed tables using strict semantics.
    /// </summary>
    void CloseAllStrict();

    /// <summary>
    /// Rebuilds indexes for the table identified by <paramref name="tableId"/>.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="includeGlobal">Whether global indexes should be included.</param>
    void RebuildIndexes(Guid tableId, bool includeGlobal = true);

    /// <summary>
    /// Rebuilds per-column and global key indexes for all materialized tables managed by this manager.
    /// </summary>
    /// <param name="includeGlobal">
    /// <see langword="true"/> to rebuild global key indexes as well as per-column indexes;
    /// <see langword="false"/> to rebuild only per-column indexes.
    /// </param>
    void RebuildAllIndexes(bool includeGlobal = true);

    /// <summary>
    /// Restarts the background delete worker responsible for processing deletions, optionally using a queue path.
    /// </summary>
    /// <param name="deleteQueuePath">Optional path to a delete queue used by the worker.</param>
    /// <returns>A task representing the asynchronous restart operation.</returns>
    Task RestartDeleteWorker(string? deleteQueuePath = null);

    /// <summary>
    /// Copies <paramref name="rows"/> rows from a source column into a destination column.
    /// </summary>
    void CopyColumn(Guid srcTableId, uint srcCol, Guid dstTableId, uint dstCol, uint rows, CapacityPolicy dstPolicy = CapacityPolicy.None);

    /// <summary>
    /// Ensures the ref-vector for a parent row exists and can accommodate reference entries.
    /// </summary>
    void EnsureRefVec(Guid parentTableId, uint refsCol, uint parentRow, CapacityPolicy policy = CapacityPolicy.None);

    /// <summary>
    /// Adds a reference from a parent row to a child table location.
    /// </summary>
    void LinkRef(Guid parentTableId, uint refsCol, uint parentRow, Guid childTableId, uint childCol = 0, uint childRow = 0, CapacityPolicy policy = CapacityPolicy.None);
}
