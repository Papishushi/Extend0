using Extend0.Metadata.Indexing.Registries.Contract;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using System.Diagnostics.CodeAnalysis;

namespace Extend0.Metadata.Contract;

/// <summary>
/// High-level manager for registering, opening, resolving, linking, and maintaining MetaDB metadata tables.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IMetaDBManager"/> owns the lifecycle of managed <see cref="IMetadataTable"/> instances that are
/// registered by name/spec, opened from disk, or materialized on demand.
/// </para>
/// <para>
/// Core responsibilities typically include:
/// </para>
/// <list type="bullet">
///   <item><description><b>Registration</b>: define tables and their schema (<see cref="RegisterTable(string, string, ColumnConfiguration[])"/>, <see cref="RegisterTable(TableSpec, bool)"/>).</description></item>
///   <item><description><b>Resolution</b>: obtain an active table instance by id or name (<see cref="GetOrCreate(Guid)"/>, <see cref="TryGetManaged(Guid, out IMetadataTable?)"/>, <see cref="TryGetTableIfCreated(string, out IMetadataTable?)"/>).</description></item>
///   <item><description><b>Open/Close</b>: open tables from a backing map path and close managed instances (<see cref="Open(string)"/>, <see cref="CloseStrict(Guid)"/>, <see cref="CloseAll"/>).</description></item>
///   <item><description><b>Data population</b>: fill or copy columns efficiently (<see cref="FillColumn"/>, <see cref="FillColumn{T}"/>, <see cref="CopyColumn"/>).</description></item>
///   <item><description><b>Relationships</b>: maintain parent/child references and ref-vectors (<see cref="LinkRef"/>, <see cref="EnsureRefVec"/>, <see cref="GetOrCreateAndLinkChild"/>).</description></item>
///   <item><description><b>Maintenance</b>: rebuild indexes and run operations under a named scope (<see cref="RebuildIndexes"/>, <see cref="Run"/>, <see cref="RunWithReindexAll"/>).</description></item>
///   <item><description><b>Ephemeral scopes</b>: open-use-close tables for one-off operations, optionally deleting backing files (<see cref="WithTableEphemeral"/>).</description></item>
/// </list>
/// <para>
/// Implementations are expected to be thread-safe where appropriate, and to honor best-effort vs strict semantics
/// as exposed by the API (e.g., <see cref="CloseAll"/> vs <see cref="CloseAllStrict"/>).
/// </para>
/// </remarks>
public interface IMetaDBManager : IMetaDBManagerCommon
{
    ICrossTableIndexesRegistry Indexes { get; }

    /// <summary>
    /// Fills a column by invoking a writer for each row, providing a pointer and size for the target cell storage.
    /// </summary>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Target column index.</param>
    /// <param name="rows">Number of rows to fill.</param>
    /// <param name="writer">
    /// Callback invoked per row: (rowIndex, destinationPointer, destinationSizeBytes).
    /// </param>
    /// <param name="policy">Capacity policy applied when growth is needed.</param>
    void FillColumn(Guid tableId, uint column, uint rows, Action<uint, nint, uint> writer, CapacityPolicy policy = CapacityPolicy.None);

    /// <summary>
    /// Fills a column by creating values using <paramref name="factory"/> and writing them into unmanaged storage.
    /// </summary>
    /// <typeparam name="T">Unmanaged value type written to the column.</typeparam>
    /// <param name="tableId">Target table id.</param>
    /// <param name="column">Target column index.</param>
    /// <param name="rows">Number of rows to fill.</param>
    /// <param name="factory">Value factory invoked per row.</param>
    /// <param name="policy">Capacity policy applied when growth is needed.</param>
    void FillColumn<T>(Guid tableId, uint column, uint rows, Func<uint, T> factory, CapacityPolicy policy = CapacityPolicy.None) where T : unmanaged;

    /// <summary>
    /// Resolves and returns the managed <see cref="IMetadataTable"/> for <paramref name="tableId"/>,
    /// creating/materializing it if needed.
    /// </summary>
    /// <param name="tableId">Identifier of the table to resolve.</param>
    /// <returns>The resolved <see cref="IMetadataTable"/> instance.</returns>
    IMetadataTable GetOrCreate(Guid tableId);

    /// <summary>
    /// Attempts to resolve a table id from a registered <paramref name="name"/>.
    /// </summary>
    /// <param name="name">Registered table name.</param>
    /// <param name="id">When this method returns, contains the resolved id if found.</param>
    /// <returns><see langword="true"/> if the name was found; otherwise <see langword="false"/>.</returns>
    bool TryGetIdByName(string name, out Guid id);

    /// <summary>
    /// Ensures a child table exists and links it into a parent ref-vector, creating the child from a spec factory if needed.
    /// </summary>
    /// <param name="parentTableId">Parent table id.</param>
    /// <param name="refsCol">Parent ref-vector column index.</param>
    /// <param name="parentRow">Parent row index.</param>
    /// <param name="childSpecFactory">Factory to create the child <see cref="TableSpec"/> when missing.</param>
    /// <param name="childCol">Child column index referenced by the parent entry.</param>
    /// <param name="childRow">Child row index referenced by the parent entry.</param>
    /// <returns>The child table id.</returns>
    Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, Func<uint, TableSpec> childSpecFactory, uint childCol = 0, uint childRow = 0);

    /// <summary>
    /// Ensures a child table exists for <paramref name="childKey"/> and links it into a parent ref-vector,
    /// creating the child from a spec factory if needed.
    /// </summary>
    /// <param name="parentTableId">Parent table id.</param>
    /// <param name="refsCol">Parent ref-vector column index.</param>
    /// <param name="parentRow">Parent row index.</param>
    /// <param name="childKey">Key used to locate or derive the child table.</param>
    /// <param name="childSpecFactory">Factory to create the child <see cref="TableSpec"/> when missing.</param>
    /// <param name="childCol">Child column index referenced by the parent entry.</param>
    /// <param name="childRow">Child row index referenced by the parent entry.</param>
    /// <returns>The child table id.</returns>
    Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, uint childKey, Func<uint, TableSpec> childSpecFactory, uint childCol = 0, uint childRow = 0);

    /// <summary>
    /// Opens a table from <paramref name="mapPath"/> and returns the resulting <see cref="IMetadataTable"/>.
    /// </summary>
    /// <param name="mapPath">Path to the backing map file.</param>
    /// <returns>The opened <see cref="IMetadataTable"/>.</returns>
    IMetadataTable Open(string mapPath);

    /// <summary>
    /// Opens a table from <paramref name="mapPath"/> and returns its id and instance.
    /// </summary>
    /// <param name="mapPath">Path to the backing map file.</param>
    /// <param name="forceRelocation">Whether to force relocation behavior during open (implementation-defined).</param>
    /// <returns>A tuple containing the table id and the opened table.</returns>
    (Guid Id, IMetadataTable Table) Open(string mapPath, bool forceRelocation = false);

    /// <summary>
    /// Runs an operation under a named scope using a manager instance.
    /// </summary>
    /// <param name="operationName">Operation name used for diagnostics and/or logging scopes.</param>
    /// <param name="action">Callback executed with the manager.</param>
    /// <param name="state">Optional state object included in diagnostics.</param>
    void Run(string operationName, Action<IMetaDBManager> action, object? state = null);

    /// <summary>
    /// Runs an asynchronous operation under a named scope using a manager instance.
    /// </summary>
    /// <param name="operationName">Operation name used for diagnostics and/or logging scopes.</param>
    /// <param name="action">Async callback executed with the manager.</param>
    /// <param name="state">Optional state object included in diagnostics.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RunAsync(string operationName, Func<IMetaDBManager, Task> action, object? state = null);

    /// <summary>
    /// Runs an operation under a named scope and rebuilds indexes for all known tables afterwards.
    /// </summary>
    /// <param name="operationName">Operation name used for diagnostics and/or logging scopes.</param>
    /// <param name="action">Callback executed with the manager.</param>
    /// <param name="state">Optional state object included in diagnostics.</param>
    /// <param name="includeGlobal">Whether global indexes should be included.</param>
    void RunWithReindexAll(string operationName, Action<IMetaDBManager> action, object? state = null, bool includeGlobal = true);

    /// <summary>
    /// Runs an asynchronous operation under a named scope and rebuilds indexes for all known tables afterwards.
    /// </summary>
    /// <param name="operationName">Operation name used for diagnostics and/or logging scopes.</param>
    /// <param name="action">Async callback executed with the manager.</param>
    /// <param name="state">Optional state object included in diagnostics.</param>
    /// <param name="includeGlobal">Whether global indexes should be included.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RunWithReindexAllAsync(string operationName, Func<IMetaDBManager, Task> action, object? state = null, bool includeGlobal = true);

    /// <summary>
    /// Runs an operation under a named scope and rebuilds indexes for a specific table afterwards.
    /// </summary>
    /// <param name="operationName">Operation name used for diagnostics and/or logging scopes.</param>
    /// <param name="tableId">Target table id.</param>
    /// <param name="action">Callback executed with the manager.</param>
    /// <param name="state">Optional state object included in diagnostics.</param>
    /// <param name="includeGlobal">Whether global indexes should be included.</param>
    void RunWithReindexTable(string operationName, Guid tableId, Action<IMetaDBManager> action, object? state, bool includeGlobal = true);

    /// <summary>
    /// Runs an asynchronous operation under a named scope and rebuilds indexes for a specific table afterwards.
    /// </summary>
    /// <param name="operationName">Operation name used for diagnostics and/or logging scopes.</param>
    /// <param name="tableId">Target table id.</param>
    /// <param name="action">Async callback executed with the manager.</param>
    /// <param name="state">Optional state object included in diagnostics.</param>
    /// <param name="includeGlobal">Whether global indexes should be included.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RunWithReindexTableAsync(string operationName, Guid tableId, Func<IMetaDBManager, Task> action, object? state, bool includeGlobal = true);

    /// <summary>
    /// Attempts to get a currently managed table instance by <paramref name="id"/>.
    /// </summary>
    /// <param name="id">Table id.</param>
    /// <param name="table">The managed table instance when found.</param>
    /// <returns><see langword="true"/> if the table is managed; otherwise <see langword="false"/>.</returns>
    bool TryGetManaged(Guid id, [NotNullWhen(true)] out IMetadataTable? table);

    /// <summary>
    /// Attempts to get a managed table by name only if it has already been created/materialized.
    /// </summary>
    /// <param name="name">Registered table name.</param>
    /// <param name="table">The table instance when found and created.</param>
    /// <returns><see langword="true"/> if the table exists and is created; otherwise <see langword="false"/>.</returns>
    bool TryGetTableIfCreated(string name, [NotNullWhen(true)] out IMetadataTable? table);

    /// <summary>
    /// Resolves the table identified by <paramref name="tableId"/> and executes <paramref name="action"/>.
    /// </summary>
    /// <param name="tableId">Identifier of the table to resolve.</param>
    /// <param name="action">Callback executed with the resolved <see cref="IMetadataTable"/>.</param>
    void WithTable(Guid tableId, Action<IMetadataTable> action);

    /// <summary>
    /// Resolves the table identified by <paramref name="tableId"/> and executes <paramref name="func"/>,
    /// returning the callback result.
    /// </summary>
    /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
    /// <param name="tableId">Identifier of the table to resolve.</param>
    /// <param name="func">Callback executed with the resolved <see cref="IMetadataTable"/>.</param>
    /// <returns>The value produced by <paramref name="func"/>.</returns>
    TResult WithTable<TResult>(Guid tableId, Func<IMetadataTable, TResult> func);

    /// <summary>
    /// Asynchronously resolves the table identified by <paramref name="tableId"/> and executes <paramref name="func"/>.
    /// </summary>
    /// <param name="tableId">Identifier of the table to resolve.</param>
    /// <param name="func">Async callback executed with the resolved <see cref="IMetadataTable"/>.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WithTableAsync(Guid tableId, Func<IMetadataTable, Task> func);

    /// <summary>
    /// Asynchronously resolves the table identified by <paramref name="tableId"/> and executes <paramref name="func"/>,
    /// returning the callback result.
    /// </summary>
    /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
    /// <param name="tableId">Identifier of the table to resolve.</param>
    /// <param name="func">Async callback executed with the resolved <see cref="IMetadataTable"/>.</param>
    /// <returns>A task producing the value returned by <paramref name="func"/>.</returns>
    Task<TResult> WithTableAsync<TResult>(Guid tableId, Func<IMetadataTable, Task<TResult>> func);

    /// <summary>
    /// Opens a table from <paramref name="mapPath"/>, executes <paramref name="action"/>, and always closes it afterwards.
    /// </summary>
    /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
    /// <param name="action">Callback executed with the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="forceRelocation">Whether to force relocation behavior during open (implementation-defined).</param>
    void WithTableEphemeral(string mapPath, Action<IMetadataTable> action, bool forceRelocation = false);

    /// <summary>
    /// Registers and opens an ephemeral table from <paramref name="spec"/>, executes <paramref name="action"/>,
    /// and then finalizes the ephemeral resources (including optional deletion of backing files).
    /// </summary>
    /// <param name="spec">Table specification used to register/open the ephemeral table.</param>
    /// <param name="action">Callback executed with the ephemeral table id and the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="createNow">When <see langword="true"/>, forces immediate creation/materialization.</param>
    /// <param name="deleteNow">When <see langword="true"/>, attempts to delete the backing files during finalization.</param>
    /// <param name="throwIfDeleteFails">
    /// When <see langword="true"/>, deletion failures are propagated; otherwise they are treated as best-effort.
    /// </param>
    void WithTableEphemeral(TableSpec spec, Action<Guid, IMetadataTable> action, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false);

    /// <summary>
    /// Opens a table from <paramref name="mapPath"/>, executes <paramref name="func"/>, and always closes it afterwards,
    /// returning the callback result.
    /// </summary>
    /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
    /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
    /// <param name="func">Callback executed with the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="forceRelocation">Whether to force relocation behavior during open (implementation-defined).</param>
    /// <returns>The value produced by <paramref name="func"/>.</returns>
    TResult WithTableEphemeral<TResult>(string mapPath, Func<IMetadataTable, TResult> func, bool forceRelocation = false);

    /// <summary>
    /// Registers and opens an ephemeral table from <paramref name="spec"/>, executes <paramref name="func"/>,
    /// and then finalizes the ephemeral resources (including optional deletion of backing files),
    /// returning the callback result.
    /// </summary>
    /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
    /// <param name="spec">Table specification used to register/open the ephemeral table.</param>
    /// <param name="func">Callback executed with the ephemeral table id and the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="createNow">When <see langword="true"/>, forces immediate creation/materialization.</param>
    /// <param name="deleteNow">When <see langword="true"/>, attempts to delete the backing files during finalization.</param>
    /// <param name="throwIfDeleteFails">
    /// When <see langword="true"/>, deletion failures are propagated; otherwise they are treated as best-effort.
    /// </param>
    /// <returns>The value produced by <paramref name="func"/>.</returns>
    TResult WithTableEphemeral<TResult>(TableSpec spec, Func<Guid, IMetadataTable, TResult> func, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false);

    /// <summary>
    /// Asynchronously opens a table from <paramref name="mapPath"/>, awaits <paramref name="func"/>, and always closes it afterwards.
    /// </summary>
    /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
    /// <param name="func">Async callback executed with the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="forceRelocation">Whether to force relocation behavior during open (implementation-defined).</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WithTableEphemeralAsync(string mapPath, Func<IMetadataTable, Task> func, bool forceRelocation = false);

    /// <summary>
    /// Asynchronously registers and opens an ephemeral table from <paramref name="spec"/>, awaits <paramref name="func"/>,
    /// and then finalizes the ephemeral resources (including optional deletion of backing files).
    /// </summary>
    /// <param name="spec">Table specification used to register/open the ephemeral table.</param>
    /// <param name="func">Async callback executed with the ephemeral table id and the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="createNow">When <see langword="true"/>, forces immediate creation/materialization.</param>
    /// <param name="deleteNow">When <see langword="true"/>, attempts to delete the backing files during finalization.</param>
    /// <param name="throwIfDeleteFails">
    /// When <see langword="true"/>, deletion failures are propagated; otherwise they are treated as best-effort.
    /// </param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WithTableEphemeralAsync(TableSpec spec, Func<Guid, IMetadataTable, Task> func, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false);

    /// <summary>
    /// Asynchronously opens a table from <paramref name="mapPath"/>, awaits <paramref name="func"/>, and always closes it afterwards,
    /// returning the callback result.
    /// </summary>
    /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
    /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
    /// <param name="func">Async callback executed with the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="forceRelocation">Whether to force relocation behavior during open (implementation-defined).</param>
    /// <returns>A task producing the value returned by <paramref name="func"/>.</returns>
    Task<TResult> WithTableEphemeralAsync<TResult>(string mapPath, Func<IMetadataTable, Task<TResult>> func, bool forceRelocation = false);

    /// <summary>
    /// Asynchronously registers and opens an ephemeral table from <paramref name="spec"/>, awaits <paramref name="func"/>,
    /// and then finalizes the ephemeral resources (including optional deletion of backing files),
    /// returning the callback result.
    /// </summary>
    /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
    /// <param name="spec">Table specification used to register/open the ephemeral table.</param>
    /// <param name="func">Async callback executed with the ephemeral table id and the opened <see cref="IMetadataTable"/>.</param>
    /// <param name="createNow">When <see langword="true"/>, forces immediate creation/materialization.</param>
    /// <param name="deleteNow">When <see langword="true"/>, attempts to delete the backing files during finalization.</param>
    /// <param name="throwIfDeleteFails">
    /// When <see langword="true"/>, deletion failures are propagated; otherwise they are treated as best-effort.
    /// </param>
    /// <returns>A task producing the value returned by <paramref name="func"/>.</returns>
    Task<TResult> WithTableEphemeralAsync<TResult>(TableSpec spec, Func<Guid, IMetadataTable, Task<TResult>> func, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false);
}
