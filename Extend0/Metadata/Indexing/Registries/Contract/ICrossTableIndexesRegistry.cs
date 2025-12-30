using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Registries.Contract;

/// <summary>
/// Cross-table (manager-level) index registry.
/// </summary>
/// <remarks>
/// <para>
/// This registry owns indexes that can span multiple tables (e.g., a global multi-table key index).
/// It provides typed retrieval helpers and lifecycle operations for clearing/removing indexes safely.
/// </para>
/// <para>
/// Implementations are expected to enforce unique names, dispose owned indexes on removal/clear,
/// and keep lookup operations thread-safe if used concurrently.
/// </para>
/// </remarks>
public interface ICrossTableIndexesRegistry : IIndexesRegistryBase
{
    /// <summary>
    /// Adds an already-constructed cross-table index instance to the registry.
    /// </summary>
    /// <typeparam name="TIndex">Concrete cross-table index type.</typeparam>
    /// <param name="index">The index instance to add.</param>
    /// <returns>The same <paramref name="index"/> instance (for fluent usage).</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="index"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when an index with the same name already exists and the implementation does not allow replacement.
    /// </exception>
    TIndex Add<TIndex>(TIndex index) where TIndex : class, ICrossTableIndex;

    /// <summary>
    /// Adds a cross-table index created lazily by the provided constructor.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <param name="indexConstructor">
    /// Factory delegate used to construct the index instance (typically only invoked when the index does not exist).
    /// </param>
    /// <returns>The added or existing index instance.</returns>
    /// <remarks>
    /// Intended for "get-or-add" style usage while keeping callers from directly depending on concrete types.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="indexConstructor"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the constructed index is incompatible with <typeparamref name="TKey"/>/<typeparamref name="TValue"/>
    /// or violates registry rules (e.g., name collision with different index kind).
    /// </exception>
    ICrossTableIndex Add<TKey, TValue>(Func<ICrossTableIndex> indexConstructor) where TKey : notnull;

    /// <summary>
    /// Clears any cross-table index state that is associated with a specific table.
    /// </summary>
    /// <param name="tableId">The identifier of the table whose contributions should be removed.</param>
    /// <remarks>
    /// Useful when a table is closed/deleted and the registry must purge references to it to avoid stale hits.
    /// Implementations may perform best-effort cleanup depending on index capabilities.
    /// </remarks>
    void ClearForTable(Guid tableId);

    /// <summary>
    /// Gets a strongly-typed cross-table index by name.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <param name="name">Logical index name.</param>
    /// <returns>The resolved index.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when no index with <paramref name="name"/> exists.</exception>
    /// <exception cref="InvalidCastException">
    /// Thrown when the index exists but does not match the requested <typeparamref name="TKey"/>/<typeparamref name="TValue"/> signature.
    /// </exception>
    ICrossTableIndex<TKey, TValue> Get<TKey, TValue>(string name) where TKey : notnull;

    /// <summary>
    /// Attempts to get an index instance by name (untyped).
    /// </summary>
    /// <param name="name">Logical index name.</param>
    /// <param name="index">Receives the index when found; otherwise null.</param>
    /// <returns><see langword="true"/> if found; otherwise <see langword="false"/>.</returns>
    bool TryGet(string name, out ICrossTableIndex? index);

    /// <summary>
    /// Attempts to get a strongly-typed cross-table index by name.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <param name="name">Logical index name.</param>
    /// <param name="index">Receives the typed index when found and compatible; otherwise null.</param>
    /// <returns>
    /// <see langword="true"/> if an index with <paramref name="name"/> exists and matches the requested generic signature;
    /// otherwise <see langword="false"/>.
    /// </returns>
    bool TryGet<TKey, TValue>(string name, out ICrossTableIndex<TKey, TValue>? index) where TKey : notnull;
}