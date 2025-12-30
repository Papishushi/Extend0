using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Registries.Contract;

/// <summary>
/// Per-table index registry.
/// </summary>
/// <remarks>
/// <para>
/// This registry owns indexes scoped to a single <see cref="IMetadataTable"/> (e.g., per-column key indexes).
/// </para>
/// <para>
/// Implementations may support rebuilding all indexes from table storage and may enforce name uniqueness.
/// </para>
/// </remarks>
public interface ITableIndexesRegistry : IIndexesRegistryBase
{
    /// <summary>
    /// Adds an already-constructed table index instance to the registry.
    /// </summary>
    /// <typeparam name="TIndex">Concrete table index type.</typeparam>
    /// <param name="index">The index instance to add.</param>
    /// <returns>The same <paramref name="index"/> instance (for fluent usage).</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="index"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when an index with the same name already exists and the implementation does not allow replacement.
    /// </exception>
    TIndex Add<TIndex>(TIndex index) where TIndex : class, ITableIndex;

    /// <summary>
    /// Adds a table index created lazily by the provided constructor.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <param name="indexConstructor">
    /// Factory delegate used to construct the index instance (typically only invoked when the index does not exist).
    /// </param>
    /// <returns>The added or existing index instance.</returns>
    /// <remarks>
    /// Intended for "get-or-add" patterns while preserving registry ownership of index lifetime.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="indexConstructor"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the constructed index is incompatible with <typeparamref name="TKey"/>/<typeparamref name="TValue"/>
    /// or violates registry rules (e.g., name collision with different index kind).
    /// </exception>
    ITableIndex Add<TKey, TValue>(Func<ITableIndex> indexConstructor) where TKey : notnull;

    /// <summary>
    /// Gets a strongly-typed table index by name.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <param name="name">Logical index name.</param>
    /// <returns>The resolved index.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when no index with <paramref name="name"/> exists.</exception>
    /// <exception cref="InvalidCastException">
    /// Thrown when the index exists but does not match the requested <typeparamref name="TKey"/>/<typeparamref name="TValue"/> signature.
    /// </exception>
    ITableIndex<TKey, TValue> Get<TKey, TValue>(string name) where TKey : notnull;

    /// <summary>
    /// Rebuilds all rebuildable indexes in the registry against the provided table.
    /// </summary>
    /// <param name="table">The table used as the source of truth for rebuild.</param>
    /// <remarks>
    /// Typical implementations call <c>Rebuild(table)</c> on indexes that support rebuilding (e.g., implement a rebuild contract),
    /// and may skip or no-op for non-rebuildable indexes.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is null.</exception>
    void Rebuild(IMetadataTable table);

    /// <summary>
    /// Attempts to get an index instance by name (untyped).
    /// </summary>
    /// <param name="name">Logical index name.</param>
    /// <param name="index">Receives the index when found; otherwise null.</param>
    /// <returns><see langword="true"/> if found; otherwise <see langword="false"/>.</returns>
    bool TryGet(string name, out ITableIndex? index);
}
