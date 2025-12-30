namespace Extend0.Metadata.Indexing.Contract;

/// <summary>
/// Contract for indexes that support rebuilding themselves by scanning the current contents of a metadata table.
/// </summary>
/// <remarks>
/// Rebuilding is typically used to restore consistency or rehydrate volatile structures
/// from durable table state. The index is expected to clear its internal state before repopulating.
/// </remarks>
public interface IRebuildableIndex : ITableIndex
{
    /// <summary>
    /// Rebuilds the index using the provided <paramref name="table"/> as the source of truth.
    /// </summary>
    /// <param name="table">The metadata table to scan during reconstruction.</param>
    void Rebuild(IMetadataTable table);
}

/// <summary>
/// Generic rebuildable index interface that supports key-based operations.
/// </summary>
/// <typeparam name="TKey">The type of key used in the index. Must be non-null.</typeparam>
/// <typeparam name="TValue">The type of value associated with each key.</typeparam>
public interface IRebuildableIndex<in TKey, TValue> : IRebuildableIndex where TKey : notnull
{
    /// <summary>
    /// Adds a new entry to the index.
    /// </summary>
    /// <param name="key">Key to add.</param>
    /// <param name="value">Value to associate with <paramref name="key"/>.</param>
    /// <returns>
    /// <see langword="true"/> if the entry was added; <see langword="false"/> if an entry with the same key already exists.
    /// </returns>
    bool Add(TKey key, TValue value);

    /// <summary>
    /// Removes an entry by key.
    /// </summary>
    /// <param name="key">Key to remove.</param>
    /// <returns>
    /// <see langword="true"/> if the key existed and was removed; otherwise <see langword="false"/>.
    /// </returns>
    bool Remove(TKey key);

    /// <summary>
    /// Tries to get the value associated with <paramref name="key"/>.
    /// </summary>
    /// <param name="key">Key to look up.</param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, contains the stored value; otherwise default.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the key exists; otherwise <see langword="false"/>.
    /// </returns>
    bool TryGetValue(TKey key, out TValue value);
}
