namespace Extend0.Metadata.Indexing.Contract;

/// <summary>
/// Non-generic contract for an index that stores entries partitioned by table id.
/// </summary>
/// <remarks>
/// Cross-table indexes are typically owned by a manager (e.g. MetaDBManager) and can be purged per table,
/// such as when a table is closed or unregistered.
/// </remarks>
public interface ICrossTableIndex : ITableIndex
{
    /// <summary>
    /// Removes all entries that belong to a specific table partition.
    /// </summary>
    /// <param name="tableId">The table id whose entries must be removed.</param>
    void ClearTable(Guid tableId);
}

/// <summary>
/// Generic contract for a cross-table index keyed by <typeparamref name="TKey"/> and storing <typeparamref name="TValue"/>,
/// partitioned by table id.
/// </summary>
/// <typeparam name="TKey">Index key type.</typeparam>
/// <typeparam name="TValue">Index value type.</typeparam>
public interface ICrossTableIndex<TKey, TValue> : ICrossTableIndex where TKey : notnull
{
    /// <summary>
    /// Adds a new entry to the index for a specific table.
    /// </summary>
    /// <param name="tableId">Owning table id for the entry.</param>
    /// <param name="key">Key to add.</param>
    /// <param name="value">Value to associate with <paramref name="key"/>.</param>
    /// <returns>
    /// <see langword="true"/> if the entry was added; <see langword="false"/> if the key already exists
    /// within that table partition.
    /// </returns>
    bool Add(Guid tableId, TKey key, TValue value);

    /// <summary>
    /// Removes an entry from a specific table partition.
    /// </summary>
    /// <param name="tableId">Owning table id.</param>
    /// <param name="key">Key to remove.</param>
    /// <returns>
    /// <see langword="true"/> if the key existed in that table partition and was removed; otherwise <see langword="false"/>.
    /// </returns>
    bool Remove(Guid tableId, TKey key);

    /// <summary>
    /// Tries to get the value associated with <paramref name="key"/> from a specific table partition.
    /// </summary>
    /// <param name="tableId">Owning table id.</param>
    /// <param name="key">Key to look up.</param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, contains the stored value; otherwise default.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the key exists in that table partition; otherwise <see langword="false"/>.
    /// </returns>
    bool TryGetValue(Guid tableId, TKey key, out TValue value);
}
