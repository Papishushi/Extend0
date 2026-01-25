using Extend0.Metadata.Contract;

namespace Extend0.Metadata.Indexing.Contract;

/// <summary>
/// Marker interface for cross-table indexes that also support rebuilding themselves by scanning a metadata table.
/// </summary>
/// <remarks>
/// Combines <see cref="ICrossTableIndex"/> partitioning with <see cref="IRebuildableIndex"/> self-reconstruction.
/// Useful for indexes that can recover or rehydrate their state from persisted table contents.
/// </remarks>
public interface ICrossTableRebuildableIndex : ICrossTableIndex
{
    /// <summary>
    /// Rebuilds the index using the provided <paramref name="manager"/> as the source of truth.
    /// </summary>
    /// <param name="manager">The metadata manager to scan during reconstruction.</param>
    Task Rebuild(IMetaDBManager manager);
}

/// <summary>
/// Generic contract for cross-table indexes keyed by <typeparamref name="TKey"/> and storing values of type <typeparamref name="TValue"/>,
/// supporting both partitioned access and index rebuilding.
/// </summary>
/// <typeparam name="TKey">The type used as key in the index. Must be non-null.</typeparam>
/// <typeparam name="TValue">The value type associated with each key.</typeparam>
public interface ICrossTableRebuildableIndex<in TKey, TValue> : ICrossTableRebuildableIndex where TKey : notnull
{
    /// <summary>
    /// Returns all table IDs that contain entries for the specified <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The key to query.</param>
    /// <returns>An array of table identifiers that include the key.</returns>
    Guid[] GetMemberTables(TKey key);

    /// <summary>
    /// Returns all table IDs that include the specified key and retrieves associated values per table.
    /// </summary>
    /// <param name="key">The key to locate.</param>
    /// <param name="value">Array of values stored under the key, aligned by table ID.</param>
    /// <returns>An array of table identifiers that hold the key.</returns>
    Guid[] GetMemberTables(TKey key, out TValue[]? value);

    /// <summary>
    /// Adds a key-value entry across the cross-table index (non-partitioned).
    /// </summary>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value to associate.</param>
    /// <returns><see langword="true"/> if the key was added; otherwise <see langword="false"/>.</returns>
    bool Add(TKey key, TValue value);

    /// <summary>
    /// Removes a key and its value across all partitions (if present).
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns><see langword="true"/> if the key existed and was removed; otherwise <see langword="false"/>.</returns>
    bool Remove(TKey key);

    /// <summary>
    /// Attempts to retrieve a value associated with the given key across any partition.
    /// </summary>
    /// <param name="key">The key to search for.</param>
    /// <param name="value">Contains the found value if successful; otherwise default.</param>
    /// <returns><see langword="true"/> if found; otherwise <see langword="false"/>.</returns>
    bool TryGetValue(TKey key, out TValue value);

    /// <summary>
    /// Adds a new entry to a specific table partition.
    /// </summary>
    /// <param name="tableId">The table partition identifier.</param>
    /// <param name="key">Key to insert.</param>
    /// <param name="value">Value associated with the key.</param>
    /// <returns><see langword="true"/> if inserted; <see langword="false"/> if already present in partition.</returns>
    bool Add(Guid tableId, TKey key, TValue value);

    /// <summary>
    /// Removes a key-value entry from a specific table partition.
    /// </summary>
    /// <param name="tableId">Partition identifier.</param>
    /// <param name="key">Key to remove.</param>
    /// <returns><see langword="true"/> if the entry was present and removed; otherwise <see langword="false"/>.</returns>
    bool Remove(Guid tableId, TKey key);

    /// <summary>
    /// Attempts to retrieve a value for the given key from a specific table partition.
    /// </summary>
    /// <param name="tableId">Table partition ID.</param>
    /// <param name="key">The key to locate.</param>
    /// <param name="value">If found, the associated value; otherwise default.</param>
    /// <returns><see langword="true"/> if found in the partition; otherwise <see langword="false"/>.</returns>
    bool TryGetValue(Guid tableId, TKey key, out TValue value);
}
