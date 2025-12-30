namespace Extend0.Metadata.Indexing.Contract;

/// <summary>
/// Defines a non-generic interface for a cross-table index that manages entries partitioned by table identifier.
/// </summary>
/// <remarks>
/// This type of index is typically managed by higher-level components like <c>MetaDBManager</c>.
/// It supports purging entries on a per-table basis, allowing for efficient cleanup when a table is closed or removed.
/// </remarks>
public interface ICrossTableIndex : ITableIndex
{
    /// <summary>
    /// Removes all index entries associated with a given table.
    /// </summary>
    /// <param name="tableId">The table identifier whose entries should be cleared.</param>
    void ClearTable(Guid tableId);

    /// <summary>
    /// Returns all table identifiers that currently contain indexed entries.
    /// </summary>
    /// <returns>An array of table IDs that have entries in this index.</returns>
    Guid[] GetMemberTables();
}

/// <summary>
/// Defines a generic interface for a cross-table index keyed by <typeparamref name="TInnerKey"/> and storing values of type <typeparamref name="TInnerValue"/>,
/// with all data partitioned by table identifier.
/// </summary>
/// <typeparam name="TInnerKey">The key type used for indexing.</typeparam>
/// <typeparam name="TInnerValue">The type of values associated with each key.</typeparam>
public interface ICrossTableIndex<in TInnerKey, TInnerValue> : ICrossTableIndex where TInnerKey : notnull
{
    /// <summary>
    /// Returns all table IDs that contain the specified key.
    /// </summary>
    /// <param name="key">The key to query.</param>
    /// <returns>An array of table IDs where the key is present.</returns>
    Guid[] GetMemberTables(TInnerKey key);

    /// <summary>
    /// Returns all table IDs where the specified key is present, and retrieves the corresponding values.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="value">An array of values associated with the key in each table.</param>
    /// <returns>An array of table IDs that hold the given key.</returns>
    Guid[] GetMemberTables(TInnerKey key, out TInnerValue[]? value);

    /// <summary>
    /// Adds a key-value entry to the index across all tables (if applicable).
    /// </summary>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value to associate with the key.</param>
    /// <returns><see langword="true"/> if the entry was added; <see langword="false"/> if the key already exists.</returns>
    bool Add(TInnerKey key, TInnerValue value);

    /// <summary>
    /// Removes a key (and its associated value) from all tables in the index.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns><see langword="true"/> if the key existed and was removed; otherwise, <see langword="false"/>.</returns>
    bool Remove(TInnerKey key);

    /// <summary>
    /// Attempts to retrieve the value associated with the given key across all tables.
    /// </summary>
    /// <param name="key">The key to search for.</param>
    /// <param name="value">If found, contains the value; otherwise, the default.</param>
    /// <returns><see langword="true"/> if the key exists; otherwise, <see langword="false"/>.</returns>
    bool TryGetValue(TInnerKey key, out TInnerValue value);

    /// <summary>
    /// Adds a key-value entry to the index under a specific table partition.
    /// </summary>
    /// <param name="tableId">The ID of the table that owns this entry.</param>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value to associate with the key.</param>
    /// <returns><see langword="true"/> if the entry was added; <see langword="false"/> if the key already exists in that table.</returns>
    bool Add(Guid tableId, TInnerKey key, TInnerValue value);

    /// <summary>
    /// Removes a key-value entry from a specific table partition.
    /// </summary>
    /// <param name="tableId">The ID of the table to remove the entry from.</param>
    /// <param name="key">The key to remove.</param>
    /// <returns><see langword="true"/> if the key was found and removed; otherwise, <see langword="false"/>.</returns>
    bool Remove(Guid tableId, TInnerKey key);

    /// <summary>
    /// Attempts to retrieve the value associated with a key from a specific table partition.
    /// </summary>
    /// <param name="tableId">The ID of the table to query.</param>
    /// <param name="key">The key to look up.</param>
    /// <param name="value">If found, contains the associated value; otherwise, the default.</param>
    /// <returns><see langword="true"/> if the key exists in the specified table; otherwise, <see langword="false"/>.</returns>
    bool TryGetValue(Guid tableId, TInnerKey key, out TInnerValue value);
}
