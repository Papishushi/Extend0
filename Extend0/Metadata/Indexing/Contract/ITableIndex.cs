using Extend0.Metadata.Contract;
using System.Collections.Concurrent;

namespace Extend0.Metadata.Indexing.Contract;

/// <summary>
/// Non-generic contract for an index registered against a table or a manager.
/// </summary>
/// <remarks>
/// <para>
/// This interface exists so indexes can be stored in heterogeneous collections
/// (for example, a <see cref="ConcurrentDictionary{TKey, TValue}"/>) while still exposing
/// identity, type metadata, lifecycle and clearing operations.
/// </para>
/// <para>
/// For typed operations (add/remove/lookup), use <see cref="ITableIndex{TKey, TValue}"/>.
/// </para>
/// </remarks>
public interface ITableIndex : IDisposable
{
    /// <summary>
    /// Gets the unique, human-readable name of the index within its owning collection.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the runtime key type used by this index.
    /// </summary>
    Type KeyType { get; }

    /// <summary>
    /// Gets the runtime value type stored by this index.
    /// </summary>
    Type ValueType { get; }

    /// <summary>
    /// Removes all entries from the index.
    /// </summary>
    void Clear();
}

/// <summary>
/// Generic contract for an index keyed by <typeparamref name="TKey"/> and storing <typeparamref name="TValue"/>.
/// </summary>
/// <typeparam name="TKey">Index key type.</typeparam>
/// <typeparam name="TValue">Index value type.</typeparam>
/// <remarks>
/// <para>
/// Intended for per-table indexes (owned by a single <see cref="IMetadataTable"/>), where lookups
/// do not require a table identifier.
/// </para>
/// <para>
/// Implementations are expected to be thread-safe.
/// </para>
/// </remarks>
public interface ITableIndex<in TKey, TValue> : ITableIndex where TKey : notnull
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
