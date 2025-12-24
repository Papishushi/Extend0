using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Definitions;
using System.Collections.Concurrent;

namespace Extend0.Metadata.Indexing.Registries
{
    /// <summary>
    /// Registry of cross-table indexes keyed by name.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is typically owned by a higher-level component (for example, a manager)
    /// to keep global / cross-table indexing concerns separate from individual table instances.
    /// </para>
    /// <para>
    /// A cross-table index stores entries partitioned by <c>TableId</c> and can purge all entries
    /// belonging to a specific table via <see cref="ClearForTable(Guid)"/>.
    /// </para>
    /// <para>
    /// Thread-safety: the registry is backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/>.
    /// Each index implementation is responsible for its own internal synchronization.
    /// </para>
    /// </remarks>
    public sealed class CrossTableIndexesRegistry : IndexesRegistryBase
    {
        /// <summary>
        /// Attempts to retrieve a registered cross-table index by name (non-generic).
        /// </summary>
        /// <param name="name">Index name.</param>
        /// <param name="index">The index instance when found; otherwise <see langword="null"/>.</param>
        /// <returns><see langword="true"/> when found; otherwise <see langword="false"/>.</returns>
        public bool TryGet(string name, out ICrossTableIndex? index)
        {
            ThrowIfDisposed();
            var result = _indexes.TryGetValue(name, out var idx);
            index = idx as ICrossTableIndex;
            return result;
        }

        /// <summary>
        /// Attempts to retrieve a typed cross-table index by name.
        /// </summary>
        /// <typeparam name="TKey">Expected key type.</typeparam>
        /// <typeparam name="TValue">Expected value type.</typeparam>
        /// <param name="name">Index name.</param>
        /// <param name="index">The typed index instance when found; otherwise <see langword="null"/>.</param>
        /// <returns>
        /// <see langword="true"/> when found and the registered index matches <typeparamref name="TKey"/>/<typeparamref name="TValue"/>;
        /// otherwise <see langword="false"/>.
        /// </returns>
        public bool TryGet<TKey, TValue>(string name, out ICrossTableIndex<TKey, TValue>? index) where TKey : notnull
        {
            ThrowIfDisposed();

            if (_indexes.TryGetValue(name, out var idx) && idx is ICrossTableIndex<TKey, TValue> typed)
            {
                index = typed;
                return true;
            }

            index = null;
            return false;
        }

        /// <summary>
        /// Creates and registers a new cross-table index.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <typeparam name="TValue">Value type.</typeparam>
        /// <param name="name">Unique index name within this registry.</param>
        /// <param name="comparer">Optional key comparer.</param>
        /// <returns>The created typed cross-table index.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="name"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown if an index with the same name already exists.</exception>
        public ICrossTableIndex<TKey, TValue> Add<TKey, TValue>(string name, IEqualityComparer<TKey>? comparer = null) where TKey : notnull
        {
            ThrowIfDisposed();

            var created = new CrossTableIndexDefinition<TKey, TValue>(name, comparer);
            if (!_indexes.TryAdd(name, created))
                throw new InvalidOperationException($"Cross-table index '{name}' already exists.");

            return created;
        }

        /// <summary>
        /// Retrieves a typed cross-table index by name.
        /// </summary>
        /// <typeparam name="TKey">Expected key type.</typeparam>
        /// <typeparam name="TValue">Expected value type.</typeparam>
        /// <param name="name">Index name.</param>
        /// <returns>The typed cross-table index instance.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if this registry has been disposed.</exception>
        /// <exception cref="KeyNotFoundException">Thrown if the index does not exist.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the index exists but is registered with different key/value types.
        /// </exception>
        public ICrossTableIndex<TKey, TValue> Get<TKey, TValue>(string name) where TKey : notnull
        {
            ThrowIfDisposed();

            if (!_indexes.TryGetValue(name, out var idx))
                throw new KeyNotFoundException($"Cross-table index '{name}' not found.");

            if (idx is not ICrossTableIndex<TKey, TValue> typed)
                throw new InvalidOperationException(
                    $"Index '{name}' is {idx.KeyType.Name}->{idx.ValueType.Name}, requested {typeof(TKey).Name}->{typeof(TValue).Name}.");

            return typed;
        }

        /// <summary>
        /// Removes all entries belonging to the given table id from every registered cross-table index.
        /// </summary>
        /// <param name="tableId">Table identifier whose entries must be purged.</param>
        public void ClearForTable(Guid tableId)
        {
            ThrowIfDisposed();

            foreach (var kv in _indexes)
                if (kv.Value is ICrossTableIndex ctIndex)
                    ctIndex.ClearTable(tableId);
        }
    }
}
