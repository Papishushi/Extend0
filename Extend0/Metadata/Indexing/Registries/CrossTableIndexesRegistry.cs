using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Definitions;
using Extend0.Metadata.Indexing.Registries.Contract;
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
    public sealed class CrossTableIndexesRegistry : IndexesRegistryBase, ICrossTableIndexesRegistry
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
        /// Creates and registers a new cross-table index instance using a user-provided constructor.
        /// </summary>
        /// <typeparam name="TKey">The type of key used by the index. Must be non-null.</typeparam>
        /// <typeparam name="TValue">The type of value stored in the index.</typeparam>
        /// <param name="indexConstructor">
        /// A factory delegate that constructs the index instance. Called only if no index with the same name exists.
        /// </param>
        /// <returns>The created index instance.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if an index with the specified <see cref="ITableIndex.Name"/> is already registered.
        /// </exception>
        public ICrossTableIndex Add<TKey, TValue>(Func<ICrossTableIndex> indexConstructor) where TKey : notnull
        {
            ThrowIfDisposed();

            var created = indexConstructor();
            if (!_indexes.TryAdd(created.Name, created))
                throw new InvalidOperationException($"Cross-table index '{created.Name}' already exists.");

            return created;
        }

        /// <summary>
        /// Registers an already-created index instance.
        /// </summary>
        /// <param name="index">Index instance to register.</param>
        /// <returns>The same instance, for fluent usage.</returns>
        /// <exception cref="InvalidOperationException">Thrown if an index with the same name already exists.</exception>
        public TIndex Add<TIndex>(TIndex index) where TIndex : class, ICrossTableIndex
        {
            ArgumentNullException.ThrowIfNull(index);
            ThrowIfDisposed();

            if (!_indexes.TryAdd(index.Name, index))
                throw new InvalidOperationException($"Index '{index.Name}' already exists.");

            return index;
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
