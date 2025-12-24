using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Definitions;

namespace Extend0.Metadata.Indexing.Registries
{
    /// <summary>
    /// Registry of per-table indexes keyed by name.
    /// </summary>
    /// <remarks>
    /// Typically owned by a <c>MetadataTable</c> instance. Provides strongly-typed access
    /// while still allowing heterogeneous enumeration via <see cref="ITableIndex"/>.
    /// </remarks>
    public sealed class TableIndexesRegistry : IndexesRegistryBase
    {
        /// <summary>
        /// Attempts to retrieve a registered index by name (non-generic).
        /// </summary>
        /// <param name="name">Index name.</param>
        /// <param name="index">The index instance when found; otherwise <see langword="null"/>.</param>
        /// <returns><see langword="true"/> when found; otherwise <see langword="false"/>.</returns>
        public bool TryGet(string name, out ITableIndex? index)
        {
            ThrowIfDisposed();
            return _indexes.TryGetValue(name, out index);
        }

        /// <summary>
        /// Creates and registers a new per-table index.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <typeparam name="TValue">Value type.</typeparam>
        /// <param name="name">Unique index name within this registry.</param>
        /// <param name="comparer">Optional key comparer.</param>
        /// <param name="capacity">Optional initial dictionary capacity.</param>
        /// <returns>The created typed index.</returns>
        /// <exception cref="InvalidOperationException">Thrown if an index with the same name already exists.</exception>
        public ITableIndex<TKey, TValue> Add<TKey, TValue>(string name, IEqualityComparer<TKey>? comparer = null, int capacity = 0) where TKey : notnull
        {
            ThrowIfDisposed();
            var created = new IndexDefinition<TKey, TValue>(name, comparer, capacity);
            if (!_indexes.TryAdd(name, created))
                throw new InvalidOperationException($"Index '{name}' already exists.");

            return created;
        }

        /// <summary>
        /// Registers an already-created index instance.
        /// </summary>
        /// <param name="index">Index instance to register.</param>
        /// <returns>The same instance, for fluent usage.</returns>
        /// <exception cref="InvalidOperationException">Thrown if an index with the same name already exists.</exception>
        public TIndex Add<TIndex>(TIndex index) where TIndex : class, ITableIndex
        {
            ArgumentNullException.ThrowIfNull(index);
            ThrowIfDisposed();

            if (!_indexes.TryAdd(index.Name, index))
                throw new InvalidOperationException($"Index '{index.Name}' already exists.");

            return index;
        }

        /// <summary>
        /// Retrieves a typed index by name.
        /// </summary>
        /// <typeparam name="TKey">Expected key type.</typeparam>
        /// <typeparam name="TValue">Expected value type.</typeparam>
        /// <param name="name">Index name.</param>
        /// <returns>The typed index instance.</returns>
        /// <exception cref="KeyNotFoundException">Thrown if the index does not exist.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the index exists but is registered with different key/value types.
        /// </exception>
        public ITableIndex<TKey, TValue> Get<TKey, TValue>(string name) where TKey : notnull
        {
            ThrowIfDisposed();
            if (!_indexes.TryGetValue(name, out var idx))
                throw new KeyNotFoundException($"Index '{name}' not found.");

            if (idx is not ITableIndex<TKey, TValue> typed)
                throw new InvalidOperationException(
                    $"Index '{name}' exists but has types {idx.KeyType.Name}->{idx.ValueType.Name}, " +
                    $"requested {typeof(TKey).Name}->{typeof(TValue).Name}.");

            return typed;
        }

        /// <summary>
        /// Clears all registered indexes and rebuilds those that implement <see cref="IRebuildableIndex"/>.
        /// </summary>
        /// <param name="table">Table to scan.</param>
        public void Rebuild(MetadataTable table)
        {
            ArgumentNullException.ThrowIfNull(table);
            ThrowIfDisposed();

            // If indexes are ephemeral, clearing is expected.
            ClearAll();

            foreach (var idx in Enumerate())
            {
                if (idx is IRebuildableIndex r)
                    r.Rebuild(table);
            }
        }
    }
}
