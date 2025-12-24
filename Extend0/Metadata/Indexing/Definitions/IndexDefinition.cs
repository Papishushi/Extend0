using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Definitions
{
    /// <summary>
    /// Default implementation for <see cref="ITableIndex{TKey, TValue}"/> backed by
    /// a <see cref="Dictionary{TKey, TValue}"/> protected by a <see cref="ReaderWriterLockSlim"/>.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <remarks>
    /// Uses a single write lock for add/remove operations to keep semantics simple and predictable.
    /// Read operations use a read lock.
    /// </remarks>
    public class IndexDefinition<TKey, TValue>(string name, IEqualityComparer<TKey>? comparer = null, int capacity = 0) : IndexBase<TKey, TValue>(name), ITableIndex<TKey, TValue> where TKey : notnull
    {
        private readonly Dictionary<TKey, TValue> _index = capacity > 0
                ? new Dictionary<TKey, TValue>(capacity, comparer)
                : new Dictionary<TKey, TValue>(comparer);

        protected Dictionary<TKey, TValue> Index => _index;

        /// <inheritdoc/>
        public override void Clear()
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { _index.Clear(); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public bool Add(TKey key, TValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return _index.TryAdd(key, value); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public bool Remove(TKey key)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return _index.Remove(key); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public bool TryGetValue(TKey key, out TValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();
            try { return _index.TryGetValue(key, out value!); }
            finally { Rwls.ExitReadLock(); }
        }
    }
}
