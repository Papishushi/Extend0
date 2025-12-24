using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Definitions
{
    /// <summary>
    /// Default implementation for <see cref="ICrossTableIndex{TKey, TValue}"/> backed by a
    /// table-partitioned dictionary: <c>TableId → (Key → Value)</c>.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <remarks>
    /// <para>
    /// Each table partition is created on demand. This makes the index suitable for global
    /// indexing scenarios in <c>MetaDBManager</c>, while still allowing efficient purging of
    /// entries for a single table via <see cref="ClearTable(Guid)"/>.
    /// </para>
    /// <para>
    /// Thread-safety is enforced by a single <see cref="ReaderWriterLockSlim"/> guarding both
    /// the outer map and the inner dictionaries.
    /// </para>
    /// </remarks>
    public class CrossTableIndexDefinition<TKey, TValue>(string name, IEqualityComparer<TKey>? keyComparer = null, int tablesCapacity = 0, int perTableCapacity = 0) : IndexBase<TKey, TValue>(name), ICrossTableIndex<TKey, TValue> where TKey : notnull
    {
        private readonly Dictionary<Guid, Dictionary<TKey, TValue>> _byTable =
            tablesCapacity > 0 ? new Dictionary<Guid, Dictionary<TKey, TValue>>(tablesCapacity) : [];
        private readonly IEqualityComparer<TKey>? _keyComparer = keyComparer;

        /// <inheritdoc/>
        public override void Clear()
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { _byTable.Clear(); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public void ClearTable(Guid tableId)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { _byTable.Remove(tableId); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public bool Add(Guid tableId, TKey key, TValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try
            {
                if (!_byTable.TryGetValue(tableId, out var dict))
                {
                    dict = perTableCapacity > 0
                        ? new Dictionary<TKey, TValue>(perTableCapacity, _keyComparer)
                        : new Dictionary<TKey, TValue>(_keyComparer);

                    _byTable[tableId] = dict;
                }

                return dict.TryAdd(key, value);
            }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public bool Remove(Guid tableId, TKey key)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return _byTable.TryGetValue(tableId, out var dict) && dict.Remove(key); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public bool TryGetValue(Guid tableId, TKey key, out TValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();
            try
            {
                if (_byTable.TryGetValue(tableId, out var dict))
                    return dict.TryGetValue(key, out value!);

                value = default!;
                return false;
            }
            finally { Rwls.ExitReadLock(); }
        }
    }
}
