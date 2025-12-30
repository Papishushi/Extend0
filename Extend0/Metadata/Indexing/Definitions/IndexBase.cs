using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Definitions
{
    /// <summary>
    /// Base type for table index implementations.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <remarks>
    /// <para>
    /// This class centralizes the common infrastructure shared by index implementations:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>A <see cref="ReaderWriterLockSlim"/> for thread-safety.</description></item>
    ///   <item><description>Basic index metadata (<see cref="Name"/>, <see cref="KeyType"/>, <see cref="ValueType"/>).</description></item>
    ///   <item><description>Dispose tracking and a helper guard (<see cref="ThrowIfDisposed"/>).</description></item>
    /// </list>
    /// <para>
    /// Derived types implement the actual storage and operations, and should call
    /// <see cref="ThrowIfDisposed"/> before accessing state.
    /// </para>
    /// </remarks>
    public abstract class IndexBase<TKey, TValue>(string name, IEqualityComparer<TKey>? comparer = null, int capacity = 0) : ITableIndex<TKey, TValue>, IDisposable where TKey : notnull
    {
        private readonly Dictionary<TKey, TValue> _index = capacity > 0
        ? new Dictionary<TKey, TValue>(capacity, comparer)
        : new Dictionary<TKey, TValue>(comparer);

        protected Dictionary<TKey, TValue> Index => _index;

        /// <summary>
        /// Reader/writer lock used to guard index state.
        /// </summary>
        /// <remarks>
        /// Implementations typically use a write lock for mutating operations and a read lock for lookups.
        /// </remarks>
        protected readonly ReaderWriterLockSlim Rwls = new(LockRecursionPolicy.NoRecursion);

        private int _disposed; // 0 = alive, 1 = disposed

        /// <summary>
        /// Gets the logical name of this index.
        /// </summary>
        public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));

        /// <summary>
        /// Gets the runtime type of the keys stored in this index.
        /// </summary>
        public Type KeyType => typeof(TKey);

        /// <summary>
        /// Gets the runtime type of the values stored in this index.
        /// </summary>
        public Type ValueType => typeof(TValue);

        /// <summary>
        /// Removes all entries from the index.
        /// </summary>
        public virtual void Clear()
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { _index.Clear(); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public virtual bool Add(TKey key, TValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return _index.TryAdd(key, value); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public virtual bool Remove(TKey key)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return _index.Remove(key); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <inheritdoc/>
        public virtual bool TryGetValue(TKey key, out TValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();
            try { return _index.TryGetValue(key, out value!); }
            finally { Rwls.ExitReadLock(); }
        }

        /// <summary>
        /// Releases resources used by the index.
        /// </summary>
        /// <remarks>
        /// This disposes the underlying <see cref="ReaderWriterLockSlim"/> and marks the index as disposed.
        /// Subsequent calls to operations guarded by <see cref="ThrowIfDisposed"/> will throw
        /// <see cref="ObjectDisposedException"/>.
        /// </remarks>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            Clear();

            Rwls.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Throws an <see cref="ObjectDisposedException"/> if the index has been disposed.
        /// </summary>
        /// <remarks>
        /// Call this at the beginning of public operations in derived types to enforce correct usage.
        /// </remarks>
        protected void ThrowIfDisposed()
            => ObjectDisposedException.ThrowIf(
                Volatile.Read(ref _disposed) != 0,
                $"{GetType().Name}('{Name}')");
    }
}
