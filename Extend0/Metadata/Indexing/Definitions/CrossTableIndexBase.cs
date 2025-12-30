using Extend0.Metadata.Indexing.Contract;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Indexing.Definitions
{
    /// <summary>
    /// Base implementation for <see cref="ICrossTableIndex{TKey, TValue}"/> backed by a partitioned map:
    /// <c>TableId → (Key → Value)</c>.
    /// </summary>
    /// <typeparam name="TInnerKey">Key type for entries inside a table partition.</typeparam>
    /// <typeparam name="TInnerValue">Value type stored per key inside a table partition.</typeparam>
    /// <param name="name">Logical name of the index instance.</param>
    /// <param name="keyComparer">
    /// Optional comparer used by all inner dictionaries. If omitted, <see cref="EqualityComparer{T}.Default"/> is used.
    /// </param>
    /// <param name="tablesCapacity">Optional initial capacity for the outer partition map.</param>
    /// <param name="perTableCapacity">Optional initial capacity for lazily created inner dictionaries.</param>
    /// <remarks>
    /// <para>
    /// <b>Data model:</b> the index is partitioned by table id (<see cref="Guid"/>). Each partition is an inner dictionary
    /// mapping <typeparamref name="TInnerKey"/> to <typeparamref name="TInnerValue"/>:
    /// <c>Guid → IDictionary&lt;TInnerKey, TInnerValue&gt;</c>.
    /// </para>
    /// <para>
    /// <b>Partition lifetime:</b> partitions are created lazily by partitioned <see cref="Add(Guid, TInnerKey, TInnerValue)"/>
    /// calls and can be removed efficiently with <see cref="ClearTable(Guid)"/> without affecting other tables.
    /// </para>
    /// <para>
    /// <b>Cross-table helpers:</b> non-partitioned overloads (e.g. <see cref="Add(TInnerKey, TInnerValue)"/>,
    /// <see cref="Remove(TInnerKey)"/>, <see cref="TryGetValue(TInnerKey, out TInnerValue)"/>) iterate all partitions and apply
    /// the operation to all of them (add/remove) or return the first hit (lookup), depending on method semantics.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> a single <see cref="ReaderWriterLockSlim"/> guards both the outer partition map and all inner
    /// dictionaries. Implementations typically take a read lock for lookups and a write lock for mutations.
    /// </para>
    /// <para>
    /// <b>Membership queries:</b> <see cref="GetMemberTables(TInnerKey)"/> and
    /// <see cref="GetMemberTables(TInnerKey, out TInnerValue[]?)"/> scan partitions once and build result buffers using a small
    /// <c>stackalloc</c> scratch for <see cref="Guid"/> ids (unmanaged) and <see cref="ArrayPool{T}"/> for any growth. Values are
    /// always buffered using <see cref="ArrayPool{T}"/> because <typeparamref name="TInnerValue"/> may be a managed type and cannot
    /// be stack-allocated safely.
    /// </para>
    /// <para>
    /// <b>Binary fixed-size lookup scratch:</b> <see cref="GetScratchLookupKey(ReadOnlySpan{byte}, int)"/> materializes a fixed-size,
    /// zero-padded key into a thread-local buffer for allocation-free lookups when an index stores keys as fixed-size
    /// <see cref="byte"/> arrays. The returned buffer is ephemeral and must not be stored.
    /// </para>
    /// <para>
    /// <b>Disposal:</b> <see cref="Dispose"/> clears the index and disposes the lock. After disposal, operations guarded by
    /// <see cref="ThrowIfDisposed"/> throw <see cref="ObjectDisposedException"/>.
    /// </para>
    /// </remarks>
    public abstract class CrossTableIndexBase<TInnerKey, TInnerValue>(string name, IEqualityComparer<TInnerKey>? keyComparer = null, int tablesCapacity = 0, int perTableCapacity = 0) :
        ITableIndex<Guid, IDictionary<TInnerKey, TInnerValue>>,
        ICrossTableIndex<TInnerKey, TInnerValue>,
        IDisposable
        where TInnerKey : notnull
    {
        /// <summary>
        /// The equality comparer used for all inner dictionaries and key comparisons.
        /// </summary>
        protected IEqualityComparer<TInnerKey> InnerKeyComparer { get; } =  keyComparer ?? EqualityComparer<TInnerKey>.Default;

        private readonly Dictionary<Guid, IDictionary<TInnerKey, TInnerValue>> _index = tablesCapacity > 0
            ? new Dictionary<Guid, IDictionary<TInnerKey, TInnerValue>>(tablesCapacity) : [];

        /// <summary>
        /// Gets the underlying partitioned dictionary.
        /// </summary>
        /// <remarks>
        /// This property exposes the live storage used by the index. Access must be synchronized via <see cref="Rwls"/>.
        /// </remarks>
        protected Dictionary<Guid, IDictionary<TInnerKey, TInnerValue>> Index => _index;

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
        public Type KeyType => typeof(Guid);

        /// <summary>
        /// Gets the runtime type of the values stored in this index.
        /// </summary>
        public Type ValueType => typeof(IDictionary<TInnerKey, TInnerValue>);

        /// <summary>
        /// Adds the given key/value entry to all existing table partitions.
        /// </summary>
        /// <param name="key">The key to add.</param>
        /// <param name="value">The value to associate with the key.</param>
        /// <returns>
        /// <see langword="true"/> if at least one table partition accepted the key;
        /// <see langword="false"/> if the key already existed in all partitions.
        /// </returns>
        /// <remarks>
        /// This method does not create new partitions. It only applies to currently existing partitions.
        /// </remarks>
        public virtual bool Add(TInnerKey key, TInnerValue value)
        {
            ThrowIfDisposed();
            var added = false;
            Rwls.EnterWriteLock();
            try
            {
                foreach (var table in Index.Values)
                    added |= table.TryAdd(key, value);
            }
            finally { Rwls.ExitWriteLock(); }
            return added;
        }

        /// <summary>
        /// Removes the given key from all table partitions.
        /// </summary>
        /// <param name="key">The key to remove.</param>
        /// <returns>
        /// <see langword="true"/> if the key existed in at least one partition and was removed;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        public virtual bool Remove(TInnerKey key)
        {
            ThrowIfDisposed();
            var removed = false;
            Rwls.EnterWriteLock();
            try
            {
                foreach (var table in Index.Values)
                    removed |= table.Remove(key);
            }
            finally { Rwls.ExitWriteLock(); }
            return removed;
        }

        /// <summary>
        /// Attempts to retrieve the value associated with the given key from any table partition.
        /// </summary>
        /// <param name="key">The key to look up.</param>
        /// <param name="value">
        /// When this method returns <see langword="true"/>, contains the value found; otherwise, contains the default value
        /// for <typeparamref name="TInnerValue"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the key was found in any table partition; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// If the key exists in multiple partitions, only the first match (by outer map enumeration order) is returned.
        /// </remarks>
        public virtual bool TryGetValue(TInnerKey key, out TInnerValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();
            try
            {
                foreach (var table in Index.Values)
                    if (table.TryGetValue(key, out value!))
                        return true;

                value = default!;
                return false;
            }
            finally { Rwls.ExitReadLock(); }
        }

        /// <summary>
        /// Removes the entire partition for a specific table.
        /// </summary>
        /// <param name="tableId">The table identifier (partition key).</param>
        public virtual void ClearTable(Guid tableId)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try 
            { 
                Index.Remove(tableId);
            }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <summary>
        /// Adds a key/value entry into a specific table partition, creating the partition if needed.
        /// </summary>
        /// <param name="tableId">The table identifier (partition key).</param>
        /// <param name="key">The key to add.</param>
        /// <param name="value">The value to associate with the key.</param>
        /// <returns><see langword="true"/> if the entry was added; otherwise, <see langword="false"/>.</returns>
        public virtual bool Add(Guid tableId, TInnerKey key, TInnerValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try
            {
                if (!Index.TryGetValue(tableId, out var dict))
                {
                    dict = perTableCapacity > 0
                        ? new Dictionary<TInnerKey, TInnerValue>(perTableCapacity, InnerKeyComparer)
                        : new Dictionary<TInnerKey, TInnerValue>(InnerKeyComparer);

                    Index[tableId] = dict;
                }

                return dict.TryAdd(key, value);
            }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <summary>
        /// Removes a key from a specific table partition.
        /// </summary>
        /// <param name="tableId">The target table identifier (partition key).</param>
        /// <param name="key">The key to remove.</param>
        /// <returns><see langword="true"/> if the key was removed; otherwise, <see langword="false"/>.</returns>
        public virtual bool Remove(Guid tableId, TInnerKey key)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return Index.TryGetValue(tableId, out var dict) && dict.Remove(key); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <summary>
        /// Attempts to retrieve a value from a specific table partition.
        /// </summary>
        /// <param name="tableId">The target table identifier (partition key).</param>
        /// <param name="key">The key to look up.</param>
        /// <param name="value">
        /// When this method returns <see langword="true"/>, contains the value found; otherwise, contains the default value
        /// for <typeparamref name="TInnerValue"/>.
        /// </param>
        /// <returns><see langword="true"/> if found in that partition; otherwise, <see langword="false"/>.</returns>
        public virtual bool TryGetValue(Guid tableId, TInnerKey key, out TInnerValue value)
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();
            try
            {
                if (Index.TryGetValue(tableId, out var dict))
                    return dict.TryGetValue(key, out value!);

                value = default!;
                return false;
            }
            finally { Rwls.ExitReadLock(); }
        }

        /// <summary>
        /// Gets the set of table partitions that currently contain the specified key.
        /// </summary>
        /// <param name="key">The key to search for.</param>
        /// <returns>
        /// An array of table identifiers (<see cref="Guid"/>) for partitions that contain the key, or
        /// <see cref="Array.Empty{T}"/> if no partitions contain it.
        /// </returns>
        /// <remarks>
        /// This method scans the outer map once and builds the result using a small stack buffer for ids and
        /// <see cref="ArrayPool{T}"/> when growth is required.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0301", Justification = "Hot-path: explicit Array.Empty<T>() usage.")]
        public virtual Guid[] GetMemberTables(TInnerKey key)
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();
            Guid[]? rentedIds = null;
            try
            {
                const int STACK_LIMIT = 128; // 16 bytes * 128 = 2KB on stack
                var cap = Index.Count <= STACK_LIMIT ? Index.Count : STACK_LIMIT;
                Span<Guid> stack = stackalloc Guid[cap];
                Span<Guid> buf = stack;

                int n = 0;

                foreach (var kvp in Index)
                {
                    if (!kvp.Value.ContainsKey(key))
                        continue;

                    if ((uint)n >= (uint)buf.Length)
                    {
                        // grow
                        var newSize = buf.Length * 2;
                        var newArr = ArrayPool<Guid>.Shared.Rent(newSize);

                        buf[..n].CopyTo(newArr);

                        if (rentedIds is not null)
                            ArrayPool<Guid>.Shared.Return(rentedIds, clearArray: false);

                        rentedIds = newArr;
                        buf = newArr;
                    }

                    buf[n++] = kvp.Key;
                }

                if (n == 0)
                    return Array.Empty<Guid>();

                var result = new Guid[n];
                buf[..n].CopyTo(result);

                return result;
            }
            finally 
            {
                if (rentedIds is not null)
                    ArrayPool<Guid>.Shared.Return(rentedIds, clearArray: false);

                Rwls.ExitReadLock(); 
            }
        }

        /// <summary>
        /// Gets the set of table partitions that currently contain the specified key and returns the values
        /// found in those partitions.
        /// </summary>
        /// <param name="key">The key to search for.</param>
        /// <param name="value">
        /// When this method returns, contains an array of values corresponding to the returned table ids (same ordering),
        /// or <see cref="Array.Empty{T}"/> when there are no matches.
        /// </param>
        /// <returns>
        /// An array of table identifiers (<see cref="Guid"/>) for partitions that contain the key, or
        /// <see cref="Array.Empty{T}"/> if no partitions contain it.
        /// </returns>
        /// <remarks>
        /// Values are buffered using <see cref="ArrayPool{T}"/> (never <c>stackalloc</c>) because
        /// <typeparamref name="TInnerValue"/> may be a managed type.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0301", Justification = "Hot-path: explicit Array.Empty<T>() usage.")]
        public virtual Guid[] GetMemberTables(TInnerKey key, out TInnerValue[]? value)
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();

            Guid[]? rentedIds = null;
            TInnerValue[]? rentedVals = null;
            try
            {
                const int STACK_LIMIT = 128; // 16 bytes * 128 = 2KB on stack
                var cap = Index.Count <= STACK_LIMIT ? Index.Count : STACK_LIMIT;
                Span<Guid> stackIds = stackalloc Guid[cap];
                Span<Guid> ids = stackIds;
                Span<TInnerValue> vals = default;

                int n = 0;

                foreach (var kvp in Index)
                {
                    if (!kvp.Value.TryGetValue(key, out var v))
                        continue;

                    if ((uint)n >= (uint)ids.Length)
                    {
                        var newSize = ids.Length * 2;

                        var newIds = ArrayPool<Guid>.Shared.Rent(newSize);
                        ids[..n].CopyTo(newIds);

                        if (rentedIds is not null)
                            ArrayPool<Guid>.Shared.Return(rentedIds, clearArray: false);

                        rentedIds = newIds;
                        ids = newIds;
                    }

                    if (rentedVals is null)
                    {
                        rentedVals = ArrayPool<TInnerValue>.Shared.Rent(ids.Length);
                        vals = rentedVals;
                    }
                    else if ((uint)n >= (uint)vals.Length)
                    {
                        var newVals = ArrayPool<TInnerValue>.Shared.Rent(ids.Length);

                        vals[..n].CopyTo(newVals);

                        ArrayPool<TInnerValue>.Shared.Return(
                            rentedVals,
                            clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TInnerValue>());

                        rentedVals = newVals;
                        vals = newVals;
                    }

                    ids[n] = kvp.Key;
                    vals[n] = v;
                    n++;
                }

                if (n == 0)
                {
                    value = Array.Empty<TInnerValue>();
                    return Array.Empty<Guid>();
                }

                var outIds = new Guid[n];
                ids[..n].CopyTo(outIds);

                var outVals = new TInnerValue[n];
                vals[..n].CopyTo(outVals);

                value = outVals;
                return outIds;
            }
            finally 
            {
                if (rentedIds is not null)
                    ArrayPool<Guid>.Shared.Return(rentedIds, clearArray: false);

                if (rentedVals is not null)
                    ArrayPool<TInnerValue>.Shared.Return(
                        rentedVals,
                        clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TInnerValue>());

                Rwls.ExitReadLock(); 
            }
        }

        /// <summary>
        /// Gets all current table partition identifiers.
        /// </summary>
        /// <returns>An array of table identifiers (<see cref="Guid"/>).</returns>
        public virtual Guid[] GetMemberTables()
        {
            ThrowIfDisposed();
            Rwls.EnterReadLock();
            try
            {
                var result = new Guid[Index.Count];
                int i = 0;
                foreach (var kvp in Index)
                {
                    result[i++] = kvp.Key;
                }
                return result;
            }
            finally { Rwls.ExitReadLock(); }
        }

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

        /// <summary>
        /// Adds a partition map into the index.
        /// </summary>
        /// <param name="key">The partition identifier (table id).</param>
        /// <param name="value">The partition dictionary.</param>
        /// <returns><see langword="true"/> if the partition was added; otherwise, <see langword="false"/>.</returns>
        public virtual bool Add(Guid key, IDictionary<TInnerKey, TInnerValue> value)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return _index.TryAdd(key, value); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <summary>
        /// Removes a partition from the index.
        /// </summary>
        /// <param name="key">The partition identifier (table id).</param>
        /// <returns><see langword="true"/> if removed; otherwise, <see langword="false"/>.</returns>
        public virtual bool Remove(Guid key)
        {
            ThrowIfDisposed();
            Rwls.EnterWriteLock();
            try { return _index.Remove(key); }
            finally { Rwls.ExitWriteLock(); }
        }

        /// <summary>
        /// Attempts to retrieve a partition dictionary by table id.
        /// </summary>
        /// <param name="key">The partition identifier (table id).</param>
        /// <param name="value">
        /// When this method returns <see langword="true"/>, contains the partition dictionary; otherwise, <see langword="default"/>.
        /// </param>
        /// <returns><see langword="true"/> if the partition exists; otherwise, <see langword="false"/>.</returns>
        public virtual bool TryGetValue(Guid key, out IDictionary<TInnerKey, TInnerValue> value)
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
        /// This clears the index, disposes the underlying <see cref="ReaderWriterLockSlim"/> and marks the index as disposed.
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
