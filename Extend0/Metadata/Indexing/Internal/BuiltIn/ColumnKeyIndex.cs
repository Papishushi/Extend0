using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Indexing.Definitions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Extend0.Metadata.Indexing.Internal.BuiltIn;

/// <summary>
/// Built-in per-column key index for a <see cref="IMetadataTable"/>.
/// </summary>
/// <param name="name">Logical name for this index definition.</param>
/// <param name="capacity">Initial capacity hint for the outer partition map (column id → dictionary).</param>
/// <remarks>
/// <para>
/// This index partitions entries by column id (<see cref="uint"/>). Each partition maps fixed-size UTF-8 keys
/// (<see cref="byte"/>[]; zero-padded) to row ids (<see cref="uint"/>).
/// </para>
/// <para>
/// <b>Thread-safety:</b> all access is guarded by the inherited <see cref="ReaderWriterLockSlim"/>.
/// </para>
/// <para>
/// <b>Key ownership:</b> stored keys are pooled buffers owned by the index and are returned to
/// <see cref="IndexFixedSizeByteArrayPool"/> on removal, clear, or rebuild.
/// </para>
/// <para>
/// The base type <see cref="RebuildableIndexDefinition{TKey, TValue}"/> provides the underlying storage and lifecycle
/// (dispose checks, locking primitive, and rebuild contract).
/// </para>
/// </remarks>
internal sealed class ColumnKeyIndex(string name, int capacity = 0)
    : RebuildableIndexDefinition<uint, Dictionary<byte[], uint>>(name, null, capacity)
{
    /// <summary>
    /// Cached fixed key size (in bytes) per column id, derived from the table schema during <see cref="Rebuild"/>.
    /// </summary>
    /// <remarks>
    /// Values are used to validate inputs and to size scratch lookup buffers for allocation-free lookups/removals.
    /// Access is expected to be protected by the index lock.
    /// </remarks>
    private readonly Dictionary<uint, int> _cachedKeySizes = [];

    /// <inheritdoc/>
    /// <remarks>
    /// <para>
    /// <b>Key ownership:</b> on success, the index becomes the owner of all key buffers stored for
    /// <paramref name="key"/>. Any incoming keys that are not already in the fixed-size representation are
    /// materialized into pooled fixed-size buffers (copy + zero-fill).
    /// </para>
    /// <para>
    /// <b>Comparer:</b> the stored inner dictionary always uses <see cref="ByteArrayComparer.Ordinal"/> so
    /// keys are matched by content, not by reference.
    /// </para>
    /// <para>
    /// <b>Semantics:</b> this method creates a new partition only. If a partition for <paramref name="key"/> already
    /// exists, it returns <see langword="false"/> and does not mutate existing state. Within the incoming
    /// <paramref name="value"/> dictionary, <b>last wins</b> if multiple input keys compare equal by content.
    /// </para>
    /// <para>
    /// <b>Pooling safety:</b> if a rented key is not inserted (e.g., because an equivalent key already exists by content),
    /// it is returned to the pool immediately. If an exception occurs while building the partition, any pooled keys that
    /// were inserted are returned before rethrowing.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> the operation is performed under a write lock.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override bool Add(uint key, Dictionary<byte[], uint> value)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (Index.ContainsKey(key))
                return false;

            if (!_cachedKeySizes.TryGetValue(key, out var keySize) || keySize <= 0)
                return false;

            var ownedInner = new Dictionary<byte[], uint>(value.Count, ByteArrayComparer.Ordinal);

            try
            {
                foreach (var kv in value)
                {
                    var input = kv.Key;
                    if (input is null || input.Length == 0)
                        continue;

                    if ((uint)input.Length > (uint)keySize)
                        continue;

                    if (!TryRentKey(keySize, input, out var ownedKey))
                        continue;

                    try
                    {
                        ref uint slot = ref CollectionsMarshal.GetValueRefOrAddDefault(
                            ownedInner, ownedKey, out bool exists);

                        slot = kv.Value; // last wins

                        if (exists) ReturnPooledKey(ownedKey); // already owned
                        else ownedKey = null; // free ownership
                    }
                    finally
                    {
                        if (ownedKey != null)
                            ReturnPooledKey(ownedKey);
                    }
                }

                Index[key] = ownedInner;
                return true;
            }
            catch
            {
                foreach (var k in ownedInner.Keys)
                    ReturnPooledKey(k);

                throw;
            }
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <para>
    /// <b>Snapshot semantics:</b> this override returns a <em>defensive copy</em> of the partition dictionary. Both the
    /// dictionary instance <em>and</em> every stored key buffer are cloned before returning.
    /// </para>
    /// <para>
    /// <b>Why the copy matters:</b> the index internally owns pooled <see cref="byte"/>[] key buffers. Exposing the live
    /// inner dictionary would leak those buffers to callers and can become unsafe after <see cref="Clear"/>,
    /// <see cref="Remove(uint)"/>, or <see cref="Rebuild(IMetadataTable)"/>, which may return keys to the pool and reuse them.
    /// This method prevents callers from observing pooled buffer reuse by returning cloned key arrays.
    /// </para>
    /// <para>
    /// <b>Comparer:</b> the returned dictionary uses <see cref="ByteArrayComparer.Ordinal"/> so lookups are content-based,
    /// matching the index semantics.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> the snapshot is built under a read lock, so enumeration over the inner dictionary cannot race
    /// with writers while cloning.
    /// </para>
    /// <para>
    /// <b>Cost:</b> this method allocates a new dictionary and one new <see cref="byte"/>[] per entry in the partition.
    /// Use it when you need a stable snapshot; prefer specialized lookup APIs (e.g. <c>TryGetRow</c>) for hot paths.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override bool TryGetValue(uint key, out Dictionary<byte[], uint> value)
    {
        ThrowIfDisposed();

        Rwls.EnterReadLock();
        try
        {
            if (!Index.TryGetValue(key, out var inner))
            {
                value = new Dictionary<byte[], uint>(0, ByteArrayComparer.Ordinal);
                return false;
            }

            var copy = new Dictionary<byte[], uint>(inner.Count, ByteArrayComparer.Ordinal);

            foreach (var kv in inner)
            {
                var src = kv.Key;
                var dst = new byte[src.Length];
                Buffer.BlockCopy(src, 0, dst, 0, dst.Length);
                copy[dst] = kv.Value;
            }

            value = copy;
            return true;
        }
        finally { Rwls.ExitReadLock(); }
    }


    /// <summary>
    /// Clears the index and returns all stored pooled key buffers (from all inner dictionaries) to the pool.
    /// </summary>
    /// <remarks>
    /// The operation is performed under a write lock. After this call, the index contains no partitions and no entries.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override void Clear()
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            foreach (var inner in Index.Values)
                foreach (var k in inner.Keys)
                    ReturnPooledKey(k);

            Index.Clear();
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Removes a key entry from a specific column partition using a fixed-size lookup key.
    /// </summary>
    /// <param name="col">The column id (outer partition key).</param>
    /// <param name="lookupFixed">
    /// A fixed-size, zero-padded lookup key whose length matches the configured key size for <paramref name="col"/>.
    /// This buffer may be caller-owned or a thread-local scratch key produced by <c>GetScratchLookupKey</c>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if a matching key existed in the column partition and was removed; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Shared core implementation used by the public <c>Remove</c> overloads. Since stored keys are pooled <see cref="byte"/>[] buffers,
    /// removal must locate the exact stored array instance (matched by content via <see cref="ByteArrayComparer.Ordinal"/>) so it can be
    /// returned to the pool.
    /// </para>
    /// <para>
    /// <b>Locking contract:</b> the caller must already hold the write lock for the duration of this call.
    /// This method does not acquire or release locks.
    /// </para>
    /// <para>
    /// <b>Preconditions:</b> <paramref name="lookupFixed"/> is expected to already be in the fixed-size representation for the column
    /// (copy + zero-fill). Length validation and scratch materialization are performed by the public overloads.
    /// </para>
    /// <para>
    /// If the column partition becomes empty after removal, the partition entry is removed from the outer index.
    /// </para>
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Remove_NoLock(uint col, byte[] lookupFixed)
    {
        if (!Index.TryGetValue(col, out var dict))
            return false;

        // Find the *stored* array instance so we can return it to the pool.
        byte[]? stored = null;
        foreach (var kv in dict)
            if (ByteArrayComparer.Ordinal.Equals(kv.Key, lookupFixed))
            {
                stored = kv.Key;
                break;
            }

        if (stored is null)
            return false;

        if (!dict.Remove(stored))
            return false;

        ReturnPooledKey(stored);

        if (dict.Count == 0)
            Index.Remove(col);

        return true;
    }

    /// <summary>
    /// Removes an entire column partition from the index.
    /// </summary>
    /// <param name="key">The column id (partition key) to remove.</param>
    /// <returns><see langword="true"/> if the column partition existed and was removed; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// <para>
    /// <b>Partition removal:</b> this removes the whole per-column dictionary and returns every stored pooled key buffer to the pool
    /// before dropping the partition.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> performed under a write lock.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override bool Remove(uint key)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (!Index.TryGetValue(key, out var dict))
                return false;

            foreach (var k in dict.Keys)
                ReturnPooledKey(k);

            return Index.Remove(key);
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Removes a key entry from a specific column partition using content-based matching, returning the stored pooled key buffer to the pool.
    /// </summary>
    /// <param name="col">The column id.</param>
    /// <param name="key">
    /// The key bytes to remove. The provided buffer does not need to be the same array instance as the one stored.
    /// Matching is performed by content using <see cref="ByteArrayComparer.Ordinal"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if a matching key existed in that column and was removed; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// If <paramref name="key"/> is shorter than the fixed key size for the column, a thread-local scratch key is used
    /// (zero-padded) to perform the lookup without allocating.
    /// </para>
    /// <para>
    /// If <paramref name="key"/> is longer than the fixed key size for the column, the method returns <see langword="false"/>.
    /// </para>
    /// <para>
    /// The operation is performed under a write lock.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public bool Remove(uint col, byte[] key)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (!_cachedKeySizes.TryGetValue(col, out var keySize) || keySize <= 0)
                return false;

            if ((uint)key.Length > (uint)keySize)
                return false;

            var lookup = key.Length == keySize ? key : GetScratchLookupKey(key, keySize);
            return Remove_NoLock(col, lookup);
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Removes a key entry from a specific column partition using a UTF-8 span key without allocating.
    /// </summary>
    /// <param name="col">The column id.</param>
    /// <param name="keyUtf8">Key bytes (usually shorter than the fixed key size).</param>
    /// <returns>
    /// <see langword="true"/> if a matching key existed in that column and was removed; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>Allocation-free:</b> materializes the fixed-size (zero-padded) lookup key into a thread-local scratch buffer and removes the
    /// stored pooled key instance that matches by content.
    /// </para>
    /// <para>
    /// If <paramref name="keyUtf8"/> is longer than the configured key size for the column, the method returns <see langword="false"/>.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> performed under a write lock.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public bool Remove(uint col, ReadOnlySpan<byte> keyUtf8)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (!_cachedKeySizes.TryGetValue(col, out var keySize) || keySize <= 0)
                return false;

            if ((uint)keyUtf8.Length > (uint)keySize)
                return false;

            var scratch = GetScratchLookupKey(keyUtf8, keySize);
            return Remove_NoLock(col, scratch);
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Removes an entry using the exact stored key buffer instance and returns it to the pool.
    /// </summary>
    /// <param name="col">The column id.</param>
    /// <param name="storedKey">
    /// The exact key buffer instance stored in the index (a pooled buffer owned by the index).
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the entry existed and was removed; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>Fast path:</b> no content scan is performed. Use only when the caller already holds the stored key instance (e.g., obtained from
    /// an internal enumeration). Passing a non-stored buffer will simply return <see langword="false"/>.
    /// </para>
    /// <para>
    /// <b>Pooling:</b> on success, <paramref name="storedKey"/> is returned to the pool. If the partition becomes empty, it is removed.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> performed under a write lock.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    internal bool RemoveExact(uint col, byte[] storedKey)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (!Index.TryGetValue(col, out var dict)) return false;
            if (!dict.Remove(storedKey)) return false;
            ReturnPooledKey(storedKey);
            if (dict.Count == 0) Index.Remove(col);
            return true;
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Attempts to retrieve the row id associated with a key inside a specific column.
    /// </summary>
    /// <param name="col">The column id.</param>
    /// <param name="key">Key bytes (may be shorter than the fixed key size for the column).</param>
    /// <param name="row">When this method returns <see langword="true"/>, receives the row id.</param>
    /// <returns><see langword="true"/> if a matching key exists for that column; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// This overload validates the key length, materializes a fixed-size (zero-padded) lookup key into a thread-local scratch buffer,
    /// and performs the dictionary lookup using content-based key comparison.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public bool TryGetRow(uint col, byte[] key, out uint row)
    {
        ThrowIfDisposed();
        row = 0;

        Rwls.EnterReadLock();
        try
        {
            if (!_cachedKeySizes.TryGetValue(col, out var keySize) || keySize <= 0)
                return false;

            if ((uint)key.Length > (uint)keySize)
                return false;

            var scratch = GetScratchLookupKey(key, keySize);

            if (Index.TryGetValue(col, out var dict))
                return dict.TryGetValue(scratch, out row);

            return false;
        }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Attempts to retrieve the row id associated with a UTF-8 key span inside a specific column without allocating.
    /// </summary>
    /// <param name="col">The column id.</param>
    /// <param name="keyUtf8">Key bytes (usually shorter than the fixed key size).</param>
    /// <param name="row">When this method returns <see langword="true"/>, receives the row id.</param>
    /// <returns><see langword="true"/> if a matching key exists for that column; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// This overload is allocation-free. It materializes the fixed-size lookup key into a thread-local scratch buffer
    /// and performs the dictionary lookup using content-based comparison.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public bool TryGetRow(uint col, ReadOnlySpan<byte> keyUtf8, out uint row)
    {
        ThrowIfDisposed();
        row = 0;

        Rwls.EnterReadLock();
        try
        {
            if (!_cachedKeySizes.TryGetValue(col, out var keySize) || keySize <= 0)
                return false;

            if ((uint)keyUtf8.Length > (uint)keySize)
                return false;

            var scratch = GetScratchLookupKey(keyUtf8, keySize);

            if (Index.TryGetValue(col, out var dict))
                return dict.TryGetValue(scratch, out row);

            return false;
        }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Sets or overwrites the row id associated with a key inside a specific column.
    /// </summary>
    /// <param name="col">The column id.</param>
    /// <param name="key">
    /// Source key bytes to store. The input does not need to be fixed-size; it will be materialized into a pooled
    /// fixed-size, zero-padded buffer owned by the index.
    /// </param>
    /// <param name="row">Row id to store.</param>
    /// <remarks>
    /// <para>
    /// <b>Key materialization:</b> the index always stores keys as fixed-size UTF-8 buffers sized according to the column schema.
    /// If <paramref name="key"/> is shorter than the fixed size, it is copied and zero-padded; if it is longer, the call is a no-op.
    /// </para>
    /// <para>
    /// <b>Ownership &amp; pooling:</b> on success, the stored key buffer is rented from <see cref="IndexFixedSizeByteArrayPool"/> and becomes
    /// owned by the index. It is returned to the pool when the entry is removed, the partition is cleared, or the index is rebuilt.
    /// If a matching key already exists (by content), the dictionary retains its original stored key instance and the newly rented
    /// buffer is immediately returned to the pool to avoid leaks.
    /// </para>
    /// <para>
    /// <b>Semantics:</b> <b>last wins</b>. The row id is overwritten if the key already exists in the column (by content).
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> performed under a write lock.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Thrown if a pooled key buffer could not be rented for storage.</exception>
    internal void Set(uint col, byte[] key, uint row)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();

        byte[]? owned = null;
        try
        {
            if (!_cachedKeySizes.TryGetValue(col, out var keySize) || keySize <= 0)
                return;

            if ((uint)key.Length > (uint)keySize)
                return;

            if (!TryRentKey(keySize, key, out owned))
                throw new InvalidOperationException("Failed to rent key buffer for index storage.");

            Set_NoLock(col, owned, row);
        }
        catch
        {
            if (owned is not null)
                ReturnPooledKey(owned);
            throw;
        }
        finally { Rwls.ExitWriteLock(); }
    }

    private void Set_NoLock(uint col, byte[] ownedKey, uint row)
    {
        if (!Index.TryGetValue(col, out var dict))
        {
            dict = new Dictionary<byte[], uint>(ByteArrayComparer.Ordinal);
            Index[col] = dict;
        }

        ref uint slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, ownedKey, out bool exists);

        // If it already existed, the dictionary kept its original stored key instance,
        // so the incoming pooled 'key' is unused -> return it to the pool.
        if (exists)
            ReturnPooledKey(ownedKey);

        slot = row; // last wins
    }

    /// <summary>
    /// Rebuilds the index by scanning <paramref name="table"/> and indexing keys per column.
    /// </summary>
    /// <param name="table">The table to scan as the source of truth.</param>
    /// <remarks>
    /// <para>
    /// <b>What it does:</b> this is a full rebuild that treats <paramref name="table"/> as the authoritative source.
    /// It drops all existing partitions and entries, then repopulates the index by enumerating the table cells.
    /// </para>
    /// <para>
    /// <b>Key-size cache:</b> the per-column fixed key sizes are recomputed from <paramref name="table"/>'s schema
    /// (<c>table.Spec.Columns[i].Size.GetKeySize()</c>) and stored in the internal cache used by lookup and mutation paths.
    /// </para>
    /// <para>
    /// <b>Pooling &amp; cleanup:</b> before rebuilding, every pooled key buffer currently stored in the index is returned to
    /// <see cref="IndexFixedSizeByteArrayPool"/> and all partitions are cleared. During the rebuild, keys are materialized into
    /// pooled fixed-size, zero-padded buffers; the index becomes the owner of those buffers.
    /// </para>
    /// <para>
    /// <b>Filtering:</b> only cells that can produce a key (via <c>TryRentKey</c>) are indexed. Cells whose keys are empty or
    /// exceed the configured fixed key size for their column are skipped.
    /// </para>
    /// <para>
    /// <b>Conflict semantics:</b> <b>last wins</b>. If multiple cells produce keys that compare equal (by content) within the same
    /// column partition, the last enumerated cell overwrites the stored row id for that key.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> the entire operation runs under a write lock.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override void Rebuild(IMetadataTable table)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            var cols = table.Spec.Columns;

            _cachedKeySizes.Clear();
            for (uint i = 0; i < cols.Length; i++)
                _cachedKeySizes[i] = cols[i].Size.GetKeySize();

            foreach (var inner in Index.Values)
                foreach (var k in inner.Keys)
                    ReturnPooledKey(k);

            Index.Clear();

            foreach (var entry in table.EnumerateCells())
            {
                if (!TryRentKey(cols, entry, out var owned)) continue;
                Set_NoLock(entry.Col, owned, entry.Row);
            }
        }
        finally { Rwls.ExitWriteLock(); }
    }
}
