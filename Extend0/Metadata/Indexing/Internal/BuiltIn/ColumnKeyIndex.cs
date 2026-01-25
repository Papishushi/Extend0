using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Definitions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Extend0.Metadata.Indexing.Internal.BuiltIn.ColumnKeyIndex;

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
    : RebuildableIndexDefinition<uint, Dictionary<byte[], Hit>>(name, null, capacity)
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
    /// <b>Hit normalization:</b> <see cref="Hit.OwnedKey"/> is rewritten to reference the <em>stored</em> pooled key instance
    /// that ends up as the dictionary key. If an equivalent key already existed (by content), the dictionary keeps its original
    /// stored key array; the newly rented buffer is returned to the pool, and the hit is updated to point to the original
    /// stored key instance. The input <see cref="Hit.OwnedKey"/> is ignored.
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
    public override bool Add(uint key, Dictionary<byte[], Hit> value)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (Index.ContainsKey(key))
                return false;

            if (!_cachedKeySizes.TryGetValue(key, out var keySize) || keySize <= 0)
                return false;

            var ownedInner = new Dictionary<byte[], Hit>(value.Count, ByteArrayComparer.Ordinal);

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
                        ref Hit slot = ref CollectionsMarshal.GetValueRefOrAddDefault(
                            ownedInner, ownedKey, out bool exists);

                        var storedKey = exists ? slot.OwnedKey : ownedKey;

                        if (exists)
                            ReturnPooledKey(ownedKey);
                        else
                            ownedKey = null; // ownership transfer

                        slot = new Hit(kv.Value.Row, storedKey); // last wins
                    }
                    finally
                    {
                        if (ownedKey is not null)
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
    /// <b>Important:</b> in the returned snapshot, <see cref="Hit.OwnedKey"/> points to the cloned key buffer (heap-allocated),
    /// not to the pooled buffer owned by the index. Therefore, it must not be returned to the pool and cannot be used with
    /// fast-path removal APIs that require the exact stored key instance.
    /// </para>
    /// <para>
    /// <b>Cost:</b> this method allocates a new dictionary and one new <see cref="byte"/>[] per entry in the partition.
    /// Use it when you need a stable snapshot; prefer specialized lookup APIs (e.g. <c>TryGetRow</c>) for hot paths.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override bool TryGetValue(uint key, out Dictionary<byte[], Hit> value)
    {
        ThrowIfDisposed();

        Rwls.EnterReadLock();
        try
        {
            if (!Index.TryGetValue(key, out var inner))
            {
                value = new Dictionary<byte[], Hit>(0, ByteArrayComparer.Ordinal);
                return false;
            }

            var copy = new Dictionary<byte[], Hit>(inner.Count, ByteArrayComparer.Ordinal);

            foreach (var kv in inner)
            {
                var src = kv.Key;
                var dst = new byte[src.Length];
                Buffer.BlockCopy(src, 0, dst, 0, dst.Length);
                // Snapshot does not point to pooled key
                copy[dst] = new Hit(kv.Value.Row, dst);
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
    /// <param name="col">Column id (outer partition key).</param>
    /// <param name="lookupFixed">
    /// Fixed-size lookup key (zero-padded) whose length matches the configured key size for <paramref name="col"/>.
    /// </param>
    /// <returns><see langword="true"/> if a matching key existed and was removed; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// <para>
    /// <b>Locking contract:</b> caller must hold the write lock.
    /// </para>
    /// <para>
    /// <b>Removal strategy:</b> performs a content-based lookup to retrieve the stored <see cref="Hit"/> and then removes the
    /// entry using <see cref="Hit.OwnedKey"/>, which is the exact dictionary key instance. This enables <c>O(1)</c> removal
    /// and safe pooled key recycling without scanning dictionary keys.
    /// </para>
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Remove_NoLock(uint col, byte[] lookupFixed)
    {
        if (!Index.TryGetValue(col, out var dict))
            return false;

        if (!dict.TryGetValue(lookupFixed, out var hit))
            return false;

        if (!dict.Remove(hit.OwnedKey))
            return false;

        ReturnPooledKey(hit.OwnedKey);

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

            var lookup = key.Length == keySize ? key : GetScratchLookupKey(key, keySize);

            if (Index.TryGetValue(col, out var dict))
            {
                var a = dict.TryGetValue(lookup, out var hit);
                row = hit.Row;
                return a;
            }

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
            {
                if (!dict.TryGetValue(scratch, out var hit))
                    return false;
                row = hit.Row;
                return true;
            }

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

    /// <summary>
    /// Sets or overwrites the row id for <paramref name="ownedKey"/> inside the column partition.
    /// </summary>
    /// <param name="col">Column id (partition key).</param>
    /// <param name="ownedKey">A pooled fixed-size key buffer to be used for insertion.</param>
    /// <param name="row">Row id to store.</param>
    /// <remarks>
    /// <para>
    /// <b>Locking contract:</b> caller must hold the write lock.
    /// </para>
    /// <para>
    /// <b>Pooled key safety:</b> if an equivalent key already exists (by content), the dictionary keeps its original stored
    /// key instance. In that case, the incoming pooled buffer is not used as a key and is returned to the pool, while the
    /// stored key instance is preserved in <see cref="Hit.OwnedKey"/>.
    /// </para>
    /// <para>
    /// <b>Semantics:</b> <b>last wins</b>.
    /// </para>
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Set_NoLock(uint col, byte[] ownedKey, uint row)
    {
        if (!Index.TryGetValue(col, out var dict))
        {
            dict = new Dictionary<byte[], Hit>(ByteArrayComparer.Ordinal);
            Index[col] = dict;
        }

        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, ownedKey, out bool exists);

        // If it already existed, the dictionary kept its original stored key instance.
        // Keep that key for OwnedKey; return the incoming pooled key.
        var storedKey = exists ? slot.OwnedKey : ownedKey;

        if (exists)
            ReturnPooledKey(ownedKey);

        slot = new Hit(row, storedKey); // last wins
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
    public override async Task Rebuild(IMetadataTable table)
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

            await foreach (var entry in table.EnumerateCells().AsAsync())
            {
                if (!TryRentKey(cols, entry, out var owned)) continue;
                Set_NoLock(entry.Col, owned, entry.Row);
            }
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Immutable per-column index hit for <see cref="ColumnKeyIndex"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Stores the row id for a key and the exact pooled key buffer instance used as the dictionary key.
    /// </para>
    /// <para>
    /// <b>Pooled key ownership:</b> <see cref="OwnedKey"/> is a fixed-size buffer rented from the internal pool.
    /// It is owned by the index while the entry exists and must be returned to the pool on removal/clear/rebuild.
    /// </para>
    /// <para>
    /// <b>Why keep <see cref="OwnedKey"/>?</b> It enables <c>O(1)</c> removals without scanning dictionary keys:
    /// callers can locate an entry by content (using a scratch lookup key) and then remove using the stored key instance.
    /// </para>
    /// </remarks>
    internal readonly struct Hit
    {
        /// <summary>
        /// Row id where the key was found.
        /// </summary>
        public readonly uint Row;

        /// <summary>
        /// The exact pooled key buffer instance stored as the dictionary key for this entry.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This buffer is owned by the index and must not be exposed or mutated outside the indexing layer.
        /// </para>
        /// </remarks>
        internal readonly byte[] OwnedKey;

        /// <summary>
        /// Creates a new hit record.
        /// </summary>
        /// <param name="row">Row id where the key was found.</param>
        /// <param name="ownedKey">The exact pooled key buffer instance used as the dictionary key.</param>
        public Hit(uint row, byte[] ownedKey)
            => (Row, OwnedKey) = (row, ownedKey);
    }
}
