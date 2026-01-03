using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Indexing.Definitions;
using System.Runtime.InteropServices;
using static Extend0.Metadata.Indexing.Internal.BuiltIn.GlobalKeyIndex;

namespace Extend0.Metadata.Indexing.Internal.BuiltIn;

/// <summary>
/// Built-in global key index for a <see cref="IMetadataTable"/> that maps a UTF-8 key to its
/// first-class location in the table as a <c>(column,row)</c> pair.
/// </summary>
/// <remarks>
/// <para>
/// This index provides a fast path for cross-column key lookups without scanning the table.
/// Keys are stored as fixed-size UTF-8 <see cref="byte"/>[] instances and values are the
/// coordinates where that key was found.
/// </para>
/// <para>
/// <b>Thread-safety:</b> protected by the inherited <see cref="ReaderWriterLockSlim"/>.
/// Lookups take a read lock; rebuild and mutations take a write lock.
/// </para>
/// <para>
/// <b>Conflict semantics:</b> <b>last wins</b>. If multiple cells yield the same key (by content),
/// the last observed entry overwrites the previous one.
/// </para>
/// <para>
/// <b>Key ownership:</b> all keys stored in the dictionary are pooled buffers owned by the index and must
/// be returned to the pool when cleared, removed, or rebuilt.
/// </para>
/// <para>
/// <b>Fixed key size:</b> this global index uses a single fixed size for all keys: the maximum non-zero
/// key size across all columns in the table schema. This makes cross-column lookups possible without
/// requiring the caller to know which column produced the key.
/// </para>
/// </remarks>
internal sealed class GlobalKeyIndex(string name, int capacity = 0)
    : RebuildableIndexDefinition<byte[], Hit>(name, ByteArrayComparer.Ordinal, capacity)
{
    /// <summary>
    /// Global fixed key size (bytes) used by this index, computed on <see cref="Rebuild(IMetadataTable)"/>.
    /// </summary>
    private int _keySize;

    /// <inheritdoc/>
    /// <remarks>
    /// <para>
    /// <b>Key ownership:</b> on success, the index becomes the owner of the stored key buffer. The input key
    /// is materialized into a pooled fixed-size buffer (copy + zero-fill) before insertion.
    /// </para>
    /// <para>
    /// <b>Semantics:</b> this method does not mutate existing entries. If an equivalent key already exists
    /// (by content), it returns <see langword="false"/> and the newly rented buffer is returned to the pool.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> performed under a write lock.
    /// </para>
    /// </remarks>
    public override bool Add(byte[] key, Hit value)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(key);

        Rwls.EnterWriteLock();
        try
        {
            if (_keySize <= 0)
                return false;

            if ((uint)key.Length > (uint)_keySize)
                return false;

            if (!TryRentKey(_keySize, key, out var ownedKey))
                return false;

            try
            {
                // Add-only: if exists, do not modify.
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Index, ownedKey, out bool exists);
                if (exists)
                {
                    ReturnPooledKey(ownedKey);
                    return false;
                }

                // IMPORTANT: stored Hit must reference the actual dictionary key instance (ownedKey).
                slot = new Hit(value.Col, value.Row, ownedKey);
                ownedKey = null!;
                return true;
            }
            finally
            {
                if (ownedKey is not null)
                    ReturnPooledKey(ownedKey);
            }
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Uses a fixed-size scratch key (copy + zero-fill) to match the stored key shape.
    /// </remarks>
    public override bool TryGetValue(byte[] key, out Hit value)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(key);

        value = default;

        Rwls.EnterReadLock();
        try
        {
            if (_keySize <= 0)
                return false;

            if ((uint)key.Length > (uint)_keySize)
                return false;

            var lookup = key.Length == _keySize ? key : GetScratchLookupKey(key, _keySize);
            return Index.TryGetValue(lookup, out value);
        }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Clears the index and returns all stored key buffers to the underlying pool.
    /// </summary>
    public override void Clear()
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            foreach (var k in Index.Keys)
                ReturnPooledKey(k);

            Index.Clear();
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <para>
    /// <b>Content-based removal:</b> callers typically cannot provide the exact stored <see cref="byte"/>[] instance because
    /// keys are pooled fixed-size buffers. This method therefore normalizes the input into a fixed-size lookup key and uses
    /// the dictionary's content comparer (<see cref="ByteArrayComparer.Ordinal"/>) to find the entry.
    /// </para>
    /// <para>
    /// <b>Pooled key recovery:</b> the value stored in the index (<see cref="Hit"/>) carries <see cref="Hit.OwnedKey"/>,
    /// which is the exact array instance used as the dictionary key. Once a hit is found, removal is performed using that
    /// stored instance, enabling <c>O(1)</c> deletion of the correct key without scanning <see cref="Dictionary{TKey, TValue}.Keys"/>.
    /// </para>
    /// <para>
    /// <b>Pooling:</b> when an entry is removed, its owned key buffer is returned to the pool.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> executed under a write lock.
    /// </para>
    /// </remarks>
    public override bool Remove(byte[] key)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(key);

        Rwls.EnterWriteLock();
        try
        {
            if (_keySize <= 0)
                return false;

            if ((uint)key.Length > (uint)_keySize)
                return false;

            var lookup = key.Length == _keySize ? key : GetScratchLookupKey(key, _keySize);

            if (!Index.TryGetValue(lookup, out var hit))
                return false;

            if (!Index.Remove(hit.OwnedKey))
                return false;

            ReturnPooledKey(hit.OwnedKey);
            return true;
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Attempts to retrieve the table location associated with the specified UTF-8 key buffer.
    /// </summary>
    /// <remarks>
    /// This method accepts variable-length input and materializes a fixed-size lookup key using a scratch buffer.
    /// </remarks>
    public bool TryGetHit(byte[] key, out Hit hit)
        => TryGetValue(key, out hit);

    /// <summary>
    /// Attempts to retrieve the table location associated with the specified UTF-8 key span without allocating.
    /// </summary>
    /// <remarks>
    /// Uses a thread-static scratch buffer sized to the global fixed key size (copy + zero-fill).
    /// </remarks>
    public bool TryGetHit(ReadOnlySpan<byte> keyUtf8, out Hit hit)
    {
        ThrowIfDisposed();
        hit = default;

        if (_keySize <= 0)
            return false;

        if ((uint)keyUtf8.Length > (uint)_keySize)
            return false;

        Rwls.EnterReadLock();
        try 
        {
            var scratch = GetScratchLookupKey(keyUtf8, _keySize); 
            return Index.TryGetValue(scratch, out hit); 
        }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Sets or overwrites the hit associated with the specified key bytes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Key materialization:</b> the input key is materialized into a pooled fixed-size buffer (copy + zero-fill).
    /// If the key is longer than the configured fixed size, the call is a no-op.
    /// </para>
    /// <para>
    /// <b>Pooling safety:</b> if an equivalent key already exists (by content), the dictionary retains its original stored
    /// key instance and the newly rented key buffer is returned to the pool to avoid leaks.
    /// </para>
    /// <para>
    /// <b>Semantics:</b> <b>last wins</b>.
    /// </para>
    /// </remarks>
    internal void Set(byte[] key, uint col, uint row)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(key);

        Rwls.EnterWriteLock();

        byte[]? owned = null;
        try
        {
            if (_keySize <= 0)
                return;

            if ((uint)key.Length > (uint)_keySize)
                return;

            if (!TryRentKey(_keySize, key, out owned))
                throw new InvalidOperationException("Failed to rent key buffer for index storage.");

            Set_NoLock(owned, col, row);
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
    /// Upserts an entry for the specified pooled fixed-size <paramref name="ownedKey"/> without taking locks.
    /// </summary>
    /// <param name="ownedKey">
    /// A pooled fixed-size key buffer. If the key does not already exist, this buffer becomes the actual dictionary
    /// key instance and its ownership is transferred to the index. If the key already exists (by content), this buffer
    /// is not used by the dictionary and is returned to the pool.
    /// </param>
    /// <param name="col">Column index where the key was found.</param>
    /// <param name="row">Row index where the key was found.</param>
    /// <remarks>
    /// <para>
    /// <b>Locking:</b> this method does not acquire locks. Callers must hold the appropriate write lock.
    /// </para>
    /// <para>
    /// <b>Pooled key ownership:</b> because the dictionary uses a content comparer for <see cref="byte"/>[] keys,
    /// <paramref name="ownedKey"/> may be considered a duplicate even if it is a different array instance.
    /// When that happens, the dictionary keeps the original stored key instance; this method preserves that instance
    /// in <see cref="Hit.OwnedKey"/> and returns the incoming pooled buffer to the pool to avoid leaks.
    /// </para>
    /// <para>
    /// <b>Semantics:</b> <b>last wins</b>. The stored hit is overwritten with the provided <paramref name="col"/> and
    /// <paramref name="row"/>.
    /// </para>
    /// </remarks>
    private void Set_NoLock(byte[] ownedKey, uint col, uint row)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Index, ownedKey, out bool exists);

        // If it already existed, the dictionary kept its original stored key instance.
        // Keep that key for OwnedKey; return the incoming pooled key.
        var storedKey = exists ? slot.OwnedKey : ownedKey;

        if (exists)
            ReturnPooledKey(ownedKey);

        slot = new Hit(col, row, storedKey); // last wins
    }

    /// <summary>
    /// Rebuilds the index by scanning <paramref name="table"/> and indexing keys from all key/value columns.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Computes the global fixed key size as the maximum non-zero key size across the table schema.
    /// </para>
    /// <para>
    /// Returns all previously stored pooled keys to the pool, clears the index, then repopulates it by enumerating cells.
    /// </para>
    /// </remarks>
    public override void Rebuild(IMetadataTable table)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(table);

        var cols = table.Spec.Columns;

        Rwls.EnterWriteLock();
        try
        {
            // Recompute global key size (max non-zero).
            int max = 0;
            for (uint i = 0; i < cols.Length; i++)
            {
                var ks = cols[i].Size.GetKeySize();
                if (ks > max) max = ks;
            }
            _keySize = max;

            // Return existing pooled keys.
            foreach (var k in Index.Keys)
                ReturnPooledKey(k);
            Index.Clear();

            if (_keySize <= 0)
                return;

            foreach (var entry in table.EnumerateCells())
            {
                if (!TryRentKey(cols, entry, out var perColOwned))
                    continue;

                // Normalize to global key size so lookup doesn't need per-column size.
                if (perColOwned.Length == _keySize)
                {
                    Set_NoLock(perColOwned, entry.Col, entry.Row);
                    continue;
                }

                if (!TryRentKey(_keySize, perColOwned, out var ownedMax))
                {
                    ReturnPooledKey(perColOwned);
                    continue;
                }

                ReturnPooledKey(perColOwned);
                Set_NoLock(ownedMax, entry.Col, entry.Row);
            }
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Immutable index hit for <see cref="GlobalKeyIndex"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Represents the location of a key inside a table as a <c>(Col, Row)</c> pair.
    /// </para>
    /// <para>
    /// <b>Pooled key ownership:</b> <see cref="OwnedKey"/> is the exact fixed-size key buffer instance stored as a
    /// dictionary key. It is typically rented from the index's internal pool and is owned by the index while the
    /// entry exists.
    /// </para>
    /// <para>
    /// <b>Why keep <see cref="OwnedKey"/>?</b> It allows <c>O(1)</c> removals and safe pool return: callers can
    /// locate the entry via a content-based lookup key, then remove using the stored key instance and recycle its
    /// buffer without scanning the dictionary keys.
    /// </para>
    /// <para>
    /// <b>Visibility:</b> <see cref="OwnedKey"/> is <c>internal</c> to prevent external code from retaining or mutating
    /// the pooled buffer. Treat it as an opaque handle managed by the index.
    /// </para>
    /// </remarks>
    internal readonly struct Hit
    {
        /// <summary>
        /// Column index where the key was found.
        /// </summary>
        public readonly uint Col;

        /// <summary>
        /// Row index where the key was found.
        /// </summary>
        public readonly uint Row;

        /// <summary>
        /// The exact pooled key buffer instance stored in the dictionary for this entry.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a fixed-size UTF-8 (or binary) key buffer used as the dictionary key. The index owns this buffer and
        /// must return it to the pool when the entry is removed, cleared, or rebuilt.
        /// </para>
        /// <para>
        /// Do not store, mutate, or expose this buffer outside the indexing layer.
        /// </para>
        /// </remarks>
        internal readonly byte[] OwnedKey;

        /// <summary>
        /// Creates a new hit record.
        /// </summary>
        /// <param name="col">Column index where the key was found.</param>
        /// <param name="row">Row index where the key was found.</param>
        /// <param name="ownedKey">The pooled key buffer instance stored as the dictionary key for this entry.</param>
        public Hit(uint col, uint row, byte[] ownedKey)
            => (Col, Row, OwnedKey) = (col, row, ownedKey);
    }

}
