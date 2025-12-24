using Extend0.Metadata.Indexing.Definitions;

namespace Extend0.Metadata.Indexing.Internal.BuiltIn;

/// <summary>
/// Built-in global key index for a <see cref="MetadataTable"/> that maps a UTF-8 key to its
/// first-class location in the table as a <c>(column,row)</c> pair.
/// </summary>
/// <remarks>
/// <para>
/// This index provides a fast path for cross-column key lookups without scanning the table.
/// Keys are stored as fixed-size UTF-8 <see cref="byte"/>[] instances and values are the
/// coordinates where that key was found.
/// </para>
/// <para>
/// Thread-safety is provided by the inherited <see cref="ReaderWriterLockSlim"/>
/// (<see cref="IndexBase{TKey, TValue}.Rwls"/>). Lookups take a read lock; rebuild and mutations
/// take a write lock.
/// </para>
/// <para>
/// Conflict semantics: <b>last wins</b>. If multiple cells yield the same key, the latest observed
/// entry overwrites the previous one.
/// </para>
/// <para>
/// Key ownership: during rebuild, keys are materialized into fixed-size arrays rented from
/// <see cref="IndexFixedSizeByteArrayPool"/> (via <c>TryRentKey</c>). Those arrays become the dictionary keys
/// and must be returned to the pool when the index is cleared or rebuilt.
/// </para>
/// <para>
/// Only columns with a non-zero fixed key size (<c>GetKeySize() != 0</c>) participate.
/// Cells without a key (or with an empty key) are skipped.
/// </para>
/// </remarks>
internal sealed class GlobalKeyIndex(string name, int capacity = 0)
    : RebuildableIndexDefinition<byte[], (uint col, uint row)>(name, ByteArrayComparer.Ordinal, capacity)
{
    /// <summary>
    /// Clears the index and returns all stored key buffers to the underlying pool.
    /// </summary>
    /// <remarks>
    /// Caller-visible behavior is equivalent to dropping the index content, but pooled
    /// buffers are recycled to reduce future allocations.
    /// </remarks>
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

    /// <summary>
    /// Attempts to retrieve the table location associated with the specified UTF-8 key buffer.
    /// </summary>
    /// <param name="key">Fixed-size UTF-8 key buffer used as a dictionary key.</param>
    /// <param name="hit">
    /// When this method returns <see langword="true"/>, contains the <c>(col,row)</c> hit.
    /// </param>
    /// <returns><see langword="true"/> if the key exists; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public bool TryGetHit(byte[] key, out (uint col, uint row) hit)
    {
        ThrowIfDisposed();
        Rwls.EnterReadLock();
        try { return Index.TryGetValue(key, out hit); }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Attempts to retrieve the table location associated with the specified key span without allocating.
    /// </summary>
    /// <param name="keyUtf8">Key bytes (usually shorter than the fixed key size).</param>
    /// <param name="keySize">
    /// Fixed key size used by the column spec. The lookup uses a thread-static scratch buffer of this size
    /// (zero-padded) to match stored dictionary keys.
    /// </param>
    /// <param name="hit">When true, receives the <c>(col,row)</c> hit.</param>
    /// <returns><see langword="true"/> if the key exists; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// This method uses <see cref="IndexKeyScratch"/> to avoid allocations for the lookup key. The scratch buffer
    /// is zero-padded to ensure comparison matches the fixed-size keys stored in the index.
    /// </remarks>
    public bool TryGetHit(ReadOnlySpan<byte> keyUtf8, out (uint col, uint row) hit)
    {
        ThrowIfDisposed();

        var scratch = GetScratchLookupKey(keyUtf8, keyUtf8.Length);

        Rwls.EnterReadLock();
        try { return Index.TryGetValue(scratch, out hit); }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Sets or overwrites the hit associated with the specified key.
    /// </summary>
    /// <param name="key">Owned key buffer (typically rented from the pool) used as the dictionary key.</param>
    /// <param name="col">Zero-based column index where the key was found.</param>
    /// <param name="row">Zero-based row index where the key was found.</param>
    /// <remarks>
    /// Uses <b>last wins</b> semantics. The caller must ensure <paramref name="key"/> is an owned buffer that
    /// will remain valid for the lifetime of the index entry (and returned to the pool when removed/cleared).
    /// </remarks>
    internal void Set(byte[] key, uint col, uint row)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try { Set_NoLock(key, (col, row)); }
        finally { Rwls.ExitWriteLock(); }
    }

    private void Set_NoLock(byte[] key, (uint col, uint row) hit) => Index[key] = hit;

    /// <summary>
    /// Rebuilds the index by scanning <paramref name="table"/> and indexing keys from all key/value columns.
    /// </summary>
    /// <param name="table">Table to scan as the source of truth.</param>
    /// <remarks>
    /// Existing keys are returned to the pool, then the index is repopulated. Value-only columns are skipped.
    /// </remarks>
    public override void Rebuild(MetadataTable table)
    {
        ThrowIfDisposed();
        var cols = table.Spec.Columns;

        Rwls.EnterWriteLock();
        try
        {
            foreach (var k in Index.Keys)
                ReturnPooledKey(k);
            Index.Clear();

            foreach (var entry in table.EnumerateCells())
            {
                if (!TryRentKey(cols, entry, out var owned)) continue;
                Set_NoLock(owned, (entry.Col, entry.Row));
            }
        }
        finally { Rwls.ExitWriteLock(); }
    }
}