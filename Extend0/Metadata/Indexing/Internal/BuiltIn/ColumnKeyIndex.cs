using Extend0.Metadata.Indexing.Definitions;
using Extend0.Metadata.Indexing.Internal;

namespace Extend0.Metadata.Indexing.Internal.BuiltIn;

/// <summary>
/// Built-in per-column key index for a <see cref="MetadataTable"/>.
/// Maps a fixed-size UTF-8 key to a row within a specific column.
/// </summary>
/// <remarks>
/// <para>
/// The outer dictionary is keyed by column id (<see cref="uint"/>). Each column maps to an inner dictionary
/// keyed by fixed-size UTF-8 <see cref="byte"/>[] buffers and returning the row id (<see cref="uint"/>).
/// </para>
/// <para>
/// Thread-safety is provided by the inherited <see cref="ReaderWriterLockSlim"/>.
/// Lookups take a read lock; rebuild and mutations take a write lock.
/// </para>
/// <para>
/// Conflict semantics: <b>last wins</b> within the inner dictionary.
/// If the same key appears multiple times in a column during rebuild, the last occurrence overwrites the previous row.
/// </para>
/// <para>
/// Key ownership: keys stored in the inner dictionaries are owned buffers (typically rented from
/// <see cref="IndexFixedSizeByteArrayPool"/> via <c>TryRentKey</c>) and must be returned to the pool when clearing
/// or rebuilding the index.
/// </para>
/// </remarks>
internal sealed class ColumnKeyIndex(string name, int capacity = 0)
    : RebuildableIndexDefinition<uint, Dictionary<byte[], uint>>(name, null, capacity)
{
    /// <summary>
    /// Clears the index and returns all stored key buffers (from all inner dictionaries) to the pool.
    /// </summary>
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
    /// Attempts to retrieve the row associated with a key inside a specific column.
    /// </summary>
    /// <param name="col">Column id.</param>
    /// <param name="key">Fixed-size UTF-8 key buffer.</param>
    /// <param name="row">When true, receives the row id.</param>
    /// <returns><see langword="true"/> if the key exists for that column; otherwise <see langword="false"/>.</returns>
    public bool TryGetRow(uint col, byte[] key, out uint row)
    {
        ThrowIfDisposed();
        Rwls.EnterReadLock();
        try
        {
            if (Index.TryGetValue(col, out var dict))
                return dict.TryGetValue(key, out row);

            row = 0;
            return false;
        }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Attempts to retrieve the row associated with a key span inside a specific column without allocating.
    /// </summary>
    /// <param name="col">Column id.</param>
    /// <param name="keyUtf8">Key bytes (usually shorter than the fixed key size).</param>
    /// <param name="keySize">Fixed key size from the column spec used to build the padded scratch key.</param>
    /// <param name="row">When true, receives the row id.</param>
    /// <returns><see langword="true"/> if the key exists for that column; otherwise <see langword="false"/>.</returns>
    public bool TryGetRow(uint col, ReadOnlySpan<byte> keyUtf8, out uint row)
    {
        ThrowIfDisposed();

        var scratch = GetScratchLookupKey(keyUtf8, keyUtf8.Length);

        Rwls.EnterReadLock();
        try
        {
            if (Index.TryGetValue(col, out var dict))
                return dict.TryGetValue(scratch, out row);

            row = 0;
            return false;
        }
        finally { Rwls.ExitReadLock(); }
    }

    /// <summary>
    /// Sets or overwrites the row associated with a key inside a specific column.
    /// </summary>
    /// <param name="col">Column id.</param>
    /// <param name="key">Owned key buffer (typically rented from the pool) used as a dictionary key.</param>
    /// <param name="row">Row id to store.</param>
    /// <remarks>
    /// Uses <b>last wins</b> semantics. The caller must ensure <paramref name="key"/> remains valid for as long as
    /// the index entry is present (and is returned to the pool when removed/cleared).
    /// </remarks>
    internal void Set(uint col, byte[] key, uint row)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try { Set_NoLock(col, key, row); }
        finally { Rwls.ExitWriteLock(); }
    }

    private void Set_NoLock(uint col, byte[] key, uint row)
    {
        if (!Index.TryGetValue(col, out var dict))
        {
            dict = new Dictionary<byte[], uint>(ByteArrayComparer.Ordinal);
            Index[col] = dict;
        }

        dict[key] = row;
    }

    /// <summary>
    /// Rebuilds the index by scanning <paramref name="table"/> and indexing keys per column.
    /// </summary>
    /// <param name="table">Table to scan as the source of truth.</param>
    public override void Rebuild(MetadataTable table)
    {
        ThrowIfDisposed();
        var cols = table.Spec.Columns;

        Rwls.EnterWriteLock();
        try
        {
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