using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Internal;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Indexing.Definitions;

/// <summary>
/// Base class for partitioned (cross-table) indexes that can be rebuilt from metadata tables and that optionally
/// pool fixed-size <see cref="byte"/>[] key buffers for low-allocation rebuild/mutation paths.
/// </summary>
/// <typeparam name="TInnerKey">
/// The key type used by the per-table dictionaries. When the runtime key type is <see cref="byte"/>[],
/// this definition provides fixed-size key materialization and pooling helpers.
/// </typeparam>
/// <typeparam name="TInnerValue">The value stored per key in each table partition.</typeparam>
/// <param name="name">Logical name identifying this index instance.</param>
/// <param name="keyComparer">
/// Optional comparer for inner key equality. For pooled binary keys this is typically a content comparer
/// (e.g. ordinal byte comparison) rather than reference equality.
/// </param>
/// <param name="tablesCapacity">Optional initial capacity for the outer partition map.</param>
/// <param name="perTableCapacity">Optional initial capacity for each inner per-table dictionary.</param>
/// <param name="keySize">
/// Fixed key size (in bytes) used for pooling/materialization helpers. Values &lt;= 0 default to 16.
/// </param>
/// <remarks>
/// <para>
/// Inherits the core partitioned index behavior from <see cref="CrossTableIndexBase{TInnerKey, TInnerValue}"/> and adds:
/// pooled fixed-size key infrastructure plus an explicit rebuild contract (<see cref="Rebuild"/>).
/// </para>
/// <para>
/// <b>Partitions</b>: the index is organized by table id (<see cref="Guid"/>). Each table id maps to an inner dictionary
/// of <typeparamref name="TInnerKey"/> to <typeparamref name="TInnerValue"/>.
/// </para>
/// <para>
/// <b>Pooling &amp; ownership (byte[] keys)</b>: when keys are stored as fixed-size <see cref="byte"/>[] buffers,
/// this type can rent buffers from an internal <see cref="IndexFixedSizeByteArrayPool"/>, copy/zero-pad keys to a stable
/// representation, and later return owned buffers on <see cref="Clear"/>, <see cref="ClearTable"/>, removals, or rebuild cleanup.
/// </para>
/// <para>
/// <b>Thread-safety</b>: cleanup operations in this type are performed under a write lock to prevent readers from observing
/// partially-cleared state. Rebuild implementations are expected to provide equivalent synchronization.
/// </para>
/// </remarks>
public abstract class CrossTableRebuildableIndexDefinition<TInnerKey, TInnerValue>(string name, IEqualityComparer<TInnerKey>? keyComparer = null, int tablesCapacity = 0, int perTableCapacity = 0, int keySize = 16) :
    CrossTableIndexBase<TInnerKey, TInnerValue>(name, keyComparer, tablesCapacity, perTableCapacity),
    ICrossTableRebuildableIndex<TInnerKey, TInnerValue> where TInnerKey : notnull
{
    private readonly int _cachedKeySize = keySize <= 0 ? 16 : keySize;

    /// <summary>Fixed key size used by this index.</summary>
    protected int CachedKeySize => _cachedKeySize;

    /// <summary>
    /// Shared pool of fixed-size <see cref="byte"/> arrays used for allocating and recycling key buffers
    /// during index rebuild and insertion operations.
    /// </summary>
    /// <remarks>
    /// This pool enables efficient reuse of key buffers across rebuild passes, avoiding heap allocations
    /// for every dictionary insertion or lookup involving binary keys. The pool guarantees that returned buffers
    /// match the requested size exactly and provides thread-safe access for concurrent usage.
    /// </remarks>
    private readonly IndexFixedSizeByteArrayPool _pool = new();

    /// <summary>
    /// Returns a previously rented key buffer to the underlying pool.
    /// </summary>
    /// <param name="key">The rented buffer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void ReturnPooledKey(byte[] key) => _pool.Return(key);

    /// <summary>
    /// Rebuilds the index by scanning the provided <paramref name="manager"/>.
    /// </summary>
    /// <param name="manager">The manager to scan and use as the source of truth.</param>
    /// <remarks>
    /// <para>
    /// Implementations typically clear current state and repopulate it based on
    /// <see cref="IMetadataTable.EnumerateCells"/> or other table-specific traversal APIs.
    /// </para>
    /// <para>
    /// This method is expected to be safe to call multiple times and should leave the index
    /// in a consistent state even if it was previously populated.
    /// </para>
    /// </remarks>
    public abstract Task Rebuild(IMetaDBManager manager);

    /// <summary>
    /// Clears the index partition for a single table and returns all pooled key buffers owned by that partition
    /// back to the internal key pool.
    /// </summary>
    /// <param name="tableId">The table identifier (partition key) whose partition should be removed.</param>
    /// <remarks>
    /// <para>
    /// This is a partition-level cleanup operation. It is typically used when a table is closed/unregistered,
    /// or when a per-table rebuild is about to occur.
    /// </para>
    /// <para>
    /// The operation is performed under a write lock so readers cannot observe a partially-cleared partition.
    /// </para>
    /// <para>
    /// Keys stored in this index are fixed-size pooled <see cref="byte"/>[] buffers owned by the index. During cleanup,
    /// each owned key buffer is returned to the internal pool to avoid memory pressure and pool leaks.
    /// </para>
    /// <para>
    /// This method assumes the partition exists. If <paramref name="tableId"/> is not present, the underlying access
    /// (<c>Index[tableId]</c>) may throw (for example, <see cref="KeyNotFoundException"/>).
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override void ClearTable(Guid tableId)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            // Return all owned (pooled) keys for this partition.
            foreach (var key in Index[tableId].Keys)
                if (key is byte[] keyBytes)
                    ReturnPooledKey(keyBytes);

            Index.Remove(tableId);
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Clears the entire index across all partitions and returns all pooled key buffers owned by the index
    /// back to the internal key pool.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the strongest cleanup operation: it removes every table partition and releases all pooled key buffers
    /// currently held by the index.
    /// </para>
    /// <para>
    /// The operation is performed under a write lock to ensure no concurrent lookups can observe partially-cleared state.
    /// </para>
    /// <para>
    /// Keys stored in this index are fixed-size pooled <see cref="byte"/>[] buffers owned by the index. During cleanup,
    /// each owned key buffer is returned to the pool to avoid memory pressure and pool leaks.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override void Clear()
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            foreach (var tableMap in Index.Values)
                foreach (var key in tableMap.Keys)
                    if (key is byte[] keyBytes)
                        ReturnPooledKey(keyBytes);

            Index.Clear();
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Materializes a fixed-size lookup key into a per-thread scratch buffer, suitable for
    /// dictionary lookups against indexes that store keys as fixed-size <see cref="byte"/> arrays.
    /// </summary>
    /// <param name="key">
    /// The source key bytes to look up. If shorter than <paramref name="fixedSize"/>, the key is
    /// copied and the remaining trailing bytes are zero-filled so the padded representation matches
    /// the stored key shape.
    /// </param>
    /// <param name="fixedSize">
    /// The exact key size (in bytes) used by the index (typically dictated by schema <c>KeySize</c>).
    /// The returned array length is always exactly this value.
    /// </param>
    /// <returns>
    /// A thread-local scratch <see cref="byte"/> array of length <paramref name="fixedSize"/> containing
    /// <paramref name="key"/> followed by zero padding.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method exists to enable allocation-free lookups when the index uses <c>byte[]</c> keys but
    /// the query key is provided as a <see cref="ReadOnlySpan{T}"/>. Since fixed-size key indexes compare
    /// the whole array, any unused tail bytes must be zeroed to ensure equality matches the persisted
    /// representation (copy + zero-fill).
    /// </para>
    /// <para>
    /// <b>Scratch lifetime:</b> the returned array is <em>ephemeral</em> and reused on the calling thread.
    /// Do not store it, return it, or use it beyond the immediate lookup. A subsequent call on the same
    /// thread may overwrite its contents.
    /// </para>
    /// <para>
    /// <b>Preconditions:</b> <paramref name="key"/> length must be less than or equal to
    /// <paramref name="fixedSize"/>. If not, the underlying copy will throw.
    /// </para>
    /// <para>
    /// Thread safety: the buffer is thread-local by design; no cross-thread synchronization is performed.
    /// </para>
    /// </remarks>
    /// <exception cref="NotSupportedException">
    /// Thrown if the index key type is not <see cref="byte"/>[].
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown if <paramref name="fixedSize"/> is not a valid positive size for a fixed key.
    /// </exception>
    protected static byte[] GetScratchLookupKey(ReadOnlySpan<byte> key, int fixedSize)
    {
        if (typeof(TInnerKey) != typeof(byte[]))
            throw new NotSupportedException($"Scratch lookup is only valid for byte[] keys.");

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(fixedSize);
        var lookupKey = Internal.IndexKeyScratch.GetScratch(fixedSize);
        Internal.IndexKeyScratch.Fill(lookupKey, key);
        return lookupKey;
    }

    /// <summary>
    /// Attempts to rent (or reuse) a byte array sized for the column key and copy the cell key into it,
    /// producing an owned <see cref="byte"/>[] suitable for use as a dictionary key.
    /// </summary>
    /// <param name="entry">The enumerated table entry providing the column id and the cell to read the key from.</param>
    /// <param name="owned">
    /// When this method returns <see langword="true"/>, contains a pooled buffer of length <c>keySize</c>
    /// filled with the cell key (and zero-padded to the fixed size). The caller owns this buffer and is
    /// responsible for returning it to <see cref="ReturnPooledKey(byte[])"/> when it is no longer needed.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the entry belongs to a key/value column and the cell exposes a non-empty key;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This helper is designed to avoid per-entry allocations during index rebuild by renting fixed-size buffers
    /// from a pool instead of calling <see cref="ReadOnlySpan{T}.ToArray"/>.
    /// </para>
    /// <para>
    /// The key size used is the fixed key size declared in the column spec (<c>cols[col].Size.GetKeySize()</c>),
    /// not the current <c>k.Length</c>. The rented buffer is always exactly <c>keySize</c>, and any unused tail
    /// bytes are cleared to zero via <see cref="IndexKeyScratch.Fill(byte[], ReadOnlySpan{byte})"/> so that
    /// dictionary comparisons are stable and deterministic.
    /// </para>
    /// <para>
    /// Value-only columns (<c>keySize == 0</c>) are skipped. Cells that do not expose a key or expose an empty key
    /// are also skipped.
    /// </para>
    /// <para>
    /// This method does not take locks; callers should ensure appropriate synchronization if required by the
    /// calling context.
    /// </para>
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">
    /// May be thrown if <paramref name="entry"/> contains an invalid column index for <paramref name="cols"/>.
    /// </exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Style",
        "IDE0301",
        Justification = "Hot-path: prefer Array.Empty<T>() to avoid any ambiguity in lowering/semantics and keep allocation behavior explicit.")]
    protected bool TryRentKey(Storage.CellRowColumnValueEntry entry, out byte[] owned)
    {
        if (!entry.Cell.HasKeyRaw() || !entry.Cell.TryGetKeyRaw(out ReadOnlySpan<byte> k) || k.Length == 0)
        { owned = Array.Empty<byte>(); return false; }

        if ((uint)k.Length > (uint)_cachedKeySize)
        { owned = Array.Empty<byte>(); return false; }

        owned = _pool.RentExact(_cachedKeySize);
        IndexKeyScratch.Fill(owned, k); // copy + zero-pad
        return true;
    }

    /// <summary>
    /// Attempts to rent a pooled fixed-size key buffer and copy the provided key into it.
    /// </summary>
    /// <param name="key">
    /// The input key bytes. May be shorter than the configured fixed size; in that case the returned buffer is
    /// zero-padded to <c>CachedKeySize</c>. If the input is longer than <c>CachedKeySize</c>, the method fails.
    /// </param>
    /// <param name="owned">
    /// When this method returns <see langword="true"/>, contains a pooled buffer of length <c>CachedKeySize</c>
    /// filled with the contents of <paramref name="key"/> and padded with trailing zeros if needed.
    /// The caller becomes the temporary owner of this buffer and must return it to the pool via
    /// <see cref="ReturnPooledKey(byte[])"/> if the buffer is not inserted/retained by the index.
    /// When this method returns <see langword="false"/>, contains <see cref="Array.Empty{T}"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if a pooled buffer was rented and populated; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This helper centralizes the fixed-size key materialization used by rebuild and mutation paths.
    /// It avoids heap allocations by renting buffers from an internal pool.
    /// </para>
    /// <para>
    /// The returned buffer is always exactly <c>CachedKeySize</c> bytes to ensure stable dictionary semantics
    /// (content comparisons become deterministic because unused tail bytes are zeroed).
    /// </para>
    /// <para>
    /// This method performs no locking. Callers must ensure appropriate synchronization when required.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">
    /// Thrown if the underlying pool/index instance has been disposed and the pool cannot service the request.
    /// </exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Style",
        "IDE0301",
        Justification = "Hot-path: prefer Array.Empty<T>() to avoid any ambiguity in lowering/semantics and keep allocation behavior explicit.")]
    protected bool TryRentKey(byte[] key, out byte[] owned)
    {
        if (key.Length == 0)
        { owned = Array.Empty<byte>(); return false; }

        if ((uint)key.Length > (uint)_cachedKeySize)
        { owned = Array.Empty<byte>(); return false; }

        owned = _pool.RentExact(_cachedKeySize);
        IndexKeyScratch.Fill(owned, key); // copy + zero-pad
        return true;
    }
}
