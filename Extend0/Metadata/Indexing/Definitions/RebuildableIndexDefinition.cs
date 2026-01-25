using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Internal;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Indexing.Definitions;

/// <summary>
/// Base type for indexes whose contents can be rebuilt from a <see cref="IMetadataTable"/>.
/// </summary>
/// <typeparam name="TKey">Index key type.</typeparam>
/// <typeparam name="TValue">Index value type.</typeparam>
/// <remarks>
/// <para>
/// A rebuildable index supports re-populating its internal state by scanning a table,
/// typically after loading data, bulk imports, or when the index is treated as ephemeral.
/// </para>
/// <para>
/// The typical lifecycle is:
/// <list type="number">
///   <item><description>Create and register the index in a registry.</description></item>
///   <item><description>Call <see cref="Rebuild(IMetadataTable)"/> to populate it.</description></item>
///   <item><description>Use index-specific lookup APIs in hot paths.</description></item>
///   <item><description>Call <see cref="ITableIndex.Clear"/> when indexes are considered ephemeral.</description></item>
/// </list>
/// </para>
/// <para>
/// Implementers should:
/// <list type="bullet">
///   <item><description>Call <see cref="ThrowIfDisposed"/> before accessing state.</description></item>
///   <item><description>Use <see cref="IndexBase{TKey, TValue}.Rwls"/> to guard rebuild/lookup operations.</description></item>
///   <item><description>Define deterministic conflict semantics (e.g., last-wins) for duplicate keys.</description></item>
/// </list>
/// </para>
/// </remarks>
public abstract class RebuildableIndexDefinition<TKey, TValue>(string name, IEqualityComparer<TKey>? comparer = null, int capacity = 0) :
    IndexBase<TKey, TValue>(name, comparer, capacity),
    IRebuildableIndex<TKey, TValue> where TKey : notnull
{
    private readonly IndexFixedSizeByteArrayPool _pool = new();

    /// <summary>
    /// Returns a previously rented key buffer to the underlying pool.
    /// </summary>
    /// <param name="key">The rented buffer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void ReturnPooledKey(byte[] key) => _pool.Return(key);

    /// <summary>
    /// Rebuilds the index by scanning the provided <paramref name="table"/>.
    /// </summary>
    /// <param name="table">The table to scan and use as the source of truth.</param>
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
    public abstract Task Rebuild(IMetadataTable table);

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
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown if <paramref name="fixedSize"/> is not a valid positive size for a fixed key.
    /// </exception>
    protected static byte[] GetScratchLookupKey(ReadOnlySpan<byte> key, int fixedSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(fixedSize);
        var lookupKey = Internal.IndexKeyScratch.GetScratch(fixedSize);
        Internal.IndexKeyScratch.Fill(lookupKey, key);
        return lookupKey;
    }

    /// <summary>
    /// Attempts to rent (or reuse) a byte array sized for the column key and copy the cell key into it,
    /// producing an owned <see cref="byte"/>[] suitable for use as a dictionary key.
    /// </summary>
    /// <param name="cols">Table column definitions used to obtain the fixed key size for each column.</param>
    /// <param name="entry">The enumerated table entry providing the column id and the cell to read the key from.</param>
    /// <param name="owned">
    /// When this method returns <see langword="true"/>, contains a pooled buffer of length <c>keySize</c>
    /// filled with the cell key (and zero-padded to the fixed size). The caller owns this buffer and is
    /// responsible for returning it to <see cref="Pool"/> when it is no longer needed (typically when clearing
    /// or rebuilding the index).
    /// When this method returns <see langword="false"/>, contains <see cref="Array.Empty{T}"/>.
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
    /// <para>
    /// Style note: <see cref="Array.Empty{T}"/> is used instead of the C# 12 collection expression (<c>[]</c>)
    /// to avoid any ambiguity in lowering/semantics in performance-sensitive code paths (see IDE0301 suppression).
    /// </para>
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">
    /// May be thrown if <paramref name="entry"/> contains an invalid column index for <paramref name="cols"/>.
    /// </exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Style",
        "IDE0301",
        Justification = "Hot-path: prefer Array.Empty<T>() to avoid any ambiguity in lowering/semantics and keep allocation behavior explicit.")]
    protected bool TryRentKey(Schema.ColumnConfiguration[] cols, Storage.CellRowColumnValueEntry entry, out byte[] owned)
    {
        var keySize = cols[(int)entry.Col].Size.GetKeySize();
        if (keySize == 0) { owned = Array.Empty<byte>(); return false; }

        if (!entry.Cell.HasKeyRaw() || !entry.Cell.TryGetKeyRaw(out ReadOnlySpan<byte> k) || k.IsEmpty)
            { owned = Array.Empty<byte>(); return false; }

        owned = _pool.RentExact(keySize);
        IndexKeyScratch.Fill(owned, k);
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
    /// The returned buffer is always exactly <paramref name="keySize"/> bytes to ensure stable dictionary semantics
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
    protected bool TryRentKey(int keySize, byte[] key, out byte[] owned)
    {
        if (key.Length == 0)
        { owned = Array.Empty<byte>(); return false; }

        if ((uint)key.Length > (uint)keySize)
        { owned = Array.Empty<byte>(); return false; }

        owned = _pool.RentExact(keySize);
        IndexKeyScratch.Fill(owned, key); // copy + zero-pad
        return true;
    }
}
