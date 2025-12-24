using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Internal;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Indexing.Definitions;

/// <summary>
/// Base type for indexes whose contents can be rebuilt from a <see cref="MetadataTable"/>.
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
///   <item><description>Call <see cref="Rebuild(MetadataTable)"/> to populate it.</description></item>
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
public abstract class RebuildableIndexDefinition<TKey, TValue>(string name, IEqualityComparer<TKey>? comparer = null, int capacity = 0) 
    : IndexDefinition<TKey, TValue>(name, comparer, capacity), IRebuildableIndex where TKey : notnull
{
    private readonly IndexFixedSizeByteArrayPool _pool = new();

    /// <summary>
    /// Returns a previously rented key buffer to the underlying pool.
    /// </summary>
    /// <param name="key">The rented buffer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReturnPooledKey(byte[] key) => _pool.Return(key);

    /// <summary>
    /// Rebuilds the index by scanning the provided <paramref name="table"/>.
    /// </summary>
    /// <param name="table">The table to scan and use as the source of truth.</param>
    /// <remarks>
    /// <para>
    /// Implementations typically clear current state and repopulate it based on
    /// <see cref="MetadataTable.EnumerateCells"/> or other table-specific traversal APIs.
    /// </para>
    /// <para>
    /// This method is expected to be safe to call multiple times and should leave the index
    /// in a consistent state even if it was previously populated.
    /// </para>
    /// </remarks>
    public abstract void Rebuild(MetadataTable table);

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

        if (!entry.Cell.TryGetKey(out ReadOnlySpan<byte> k) || k.Length == 0)
        { owned = Array.Empty<byte>(); return false; }

        owned = _pool.RentExact(keySize);
        IndexKeyScratch.Fill(owned, k);
        return true;
    }
}
