using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Refs
{
    /// <summary>
    /// Static helpers to manage a refs-vector stored inline in a parent cell VALUE buffer.
    /// </summary>
    public static class MetadataRefLinker
    {
        /// <summary>
        /// Ensures the refs-vector overlay exists for a given parent cell by initializing its header
        /// if the buffer is considered "virgin" and the policy allows it.
        /// </summary>
        /// <param name="parent">Parent table that owns the refs column.</param>
        /// <param name="refsCol">Zero-based refs column index in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index.</param>
        /// <param name="policy">Capacity policy applied when initialization is needed.</param>
        /// <remarks>
        /// This does not resize anything. It only initializes the VALUE header in-place.
        /// "Virgin" is treated as a buffer where both Count and Flags are zero (common after allocation/clear).
        /// If you need a stricter definition, switch to checking <see cref="MetadataTableRefVec.IsInitialized(System.ReadOnlySpan{byte})"/>.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void EnsureRefVec(MetadataTable parent, uint refsCol, uint parentRow, CapacityPolicy policy)
        {
            var cell = parent.GetOrCreateCell(refsCol, parentRow);

            if (cell.ValueSize < MetadataTableRefVec.HeaderSize)
            {
                if (policy == CapacityPolicy.Throw)
                    throw new InvalidOperationException($"Refs vector buffer too small at ({parent.Spec.Name}, col={refsCol}, row={parentRow}).");
                return;
            }

            var buf = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);

            // Treat "virgin" as Count==0 && Flags==0 (fast, no extra reads)
            // If you prefer: if (!MetadataTableRefVec.IsInitialized(buf)) { ... }
            if (MetadataTableRefVec.GetCount(buf) == 0 && !MetadataTableRefVec.IsInitialized(buf))
                MetadataTableRefVec.Init(buf, markInitialized: true);
        }

        /// <summary>
        /// Tries to find a child table reference in the parent's refs vector by a stable key stored in <see cref="MetadataTableRef.Reserved"/>.
        /// </summary>
        /// <param name="parent">Parent table that owns the refs column.</param>
        /// <param name="refsCol">Zero-based index of the refs column in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index.</param>
        /// <param name="childKey">Stable key that identifies the child within the row.</param>
        /// <param name="childTableId">Outputs the found child table id if present.</param>
        /// <returns><see langword="true"/> if found; otherwise <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool TryFindChildByKey(MetadataTable parent, uint refsCol, uint parentRow, uint childKey, out Guid childTableId)
        {
            var cell = parent.GetOrCreateCell(refsCol, parentRow);
            var buf = new ReadOnlySpan<byte>(cell.GetValuePointer(), cell.ValueSize);

            var items = MetadataTableRefVec.Items(buf, out _);
            for (int i = 0; i < items.Length; i++)
            {
                ref readonly var e = ref items[i];
                if (e.Reserved == childKey)
                {
                    childTableId = e.TableId;
                    return true;
                }
            }

            childTableId = default;
            return false;
        }

        /// <summary>
        /// Ensures that the parent's refs vector contains a link to the specified child, without inserting duplicates.
        /// </summary>
        /// <param name="parent">Parent table that owns the refs column.</param>
        /// <param name="refsCol">Zero-based refs column index in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index.</param>
        /// <param name="childTableId">Target child table id.</param>
        /// <param name="childCol">Target child column index.</param>
        /// <param name="childRow">Target child row index.</param>
        /// <param name="childKey">Stable key stored in <see cref="MetadataTableRef.Reserved"/>.</param>
        /// <param name="policy">Capacity policy for initializing the vector header.</param>
        /// <remarks>
        /// Idempotency here is enforced by the triple (<paramref name="childTableId"/>, <paramref name="childCol"/>, <paramref name="childRow"/>).
        /// If you want idempotency by key instead, replace the triple check with <see cref="TryFindChildByKey"/>.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the refs vector is full.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void EnsureLinkRefNoDup(MetadataTable parent, uint refsCol, uint parentRow, Guid childTableId, uint childCol, uint childRow, uint childKey, CapacityPolicy policy)
        {
            EnsureRefVec(parent, refsCol, parentRow, policy);

            var cell = parent.GetOrCreateCell(refsCol, parentRow);
            var buf = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);

            // Idempotency by triple
            if (MetadataTableRefVec.Find(buf, childTableId, childCol, childRow) >= 0)
                return;

            var r = new MetadataTableRef(childTableId, childCol, childRow, childKey);
            if (!MetadataTableRefVec.TryAdd(buf, in r, cell.ValueSize))
                throw new InvalidOperationException($"Refs vector full at ({parent.Spec.Name}, col={refsCol}, row={parentRow}).");
        }

        /// <summary>
        /// Ensures that the parent's refs vector contains a link for <paramref name="childKey"/>; if the key already exists, it does nothing.
        /// </summary>
        /// <param name="parent">Parent table that owns the refs column.</param>
        /// <param name="refsCol">Zero-based refs column index in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index.</param>
        /// <param name="childTableId">Target child table id.</param>
        /// <param name="childCol">Target child column index.</param>
        /// <param name="childRow">Target child row index.</param>
        /// <param name="childKey">Stable key stored in <see cref="MetadataTableRef.Reserved"/>.</param>
        /// <param name="policy">Capacity policy for initializing the vector header.</param>
        /// <remarks>
        /// This is the "N children per parentRow" semantic: uniqueness is by key, not by triple.
        /// If the key exists but points to a different table, you may want a Replace/Throw variant.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the refs vector is full.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void EnsureLinkRefNoDupByKey(MetadataTable parent, uint refsCol, uint parentRow, Guid childTableId, uint childCol, uint childRow, uint childKey, CapacityPolicy policy)
        {
            EnsureRefVec(parent, refsCol, parentRow, policy);

            if (TryFindChildByKey(parent, refsCol, parentRow, childKey, out _))
                return;

            var cell = parent.GetOrCreateCell(refsCol, parentRow);
            var buf = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);

            var r = new MetadataTableRef(childTableId, childCol, childRow, childKey);
            if (!MetadataTableRefVec.TryAdd(buf, in r, cell.ValueSize))
                throw new InvalidOperationException($"Refs vector full at ({parent.Spec.Name}, col={refsCol}, row={parentRow}).");
        }
    }
}
