namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Represents a cell store that supports explicit compaction.
    /// </summary>
    /// <remarks>
    /// Compaction is an optional, implementation-defined operation intended to
    /// reduce the memory footprint of the store by releasing unused capacity and/or
    /// reorganizing internal data structures.
    ///
    /// <para>
    /// Compaction is expected to be a <b>best-effort</b> operation:
    /// implementations may partially compact or decide that no action is needed.
    /// </para>
    ///
    /// <para>
    /// Implementations are not required to be thread-safe. Callers must ensure
    /// exclusive access to the store while <see cref="Compact"/> is executing.
    /// </para>
    ///
    /// <para>
    /// Compaction must not change the logical contents of the store nor invalidate
    /// existing logical row/column semantics, but may invalidate internal buffers,
    /// cached spans, or previously obtained raw pointers.
    /// </para>
    /// </remarks>
    internal interface ICompactableStore : ICellStore
    {
        /// <summary>
        /// Attempts to compact the underlying storage to reduce memory usage.
        /// </summary>
        /// <remarks>
        /// Implementations should release unused capacity and/or reorganize internal
        /// data structures to improve memory locality. The exact strategy is
        /// implementation-defined.
        /// </remarks>
        void Compact();
    }
}
