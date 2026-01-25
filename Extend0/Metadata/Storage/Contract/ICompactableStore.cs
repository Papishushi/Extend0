namespace Extend0.Metadata.Storage.Contract
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
    public interface ICompactableStore : ICellStore
    {
        /// <summary>
        /// Compacts the underlying storage to reduce memory usage and improve locality.
        /// </summary>
        /// <param name="strict">
        /// When <see langword="true"/>, the implementation must enforce stricter compaction guarantees,
        /// such as fully releasing unused capacity when possible and/or performing deeper reorganization.
        /// When <see langword="false"/>, the implementation may apply a best-effort/lightweight compaction.
        /// </param>
        /// <param name="cancellationToken">Token used to cancel the compaction operation.</param>
        /// <returns>A task that completes when compaction has finished.</returns>
        /// <remarks>
        /// <para>
        /// Compaction is implementation-defined and may include releasing unused capacity, rebuilding internal
        /// structures, defragmenting, or rewriting backing storage to improve memory locality.
        /// </para>
        /// <para>
        /// Warning: compaction may relocate data. Any previously obtained spans, pointers, offsets, or cached views
        /// into the store may become invalid after this call. Do not hold unmanaged views across a compaction.
        /// </para>
        /// <para>
        /// Concurrency: implementations are expected to be thread-safe according to the store contract, but callers
        /// should assume compaction is a mutating operation and avoid concurrent readers/writers unless explicitly supported.
        /// </para>
        /// </remarks>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when <paramref name="strict"/> is <see langword="true"/> and strict compaction guarantees cannot be satisfied.
        /// </exception>
        Task Compact(bool strict, CancellationToken cancellationToken);
    }
}
