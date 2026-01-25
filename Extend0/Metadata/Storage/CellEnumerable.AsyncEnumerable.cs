namespace Extend0.Metadata.Storage
{
    public readonly partial struct CellEnumerable
    {
        /// <summary>
        /// Async-compatible wrapper around <see cref="CellEnumerable"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The wrapper preserves ordering and filtering semantics of <see cref="GetEnumerator"/>:
        /// it yields only cells that exist and does not create missing cells.
        /// </para>
        /// <para>
        /// Cancellation is honored by the returned <see cref="AsyncEnumerator"/> on each move operation.
        /// </para>
        /// </remarks>
        public readonly struct AsyncEnumerable : IAsyncEnumerable<CellRowColumnValueEntry>
        {
            private readonly CellEnumerable _src;

            /// <summary>
            /// Initializes an async wrapper over a source <see cref="CellEnumerable"/>.
            /// </summary>
            /// <param name="src">The source enumerable to expose as async.</param>
            internal AsyncEnumerable(CellEnumerable src) => _src = src;

            /// <summary>
            /// Returns an async enumerator that delegates to the underlying synchronous enumerator.
            /// </summary>
            /// <param name="cancellationToken">A token used to cancel enumeration.</param>
            /// <returns>An async enumerator for the wrapped source.</returns>
            public AsyncEnumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
                => new(_src.GetEnumerator(), cancellationToken);

            /// <inheritdoc />
            IAsyncEnumerator<CellRowColumnValueEntry> IAsyncEnumerable<CellRowColumnValueEntry>.GetAsyncEnumerator(
                CancellationToken cancellationToken)
                => GetAsyncEnumerator(cancellationToken);
        }
    }
}
