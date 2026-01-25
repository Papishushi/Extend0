namespace Extend0.Metadata.Storage
{
    public readonly partial struct CellEnumerable
    {
        /// <summary>
        /// Async enumerator that delegates to the underlying synchronous enumerator.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This type does not perform background work. Each <see cref="MoveNextAsync"/> call executes synchronously
        /// and returns a completed <see cref="ValueTask{TResult}"/>.
        /// </para>
        /// <para>
        /// Cancellation is checked at the beginning of <see cref="MoveNextAsync"/> only.
        /// </para>
        /// </remarks>
        public readonly struct AsyncEnumerator : IAsyncEnumerator<CellRowColumnValueEntry>
        {
            private readonly Enumerator _e;
            private readonly CancellationToken _ct;

            /// <summary>
            /// Creates a new async enumerator.
            /// </summary>
            /// <param name="e">The underlying synchronous enumerator.</param>
            /// <param name="ct">A token used to cancel enumeration.</param>
            internal AsyncEnumerator(Enumerator e, CancellationToken ct)
            {
                _e = e;
                _ct = ct;
            }

            /// <summary>
            /// Gets the current element in the sequence.
            /// </summary>
            public readonly CellRowColumnValueEntry Current => _e.Current;

            /// <summary>
            /// Advances the enumerator asynchronously to the next element in the sequence.
            /// </summary>
            /// <returns>
            /// A task-like object that completes with <see langword="true"/> if the enumerator advanced
            /// to the next element; otherwise, <see langword="false"/>.
            /// </returns>
            /// <exception cref="OperationCanceledException">Thrown if cancellation was requested.</exception>
            public ValueTask<bool> MoveNextAsync()
            {
                _ct.ThrowIfCancellationRequested();
                return new ValueTask<bool>(_e.MoveNext());
            }

            /// <summary>
            /// Performs async cleanup of resources used by the enumerator.
            /// </summary>
            /// <remarks>
            /// The underlying enumerator is disposed and internal state is cleared.
            /// </remarks>
            public ValueTask DisposeAsync()
            {
                _e.Dispose();
                return default;
            }
        }
    }
}
