using Microsoft.Extensions.Logging;
using System.Collections;

namespace Extend0.Metadata.Diagnostics
{
    /// <summary>
    /// Lightweight, allocation-free scope state used by <see cref="ILogger.BeginScope{TState}(TState)"/>
    /// for MetaDB operations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Exposes <c>op</c>, <c>ts</c> (UTC timestamp) and optional <c>state</c> as structured
    /// key–value pairs without allocating a <see cref="Dictionary{TKey, TValue}"/>.
    /// </para>
    /// <para>
    /// This is consumed by <see cref="OpScope"/> and the <see cref="LoggerOps.BeginOp"/>
    /// extension to provide cheap, structured logging scopes.
    /// </para>
    /// </remarks>
    public readonly struct OpScopeState(string op, object? state) : IReadOnlyList<KeyValuePair<string, object?>>
    {
        /// <summary>
        /// UTC timestamp captured at construction time for this scope state.
        /// </summary>
        private readonly DateTimeOffset _ts = DateTimeOffset.UtcNow;

        /// <summary>
        /// Gets the number of key–value pairs exposed by this scope state.
        /// </summary>
        /// <remarks>
        /// Returns <c>2</c> when <c>state</c> is <see langword="null"/> (only <c>op</c> and <c>ts</c>),
        /// or <c>3</c> when a non-null <c>state</c> object is present.
        /// </remarks>
        public int Count => state is null ? 2 : 3;

        /// <summary>
        /// Gets the key–value pair at the specified index.
        /// </summary>
        /// <param name="index">
        /// Zero-based index of the entry to retrieve:
        /// <list type="bullet">
        ///   <item><description><c>0</c> → <c>("op", op)</c></description></item>
        ///   <item><description><c>1</c> → <c>("ts", ts)</c> (UTC timestamp)</description></item>
        ///   <item><description><c>2</c> → <c>("state", state)</c> (only when <c>state</c> is not <see langword="null"/>)</description></item>
        /// </list>
        /// </param>
        /// <returns>The requested key–value pair.</returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="index"/> is outside the valid range for the current instance.
        /// </exception>
        public KeyValuePair<string, object?> this[int index] => index switch
        {
            0 => new("op", op),
            1 => new("ts", _ts),
            2 => new("state", state),
            _ => throw new ArgumentOutOfRangeException(nameof(index))
        };

        /// <summary>
        /// Returns a value-type enumerator that iterates over the
        /// key–value pairs exposed by this scope state.
        /// </summary>
        /// <returns>An <see cref="Enumerator"/> for this instance.</returns>
        public Enumerator GetEnumerator() => new(this);

        /// <summary>
        /// Returns a generic enumerator that iterates over the key–value pairs
        /// in this scope state.
        /// </summary>
        /// <returns>
        /// An <see cref="IEnumerator{T}"/> for <see cref="KeyValuePair{TKey, TValue}"/>
        /// entries representing the structured scope data.
        /// </returns>
        IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Returns a non-generic enumerator that iterates over the key–value pairs
        /// in this scope state.
        /// </summary>
        /// <returns>
        /// An <see cref="IEnumerator"/> for the structured scope data.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Struct-based enumerator over the key–value pairs of an <see cref="OpScopeState"/>.
        /// </summary>
        /// <remarks>
        /// This enumerator is allocation-free and designed for use by logging infrastructure
        /// when consuming scope state for structured logging.
        /// </remarks>
        public struct Enumerator(OpScopeState s) : IEnumerator<KeyValuePair<string, object?>>
        {
            private int _i = -1;

            /// <summary>
            /// Gets the key–value pair at the current position of the enumerator.
            /// </summary>
            public readonly KeyValuePair<string, object?> Current => s[_i];

            /// <summary>
            /// Gets the current element in the collection.
            /// </summary>
            /// <returns>The current element as an <see cref="object"/>.</returns>
            readonly object IEnumerator.Current => Current;

            /// <summary>
            /// Advances the enumerator to the next element of the collection.
            /// </summary>
            /// <returns>
            /// <see langword="true"/> if the enumerator was successfully advanced
            /// to the next element; <see langword="false"/> if the enumerator has
            /// passed the end of the collection.
            /// </returns>
            public bool MoveNext()
            {
                _i++;
                return _i < s.Count;
            }

            /// <summary>
            /// Sets the enumerator to its initial position, which is before
            /// the first element in the collection.
            /// </summary>
            public void Reset() => _i = -1;

            /// <summary>
            /// Performs application-defined tasks associated with freeing,
            /// releasing, or resetting unmanaged resources.
            /// </summary>
            /// <remarks>
            /// This enumerator does not hold unmanaged resources, so this method
            /// is a no-op.
            /// </remarks>
            public readonly void Dispose() { /*Nothing to do here*/ }
        }
    }
}
