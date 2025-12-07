using Microsoft.Extensions.Logging;
using System;
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
    public readonly struct OpScopeState(string op, object? state) : IReadOnlyList<KeyValuePair<string, object?>>, IEquatable<OpScopeState>
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
        /// Gets the opaque state payload associated with this logging scope.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This value is optional and may be <see langword="null"/>. When present, it is exposed
        /// as the third structured field (<c>"state"</c>) in the scope.
        /// </para>
        /// <para>
        /// The <see cref="State"/> value is passed through unmodified and may be of any type,
        /// typically an object carrying contextual details for the MetaDB operation.
        /// </para>
        /// </remarks>
        public readonly object? State => state;

        /// <summary>
        /// Gets the operation identifier associated with this scope.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a short, human-readable string describing the MetaDB operation being executed
        /// (e.g., <c>"table.create"</c>, <c>"column.write"</c>, <c>"flush"</c>).
        /// </para>
        /// <para>
        /// The operation name is always included and forms the first structured key–value entry
        /// (<c>"op"</c>) exposed by this scope to logging infrastructure.
        /// </para>
        /// </remarks>
        public readonly string Op => op;

        /// <summary>
        /// Gets the UTC timestamp captured at the moment this scope state was created.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This timestamp is generated once per <see cref="OpScopeState"/> instance at construction
        /// time and remains constant for the lifetime of the struct.
        /// </para>
        /// <para>
        /// It is emitted as the structured field <c>"ts"</c> when the scope is consumed by the
        /// logging pipeline, allowing log aggregators to correlate the start time of MetaDB
        /// operations with their associated messages.
        /// </para>
        /// </remarks>
        public readonly DateTimeOffset Ts => _ts;

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
        /// Indicates whether the current <see cref="OpScopeState"/> instance is equal
        /// to another <see cref="OpScopeState"/>.
        /// </summary>
        /// <param name="other">The other scope state to compare with.</param>
        /// <returns>
        /// <see langword="true"/> if both instances have the same operation name, timestamp
        /// and state payload; otherwise, <see langword="false"/>.
        /// </returns>
        public bool Equals(OpScopeState other) =>
            string.Equals(op, other.Op, StringComparison.Ordinal) &&
            Equals(state, other.State) &&
            _ts.Equals(other._ts);

        /// <summary>
        /// Determines whether the specified object is equal to the current
        /// <see cref="OpScopeState"/> instance.
        /// </summary>
        /// <param name="obj">The object to compare with this instance.</param>
        /// <returns>
        /// <see langword="true"/> if <paramref name="obj"/> is an <see cref="OpScopeState"/>
        /// and represents the same operation, timestamp and state; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        public override bool Equals(object? obj) => obj is OpScopeState other && Equals(other);

        /// <summary>
        /// Compares two <see cref="OpScopeState"/> instances for value equality.
        /// </summary>
        /// <param name="left">The first instance to compare.</param>
        /// <param name="right">The second instance to compare.</param>
        /// <returns>
        /// <see langword="true"/> if both instances are equal; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        public static bool operator ==(OpScopeState left, OpScopeState right) => left.Equals(right);

        /// <summary>
        /// Compares two <see cref="OpScopeState"/> instances for inequality.
        /// </summary>
        /// <param name="left">The first instance to compare.</param>
        /// <param name="right">The second instance to compare.</param>
        /// <returns>
        /// <see langword="true"/> if the instances are not equal; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        public static bool operator !=(OpScopeState left, OpScopeState right) => !left.Equals(right);

        /// <summary>
        /// Returns a hash code for this <see cref="OpScopeState"/> instance.
        /// </summary>
        /// <returns>
        /// A 32-bit signed integer hash code combining the operation name,
        /// timestamp and optional state payload.
        /// </returns>
        public override int GetHashCode()
        {
            // op is a string (use ordinal semantics), state can be null or any object,
            // and _ts is a value type. HashCode.Combine handles null correctly.
            int opHash = op is null ? 0 : StringComparer.Ordinal.GetHashCode(op);
            return HashCode.Combine(opHash, state, _ts);
        }

        /// <summary>
        /// Struct-based enumerator over the key–value pairs of an <see cref="OpScopeState"/>.
        /// </summary>
        /// <remarks>
        /// This is a <c>record struct</c>, so value equality and hash code are generated
        /// automatically from its fields (<see cref="State"/> and <see cref="Index"/>).
        /// </remarks>
        public record struct Enumerator(OpScopeState State) : IEquatable<Enumerator>, IEnumerator<KeyValuePair<string, object?>>
        {
            private int _i = -1;

            /// <summary>
            /// Gets the current zero-based index of the enumerator within the scope state.
            /// </summary>
            public readonly int Index => _i;

            /// <summary>
            /// Gets the key–value pair at the current position of the enumerator.
            /// </summary>
            public KeyValuePair<string, object?> Current => State[_i];

            /// <inheritdoc />
            object IEnumerator.Current => Current;

            /// <inheritdoc />
            public bool MoveNext()
            {
                _i++;
                return _i < State.Count;
            }

            /// <inheritdoc />
            public void Reset() => _i = -1;

            /// <inheritdoc />
            public readonly void Dispose()
            {
                // No unmanaged resources to release.
            }
        }
    }
}