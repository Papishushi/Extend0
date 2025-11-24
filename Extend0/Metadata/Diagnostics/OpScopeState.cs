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
        private readonly DateTimeOffset _ts = DateTimeOffset.UtcNow;

        public int Count => state is null ? 2 : 3;

        public KeyValuePair<string, object?> this[int index] => index switch
        {
            0 => new("op", op),
            1 => new("ts", _ts),
            2 => new("state", state),
            _ => throw new ArgumentOutOfRangeException(nameof(index))
        };

        public Enumerator GetEnumerator() => new(this);

        IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator() => GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public struct Enumerator(OpScopeState s) : IEnumerator<KeyValuePair<string, object?>>
        {
            private int _i = -1;

            public readonly KeyValuePair<string, object?> Current => s[_i];
            readonly object IEnumerator.Current => Current;
            public bool MoveNext() { _i++; return _i < s.Count; }
            public void Reset() { _i = -1; }
            public readonly void Dispose() { }
        }
    }
}
