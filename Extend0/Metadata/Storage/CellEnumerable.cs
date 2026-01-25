using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Storage.Contract;
using Extend0.Metadata.Storage.Internal;
using System.Collections;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Lightweight enumerable over all cells in an <see cref="ICellStore"/>.
    /// </summary>
    /// <remarks>
    /// This struct abstracts over the concrete store implementation and can iterate
    /// both <see cref="InMemoryStore"/> and <see cref="MappedStore"/> instances.
    /// Instances are value-type views over a specific backing store.
    /// </remarks>
    public readonly partial struct CellEnumerable : IEquatable<CellEnumerable>, IEnumerable<CellRowColumnValueEntry>
    {
        private readonly ICellStore? _store;

        /// <summary>
        /// Creates an enumerable for a <see cref="ICellStore"/> store.
        /// </summary>
        /// <param name="store">The <see cref="ICellStore"/> to enumerate.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="store"/> is not a supported implementation
        /// (<see cref="InMemoryStore"/> or <see cref="MappedStore"/>).
        /// </exception>
        public CellEnumerable(ICellStore store)
        {
            if (store is InMemoryStore memStore) _store = memStore;
            else if (store is MappedStore mappedStore) _store = mappedStore;
            else throw new ArgumentException("Unsupported ICellStore implementation.", nameof(store));
        }

        /// <summary>
        /// Determines whether this instance and another <see cref="CellEnumerable"/>
        /// represent enumeration over the same underlying store instance.
        /// </summary>
        /// <param name="other">The other <see cref="CellEnumerable"/> to compare with.</param>
        /// <returns>
        /// <see langword="true"/> if both enumerables target the same <see cref="ICellStore"/>
        /// instance (including both being default/unbound); otherwise, <see langword="false"/>.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(CellEnumerable other) => ReferenceEquals(_store, other._store); // ReferenceEquals(null, null) == true, so this also covers both as 'default'.

        /// <summary>
        /// Returns a struct enumerator that iterates all cells in the underlying store.
        /// </summary>
        /// <remarks>
        /// The iteration walks row by row within each column, in column index order,
        /// yielding a tuple containing the column index, row index and <see cref="MetadataCell"/>.
        /// </remarks>
        /// <returns>
        /// An <see cref="Enumerator"/> positioned before the first cell.
        /// </returns>
        public Enumerator GetEnumerator() => _store switch
        {
            MappedStore mapped => Enumerator.ForMapped(mapped),
            InMemoryStore mem => Enumerator.ForMemory(mem),
            null => default, // Move next will return false
            _ => throw new InvalidOperationException("Unsupported ICellStore implementation.")
        };

        /// <summary>
        /// Returns an enumerator that iterates through all cells in the store.
        /// </summary>
        /// <returns>
        /// An enumerator that can be used in <c>foreach</c> loops to iterate
        /// over <c>(Col, Row, Cell)</c> triples.
        /// </returns>
        IEnumerator<CellRowColumnValueEntry> IEnumerable<CellRowColumnValueEntry>.GetEnumerator()
        {
            var e = GetEnumerator();
            while (e.MoveNext())
                yield return e.Current;
        }

        /// <summary>
        /// Returns a non-generic enumerator that iterates through all cells in the store.
        /// </summary>
        /// <returns>
        /// A non-generic <see cref="IEnumerator"/> wrapping the struct enumerator.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<CellRowColumnValueEntry>)this).GetEnumerator();

        /// <summary>
        /// Determines whether this instance and a specified object, which must also be
        /// a <see cref="CellEnumerable"/>, represent the same underlying store.
        /// </summary>
        /// <param name="obj">The object to compare with this instance.</param>
        /// <returns>
        /// <see langword="true"/> if <paramref name="obj"/> is a <see cref="CellEnumerable"/>
        /// that targets the same store; otherwise, <see langword="false"/>.
        /// </returns>
        public override bool Equals(object? obj) => obj is CellEnumerable e && Equals(e);

        /// <summary>
        /// Determines whether two <see cref="CellEnumerable"/> instances are equal.
        /// </summary>
        /// <param name="left">The first value to compare.</param>
        /// <param name="right">The second value to compare.</param>
        /// <returns>
        /// <see langword="true"/> if both enumerables target the same store; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        public static bool operator ==(CellEnumerable left, CellEnumerable right) => left.Equals(right);

        /// <summary>
        /// Determines whether two <see cref="CellEnumerable"/> instances are not equal.
        /// </summary>
        /// <param name="left">The first value to compare.</param>
        /// <param name="right">The second value to compare.</param>
        /// <returns>
        /// <see langword="true"/> if the enumerables target different stores; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        public static bool operator !=(CellEnumerable left, CellEnumerable right) => !(left == right);

        /// <summary>
        /// Returns a hash code for this <see cref="CellEnumerable"/>.
        /// </summary>
        /// <returns>
        /// A hash code derived from the identity and kind of the underlying store
        /// (in-memory, mapped or none).
        /// </returns>
        /// <remarks>
        /// Two <see cref="CellEnumerable"/> instances that are considered equal by
        /// <see cref="Equals(CellEnumerable)"/> are guaranteed to produce the same hash code.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            if (_store is null)
                return 0; // default enumerable

            unchecked
            {
                int hash = 17;

                if (_store is InMemoryStore)
                    hash = (hash * 31) + 1; // mode: memory
                else if (_store is MappedStore)
                    hash = (hash * 31) + 2; // mode: mapped
                else
                    hash = (hash * 31) + 3; // some future ICellStore

                hash = (hash * 31) + RuntimeHelpers.GetHashCode(_store);
                return hash;
            }
        }

        /// <summary>
        /// Wraps this synchronous cell enumerable as an <see cref="IAsyncEnumerable{T}"/> so it can be consumed with
        /// <c>await foreach</c>.
        /// </summary>
        /// <remarks>
        /// This does not make the underlying enumeration truly asynchronous; it only provides an async-compatible
        /// surface while preserving the exact iteration semantics of the synchronous enumerator.
        /// </remarks>
        public AsyncEnumerable AsAsync() => new(this);
    }
}
