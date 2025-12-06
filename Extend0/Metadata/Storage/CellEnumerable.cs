using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;
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
    public readonly struct CellEnumerable : IEquatable<CellEnumerable>, IEnumerable<CellRowColumnValueEntry>
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
        internal CellEnumerable(ICellStore store)
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
        /// Struct-based enumerator over metadata cells.
        /// </summary>
        /// <remarks>
        /// The enumerator supports both in-memory and memory-mapped stores and
        /// yields the current cell as <c>(Col, Row, Cell)</c>.
        /// </remarks>
        public struct Enumerator : IEquatable<Enumerator>, IEnumerator<CellRowColumnValueEntry>
        {
            // shared state
            private uint _c, _r;
            private uint _colCount;

            // mode
            private InMemoryStore? _mem;
            private MappedStore? _mapped;

            /// <summary>
            /// Gets the current cell triple: column index, row index and cell instance.
            /// </summary>
            public CellRowColumnValueEntry Current { get; private set; }

            readonly object IEnumerator.Current => Current;

            /// <summary>
            /// Creates an enumerator bound to an in-memory store.
            /// </summary>
            /// <param name="s">The <see cref="InMemoryStore"/> to iterate.</param>
            /// <returns>
            /// A new <see cref="Enumerator"/> that will enumerate all cells in <paramref name="s"/>.
            /// </returns>
            internal static Enumerator ForMemory(InMemoryStore s) => new()
            {
                _mem = s,
                _mapped = null,
                _c = 0,
                _r = uint.MaxValue, // start before first
                _colCount = s.ColumnCount,
            };

            /// <summary>
            /// Creates an enumerator bound to a memory-mapped store.
            /// </summary>
            /// <param name="s">The <see cref="MappedStore"/> to iterate.</param>
            /// <returns>
            /// A new <see cref="Enumerator"/> that will enumerate all cells in <paramref name="s"/>.
            /// </returns>
            internal static Enumerator ForMapped(MappedStore s) => new()
            {
                _mem = null,
                _mapped = s,
                _c = 0,
                _r = uint.MaxValue, // start before first
                _colCount = s.ColumnCount
            };

            /// <summary>
            /// Advances the enumerator to the next cell, if any.
            /// </summary>
            /// <returns>
            /// <see langword="true"/> if a new cell is available in <see cref="Current"/>;
            /// otherwise, <see langword="false"/> when the enumeration is complete.
            /// </returns>
            public bool MoveNext()
            {
                if (_mapped is not null) return MoveNextMapped();
                if (_mem is not null) return MoveNextMem();
                return false; // default enumerator with no store
            }

            /// <summary>
            /// Advances the enumerator for an <see cref="InMemoryStore"/>.
            /// </summary>
            /// <remarks>
            /// Iterates all columns and rows using <see cref="InMemoryStore.MetaAt(uint)"/> and
            /// <see cref="InMemoryStore.GetOrCreateCell(uint,uint,in ColumnConfiguration)"/>.
            /// </remarks>
            /// <returns>
            /// <see langword="true"/> if a new cell is available in <see cref="Current"/>;
            /// otherwise, <see langword="false"/> when the enumeration is complete.
            /// </returns>
            private bool MoveNextMem()
            {
                var s = _mem!;
                // first advance
                if (_r == uint.MaxValue)
                {
                    if (_colCount == 0) return false;
                    _r = 0;
                }
                else
                {
                    _r++;
                }

                // carry rows/cols
                while (_c < _colCount && _r >= s.MetaAt(_c).InitialCapacity)
                {
                    _c++;
                    if (_c >= _colCount) return false;
                    _r = 0;
                }
                if (_c >= _colCount) return false;

                var meta = s.MetaAt(_c);
                var cell = s.GetOrCreateCell(_c, _r, meta);
                Current = (_c, _r, cell);
                return true;
            }

            /// <summary>
            /// Advances the enumerator for a <see cref="MappedStore"/>.
            /// </summary>
            /// <remarks>
            /// Iterates all columns and rows using <see cref="MappedStore.GetRowCapacity(uint)"/>,
            /// <see cref="MappedStore.GetColumnConfiguration(uint)"/> and
            /// <see cref="MappedStore.GetOrCreateCell(uint,uint,in ColumnConfiguration)"/>.
            /// </remarks>
            /// <returns>
            /// <see langword="true"/> if a new cell is available in <see cref="Current"/>;
            /// otherwise, <see langword="false"/> when the enumeration is complete.
            /// </returns>
            private bool MoveNextMapped()
            {
                var s = _mapped!;
                if (_r == uint.MaxValue)
                {
                    if (_colCount == 0) return false;
                    _r = 0;
                }
                else
                {
                    _r++;
                }

                while (_c < _colCount && _r >= s.GetRowCapacity(_c))
                {
                    _c++;
                    if (_c >= _colCount) return false;
                    _r = 0;
                }
                if (_c >= _colCount) return false;

                var meta = s.GetColumnConfiguration(_c);
                var cell = s.GetOrCreateCell(_c, _r, meta);
                Current = (_c, _r, cell);
                return true;
            }

            /// <summary>
            /// Determines whether this enumerator instance is equal to another
            /// <see cref="Enumerator"/> by comparing position and underlying store.
            /// </summary>
            /// <param name="other">The other enumerator to compare with.</param>
            /// <returns>
            /// <see langword="true"/> if both enumerators point to the same position
            /// over the same underlying store; otherwise, <see langword="false"/>.
            /// </returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly bool Equals(Enumerator other)
            {
                if (_c == other._c &&
                    _r == other._r &&
                    _colCount == other._colCount)
                {
                    if (_mem is not null && other._mem is not null)
                        return ReferenceEquals(_mem, other._mem);
                    else if (_mapped is not null && other._mapped is not null)
                        return ReferenceEquals(_mapped, other._mapped);
                    else if (_mem is null && other._mem is null &&
                             _mapped is null && other._mapped is null)
                        return true;
                }
                return false;
            }

            /// <summary>
            /// Determines whether the specified object is equal to the current enumerator.
            /// </summary>
            /// <param name="obj">The object to compare with this enumerator.</param>
            /// <returns>
            /// <see langword="true"/> if <paramref name="obj"/> is an <see cref="Enumerator"/>
            /// with the same position and underlying store; otherwise, <see langword="false"/>.
            /// </returns>
            public override readonly bool Equals(object? obj) =>
                obj is Enumerator enumerator && Equals(enumerator);

            /// <summary>
            /// Determines whether two enumerators are equal.
            /// </summary>
            /// <param name="left">The first enumerator to compare.</param>
            /// <param name="right">The second enumerator to compare.</param>
            /// <returns>
            /// <see langword="true"/> if both enumerators are equal according to
            /// <see cref="Equals(Enumerator)"/>; otherwise, <see langword="false"/>.
            /// </returns>
            public static bool operator ==(Enumerator left, Enumerator right) => left.Equals(right);

            /// <summary>
            /// Determines whether two enumerators are not equal.
            /// </summary>
            /// <param name="left">The first enumerator to compare.</param>
            /// <param name="right">The second enumerator to compare.</param>
            /// <returns>
            /// <see langword="true"/> if the enumerators are not equal; otherwise,
            /// <see langword="false"/>.
            /// </returns>
            public static bool operator !=(Enumerator left, Enumerator right) => !(left == right);

            /// <summary>
            /// Returns a hash code for this enumerator.
            /// </summary>
            /// <returns>
            /// A hash code that combines the current position (column, row), 
            /// the store shape (column count) and the identity
            /// of the underlying store instance (in-memory or mapped).
            /// </returns>
            /// <remarks>
            /// Two <see cref="Enumerator"/> instances that are considered equal by
            /// <see cref="Equals(Enumerator)"/> are guaranteed to produce the same
            /// hash code. The hash code is not guaranteed to be unique and different
            /// enumerators may still collide, as is typical for hash functions.
            /// </remarks>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override readonly int GetHashCode()
            {
                unchecked
                {
                    int hash = 17;

                    hash = (hash * 31) + (int)_c;
                    hash = (hash * 31) + (int)_r;
                    hash = (hash * 31) + (int)_colCount;

                    // Encode which backing store is used and its identity,
                    // mirroring the logic in Equals(Enumerator).
                    if (_mem is not null)
                    {
                        hash = (hash * 31) + 1; // mode marker: memory
                        hash = (hash * 31) + RuntimeHelpers.GetHashCode(_mem);
                    }
                    else if (_mapped is not null)
                    {
                        hash = (hash * 31) + 2; // mode marker: mapped
                        hash = (hash * 31) + RuntimeHelpers.GetHashCode(_mapped);
                    }
                    else
                    {
                        hash = (hash * 31) + 0; // mode marker: none
                    }

                    return hash;
                }
            }

            /// <summary>
            /// Resets the enumerator to its initial position, which is
            /// before the first cell in the underlying store.
            /// </summary>
            public void Reset()
            {
                _c = 0;
                _r = uint.MaxValue;
                Current = default;
            }

            /// <summary>
            /// Releases resources held by this enumerator.
            /// </summary>
            /// <remarks>
            /// <para>
            /// The enumerator is a lightweight view over an existing
            /// <see cref="InMemoryStore"/> or <see cref="MappedStore"/> and
            /// does not own the underlying store.
            /// Calling <see cref="Dispose"/> does <b>not</b> dispose the store;
            /// that remains the responsibility of the owner (e.g. <see cref="MetadataTable"/>).
            /// </para>
            /// <para>
            /// This implementation simply drops references to the backing store so the
            /// GC can collect it once no other references remain. The iteration state
            /// (<c>_c</c>, <c>_r</c>, <see cref="Current"/>) is left intact.
            /// </para>
            /// </remarks>
            public void Dispose()
            {
                // Do not dispose the underlying store: we don't own it.
                _mem    = null;
                _mapped = null;
            }
        }
    }
}
