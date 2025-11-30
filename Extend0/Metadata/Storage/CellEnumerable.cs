using Extend0.Metadata.Schema;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Lightweight enumerable over all cells in an <see cref="ICellStore"/>.
    /// </summary>
    /// <remarks>
    /// This struct abstracts over the concrete store implementation and can iterate
    /// both <see cref="InMemoryStore"/> and <see cref="MappedStore"/> instances.
    /// </remarks>
    public readonly struct CellEnumerable
    {
        private readonly InMemoryStore? _mem;
        private readonly MappedStore? _mapped;

        /// <summary>
        /// Creates an enumerable for a <see cref="ICellStore"/> store.
        /// </summary>
        /// <param name="store">The <see cref="ICellStore"/> to enumerate.</param>
        internal CellEnumerable(ICellStore store)
        {
            if (store is InMemoryStore memStore)
            {
                _mem = memStore;
                _mapped = null;
            }
            else if (store is MappedStore mappedStore)
            {
                _mem = null;
                _mapped = mappedStore;
            }
            else
                throw new ArgumentException("Unsupported ICellStore implementation.", nameof(store));
        }

        /// <summary>
        /// Returns a struct enumerator that iterates all cells in the underlying store.
        /// </summary>
        /// <remarks>
        /// The iteration walks row by row within each column, in column index order,
        /// yielding a tuple containing the column index, row index and <see cref="MetadataCell"/>.
        /// </remarks>
        public Enumerator GetEnumerator() =>
            _mapped is not null
                ? Enumerator.ForMapped(_mapped)
                : Enumerator.ForMemory(_mem!);

        /// <summary>
        /// Struct-based enumerator over metadata cells.
        /// </summary>
        /// <remarks>
        /// The enumerator supports both in-memory and memory-mapped stores and
        /// yields the current cell as <c>(Col, Row, Cell)</c>.
        /// </remarks>
        public struct Enumerator : IEquatable<Enumerator>
        {
            // shared state
            private uint _c, _r;
            private uint _colCount;
            private uint _rowCap;

            // mode
            private InMemoryStore? _mem;
            private MappedStore? _mapped;

            /// <summary>
            /// Gets the current cell triple: column index, row index and cell instance.
            /// </summary>
            public (uint Col, uint Row, CodeGen.MetadataCell Cell) Current { get; private set; }

            /// <summary>
            /// Creates an enumerator bound to an in-memory store.
            /// </summary>
            /// <param name="s">The <see cref="InMemoryStore"/> to iterate.</param>
            internal static Enumerator ForMemory(InMemoryStore s) => new()
            {
                _mem = s,
                _mapped = null,
                _c = 0,
                _r = uint.MaxValue, // start before first
                _colCount = s.ColumnCount,
                _rowCap = 0
            };

            /// <summary>
            /// Creates an enumerator bound to a memory-mapped store.
            /// </summary>
            /// <param name="s">The <see cref="MappedStore"/> to iterate.</param>
            internal static Enumerator ForMapped(MappedStore s) => new()
            {
                _mem = null,
                _mapped = s,
                _c = 0,
                _r = uint.MaxValue, // start before first
                _colCount = s.ColumnCount,
                _rowCap = 0
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
                return MoveNextMem();
            }

            /// <summary>
            /// Advances the enumerator for an <see cref="InMemoryStore"/>.
            /// </summary>
            /// <remarks>
            /// Iterates all columns and rows using <see cref="InMemoryStore.MetaAt"/> and
            /// <see cref="InMemoryStore.GetOrCreateCell(uint,uint,in ColumnConfiguration)"/>.
            /// </remarks>
            private bool MoveNextMem()
            {
                var s = _mem!;
                // first advance
                if (_r == uint.MaxValue)
                {
                    if (_colCount == 0) return false;
                    _rowCap = s.MetaAt(0).InitialCapacity;
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
            private bool MoveNextMapped()
            {
                var s = _mapped!;
                if (_r == uint.MaxValue)
                {
                    if (_colCount == 0) return false;
                    _rowCap = s.GetRowCapacity(0);
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
                    _colCount == other._colCount &&
                    _rowCap == other._rowCap)
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
            public override readonly bool Equals(object? obj) => obj is Enumerator enumerator && Equals(enumerator);

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
            /// the store shape (column count and row capacity) and the identity
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
                    hash = (hash * 31) + (int)_rowCap;

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

        }
    }
}
