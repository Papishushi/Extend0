using Extend0.Metadata.Storage.Internal;
using System.Collections;

namespace Extend0.Metadata.Storage
{
    public readonly partial struct CellEnumerable
    {
        /// <summary>
        /// Struct-based enumerator over metadata cells.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This enumerator iterates column-by-column and row-by-row, yielding only cells that already
        /// exist in the underlying store. It intentionally does <b>not</b> create missing cells during
        /// enumeration.
        /// </para>
        /// <para>
        /// Enumeration order is stable: columns are visited in ascending index order and each column
        /// is scanned from row 0 up to its current capacity.
        /// </para>
        /// <para>
        /// This is a <c>record struct</c>, so value equality and hash code are generated automatically
        /// from its fields (column/row position, column count and backing store references).
        /// </para>
        /// </remarks>
        public record struct Enumerator : IEquatable<Enumerator>, IEnumerator<CellRowColumnValueEntry>
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
            /// <remarks>
            /// The value is undefined before the first successful call to <see cref="MoveNext"/>
            /// and after <see cref="Reset"/> until <see cref="MoveNext"/> succeeds again.
            /// </remarks>
            public CellRowColumnValueEntry Current { get; private set; }

            /// <summary>
            /// Gets the current element in the collection (non-generic).
            /// </summary>
            readonly object IEnumerator.Current => Current;

            /// <summary>
            /// Creates an enumerator bound to an in-memory store.
            /// </summary>
            /// <param name="s">The in-memory store to enumerate.</param>
            /// <returns>An enumerator positioned before the first element.</returns>
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
            /// <param name="s">The mapped store to enumerate.</param>
            /// <returns>An enumerator positioned before the first element.</returns>
            internal static Enumerator ForMapped(MappedStore s) => new()
            {
                _mem = null,
                _mapped = s,
                _c = 0,
                _r = uint.MaxValue, // start before first
                _colCount = s.ColumnCount
            };

            /// <summary>
            /// Advances the enumerator to the next existing cell.
            /// </summary>
            /// <returns>
            /// <see langword="true"/> if the enumerator successfully advanced to the next element;
            /// <see langword="false"/> if the end of the sequence was reached or the enumerator is default/unbound.
            /// </returns>
            public bool MoveNext()
            {
                if (_mapped is not null) return MoveNextMapped();
                if (_mem is not null) return MoveNextMem();
                return false; // default enumerator with no store
            }

            /// <summary>
            /// Advances enumeration for an <see cref="InMemoryStore"/>.
            /// </summary>
            /// <remarks>
            /// This method skips non-existent cells (cells that would require allocation/creation) and yields only
            /// already created entries.
            /// </remarks>
            private bool MoveNextMem()
            {
                var s = _mem!;
                if (_r == uint.MaxValue)
                {
                    if (_colCount == 0) return false;
                    _r = 0;
                }
                else _r++;

                while (true)
                {
                    // jump columns when row capacity is exceeded
                    while (_c < _colCount && _r >= s.MetaAt(_c).InitialCapacity)
                    {
                        _c++;
                        if (_c >= _colCount) return false;
                        _r = 0;
                    }
                    if (_c >= _colCount) return false;

                    // IMPORTANT: do NOT create while enumerating
                    if (!s.TryGetCell(_c, _r, out var cell))
                    {
                        _r++;
                        continue;
                    }

                    // all-created => if it exists, yield it (even if keyless / value-only)
                    Current = (_c, _r, cell);
                    return true;
                }
            }

            /// <summary>
            /// Advances enumeration for a <see cref="MappedStore"/>.
            /// </summary>
            /// <remarks>
            /// This method skips non-existent cells and yields only entries that are already present in the mapping.
            /// </remarks>
            private bool MoveNextMapped()
            {
                var s = _mapped!;
                if (_r == uint.MaxValue)
                {
                    if (_colCount == 0) return false;
                    _r = 0;
                }
                else _r++;

                while (true)
                {
                    while (_c < _colCount && _r >= s.GetRowCapacity(_c))
                    {
                        _c++;
                        if (_c >= _colCount) return false;
                        _r = 0;
                    }
                    if (_c >= _colCount) return false;

                    if (!s.TryGetCell(_c, _r, out var cell))
                    {
                        _r++;
                        continue;
                    }

                    // all-created => if it exists, yield it (even if keyless / value-only)
                    Current = (_c, _r, cell);
                    return true;
                }
            }

            /// <summary>
            /// Resets the enumerator to its initial position (before the first element).
            /// </summary>
            /// <remarks>
            /// This does not change the underlying store, only the cursor state.
            /// </remarks>
            public void Reset()
            {
                _c = 0;
                _r = uint.MaxValue;
                Current = default;
            }

            /// <summary>
            /// Releases references held by this enumerator.
            /// </summary>
            /// <remarks>
            /// The enumerator does not own the underlying store and therefore does not dispose it.
            /// This method only clears internal references so the enumerator becomes unusable afterwards.
            /// </remarks>
            public void Dispose()
            {
                // Do not dispose the underlying store: we don't own it.
                _mem = null;
                _mapped = null;
            }
        }
    }
}
