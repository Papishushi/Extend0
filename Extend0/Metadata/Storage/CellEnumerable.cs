using Extend0.Metadata.Schema;

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
        /// Creates an enumerable for an in-memory store.
        /// </summary>
        /// <param name="mem">The <see cref="InMemoryStore"/> to enumerate.</param>
        internal CellEnumerable(InMemoryStore mem)
        {
            _mem = mem;
            _mapped = null;
        }

        /// <summary>
        /// Creates an enumerable for a memory-mapped store.
        /// </summary>
        /// <param name="mapped">The <see cref="MappedStore"/> to enumerate.</param>
        internal CellEnumerable(MappedStore mapped)
        {
            _mem = null;
            _mapped = mapped;
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
        public struct Enumerator
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
        }
    }
}
