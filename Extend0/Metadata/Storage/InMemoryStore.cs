using Extend0.Metadata.Schema;
using Extend0.Metadata.CodeGen;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// In-memory implementation of <see cref="ICellStore"/> used for testing,
    /// transient tables and scenarios where persistence is not required.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Cells are stored in a dictionary keyed by a packed <c>(column,row)</c> pair,
    /// so the layout is not contiguous in memory and does not mirror the
    /// <see cref="MappedStore"/> file layout.
    /// </para>
    /// <para>
    /// This store also implements <see cref="ITryGrowableStore"/> to support
    /// row-based auto-growth policies such as <see cref="CapacityPolicy.AutoGrowZeroInit"/>.
    /// </para>
    /// </remarks>
    internal sealed class InMemoryStore : ICellStore, ITryGrowableStore
    {
        private readonly Dictionary<ulong, MetadataCell> _cells;
        private readonly ColumnConfiguration[] _columns;

        /// <summary>
        /// Logical row count per column (max row index + 1).
        /// </summary>
        /// <remarks>
        /// This tracks how many rows are considered "materialized" for each column,
        /// independently of how many entries are actually present in <see cref="_cells"/>.
        /// </remarks>
        private readonly Dictionary<uint, uint> _rowsPerColumn = [];

        /// <inheritdoc/>
        public int Count => _cells.Count;

        /// <summary>
        /// Packs a column and row index into a single 64-bit key for dictionary lookup.
        /// </summary>
        /// <param name="c">Zero-based column index.</param>
        /// <param name="r">Zero-based row index.</param>
        /// <returns>A 64-bit value encoding <paramref name="c"/> and <paramref name="r"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Pack(uint c, uint r) => (ulong)c << 32 | r;

        /// <summary>
        /// Creates a new in-memory store from the given column configurations.
        /// </summary>
        /// <param name="cols">Column metadata defining sizes, names and initial capacity.</param>
        /// <remarks>
        /// The constructor pre-allocates the internal dictionary with a capacity based on
        /// the sum of <see cref="ColumnConfiguration.InitialCapacity"/> for all columns,
        /// to minimize rehashing when cells are created.
        /// </remarks>
        public InMemoryStore(IEnumerable<ColumnConfiguration> cols)
        {
            _columns = cols as ColumnConfiguration[] ?? [.. cols];
            uint total = 0;
            foreach (var c in _columns) total += c.InitialCapacity;
            _cells = new Dictionary<ulong, MetadataCell>(checked((int)total));
        }

        /// <summary>
        /// Gets the number of columns in this store.
        /// </summary>
        internal uint ColumnCount => (uint)_columns.Length;

        /// <summary>
        /// Returns the column metadata for the given column index.
        /// </summary>
        /// <param name="c">Zero-based column index.</param>
        internal ref readonly ColumnConfiguration MetaAt(uint c) => ref _columns[(int)c];

        /// <summary>
        /// Gets or creates a cell for the specified column and row using its configured metadata.
        /// </summary>
        /// <param name="col">Zero-based column index.</param>
        /// <param name="row">Zero-based row index.</param>
        /// <returns>The existing or newly created <see cref="MetadataCell"/>.</returns>
        internal MetadataCell GetOrCreateCell(uint col, uint row) => GetOrCreateCell(col, row, _columns[(int)col]);

        /// <inheritdoc/>
        public bool TryGetCell(uint col, uint row, out MetadataCell cell)
            => _cells.TryGetValue(Pack(col, row), out cell!);

        /// <summary>
        /// Gets or creates a cell for the specified column and row, using the provided metadata.
        /// </summary>
        /// <param name="col">Zero-based column index.</param>
        /// <param name="row">Zero-based row index.</param>
        /// <param name="meta">Column configuration describing key/value sizes and name.</param>
        /// <returns>The existing or newly created <see cref="MetadataCell"/>.</returns>
        /// <remarks>
        /// <para>
        /// When a new cell is created, its key is initialized to the column name (UTF-8).
        /// </para>
        /// <para>
        /// This method also updates the logical row count for the column in
        /// <see cref="_rowsPerColumn"/>.
        /// </para>
        /// </remarks>
        public MetadataCell GetOrCreateCell(uint col, uint row, in ColumnConfiguration meta)
        {
            var k = Pack(col, row);
            if (_cells.TryGetValue(k, out var cell)) return cell;

            cell = new MetadataCell(meta.Size);
            cell.TrySetKey(meta.Name);
            _cells[k] = cell;

            // Track logical length (max row index + 1)
            if (_rowsPerColumn.TryGetValue(col, out var have))
            {
                if (row + 1u > have)
                    _rowsPerColumn[col] = row + 1u;
            }
            else
            {
                _rowsPerColumn[col] = row + 1u;
            }

            return cell;
        }

        /// <inheritdoc/>
        public CellEnumerable EnumerateCells() => new(this);

        /// <inheritdoc/>
        /// <remarks>
        /// The in-memory store does not expose a contiguous block of value bytes per column,
        /// so this method always returns <see langword="false"/> and <paramref name="block"/>
        /// is set to its default value.
        /// </remarks>
        public bool TryGetColumnBlock(uint column, out ColumnBlock block)
        {
            block = default;
            return false; // no contiguous block exposed in the in-memory layout
        }

        /// <summary>
        /// Disposes all stored <see cref="MetadataCell"/> instances and clears the store.
        /// </summary>
        public void Dispose()
        {
            foreach (var c in _cells.Values) c.Dispose();
            _cells.Clear();
        }

        /// <inheritdoc/>
        /// <remarks>
        /// <para>
        /// Growing a column in the in-memory store materializes empty cells up to
        /// <paramref name="minRows"/>. For each new cell:
        /// </para>
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///       The key is initialized to <see cref="ColumnConfiguration.Name"/>.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       If <paramref name="zeroInit"/> is <see langword="true"/>, the value buffer
        ///       is cleared to zero.
        ///     </description>
        ///   </item>
        /// </list>
        /// </remarks>
        public bool TryGrowColumnTo(uint column, uint minRows, in ColumnConfiguration meta, bool zeroInit)
        {
            if (minRows == 0) return true;

            _rowsPerColumn.TryGetValue(column, out var have);
            if (have >= minRows) return true;

            for (uint r = have; r < minRows; r++)
            {
                var key = Pack(column, r);
                if (_cells.ContainsKey(key)) continue; // defensive (should not happen)

                var cell = new MetadataCell(meta.Size);
                cell.TrySetKey(meta.Name);

                if (zeroInit)
                {
                    unsafe
                    {
                        new Span<byte>(cell.GetValuePointer(), cell.ValueSize).Clear();
                    }
                }

                _cells[key] = cell;
            }

            _rowsPerColumn[column] = minRows;
            return true;
        }
    }
}
