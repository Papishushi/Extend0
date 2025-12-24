using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage.Files;
using System.Buffers;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Text;

namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Column store backed by a memory-mapped file.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="MappedStore"/> owns a <see cref="MemoryMappedFile"/> and a single view accessor
    /// that covers the entire table file. It exposes cells as <see cref="MetadataCell"/> views
    /// over the mapped region, without copying.
    /// </para>
    /// <para>
    /// The on-disk layout is:
    /// </para>
    /// <list type="number">
    ///   <item><description><see cref="FileHeader"/> at offset 0.</description></item>
    ///   <item><description>
    ///     An array of <see cref="ColumnDesc"/> entries, one per column, starting at
    ///     <see cref="FileHeader.ColumnsTableOffset"/>.
    ///   </description></item>
    ///   <item><description>
    ///     For each column, a slab of <c>RowCapacity</c> entries, each of
    ///     <c>KeySize + ValueSize</c> bytes, aligned to 64 bytes.
    ///   </description></item>
    /// </list>
    /// <para>
    /// When a column is grown via <see cref="TryGrowColumnTo"/>, its slab is relocated to the
    /// end of the file and extended, updating the corresponding <see cref="ColumnDesc"/>.
    /// </para>
    /// </remarks>
    internal sealed unsafe class MappedStore : ITryGrowableStore, ICompactableStore
    {
        private MemoryMappedFile _mmf;
        private MemoryMappedViewAccessor _view;
        private byte* _base;               // Base address of the mapping
        private FileHeader* _hdr;
        private ColumnDesc* _cols;
        private readonly string _path;     // Path to the mapped file
        private long _length;              // Current file length

        /// <summary>
        /// Column names pre-encoded as UTF-8, truncated to <c>KeySize - 1</c> bytes.
        /// Used to build per-row keys like <c>"ColumnName:Row"</c> directly in the mapped buffer.
        /// </summary>
        private readonly byte[][] _colNameUtf8;

        /// <inheritdoc/>
        public int Count { get; private set; }

        /// <summary>
        /// Gets the number of columns in the mapped table, as reported by the header.
        /// </summary>
        internal uint ColumnCount => _hdr->ColumnCount;

        /// <summary>
        /// Magic value stored in <see cref="FileHeader.Magic"/> to identify a valid
        /// mapped store file created by this implementation.
        /// </summary>
        /// <remarks>
        /// The constant corresponds to the ASCII literal <c>'LBTM'</c> encoded as a
        /// 32-bit integer.
        /// </remarks>
        private const int MAGIC_VALUE = 0x4C42544D; // 'LBTM'

        /// <summary>
        /// Returns the current row capacity for the given column.
        /// </summary>
        /// <param name="c">Zero-based column index.</param>
        internal uint GetRowCapacity(uint c) => _cols[c].RowCapacity;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ColumnConfiguration FromDesc(in ColumnDesc cd, int index) =>
            new(Size: MetadataEntrySizeExtensions.PackUnchecked(cd.KeySize, cd.ValueSize),
                Name: $"c{index}",
                ReadOnly: false,
                InitialCapacity: cd.RowCapacity);

        /// <summary>
        /// Builds a <see cref="ColumnConfiguration"/> snapshot for a mapped column.
        /// </summary>
        /// <param name="c">Zero-based column index.</param>
        /// <returns>
        /// A new <see cref="ColumnConfiguration"/> instance describing key/value sizes,
        /// logical name and initial capacity of the column.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The current implementation synthesizes column names as <c>"c{index}"</c> because
        /// original names are not yet persisted to disk.
        /// </para>
        /// </remarks>
        internal ColumnConfiguration GetColumnConfiguration(uint c)
        {
            ref readonly var cd = ref _cols[c];
            return FromDesc(cd, (int)c);
        }

        /// <summary>
        /// Creates a new memory-mapped store from a <see cref="TableSpec"/>, allocating
        /// or growing the backing file as needed.
        /// </summary>
        /// <param name="spec">
        /// Table specification describing the target map path, the column layout
        /// (key/value sizes), and initial row capacities.
        /// </param>
        /// <remarks>
        /// <para>
        /// The constructor performs the following steps:
        /// </para>
        /// <list type="number">
        ///   <item><description>
        ///     Computes the required file size by laying out the <see cref="FileHeader"/>,
        ///     the <see cref="ColumnDesc"/> table, and all column areas according to
        ///     the <paramref name="spec"/> (including alignment padding).
        ///   </description></item>
        ///   <item><description>
        ///     Pre-encodes column names as UTF-8 and stores them in <see cref="_colNameUtf8"/>,
        ///     truncating each name to at most <c>KeySize - 1</c> bytes.
        ///   </description></item>
        ///   <item><description>
        ///     Ensures the target directory exists, persists the <see cref="TableSpec"/> to
        ///     disk (via <see cref="TableSpec.SaveToDirectory(string)"/>), and creates or
        ///     grows the backing file to the computed size.
        ///   </description></item>
        ///   <item><description>
        ///     Memory-maps the file, computes the base pointer for the view, and initializes
        ///     <see cref="_hdr"/> and <see cref="_cols"/> to point to the header and
        ///     column descriptor array respectively.
        ///   </description></item>
        ///   <item><description>
        ///     If the file does not contain the expected magic value, initializes the
        ///     <see cref="FileHeader"/> and writes the computed <see cref="ColumnDesc"/>
        ///     descriptors, flushing the view to disk.
        ///   </description></item>
        /// </list>
        /// </remarks>
        public MappedStore(TableSpec spec)
        {
            var columns = spec.Columns;

            long headerSize = sizeof(FileHeader);
            long colsSize = (long)columns.Length * sizeof(ColumnDesc);
            long cursor = headerSize + colsSize;

            ColumnDesc[]? rented = null;
            scoped Span<ColumnDesc> batch;

            if (columns.Length <= 256) batch = stackalloc ColumnDesc[columns.Length];
            else
            {
                rented = ArrayPool<ColumnDesc>.Shared.Rent(columns.Length);
                batch = rented.AsSpan(0, columns.Length);
            }

            _colNameUtf8 = new byte[columns.Length][];

            cursor = MapFile(columns, cursor, batch);

            long fileSize = cursor;
            _length = fileSize;

            var fullPath = Path.GetFullPath(spec.MapPath);
            _path = fullPath;

            var dir = Path.GetDirectoryName(fullPath)!;
            Directory.CreateDirectory(dir);
            spec.SaveToDirectory(dir);

            using (var fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
            {
                if (fs.Length < fileSize) fs.SetLength(fileSize);
            }

            _mmf  = MemoryMappedFile.CreateFromFile(fullPath, FileMode.Open, null, capacity: 0, MemoryMappedFileAccess.ReadWrite);
            _view = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref _base);

            _hdr  = (FileHeader*)_base;
            _cols = (ColumnDesc*)(_base + sizeof(FileHeader));

            // Initialize header and column descriptors if the file is new / uninitialized
            InitializeMappedFile(columns, batch);

            if (rented is not null) ArrayPool<ColumnDesc>.Shared.Return(rented, clearArray: true);
        }

        /// <summary>
        /// Computes the physical layout of all columns, updating a temporary
        /// <see cref="ColumnDesc"/> buffer and returning the final file size cursor.
        /// </summary>
        /// <param name="columns">
        /// Column configurations taken from the <see cref="TableSpec"/>.
        /// </param>
        /// <param name="cursor">
        /// Initial byte offset in the file, typically just after the header
        /// and the column descriptor table.
        /// </param>
        /// <param name="temp">
        /// Temporary span used to store computed <see cref="ColumnDesc"/> entries
        /// (key/value sizes, row capacities and base offsets).
        /// </param>
        /// <returns>
        /// The updated cursor positioned just after the last column region; this
        /// value represents the required file size in bytes.
        /// </returns>
        /// <remarks>
        /// <para>
        /// For each column, this method:
        /// </para>
        /// <list type="number">
        ///   <item><description>
        ///     Computes key and value sizes from the configured column size kind
        ///     (via <c>c.Size.GetKeySize()</c> and <c>c.Size.GetValueSize()</c>).
        ///   </description></item>
        ///   <item><description>
        ///     Aligns the current <paramref name="cursor"/> to a 64-byte boundary
        ///     (via <see cref="AlignUp(long, int)"/> with <c>a = 64</c>) and stores it as
        ///     <see cref="ColumnDesc.BaseOffset"/>.
        ///   </description></item>
        ///   <item><description>
        ///     Advances the cursor by <c>entrySize * InitialCapacity</c>, where
        ///     <c>entrySize = keySize + valueSize</c>.
        ///   </description></item>
        ///   <item><description>
        ///     Pre-encodes the column name in UTF-8 and stores it in
        ///     <see cref="_colNameUtf8"/>, truncating to <c>KeySize - 1</c>
        ///     bytes to leave room for a null terminator.
        ///   </description></item>
        /// </list>
        /// </remarks>
        private long MapFile(ColumnConfiguration[] columns, long cursor, Span<ColumnDesc> temp)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                ref readonly var c = ref columns[i];
                int keySize = c.Size.GetKeySize();
                int valueSize = c.Size.GetValueSize();
                int entrySize = checked(keySize + valueSize);

                cursor = AlignUp(cursor);
                temp[i] = new ColumnDesc
                (
                    keySize: keySize,
                    valueSize: valueSize,
                    rowCapacity: c.InitialCapacity,
                    baseOffset: cursor
                );
                cursor += entrySize * c.InitialCapacity;

                // Pre-encode column name as UTF-8 (truncated to keySize - 1)
                int max = Math.Max(0, keySize - 1);
                var bytes = Encoding.UTF8.GetBytes(c.Name);
                _colNameUtf8[i] = bytes.Length > max ? bytes.AsSpan(0, max).ToArray() : bytes;
            }

            return cursor;
        }

        /// <summary>
        /// Initializes the memory-mapped file header and column descriptors
        /// when the file is new or uninitialized.
        /// </summary>
        /// <param name="columns">
        /// Column configurations taken from the <see cref="TableSpec"/> used
        /// to construct this <see cref="MappedStore"/>.
        /// </param>
        /// <param name="temp">
        /// Temporary buffer containing the computed <see cref="ColumnDesc"/> entries
        /// (key/value sizes, row capacities and base offsets).
        /// </param>
        /// <remarks>
        /// <para>
        /// If the current <see cref="_hdr"/> does not contain the expected
        /// <see cref="MAGIC_VALUE"/>, this method:
        /// </para>
        /// <list type="number">
        ///   <item><description>
        ///     Writes a fresh <see cref="FileHeader"/> with magic, version,
        ///     column count and the offset of the column descriptor table.
        ///   </description></item>
        ///   <item><description>
        ///     Copies the precomputed <see cref="ColumnDesc"/> entries from
        ///     <paramref name="temp"/> into the mapped descriptor array
        ///     referenced by <see cref="_cols"/>.
        ///   </description></item>
        ///   <item><description>
        ///     Flushes the <see cref="_view"/> to ensure the header and descriptors
        ///     are persisted to disk.
        ///   </description></item>
        /// </list>
        /// <para>
        /// If the magic value is already present, the method is a no-op and assumes
        /// the existing header and descriptors are valid.
        /// </para>
        /// </remarks>
        private void InitializeMappedFile(ColumnConfiguration[] columns, Span<ColumnDesc> temp)
        {
            if (_hdr->Magic == MAGIC_VALUE) return;
            *_hdr = new FileHeader
            {
                Magic              = MAGIC_VALUE,
                Version            = 1,
                ColumnCount        = (ushort)columns.Length,
                ColumnsTableOffset = sizeof(FileHeader)
            };
            for (int i = 0; i < columns.Length; i++) _cols[i] = temp[i];
            _view.Flush();
        }

        /// <summary>
        /// Rebuilds the memory mapping after growing or shrinking the underlying file.
        /// </summary>
        /// <param name="newLength">New required minimum file length in bytes.</param>
        /// <remarks>
        /// <para>
        /// This method flushes and disposes the current view and mapping, resizes the file
        /// if needed, and then creates a new mapping and view, refreshing the internal
        /// header and column descriptor pointers.
        /// </para>
        /// </remarks>
        private void Remap(long newLength)
        {
            // Flush & drop current view
            _view.Flush();
            if (_base != null)
                _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _view.Dispose();
            _mmf.Dispose();

            // Ensure file is at least newLength
            using (var fs = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
            {
                if (fs.Length < newLength) fs.SetLength(newLength);
                _length = fs.Length;
            }

            // Re-map whole file
            _mmf  = MemoryMappedFile.CreateFromFile(_path, FileMode.Open, null, 0, MemoryMappedFileAccess.ReadWrite);
            _view = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref _base);

            _hdr  = (FileHeader*)_base;
            _cols = (ColumnDesc*)(_base + _hdr->ColumnsTableOffset);
        }

        /// <summary>
        /// Aligns a value up to the next multiple of <paramref name="a"/>.
        /// </summary>
        /// <param name="v">
        /// Value to align (typically a byte offset).
        /// </param>
        /// <param name="a">
        /// Alignment in bytes. Must be a positive integer. Defaults to 64 bytes,
        /// which matches the slab alignment used by <see cref="MappedStore"/>.
        /// </param>
        /// <returns>
        /// The smallest multiple of <paramref name="a"/> that is greater than or equal to
        /// <paramref name="v"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This is a classic integer alignment helper:
        /// </para>
        /// <code>
        /// // Example: v = 70, a = 64 → returns 128
        /// result = AlignUp(70, 64);
        /// </code>
        /// <para>
        /// The method assumes <paramref name="a"/> &gt; 0. Passing zero or a negative value
        /// results in undefined behavior (integer division by zero or bogus results).
        /// </para>
        /// </remarks>
        [DebuggerStepThrough]
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static long AlignUp(long v, int a = 64) => unchecked(v + (a - 1)) / a * a;


        /// <summary>
        /// Computes a raw pointer to the beginning of the (key,value) entry
        /// for the given column and row.
        /// </summary>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="row">Zero-based row index.</param>
        /// <returns>Pointer to the first byte of the entry in the mapped region.</returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="column"/> or <paramref name="row"/> is outside the mapped capacity.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* CellPtr(uint column, uint row)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(column, _hdr->ColumnCount);
            ref var cd = ref _cols[column];
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(row, cd.RowCapacity);
            return _base + cd.BaseOffset + row * cd.EntrySizeBytes;
        }

        /// <inheritdoc/>
        public bool TryGetCell(uint col, uint row, out MetadataCell cell)
        {
            if (col >= _hdr->ColumnCount) { cell = default; return false; }
            ref var cd = ref _cols[col];
            if (row >= cd.RowCapacity) { cell = default; return false; }
            var size = MetadataEntrySizeExtensions.PackUnchecked(cd.KeySize, cd.ValueSize);
            cell = MetadataCell.FromPointer(size, CellPtr(col, row), owns: false);
            return true;
        }

        /// <summary>
        /// Tries to expose the raw value region of an entire column as a contiguous block.
        /// </summary>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="block">
        /// When this method returns <see langword="true"/>, contains a <see cref="ColumnBlock"/>
        /// describing the contiguous value region for the column.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the column has a contiguous block representation;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// For <see cref="MappedStore"/> this always returns <see langword="true"/> for valid
        /// column indices, since each column is stored as a contiguous slab.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetColumnBlock(uint column, out ColumnBlock block)
        {
            // Minimal validation; assumes column < ColumnCount if the caller is well-behaved.
            ref readonly ColumnDesc desc = ref _cols[column];

            var stride = desc.KeySize + desc.ValueSize;
            var valueOffset = desc.KeySize;
            var colBase = _base + desc.BaseOffset;

            block = new ColumnBlock(
                @base: colBase,
                stride: stride,
                valueSize: desc.ValueSize,
                valueOffset: valueOffset
            );
            return true;
        }

        /// <inheritdoc/>
        public MetadataCell GetOrCreateCell(uint col, uint row, in ColumnConfiguration meta)
        {
            ref var cd = ref _cols[col];
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(row, cd.RowCapacity);

            var size = MetadataEntrySizeExtensions.PackUnchecked(cd.KeySize, cd.ValueSize);
            var cell = MetadataCell.FromPointer(size, CellPtr(col, row), owns: false);

            var name = _colNameUtf8[(int)col];     // already UTF-8, no NUL
            int cap = cd.KeySize;

            // Encode row index as UTF-8
            Span<byte> rowBuf = stackalloc byte[20]; // enough for uint
            if (!System.Buffers.Text.Utf8Formatter.TryFormat(row, rowBuf, out int rowLen))
                rowLen = 0;

            int needed = name.Length + 1 + rowLen;  // "name" + ':' + row
            if (needed >= cap)
            {
                // Not enough room: truncate column name to leave space for ":row"
                int maxName = Math.Max(0, cap - 1 - rowLen);
                needed = maxName + 1 + rowLen;
                if (needed >= cap) return cell; // still no room: leave key empty

                var dst = new Span<byte>(CellPtr(col, row), cap);
                dst.Clear();
                name.AsSpan(0, maxName).CopyTo(dst);
                dst[maxName] = (byte)':';
                rowBuf[..rowLen].CopyTo(dst[(maxName + 1)..]);
            }
            else
            {
                var dst = new Span<byte>(CellPtr(col, row), cap);
                dst.Clear();
                name.CopyTo(dst);
                int w = name.Length;
                dst[w++] = (byte)':';
                rowBuf[..rowLen].CopyTo(dst[w..]);
            }

            return cell;
        }

        /// <summary>
        /// Tries to load the column configuration array from an existing mapped file.
        /// </summary>
        /// <param name="path">Path to the metadata table file.</param>
        /// <param name="columns">
        /// When this method returns <see langword="true"/>, contains an array of
        /// <see cref="ColumnConfiguration"/> reconstructed from the file header.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the file exists, has a valid header and column descriptors;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        public static bool TryLoadColumns(string path, out ColumnConfiguration[] columns)
        {
            columns = [];
            if (!File.Exists(path)) return false;

            using var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
            using var view = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            byte* basePtr = null;
            try
            {
                view.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
                if (basePtr == null) return false;

                var hdr = (FileHeader*)basePtr;
                if (hdr->Magic != MAGIC_VALUE) return false;

                var cols = new ColumnConfiguration[hdr->ColumnCount];
                var cdesc = (ColumnDesc*)(basePtr + hdr->ColumnsTableOffset);
                for (int i = 0; i < cols.Length; i++) cols[i] = FromDesc(cdesc[i], i);
                columns = cols;
                return true;
            }
            finally
            {
                if (basePtr != null) view.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        /// <inheritdoc/>
        public CellEnumerable EnumerateCells() => new(this);

        /// <summary>
        /// Releases the memory-mapped view and the underlying mapping.
        /// </summary>
        public void Dispose()
        {
            if (_base != null)
            {
                _view.Flush();
                _view?.SafeMemoryMappedViewHandle.ReleasePointer();
            }
            _view?.Dispose();
            _mmf?.Dispose();
        }

        /// <inheritdoc/>
        /// <remarks>
        /// <para>
        /// This implementation grows a column by:
        /// </para>
        /// <list type="number">
        ///   <item><description>
        ///     Appending a new slab for the column at the end of the file, aligned to 64 bytes.
        ///   </description></item>
        ///   <item><description>
        ///     Remapping the file to cover the new length.
        ///   </description></item>
        ///   <item><description>
        ///     Copying the existing column payload into the new slab and optionally zeroing
        ///     the tail of the value region for new rows.
        ///   </description></item>
        ///   <item><description>
        ///     Updating the <see cref="ColumnDesc.BaseOffset"/> and <see cref="ColumnDesc.RowCapacity"/>
        ///     in place and flushing the view.
        ///   </description></item>
        /// </list>
        /// </remarks>
        public bool TryGrowColumnTo(uint column, uint minRows, in ColumnConfiguration meta, bool zeroInit)
        {
            if (minRows == 0) return true;
            if (column >= _hdr->ColumnCount) return false;

            ref var cd = ref _cols[column];

            // Validate sizes (we only grow rows; key/value sizes are fixed)
            int keySize = meta.Size.GetKeySize();
            int valSize = meta.Size.GetValueSize();
            if (keySize != cd.KeySize || valSize != cd.ValueSize)
                throw new InvalidOperationException($"GrowColumnTo: size mismatch. meta=({keySize},{valSize}) mapped=({cd.KeySize},{cd.ValueSize})");

            if (minRows <= cd.RowCapacity) return true; // nothing to do

            uint oldCap = cd.RowCapacity;
            uint newCap = minRows;

            long entrySize = (long)cd.KeySize + cd.ValueSize;
            long oldBytes = entrySize * oldCap;
            long newBytes = entrySize * newCap;

            // Append at end of file (keep 64B alignment)
            long newOffset = AlignUp(_length);
            long newFileLen = newOffset + newBytes;

            // Ensure mapping covers the new length
            Remap(newFileLen);

            // After remap, pointers are refreshed; re-take ref
            ref var cd2 = ref _cols[column];
            Debug.Assert(cd2.BaseOffset == cd.BaseOffset); // same struct instance in file

            // Copy existing payload to new spot
            byte* oldBase = _base + cd2.BaseOffset;
            byte* newBase = _base + newOffset;

            Buffer.MemoryCopy(oldBase, newBase, newBytes, oldBytes);

            // Zero-init the extra rows region if requested
            if (zeroInit && newBytes > oldBytes)
            {
                var tail = new Span<byte>(newBase + oldBytes, checked((int)(newBytes - oldBytes)));
                tail.Clear();
            }

            // Update descriptor in-place
            cd2.BaseOffset  = newOffset;
            cd2.RowCapacity = newCap;

            // Persist metadata update
            _view.Flush();
            return true;
        }

        /// <inheritdoc/>
        bool ITryGrowableStore.TryGetColumnCapacity(uint column, out uint rowCapacity)
        {
            if (column >= _hdr->ColumnCount)
            {
                rowCapacity = 0;
                return false;
            }

            rowCapacity = _cols[column].RowCapacity;
            return true;
        }

        public IEnumerator<CellRowColumnValueEntry> GetEnumerator() => EnumerateCells().GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        void ICompactableStore.Compact()
        {
            throw new NotImplementedException();
        }
    }
}