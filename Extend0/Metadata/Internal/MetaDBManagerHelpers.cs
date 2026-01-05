using Extend0.Metadata.Contract;
using Extend0.Metadata.Refs;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Internal;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Extend0.Metadata.Internal
{
    /// <summary>
    /// Collection of low-level helper routines used by <see cref="MetaDBManager"/> to
    /// manipulate column data in <see cref="IMetadataTable"/> instances.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This type centralizes high-performance operations such as:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Linking reference vectors between parent/child tables.</description></item>
    ///   <item><description>Block-based and strided column copies using <see cref="ColumnBlock"/>.</description></item>
    ///   <item><description>Per-cell fallbacks when block access is not available.</description></item>
    ///   <item><description>Heuristic batch-size computation for cache-friendly loops.</description></item>
    ///   <item><description>Batch-oriented column fills (per-cell, strided and contiguous fast paths).</description></item>
    /// </list>
    /// <para>
    /// Most methods are <see langword="internal"/> and <see langword="unsafe"/>, and are intended
    /// to be used only by the metadata storage infrastructure rather than by external callers.
    /// </para>
    /// </remarks>
    [SuppressMessage("Reliability", "CA2014", Justification = "SO is controlled and the stack allocations in loops are limited.")]
    [SuppressMessage("CodeQuality", "IDE0079", Justification = "False positive.")]
    internal static class MetaDBManagerHelpers
    {
        /// <summary>
        /// Inserts a reference into the parent's refs vector at
        /// (<paramref name="refsCol"/>, <paramref name="parentRow"/>).
        /// </summary>
        /// <param name="parent">Parent table.</param>
        /// <param name="refsCol">Zero-based index of the refs column.</param>
        /// <param name="parentRow">Zero-based row index in the parent.</param>
        /// <param name="tref">Reference (child table/column/row) to insert.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the refs vector is full for the current VALUE size
        /// (increase VALUE bytes or implement an overflow strategy).
        /// </exception>
        /// <remarks>
        /// This method assumes the cell is already initialized; call
        /// <see cref="EnsureRefVec(IMetadataTable, uint, uint, CapacityPolicy)"/> first if unsure.
        /// </remarks>
        internal static unsafe void LinkRef(IMetadataTable parent, uint refsCol, uint parentRow, in MetadataTableRef tref)
        {
            var cell = parent.GetOrCreateCell(refsCol, parentRow);
            var buf = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);

            if (!MetadataTableRefVec.TryAdd(buf, in tref, buf.Length))
                throw new InvalidOperationException("Refs vector is full; increase VALUE size or add an overflow strategy.");
        }

        /// <summary>
        /// Attempts to determine whether the parent's refs vector already contains the specified reference.
        /// </summary>
        /// <param name="table">The parent <see cref="IMetadataTable"/> that owns the refs column.</param>
        /// <param name="refsCol">Zero-based index of the refs column inside <paramref name="table"/>.</param>
        /// <param name="parentRow">Zero-based row index in <paramref name="table"/> whose refs vector is inspected.</param>
        /// <param name="tref">
        /// The reference to search for. A match is performed against the triple
        /// (<see cref="MetadataTableRef.TableId"/>, <see cref="MetadataTableRef.Column"/>, <see cref="MetadataTableRef.Row"/>).
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the refs vector is initialized and an entry matching <paramref name="tref"/> is found;
        /// otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This is a hot-path helper used to avoid inserting duplicate references.
        /// It reads the VALUE buffer of the cell (<paramref name="refsCol"/>, <paramref name="parentRow"/>) and performs a
        /// linear search using <see cref="MetadataTableRefVec.Find"/>.
        /// </para>
        /// <para>
        /// If the refs vector is not initialized (see <see cref="MetadataTableRefVec.IsInitialized(ReadOnlySpan{byte})"/>),
        /// the method returns <see langword="false"/> without allocating or throwing. Callers are expected to have ensured
        /// initialization via an <c>EnsureRefVec</c>-style routine when required.
        /// </para>
        /// <para>
        /// This method uses unsafe pointer access to avoid copying. The <see cref="IMetadataTable"/> must remain alive and
        /// the underlying cell buffer must not be reallocated while the returned span is in use.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="refsCol"/> or <paramref name="parentRow"/> is outside the table bounds.
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        /// Thrown if <paramref name="table"/> has been disposed.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe bool TryHasRef(IMetadataTable table, uint refsCol, uint parentRow, in MetadataTableRef tref)
        {
            var cell = table.GetOrCreateCell(refsCol, parentRow);
            var buf = new ReadOnlySpan<byte>(cell.GetValuePointer(), cell.ValueSize);

            // If not initialized, there are no refs (EnsureRefVec initializes it beforehand).
            if (!MetadataTableRefVec.IsInitialized(buf))
                return false;

            return MetadataTableRefVec.Find(buf, tref.TableId, tref.Column, tref.Row) >= 0;
        }

        /// <summary>
        /// Copies an entire column from a source <see cref="IMetadataTable"/> to a destination table
        /// using block-based, strided memory copies when possible.
        /// </summary>
        /// <param name="src">
        /// The source <see cref="IMetadataTable"/> containing the column to copy.
        /// </param>
        /// <param name="srcCol">
        /// The zero-based index of the source column.
        /// </param>
        /// <param name="dst">
        /// The destination <see cref="IMetadataTable"/> that will receive the data.
        /// </param>
        /// <param name="dstCol">
        /// The zero-based index of the destination column.
        /// </param>
        /// <param name="rows">
        /// The number of rows to copy.
        /// </param>
        /// <param name="batchSize">
        /// Maximum number of rows to copy per batch when performing strided copies.
        /// The effective batch size is rounded up to the next multiple of 4.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if both columns expose fixed-size <see cref="ColumnBlock"/>s and
        /// the copy operation completed successfully; <see langword="false"/> if either the source
        /// or destination block could not be obtained, so the caller can fall back to a per-cell path.
        /// </returns>
        /// <remarks>
        /// <para>
        /// If both source and destination column blocks are contiguous (VALUE size equals stride),
        /// the method uses a single call to <see cref="Buffer.MemoryCopy"/> for a fast bulk copy.
        /// </para>
        /// <para>
        /// For non-contiguous (strided) layouts, the method uses specialized strided copy routines
        /// for common VALUE sizes (<c>64</c>, <c>128</c> and <c>256</c> bytes) and falls back to a
        /// generic strided implementation for other sizes.
        /// </para>
        /// <para>
        /// If <paramref name="rows"/> is zero or the column VALUE size is zero, the method returns
        /// <see langword="true"/> without performing any copy.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the source and destination column blocks have different <c>ValueSize</c>.
        /// </exception>
        internal static bool PerBlockStridedCopy(MetadataTable src, uint srcCol, MetadataTable dst, uint dstCol, uint rows, int batchSize)
        {
            // Fast exit when blocks are not available
            if (!src.TryGetColumnBlock(srcCol, out ColumnBlock srcBlk) ||
                !dst.TryGetColumnBlock(dstCol, out ColumnBlock dstBlk))
                return false;

            if (srcBlk.ValueSize != dstBlk.ValueSize)
                throw new InvalidOperationException($"CopyColumn: VALUE sizes differ (src {srcBlk.ValueSize} != dst {dstBlk.ValueSize})");

            var valueSize = (uint)srcBlk.ValueSize;
            if (valueSize == 0 || rows == 0) return true;

            // ===== FAST-PATH: contiguous (values back-to-back) =====
            if (MemCopy(rows, srcBlk, dstBlk)) return true;

            // ===== STRIDED =====
            StridedCopy(rows, batchSize, srcBlk, dstBlk, valueSize);

            return true;
        }

        /// <summary>
        /// Prepares and executes a strided column copy between two fixed-size
        /// <see cref="ColumnBlock"/> instances.
        /// </summary>
        /// <param name="rows">
        /// Total number of rows to copy.
        /// </param>
        /// <param name="batchSize">
        /// Requested maximum number of rows to process per batch. The effective value is
        /// clamped to at least 1 and rounded up to the next multiple of 4 to match the
        /// unrolled inner loops.
        /// </param>
        /// <param name="srcBlk">
        /// Source <see cref="ColumnBlock"/> describing the VALUE layout in the origin column.
        /// </param>
        /// <param name="dstBlk">
        /// Destination <see cref="ColumnBlock"/> describing the VALUE layout in the target column.
        /// </param>
        /// <param name="valueSize">
        /// Size in bytes of each VALUE payload. This is used by
        /// <see cref="StridedCopySelector(uint, uint, nuint, nuint, byte*, byte*, uint)"/>
        /// to select the appropriate specialized worker.
        /// </param>
        /// <remarks>
        /// This helper normalizes the pitches and starting pointers for source and destination
        /// and then delegates to <see cref="StridedCopySelector(uint, uint, nuint, nuint, byte*, byte*, uint)"/>
        /// to perform the actual batched, strided copy.
        /// </remarks>
        private static unsafe void StridedCopy(uint rows, int batchSize, ColumnBlock srcBlk, ColumnBlock dstBlk, uint valueSize)
        {
            var sPitch = (nuint)srcBlk.Stride;
            var dPitch = (nuint)dstBlk.Stride;
            var sStart = srcBlk.GetValuePtr(0);
            var dStart = dstBlk.GetValuePtr(0);

            StridedCopySelector(rows, valueSize, sPitch, dPitch, sStart, dStart, AlignBatchTo4(batchSize));
        }

        /// <summary>
        /// Attempts a single bulk memory copy for the requested number of rows when both
        /// source and destination column blocks are laid out contiguously.
        /// </summary>
        /// <param name="rows">
        /// Number of rows whose VALUE payloads should be copied.
        /// </param>
        /// <param name="srcBlk">
        /// Source <see cref="ColumnBlock"/> describing the VALUE layout in the origin column.
        /// </param>
        /// <param name="dstBlk">
        /// Destination <see cref="ColumnBlock"/> describing the VALUE layout in the target column.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a single contiguous <see cref="Buffer.MemoryCopy"/> was performed;
        /// otherwise <see langword="false"/>, so that the caller can fall back to a strided copy path.
        /// </returns>
        /// <remarks>
        /// Assumes both blocks have the same <c>ValueSize</c>; this is enforced by the caller
        /// (see <see cref="PerBlockStridedCopy"/>).
        /// </remarks>
        private static unsafe bool MemCopy(uint rows, ColumnBlock srcBlk, ColumnBlock dstBlk)
        {
            bool srcContig = srcBlk.Stride == srcBlk.ValueSize;
            bool dstContig = dstBlk.Stride == dstBlk.ValueSize;

            if (!(srcContig && dstContig))
                return false;

            byte* s0 = srcBlk.GetValuePtr(0);
            byte* d0 = dstBlk.GetValuePtr(0);

            long total = rows * srcBlk.ValueSize;
            Buffer.MemoryCopy(s0, d0, total, total);
            return true;
        }

        /// <summary>
        /// Selects the appropriate strided copy routine based on VALUE size and executes
        /// the copy in batches for the given source and destination layouts.
        /// </summary>
        /// <param name="rows">
        /// Total number of rows to copy.
        /// </param>
        /// <param name="valueSize">
        /// Size in bytes of each VALUE payload in the column.
        /// </param>
        /// <param name="sPitch">
        /// Stride in bytes between consecutive rows in the source column.
        /// </param>
        /// <param name="dPitch">
        /// Stride in bytes between consecutive rows in the destination column.
        /// </param>
        /// <param name="sStart">
        /// Pointer to the first VALUE in the source column.
        /// </param>
        /// <param name="dStart">
        /// Pointer to the first VALUE in the destination column.
        /// </param>
        /// <param name="maxBatch">
        /// Maximum number of rows to process per batch; typically already aligned
        /// to a multiple of 4 for unrolled inner loops.
        /// </param>
        /// <remarks>
        /// For common fixed VALUE sizes (<c>64</c>, <c>128</c>, <c>256</c> bytes), this method
        /// delegates to specialized unrolled workers; for other sizes it uses a generic
        /// strided copy implementation.
        /// </remarks>
        private static unsafe void StridedCopySelector(uint rows, uint valueSize, nuint sPitch, nuint dPitch, byte* sStart, byte* dStart, int maxBatch)
        {
            switch (valueSize)
            {
                case 64:
                    CopyStridedBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, &StridedCopy64);
                    break;
                case 128:
                    CopyStridedBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, &StridedCopy128);
                    break;
                case 256:
                    CopyStridedBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, &StridedCopy256);
                    break;
                default:
                    CopyStridedGenericBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, valueSize);
                    break;
            }
        }

        /// <summary>
        /// Executes a strided copy loop in batches, delegating the actual per-row copy
        /// to a specialized worker for a fixed VALUE size.
        /// </summary>
        /// <param name="rows">Total number of rows to copy.</param>
        /// <param name="maxBatch">
        /// Maximum number of rows to process per batch. Typically already aligned
        /// to a multiple of 4 for unrolled inner loops.
        /// </param>
        /// <param name="dStart">Pointer to the first destination VALUE in the column.</param>
        /// <param name="sStart">Pointer to the first source VALUE in the column.</param>
        /// <param name="dPitch">Destination stride in bytes between consecutive rows.</param>
        /// <param name="sPitch">Source stride in bytes between consecutive rows.</param>
        /// <param name="worker">
        /// Function pointer that performs the actual strided copy for a given batch
        /// and VALUE size (e.g., <see cref="StridedCopy64"/>, <see cref="StridedCopy128"/>).
        /// </param>
        private static unsafe void CopyStridedBatched(uint rows, int maxBatch, byte* dStart, byte* sStart, nuint dPitch, nuint sPitch, delegate*<byte*, byte*, nuint, nuint, uint, void> worker)
        {
            ForEachRowBatch(rows, maxBatch, (start, count) =>
            {
                worker(dStart + dPitch * start, sStart + sPitch * start, dPitch, sPitch, (uint)count);
            });
        }

        /// <summary>
        /// Executes a strided copy loop in batches for arbitrary VALUE sizes,
        /// using <see cref="StridedCopyGeneric"/> as the inner worker.
        /// </summary>
        /// <param name="rows">Total number of rows to copy.</param>
        /// <param name="maxBatch">
        /// Maximum number of rows to process per batch. Typically already aligned
        /// to a multiple of 4 for unrolled inner loops.
        /// </param>
        /// <param name="dStart">Pointer to the first destination VALUE in the column.</param>
        /// <param name="sStart">Pointer to the first source VALUE in the column.</param>
        /// <param name="dPitch">Destination stride in bytes between consecutive rows.</param>
        /// <param name="sPitch">Source stride in bytes between consecutive rows.</param>
        /// <param name="valueSize">Size in bytes of each VALUE payload to copy.</param>
        private static unsafe void CopyStridedGenericBatched(uint rows, int maxBatch, byte* dStart, byte* sStart, nuint dPitch, nuint sPitch, uint valueSize)
        {
            ForEachRowBatch(rows, maxBatch, (start, count) =>
            {
                var d = dStart + dPitch * start;
                var s = sStart + sPitch * start;
                StridedCopyGeneric(d, s, dPitch, sPitch, valueSize, (uint)count);
            });
        }

        /// <summary>
        /// Copies values row-by-row from a source column to a destination column,
        /// using per-cell accessors as a fallback when block-based copy is not applicable.
        /// </summary>
        /// <param name="src">The source <see cref="IMetadataTable"/> containing the column to copy.</param>
        /// <param name="srcCol">The zero-based index of the source column.</param>
        /// <param name="dst">The destination <see cref="IMetadataTable"/> that will receive the data.</param>
        /// <param name="dstCol">The zero-based index of the destination column.</param>
        /// <param name="rows">The number of rows to copy.</param>
        /// <remarks>
        /// <para>
        /// This method retrieves each cell via <c>TryGetCell</c>/<c>GetOrCreateCell</c> and copies the
        /// value bytes with <see cref="Buffer.MemoryCopy"/>. It is intended as a correctness-oriented
        /// fallback when contiguous or strided block copies cannot be used.
        /// </para>
        /// <para>
        /// The method enforces that source and destination cells for each row have the same value size.
        /// Any mismatch results in an exception.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when a source cell for a given row is missing, or when the value sizes
        /// of the source and destination cells differ for any row.
        /// </exception>
        internal static unsafe void PerCellCopy(IMetadataTable src, uint srcCol, IMetadataTable dst, uint dstCol, uint rows)
        {
            for (uint row = 0; row < rows; row++)
            {
                if (!src.TryGetCell(srcCol, row, out var sCell))
                    throw new InvalidOperationException($"CopyColumn: src missing row {row}.");

                var dCell = dst.GetOrCreateCell(dstCol, row);
                if (dCell.ValueSize != sCell.ValueSize)
                    throw new InvalidOperationException($"CopyColumn: row {row} VALUE sizes differ (src {sCell.ValueSize} != dst {dCell.ValueSize})");

                Buffer.MemoryCopy(
                    sCell.GetValuePointer(),
                    dCell.GetValuePointer(),
                    dCell.ValueSize,
                    sCell.ValueSize);
            }
        }

        // ===== helpers strided =====

        /// <summary>
        /// Specialized strided copy for 64-byte VALUEs, unrolled in groups of four rows
        /// to reduce loop overhead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopy64(byte* d, byte* s, nuint dPitch, nuint sPitch, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, 64);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, 64);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, 64);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, 64);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, 64);
                d += dPitch; s += sPitch;
            }
        }
        /// <summary>
        /// Specialized strided copy for 128-byte VALUEs, unrolled in groups of four rows.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopy128(byte* d, byte* s, nuint dPitch, nuint sPitch, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, 128);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, 128);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, 128);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, 128);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, 128);
                d += dPitch; s += sPitch;
            }
        }
        /// <summary>
        /// Specialized strided copy for 256-byte VALUEs, unrolled in groups of four rows.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopy256(byte* d, byte* s, nuint dPitch, nuint sPitch, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, 256);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, 256);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, 256);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, 256);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, 256);
                d += dPitch; s += sPitch;
            }
        }
        /// <summary>
        /// Generic strided copy for arbitrary VALUE sizes, with a four-row unrolled inner loop.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopyGeneric(byte* d, byte* s, nuint dPitch, nuint sPitch, uint valueSize, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, valueSize);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, valueSize);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, valueSize);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, valueSize);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, valueSize);
                d += dPitch; s += sPitch;
            }
        }

        /// <summary>
        /// Validates that a <see cref="TableSpec"/> contains all required fields.
        /// </summary>
        /// <param name="spec">The table specification to validate.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when the table name or map path is null/whitespace, or when the column list is null or empty.
        /// </exception>
        internal static void ValidateTableSpec(TableSpec spec)
        {
            if (string.IsNullOrWhiteSpace(spec.Name))
                throw new ArgumentException("TableSpec.Name cannot be null or whitespace.", nameof(spec));

            if (string.IsNullOrWhiteSpace(spec.MapPath))
                throw new ArgumentException("TableSpec.MapPath cannot be null or whitespace.", nameof(spec));

            if (spec.Columns is null || spec.Columns.Length == 0)
                throw new ArgumentException("TableSpec.Columns must contain at least one column.", nameof(spec));
        }

        /// <summary>
        /// Computes a heuristic batch size for column operations based on the VALUE size.
        /// </summary>
        /// <param name="valueSize">
        /// Per-row VALUE size in bytes. When zero (unknown or variable-size),
        /// a conservative default is returned.
        /// </param>
        /// <returns>
        /// A batch size in rows, clamped to a safe range and aligned to a multiple of four
        /// to play nicely with unrolled loops.
        /// </returns>
        /// <remarks>
        /// The heuristic targets roughly half of an L2 cache per core (~256 KiB)
        /// assuming a read+write pattern per row.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static int ComputeBatchFromValueSize(uint valueSize)
        {
            // Heurística segura (bandwidth-bound). target ≈ mitad de L2 por núcleo (~256 KiB).
            const int targetBytes = 256 * 1024;
            const int minBatch = 64, maxBatch = 4096;

            if (valueSize == 0) return MetaDBManager.DEFAULT_BATCH_SIZE;   // columna variable o desconocida → conservador
            int perRow = (int)(valueSize * 2u);              // read + write
            int b = Math.Clamp(targetBytes / Math.Max(1, perRow), minBatch, maxBatch);
            // align to 4 for unrolled loops
            return AlignBatchTo4(b);
        }

        /// <summary>
        /// Attempts to determine the VALUE size (in bytes) for a given column.
        /// </summary>
        /// <param name="table">Target table.</param>
        /// <param name="col">Zero-based column index.</param>
        /// <returns>
        /// The VALUE size in bytes when discoverable (e.g. via <see cref="ColumnBlock"/> or
        /// an existing cell), or <c>0</c> if the column is variable-sized or empty.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static uint GetColumnValueSize(IMetadataTable table, uint col)
        {
            if (table is MetadataTable mdTable && mdTable.TryGetColumnBlock(col, out var blk)) return (uint)blk.ValueSize;
            if (table.TryGetCell(col, 0, out var cell)) return (uint)cell.ValueSize;
            return 0; // desconocido/variable
        }

        /// <summary>Fills a column cell-by-cell using a per-row factory, batching value creation and using stack allocation or an array pool depending on the total byte size.</summary>
        /// <typeparam name="T">
        /// Unmanaged value type written into each row. The raw bytes of <typeparamref name="T"/>
        /// are copied directly into the VALUE buffer of each cell.
        /// </typeparam>
        /// <param name="table">
        /// Target <see cref="IMetadataTable"/> whose column will be filled.
        /// </param>
        /// <param name="column">
        /// Zero-based column index to fill.
        /// </param>
        /// <param name="rows">
        /// Number of rows to fill, starting at 0.
        /// </param>
        /// <param name="factory">
        /// Factory that produces a value for a given row index. It is invoked once per row
        /// in each batch and its result is written into the corresponding cell.
        /// </param>
        /// <param name="batchSize">
        /// Maximum number of rows processed per batch when generating and writing values.
        /// Larger batches reduce per-call overhead but increase transient memory usage.
        /// </param>
        /// <param name="tSize">
        /// Size in bytes of <typeparamref name="T"/>; used to compute the total batch byte size
        /// and decide whether to use stack allocation or rent from the shared <see cref="ArrayPool{T}"/>.
        /// </param>
        /// <param name="MaxStackBytes">
        /// Maximum total number of bytes allowed on the stack for a single batch. When
        /// <c>batchSize * tSize</c> exceeds this threshold, the method rents a temporary
        /// buffer from <see cref="ArrayPool{T}.Shared"/> instead of using <c>stackalloc</c>.
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when a cell's VALUE buffer is smaller than <paramref name="tSize"/> for any row,
        /// i.e. when <c>cell.ValueSize &lt; tSize</c>, indicating a mismatch between the column
        /// layout and the size of <typeparamref name="T"/>.
        /// </exception>
        /// <remarks>
        /// <para>
        /// This routine is used when a contiguous <see cref="ColumnBlock"/> fast-path is not
        /// available and the column must be populated via per-cell access. Values are generated
        /// in batches into a temporary span and then written into the underlying cells using
        /// <see cref="Unsafe.WriteUnaligned{T}(void*, T)"/>.
        /// </para>
        /// <para>
        /// The method does not clear rented buffers when returning them to the pool, as the
        /// contents are transient and not reused outside the current batch.
        /// </para>
        /// </remarks>
        internal static unsafe void PerCellFill<T>(IMetadataTable table, uint column, uint rows, Func<uint, T> factory, int batchSize, int tSize, int MaxStackBytes) where T : unmanaged
        {
            ForEachBatch(rows, factory, batchSize, tSize, MaxStackBytes, (start, batch) =>
            {
                for (int i = 0; i < batch.Length; i++)
                {
                    uint row = start + (uint)i;
                    var cell = table.GetOrCreateCell(column, row);

                    if (cell.ValueSize < tSize)
                        throw new InvalidOperationException(
                            $"[{table}] col={column} row={row}: VALUE {cell.ValueSize} < sizeof({typeof(T).Name})={tSize}");

                    Unsafe.WriteUnaligned(cell.GetValuePointer(), batch[i]);
                }
            });
        }

        /// <summary>Fills a fixed-size VALUE column using strided writes over a <see cref="ColumnBlock"/>, batching allocations and using stack or pooled buffers based on byte size.</summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="rows">Number of rows to fill, starting at 0.</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="batchSize">Maximum number of rows processed per batch when generating and writing values.</param>
        /// <param name="tSize">Size in bytes of <typeparamref name="T"/>; used to compute batch byte size.</param>
        /// <param name="MaxStackBytes">Maximum total bytes allowed on the stack before falling back to the shared array pool.</param>
        /// <param name="blk">Column block describing the underlying fixed-size VALUE layout (base pointer, stride and offsets).</param>
        internal static unsafe void StridedFill<T>(uint rows, Func<uint, T> factory, int batchSize, int tSize, int MaxStackBytes, ColumnBlock blk) where T : unmanaged
        {
            byte* colBase = blk.Base + blk.ValueOffset;
            nuint pitch = (nuint)blk.Stride;

            ForEachBatch(rows, factory, batchSize, tSize, MaxStackBytes, (start, batch) =>
            {
                byte* p = colBase + pitch * start;

                for (int i = 0; i < batch.Length; i++)
                {
                    Unsafe.WriteUnaligned(p, batch[i]);
                    p += pitch;
                }
            });
        }

        /// <summary>Attempts a contiguous fast-path column fill over a <see cref="ColumnBlock"/> when VALUEs are tightly packed and match <typeparamref name="T"/> in size.</summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="rows">Number of rows to fill, starting at 0.</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="batchSize">Maximum number of rows processed per batch when generating values.</param>
        /// <param name="tSize">Size in bytes of <typeparamref name="T"/>; must match <see cref="ColumnBlock.ValueSize"/> for the fast path to be used.</param>
        /// <param name="MaxStackBytes">Maximum total bytes allowed on the stack before falling back to the shared array pool.</param>
        /// <param name="blk">Column block describing the contiguous VALUE layout (base pointer, stride and offsets).</param>
        /// <returns><see langword="true"/> if the contiguous fast path was taken and all rows were written; otherwise <see langword="false"/> so that callers can fall back to strided or per-cell paths.</returns>
        internal static unsafe bool TryContiguousFill<T>(uint rows, Func<uint, T> factory, int batchSize, int tSize, int MaxStackBytes, ColumnBlock blk) where T : unmanaged
        {
            // Only when truly contiguous AND sizes match exactly.
            if (blk.Stride != blk.ValueSize || tSize != blk.ValueSize) return false;
            byte* basePtr = blk.GetValuePtr(0);

            ForEachBatch(rows, factory, batchSize, tSize, MaxStackBytes, (start, batch) =>
            {
                var srcBytes = MemoryMarshal.AsBytes(batch);

                fixed (byte* srcPtr = &MemoryMarshal.GetReference(srcBytes))
                {
                    nuint offset = start * (nuint)blk.ValueSize;
                    byte* dest = basePtr + offset;

                    Buffer.MemoryCopy(srcPtr, dest, srcBytes.Length, srcBytes.Length);
                }
            });

            return true;
        }

        /// <summary>
        /// Iterates over the specified row range in batches, materializing values into a temporary
        /// buffer and delegating the write logic for each batch to the provided <paramref name="action"/>.
        /// </summary>
        /// <typeparam name="T">
        /// Unmanaged value type produced by <paramref name="factory"/> and stored in the batch buffer.
        /// </typeparam>
        /// <param name="rows">
        /// Total number of rows to process, starting from row index 0 up to <c>rows - 1</c>.
        /// </param>
        /// <param name="factory">
        /// Factory function that produces a value for a given row index. It is invoked once per row
        /// in each batch to populate the temporary buffer.
        /// </param>
        /// <param name="batchSize">
        /// Maximum number of rows processed per batch. Larger values reduce per-call overhead but
        /// increase the size of the temporary buffer.
        /// </param>
        /// <param name="tSize">
        /// Size in bytes of <typeparamref name="T"/>. Used together with <paramref name="batchSize"/>
        /// to compute the total byte size of each batch.
        /// </param>
        /// <param name="maxStackBytes">
        /// Maximum total number of bytes allowed on the stack for a single batch. When
        /// <c>batchSize * tSize</c> exceeds this threshold, the method rents a buffer from
        /// <see cref="ArrayPool{T}.Shared"/> instead of using <c>stackalloc</c>.
        /// </param>
        /// <param name="action">
        /// Callback that receives the starting row index of the batch and the span containing the
        /// generated values. It is responsible for writing the batch to the underlying storage.
        /// </param>
        /// <remarks>
        /// <para>
        /// This method centralizes the logic for:
        /// </para>
        /// <list type="bullet">
        ///   <item><description>Batching rows into chunks.</description></item>
        ///  <item><description>Choosing between stack allocation and pooled arrays based on byte size.</description></item>
        ///   <item><description>Invoking the per-row <paramref name="factory"/> to populate a batch.</description></item>
        ///   <item><description>Returning rented buffers to the shared pool.</description></item>
        /// </list>
        /// Callers provide the write strategy via <paramref name="action"/> so that different
        /// filling patterns (per-cell, strided, contiguous, etc.) can share the same batching logic.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ForEachBatch<T>(uint rows, Func<uint, T> factory, int batchSize, int tSize, int maxStackBytes, Action<uint, Span<T>> action) where T : unmanaged
        {
            var castedSize = (uint)batchSize;
            for (uint start = 0; start < rows; start += castedSize)
            {
                int count = GetCount(rows, castedSize, start);

                int bytes = count * tSize;
                T[]? rented = null;
                scoped Span<T> batch;

                if (bytes <= maxStackBytes) batch = stackalloc T[count];
                else
                {
                    rented = ArrayPool<T>.Shared.Rent(count);
                    batch = rented.AsSpan(0, count);
                }

                // Produce values
                for (int i = 0; i < count; i++)
                    batch[i] = factory(start + (uint)i);

                // Consumer decides how to write them
                action(start, batch);

                if (rented is not null) ArrayPool<T>.Shared.Return(rented);
            }
        }

        /// <summary>
        /// Computes the number of remaining items available from a given starting index,
        /// clamped to a maximum batch size.
        /// </summary>
        /// <param name="rows">
        /// Total number of rows available.
        /// </param>
        /// <param name="castedSize">
        /// Maximum number of items allowed in this batch (already cast to <see cref="uint"/>).
        /// </param>
        /// <param name="start">
        /// Starting row index for the current batch.
        /// </param>
        /// <returns>
        /// The number of items to process in the batch, guaranteed to be between
        /// <c>0</c> and <paramref name="castedSize"/>, and never exceeding the remaining rows.
        /// </returns>
        /// <remarks>
        /// This helper is used to safely compute batch counts without overflowing or
        /// producing negative values when nearing the end of the row range.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetCount(uint rows, uint castedSize, uint start) => (int)Math.Min(castedSize, rows - start);

        /// <summary>
        /// Iterates over a row range in fixed-size batches and invokes the provided callback
        /// for each batch.
        /// </summary>
        /// <param name="rows">
        /// Total number of rows to process, starting from row index 0 up to <c>rows - 1</c>.
        /// </param>
        /// <param name="batchSize">
        /// Maximum number of rows to include in each batch. The last batch may contain fewer
        /// rows if <paramref name="rows"/> is not an exact multiple of <paramref name="batchSize"/>.
        /// </param>
        /// <param name="action">
        /// Callback invoked once per batch, receiving the starting row index of the batch
        /// and the number of rows in that batch.
        /// </param>
        /// <remarks>
        /// This helper only performs batch segmentation and delegates the actual work
        /// to <paramref name="action"/>. It is intended to be used by higher-level
        /// routines that need to iterate over rows in chunks (e.g. for cache-friendly
        /// processing or to keep inner loops small and predictable).
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ForEachRowBatch(uint rows, int batchSize, Action<uint, int> action)
        {
            var castedSize = (uint)batchSize;
            for (uint start = 0; start < rows; start += castedSize)
            {
                var count = GetCount(rows, castedSize, start);
                action(start, count);
            }
        }

        /// <summary>
        /// Attempts a block-based fast path for filling a fixed-size VALUE column
        /// using a raw writer callback over a <see cref="ColumnBlock"/>.
        /// </summary>
        /// <param name="table">Target <see cref="IMetadataTable"/>.</param>
        /// <param name="column">Zero-based index of the column to fill.</param>
        /// <param name="rows">Number of rows to fill (starting at 0).</param>
        /// <param name="writer">
        /// Callback invoked for each row with the row index, a pointer to the VALUE
        /// buffer and its size in bytes.
        /// </param>
        /// <param name="batchSize">
        /// Maximum number of rows to visit per outer-loop batch. Used to keep the
        /// inner loop small and branch-free.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a fixed-size <see cref="ColumnBlock"/> was
        /// available and all rows were processed using the block-based fast path;
        /// <see langword="false"/> if the column does not expose a suitable block
        /// (in which case callers should fall back to per-cell mode).
        /// </returns>
        /// <remarks>
        /// This helper is used as the “fast path” by
        /// <see cref="FillColumn(IMetadataTable, uint, uint, Action{uint, nint, uint}, CapacityPolicy, int)"/>.
        /// It only applies when the column reports a non-zero, fixed VALUE size.
        /// Variable-size columns always return <see langword="false"/>.
        /// </remarks>
        internal static unsafe bool TryContiguousFillRaw(MetadataTable table, uint column, uint rows, Action<uint, nint, uint> writer, int batchSize)
        {
            if (!table.TryGetColumnBlock(column, out var blk))
                return false;

            // Variable-size values cannot use the block fast path; fall back.
            if (blk.ValueSize == 0)
                return false;

            byte* valueBase = blk.Base + blk.ValueOffset;
            uint vsize = (uint)blk.ValueSize;
            nuint pitch = (nuint)blk.Stride;

            // Batching keeps the hot loop short and branch-free
            ForEachRowBatch(rows, batchSize, (start, count) =>
            {
                byte* p = valueBase + pitch * start;
                for (int i = 0; i < count; i++)
                {
                    writer(start + (uint)i, (nint)p, vsize);
                    p += pitch;
                }
            });

            return true;
        }

        /// <summary>
        /// Fills a column cell-by-cell using the raw writer callback as a fallback
        /// when no suitable fixed-size <see cref="ColumnBlock"/> is available.
        /// </summary>
        /// <param name="table">Target <see cref="IMetadataTable"/>.</param>
        /// <param name="column">Zero-based index of the column to fill.</param>
        /// <param name="rows">Number of rows to fill (starting at 0).</param>
        /// <param name="writer">
        /// Callback invoked for each row with the row index, a pointer to the VALUE
        /// buffer and its size in bytes.
        /// </param>
        /// <param name="batchSize">
        /// Chunk size used only to segment the outer loop; does not affect the
        /// semantics of the operation.
        /// </param>
        /// <remarks>
        /// <para>
        /// This helper uses <see cref="IMetadataTable.GetOrCreateCell(uint, uint)"/>
        /// per row to retrieve the target cell and forwards its VALUE pointer and
        /// size to the provided <paramref name="writer"/>.
        /// </para>
        /// <para>
        /// Intended as a correctness-oriented fallback when block-based access
        /// cannot be used (e.g. variable-size columns, missing blocks, etc.).
        /// </para>
        /// </remarks>
        internal static unsafe void PerCellFillRaw(IMetadataTable table, uint column, uint rows, Action<uint, nint, uint> writer, int batchSize)
        {
            ForEachRowBatch(rows, batchSize, (start, count) =>
            {
                for (int i = 0; i < count; i++)
                {
                    uint row = start + (uint)i;
                    var cell = table.GetOrCreateCell(column, row);
                    writer(row, (nint)cell.GetValuePointer(), (uint)cell.ValueSize);
                }
            });
        }

        /// <summary>
        /// Aligns a batch size to the next multiple of 4.
        /// </summary>
        /// <param name="batch">Unaligned batch size; must be strictly positive.</param>
        /// <returns>
        /// The smallest multiple of 4 greater than or equal to <paramref name="batch"/>.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when <paramref name="batch"/> is less than or equal to zero.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int AlignBatchTo4(int batch)
        {
            if (batch <= 0) throw new InvalidOperationException("Batch size must be a strictly positive number.");
            return batch + 3 & ~3;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void EnsureDirForFile(string filePath)
        {
            var dir = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrWhiteSpace(dir))
                Directory.CreateDirectory(dir);
        }

        /// <summary>
        /// Attempts to delete a file at <paramref name="path"/>, retrying a small number of times to tolerate transient I/O and locking conditions.
        /// </summary>
        /// <param name="path">File path to delete.</param>
        /// <param name="attempts">Maximum number of delete attempts. Each retry uses a small increasing backoff.</param>
        /// <returns><see langword="true"/> if the file was deleted or did not exist; otherwise <see langword="false"/>.</returns>
        /// <remarks>
        /// <para>This helper treats "file missing" as success.</para>
        /// <para>It retries only for common transient cases: <see cref="IOException"/> and <see cref="UnauthorizedAccessException"/> (the latter is often returned when a file is temporarily read-only or locked by another process).</para>
        /// <para>After exhausting retries, it returns <c>!File.Exists(path)</c> to handle races where the file was deleted by another actor between attempts.</para>
        /// </remarks>
        internal static async Task<bool> TryDeleteWithRetries(string path, int attempts = 8)
        {
            for (int i = 0; i < attempts; i++)
            {
                try { File.Delete(path); return true; }
                catch (FileNotFoundException) { return true; }
                catch (DirectoryNotFoundException) { return true; }
                catch (IOException) when (i < attempts - 1) { await Task.Delay(10 * (i + 1)); }
                catch (UnauthorizedAccessException) when (i < attempts - 1) { await Task.Delay(10 * (i + 1)); }
            }

            // Last try
            try { File.Delete(path); return true; }
            catch (FileNotFoundException) { return true; }
            catch (DirectoryNotFoundException) { return true; }
            catch { return !File.Exists(path); }
        }

        /// <summary>
        /// Attempts to rename an existing file to a unique "move-aside" name to enable eventual deletion.
        /// </summary>
        /// <param name="path">Original file path.</param>
        /// <returns>
        /// The moved file path when the file existed and was moved; otherwise <see langword="null"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The moved name is generated by appending <c>.deleting.&lt;guid&gt;</c> to the original path.
        /// </para>
        /// <para>
        /// This method does not catch exceptions: callers typically wrap it to decide whether to log,
        /// retry, or enqueue cleanup when the move fails (e.g., due to OS sharing locks).
        /// </para>
        /// </remarks>
        internal static string? TryMoveAside(string path)
        {
            if (!File.Exists(path)) return null;
            var moved = path + ".deleting." + Guid.NewGuid().ToString("N");
            File.Move(path, moved);
            return moved;
        }

        /// <summary>
        /// Computes the maximum deletion work to attempt in a single cycle, scaling with backlog size.
        /// </summary>
        /// <param name="backlog">Current pending deletion count.</param>
        /// <param name="baseDeletes">Baseline maximum deletions per cycle.</param>
        /// <param name="baseAttempts">Baseline maximum attempts per cycle.</param>
        /// <param name="maxDeletesPerCycle">Result: max successful deletions before stopping the cycle.</param>
        /// <param name="maxAttemptsPerCycle">Result: max delete attempts before stopping the cycle.</param>
        /// <remarks>
        /// Scaling ensures that when the queue is large, the worker can apply more pressure,
        /// but remains bounded to avoid starving the process.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ComputeCycleBudget(int backlog, int baseDeletes, int baseAttempts, out int maxDeletesPerCycle, out int maxAttemptsPerCycle)
        {
            maxDeletesPerCycle = baseDeletes;
            maxAttemptsPerCycle = baseAttempts;

            // Simple tiering. Adjust thresholds freely.
            if (backlog >= 10_000) { maxDeletesPerCycle *= 8; maxAttemptsPerCycle *= 8; }
            else if (backlog >= 1_000) { maxDeletesPerCycle *= 4; maxAttemptsPerCycle *= 4; }
            else if (backlog >= 256) { maxDeletesPerCycle *= 2; maxAttemptsPerCycle *= 2; }
        }

        /// <summary>
        /// Estimates the number of new items enqueued while a deletion cycle was running.
        /// </summary>
        /// <param name="backlogBefore">Backlog size at cycle start.</param>
        /// <param name="backlogAfter">Backlog size at cycle end.</param>
        /// <param name="deletedThisCycle">Number of deletions that succeeded during the cycle.</param>
        /// <returns>Estimated new enqueued items (never negative).</returns>
        /// <remarks>
        /// Based on:
        /// <code>
        /// backlogAfter ≈ backlogBefore - deleted + arrivals  => arrivals ≈ backlogAfter - (backlogBefore - deleted)
        /// </code>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int EstimateArrivals(int backlogBefore, int backlogAfter, int deletedThisCycle)
        {
            int arrivals = backlogAfter - (backlogBefore - deletedThisCycle);
            return arrivals > 0 ? arrivals : 0;
        }

        /// <summary>
        /// Determines whether the worker is under “storm” conditions where compaction should be avoided.
        /// </summary>
        /// <param name="backlogBefore">Backlog size at cycle start.</param>
        /// <param name="backlogAfter">Backlog size at cycle end.</param>
        /// <param name="deletedThisCycle">Successful deletions in the cycle.</param>
        /// <param name="arrivalsThisCycle">Estimated new enqueued items during the cycle.</param>
        /// <returns><see langword="true"/> if storm conditions are detected; otherwise <see langword="false"/>.</returns>
        /// <remarks>
        /// Storm conditions typically mean:
        /// <list type="bullet">
        ///   <item><description>Backlog is not shrinking (trend ≥ 0)</description></item>
        ///   <item><description>Arrivals keep up with or exceed deletes</description></item>
        ///   <item><description>Backlog is large enough that compaction churn is undesirable</description></item>
        /// </list>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsStorm(int backlogBefore, int backlogAfter, int deletedThisCycle, int arrivalsThisCycle)
        {
            int trend = backlogAfter - backlogBefore;
            if (trend >= 0) return true;
            if (arrivalsThisCycle >= deletedThisCycle) return true;

            // Conservative: treat big backlogs as storm by default.
            if (backlogAfter >= 1_000) return true;

            return false;
        }

        /// <summary>
        /// Updates the compaction cooldown based on storm vs drain mode to throttle disk rewrites.
        /// </summary>
        /// <param name="storm">Whether storm conditions are active.</param>
        /// <param name="stormScore">
        /// Storm persistence score used to avoid jitter: increases with sustained storm, decreases when draining.
        /// </param>
        /// <param name="compactCooldownMs">Current cooldown in milliseconds (updated in place).</param>
        /// <param name="minCooldownMs">Minimum allowed cooldown.</param>
        /// <param name="maxCooldownMs">Maximum allowed cooldown.</param>
        /// <remarks>
        /// In storm mode the cooldown grows quickly (fewer compactions).
        /// In drain mode the cooldown shrinks (more compactions) to compact the append-only log.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void UpdateCompactionCooldown(bool storm, ref int stormScore, ref int compactCooldownMs, int minCooldownMs, int maxCooldownMs)
        {
            if (storm)
            {
                stormScore = Math.Min(10, stormScore + 1);

                int mult = 2 + stormScore / 3; // ~2..5
                long next = (long)compactCooldownMs * mult;
                compactCooldownMs = (int)Math.Min(maxCooldownMs, next);
            }
            else
            {
                stormScore = Math.Max(0, stormScore - 2);
                compactCooldownMs = Math.Max(minCooldownMs, compactCooldownMs / 2);
            }
        }

        /// <summary>
        /// Computes the delay before the next cycle based on progress and contention signals.
        /// </summary>
        /// <param name="stats">Cycle stats.</param>
        /// <param name="busyDelayMs">Delay used when work is being done.</param>
        /// <param name="idleDelayMs">Delay used when no progress is made.</param>
        /// <returns>Delay in milliseconds.</returns>
        /// <remarks>
        /// If no deletions succeeded and success rate is very low, the worker backs off more aggressively
        /// to avoid burning CPU while files remain locked.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ComputeNextDelayMs(in MetaDBManager.DeleteCycleStats stats, int busyDelayMs, int idleDelayMs)
        {
            if (stats.Deleted > 0)
                return busyDelayMs;

            // If essentially everything is locked, don't spin.
            if (stats.SuccessRate < 0.05)
                return idleDelayMs;

            return idleDelayMs / 2;
        }

        /// <summary>
        /// Awaits a delay while tolerating cancellation without throwing out of the worker loop.
        /// </summary>
        /// <param name="ms">Delay in milliseconds.</param>
        /// <param name="ct">Cancellation token.</param>
        internal static async Task DelaySafe(int ms, CancellationToken ct)
        {
            try { await Task.Delay(ms, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }

        /// <summary>
        /// Ensures that the specified <paramref name="column"/> in <paramref name="table"/> can hold at least
        /// <paramref name="neededRows"/> rows, applying the requested <paramref name="policy"/>.
        /// </summary>
        /// <param name="table">Target metadata table.</param>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="neededRows">Minimum required row count (capacity) for the column.</param>
        /// <param name="policy">
        /// Capacity policy to apply:
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///     <see cref="CapacityPolicy.Throw"/> – do not grow; fail if the current capacity is insufficient.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///     <see cref="CapacityPolicy.AutoGrowZeroInit"/> – attempt to grow the column to
        ///     <paramref name="neededRows"/> (zero-initializing new rows when supported) and verify.
        ///     </description>
        ///   </item>
        /// </list>
        /// </param>
        /// <remarks>
        /// <para>
        /// This method prefers a deterministic capacity check when the table/store can report capacity via
        /// <c>TryGetColumnCapacity</c>. If that information is not available, it falls back to “probe” semantics:
        /// touching the last required row using <see cref="IMetadataTable.GetOrCreateCell(uint, uint)"/> to infer
        /// whether the requested index is within bounds.
        /// </para>
        /// <para>
        /// When <paramref name="policy"/> is <see cref="CapacityPolicy.Throw"/>, the method does not attempt to grow
        /// and will rethrow range/disposal-related exceptions originating from the table/store.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="column"/> is outside the table's column range.
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        /// Propagated if the underlying table/store has been disposed and cannot serve the request.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the capacity is insufficient under <see cref="CapacityPolicy.Throw"/>, or when growth is requested
        /// but fails, or when growth reports success but the resulting capacity is still insufficient.
        /// </exception>
        private static void EnsureRowCapacity(IMetadataTable table, uint column, uint neededRows, CapacityPolicy policy)
        {
            if (neededRows == 0) return;

            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(column, (uint)table.ColumnCount);

            // Deterministic check when available
            if (table.TryGetColumnCapacity(column, out var cap))
            {
                if (cap >= neededRows) return;

                if (policy == CapacityPolicy.Throw)
                    throw new InvalidOperationException(
                        $"Column {column}: insufficient row capacity ({cap}) for {neededRows} rows.");

                if (!table.TryGrowColumnTo(column, neededRows, zeroInit: true))
                    throw new InvalidOperationException(
                        $"GrowColumnTo could not grow column {column} to {neededRows} rows.");

                if (!table.TryGetColumnCapacity(column, out cap) || cap < neededRows)
                    throw new InvalidOperationException(
                        $"GrowColumnTo reported success but capacity is still {cap} < {neededRows} (col={column}, table={table.Spec.Name}).");

                return;
            }

            // No capacity info: fallback to probe semantics
            if (policy == CapacityPolicy.Throw)
            {
                table.GetOrCreateCell(column, neededRows - 1); // rethrows exact
                return;
            }

            if (!table.TryGrowColumnTo(column, neededRows, zeroInit: true))
                throw new InvalidOperationException(
                    $"GrowColumnTo could not grow column {column} to {neededRows} rows.");

            table.GetOrCreateCell(column, neededRows - 1);
        }

        /// <summary>
        /// Core implementation for filling a blittable column using a per-row factory,
        /// with support for column-block fast paths, batching and per-cell fallback.
        /// </summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="table">Target table.</param>
        /// <param name="column">Zero-based column index to fill.</param>
        /// <param name="rows">Number of rows to fill (starting at 0).</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="policy">
        /// Capacity policy controlling how to react when the current row capacity is insufficient.
        /// </param>
        /// <param name="batchSize">
        /// Optional batch size override. When not provided, a heuristic based on VALUE size is used.
        /// </param>
        /// <remarks>
        /// The method tries, in order:
        /// <list type="number">
        ///   <item><description>
        ///   A contiguous <see cref="ColumnBlock"/> fast path where VALUEs are back-to-back
        ///   and <typeparamref name="T"/> matches the VALUE size.
        ///   </description></item>
        ///   <item><description>
        ///   A strided write fast path using pointer arithmetic.
        ///   </description></item>
        ///   <item><description>
        ///   A per-cell fallback using <see cref="IMetadataTable.GetOrCreateCell(uint, uint)"/>.
        ///   </description></item>
        /// </list>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static void FillColumn<T>(IMetadataTable table, uint column, uint rows, Func<uint, T> factory, CapacityPolicy policy, int batchSize = MetaDBManager.DEFAULT_BATCH_SIZE) where T : unmanaged
        {
            if (rows == 0) return;

            EnsureRowCapacity(table, column, rows, policy);

            int tSize = Unsafe.SizeOf<T>();

            // Hard cap for stackalloc usage to avoid SO (tune: 8–32 KB)
            const int MaxStackBytes = 16 * 1024;

            // ── FAST PATH: via column block (MMF / flat buffer) ────────────────────
            if (table is MetadataTable mdTable && mdTable.TryGetColumnBlock(column, out var blk))
            {
                // Variable-size values cannot use the block fast path; fall back.
                if (blk.ValueSize == 0)
                {
                    PerCellFill(table, column, rows, factory, batchSize, tSize, MaxStackBytes);
                    return;
                }

                if (tSize > blk.ValueSize)
                    throw new InvalidOperationException(
                        $"[{table}] col={column}: VALUE {blk.ValueSize} < sizeof({typeof(T).Name})={tSize}");

                if (TryContiguousFill(rows, factory, batchSize, tSize, MaxStackBytes, blk)) return;
                StridedFill(rows, factory, batchSize, tSize, MaxStackBytes, blk);
                return;
            }

            // ── SLOW PATH: per-cell (compat) — still batch the factory ─────────────
            PerCellFill(table, column, rows, factory, batchSize, tSize, MaxStackBytes);
        }

        /// <summary>
        /// Core implementation for filling a column using a raw writer callback that
        /// receives a pointer to the VALUE buffer per row.
        /// </summary>
        /// <param name="table">Target table whose column will be written.</param>
        /// <param name="column">Zero-based column index to fill.</param>
        /// <param name="rows">Number of logical rows to fill (starting at 0).</param>
        /// <param name="writer">
        /// Callback invoked once per row with:
        /// <list type="bullet">
        ///   <item><description>The zero-based row index.</description></item>
        ///   <item><description>A pointer to the row VALUE buffer.</description></item>
        ///   <item><description>The VALUE buffer size in bytes.</description></item>
        /// </list>
        /// The callback is responsible for writing exactly the intended payload
        /// into the provided buffer.
        /// </param>
        /// <param name="policy">
        /// Capacity policy controlling how to react when the current row capacity
        /// is insufficient (e.g. <see cref="CapacityPolicy.Throw"/> or
        /// <see cref="CapacityPolicy.AutoGrowZeroInit"/>).
        /// </param>
        /// <param name="batchSize">
        /// Optional batch size for the column-block fast path. When a
        /// <see cref="ColumnBlock"/> is not available, it is only used to chunk
        /// the per-cell loop.
        /// </param>
        /// <remarks>
        /// <para>
        /// When a fixed-size <see cref="ColumnBlock"/> is available for the target
        /// column, the method uses a strided pointer-based loop over the underlying
        /// memory-mapped buffer to minimize per-row overhead.
        /// </para>
        /// <para>
        /// If no suitable column block exists (or the VALUE size is variable),
        /// the method falls back to per-cell access using
        /// <see cref="IMetadataTable.GetOrCreateCell(uint, uint)"/>.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static void FillColumn(IMetadataTable table, uint column, uint rows, Action<uint, nint, uint> writer, CapacityPolicy policy, int batchSize = MetaDBManager.DEFAULT_BATCH_SIZE)
        {
            if (rows == 0) return;

            EnsureRowCapacity(table, column, rows, policy);

            // ── FAST PATH: via column block (MMF / flat buffer) ────────────────────
            if (table is MetadataTable mdTable && TryContiguousFillRaw(mdTable, column, rows, writer, batchSize)) return; // BLOCK ACCESS IS INTERNAL, ONLY FOR MetadataTable
            PerCellFillRaw(table, column, rows, writer, batchSize);
        }

        // ─────────────────────────────────────────────────────────────────────
        // COPY
        // ─────────────────────────────────────────────────────────────────────
        /// <summary>
        /// Core implementation that copies VALUE payloads row-by-row from a source column
        /// to a destination column, possibly across different tables.
        /// </summary>
        /// <param name="src">Source table.</param>
        /// <param name="srcCol">Source column index.</param>
        /// <param name="dst">Destination table.</param>
        /// <param name="dstCol">Destination column index.</param>
        /// <param name="rows">Number of rows to copy (starting at 0).</param>
        /// <param name="dstPolicy">
        /// Capacity policy for the destination column.
        /// </param>
        /// <param name="batchSize">
        /// Batch size for the block-based fast paths. Used to chunk strided copies.
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when VALUE sizes differ between source and destination, when source rows
        /// are missing, or when destination capacity cannot be ensured under the selected policy.
        /// </exception>
        /// <remarks>
        /// <para>
        /// Fast paths use <see cref="ColumnBlock"/>s for both source and destination:
        /// </para>
        /// <list type="bullet">
        ///   <item><description>
        ///   Contiguous→contiguous copy (single bulk <see cref="Buffer.MemoryCopy"/>).
        ///   </description></item>
        ///   <item><description>
        ///   Strided copy specialized for VALUE sizes of 64, 128 or 256 bytes.
        ///   </description></item>
        ///   <item><description>
        ///   A generic strided copy for other VALUE sizes.
        ///   </description></item>
        /// </list>
        /// If no column blocks are available, the method falls back to per-cell copying.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static void CopyColumn(IMetadataTable src, uint srcCol, IMetadataTable dst, uint dstCol, uint rows, CapacityPolicy dstPolicy, int batchSize = MetaDBManager.DEFAULT_BATCH_SIZE)
        {
            EnsureRowCapacity(dst, dstCol, rows, dstPolicy);

            if (src is MetadataTable srcTable && dst is MetadataTable dstTable) // BLOCK ACCESS IS INTERNAL, ONLY FOR MetadataTable
            {
                bool done = PerBlockStridedCopy(srcTable, srcCol, dstTable, dstCol, rows, batchSize);
                if (done) return;
            }

            // ===== Fallback: API por celda =====
            PerCellCopy(src, srcCol, dst, dstCol, rows);
        }

        // ─────────────────────────────────────────────────────────────────────
        // REFS (parent → child links)
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Ensures the ref vector for (<paramref name="refsCol"/>, <paramref name="parentRow"/>) exists and is ready.
        /// Returns <c>true</c> if it performed first-time initialization on this call.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if VALUE is too small to hold at least one entry (needs at least
        /// <c>HeaderSize + EntrySize</c> bytes), or if growth is required but disallowed/misconfigured.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe bool EnsureRefVec(IMetadataTable table, uint refsCol, uint parentRow, CapacityPolicy policy)
        {
            // 1) Ensure row capacity (may grow)
            EnsureRowCapacity(table, refsCol, parentRow + 1, policy);

            // 2) Get VALUE buffer
            var cell = table.GetOrCreateCell(refsCol, parentRow);
            int valueSize = cell.ValueSize;
            var buf = new Span<byte>(cell.GetValuePointer(), valueSize);

            // 3) Validate minimum size for 1 entry
            int minBytes = MetadataTableRefVec.HeaderSize + MetadataTableRefVec.EntrySize;
            if (valueSize < minBytes)
                throw new InvalidOperationException(
                    $"REFS: cell VALUE={valueSize} is insufficient; needs ≥ {minBytes} " +
                    $"({MetadataTableRefVec.HeaderSize}-byte header + {MetadataTableRefVec.EntrySize}-byte entry).");

            // 4) Initialize once (distinguish 'fresh' from 'empty but inited')
            if (!MetadataTableRefVec.IsInitialized(buf))
            {
                MetadataTableRefVec.Init(buf);
                return true; // did init just now
            }

            return false; // already initialized (maybe empty)
        }
    }
}