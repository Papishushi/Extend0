using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Extend0.Metadata.Refs
{
    /// <summary>
    /// Utilities to manage a compact, fixed-capacity vector of <see cref="MetadataTableRef"/> entries
    /// stored directly inside a cell's VALUE buffer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This helper assumes a <b>packed binary layout (little-endian)</b> for the VALUE:
    /// </para>
    /// <para>
    /// <c>[0..1]</c>  = <c>Count</c> (<see cref="ushort"/>) – logical number of entries<br/>
    /// <c>[2..3]</c>  = <c>Flags</c> (<see cref="ushort"/>) – bitfield (e.g. <c>Initialized</c>)<br/>
    /// <c>[4..]</c>   = contiguous array of <see cref="MetadataTableRef"/> entries (each <see cref="EntrySize"/> bytes)
    /// </para>
    /// <para>
    /// Capacity is derived from the underlying VALUE size: the header takes <see cref="HeaderSize"/> bytes,
    /// and the remaining bytes are divided into fixed slots of <see cref="EntrySize"/> bytes each.
    /// </para>
    /// <para>
    /// The API is intentionally low-level and span-based so it can be used on top of <see cref="MetadataCell"/>
    /// without allocations, and so that callers can treat the VALUE buffer as a ref-vector “overlay”
    /// in any column configured with enough <c>ValueBytes</c>.
    /// </para>
    /// </remarks>
    public static class MetadataTableRefVec
    {
        /// <summary>
        /// Size in bytes of the header region (<c>Count</c> + <c>Flags</c>).
        /// </summary>
        public static int HeaderSize => 4;

        /// <summary>
        /// Bit mask for the <c>Initialized</c> flag inside the <c>Flags</c> field.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The current helpers do not require this flag to be set, but it can be used by higher-level
        /// code to distinguish between “never touched” buffers and those explicitly initialized with
        /// <see cref="Init(Span{byte})"/>.
        /// </para>
        /// </remarks>
        private const ushort InitializedMask = 0x01; // bit0 means “inited”

        /// <summary>
        /// Size in bytes of a single <see cref="MetadataTableRef"/> entry within the vector.
        /// </summary>
        /// <remarks>Typically 32 bytes with the current <see cref="MetadataTableRef"/> layout.</remarks>
        public static readonly int EntrySize = Unsafe.SizeOf<MetadataTableRef>(); // typically 32

        /// <summary>
        /// Computes how many <see cref="MetadataTableRef"/> entries can fit in a VALUE buffer of the given size.
        /// </summary>
        /// <param name="valueSizeBytes">Total VALUE size in bytes (header + entries region).</param>
        /// <returns>
        /// Maximum number of entries that can fit; <c>0</c> if the buffer is too small to hold even the header.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Capacity(int valueSizeBytes)
            => valueSizeBytes <= HeaderSize ? 0 : (valueSizeBytes - HeaderSize) / EntrySize;

        // ---- Count (ushort) -------------------------------------------------

        /// <summary>
        /// Reads the <c>Count</c> field (first 2 bytes) from a VALUE buffer.
        /// </summary>
        /// <remarks>Caller must ensure <paramref name="buf"/> is at least 2 bytes long.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort GetCount(ReadOnlySpan<byte> buf)
            => MemoryMarshal.Read<ushort>(buf);

        /// <inheritdoc cref="GetCount(ReadOnlySpan{byte})"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort GetCount(Span<byte> buf)
            => MemoryMarshal.Read<ushort>(buf);

        /// <summary>
        /// Writes the <c>Count</c> field (first 2 bytes) into a VALUE buffer.
        /// </summary>
        /// <remarks>Caller must ensure <paramref name="buf"/> is at least 2 bytes long.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetCount(Span<byte> buf, ushort count)
            => MemoryMarshal.Write(buf, in count);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ref ushort Flags(Span<byte> buf)
            => ref MemoryMarshal.AsRef<ushort>(buf.Slice(2, 2));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ushort Flags(ReadOnlySpan<byte> buf)
            => MemoryMarshal.Read<ushort>(buf.Slice(2, 2));

        /// <summary>
        /// Returns whether the vector has been explicitly marked as initialized using the <c>Flags</c> field.
        /// </summary>
        /// <remarks>
        /// This flag is optional from the vector logic point of view; it is provided so that callers
        /// can differentiate between “virgin” buffers and ones that were passed through <see cref="Init"/>.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsInitialized(ReadOnlySpan<byte> buf)
            => (Flags(buf) & InitializedMask) != 0;

        /// <summary>
        /// Sets the <c>Initialized</c> bit in the <c>Flags</c> field.
        /// </summary>
        /// <remarks>
        /// This is called by <see cref="Init(Span{byte}, bool)"/> when <paramref name="buf"/>
        /// is initialized with <c>markInitialized == true</c>, but you can also invoke it
        /// manually if you build the header yourself.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void MarkInitialized(Span<byte> buf)
            => Flags(buf) = (ushort)(Flags(buf) | InitializedMask);

        // ---- Init -----------------------------------------------------------

        /// <summary>
        /// Initializes the buffer to an empty vector: zeroes the entire VALUE and sets <c>Count = 0</c>.
        /// Optionally marks the <c>Initialized</c> flag.
        /// </summary>
        /// <param name="buf">Writable VALUE buffer (header + entries region).</param>
        /// <param name="markInitialized">
        /// If <see langword="true"/>, also sets the <c>Initialized</c> bit in the header.
        /// </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Init(Span<byte> buf, bool markInitialized = true)
        {
            buf.Clear();
            SetCount(buf, 0);

            if (markInitialized)
                MarkInitialized(buf);
        }

        // ---- Items view (trimmed to actual available bytes) ----------------

        /// <summary>
        /// Returns a writable span over the entry region, trimmed so it does not exceed the bytes
        /// that actually fit in the buffer.
        /// </summary>
        /// <param name="buf">Writable VALUE buffer.</param>
        /// <param name="count">Outputs the stored count from the header (even if truncated).</param>
        /// <returns>
        /// A span over the entries region. Its length is <= <paramref name="count"/> if the buffer
        /// is shorter than the declared capacity.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Span<MetadataTableRef> Items(Span<byte> buf, out ushort count)
        {
            count = GetCount(buf);
            int availEntries = Math.Max(0, (buf.Length - HeaderSize) / EntrySize);
            int take = Math.Min(count, (ushort)availEntries);
            var data = buf.Slice(HeaderSize, take * EntrySize);
            return MemoryMarshal.Cast<byte, MetadataTableRef>(data);
        }

        /// <summary>
        /// Read-only version of <see cref="Items(Span{byte}, out ushort)"/> for VALUE buffers.
        /// </summary>
        /// <param name="buf">Read-only VALUE buffer.</param>
        /// <param name="count">Outputs the stored count from the header (even if truncated).</param>
        /// <returns>
        /// A read-only span over the entries region. Its length is <= <paramref name="count"/> if
        /// the buffer is shorter than the declared capacity.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlySpan<MetadataTableRef> Items(ReadOnlySpan<byte> buf, out ushort count)
        {
            count = GetCount(buf);
            int availEntries = Math.Max(0, (buf.Length - HeaderSize) / EntrySize);
            int take = Math.Min(count, (ushort)availEntries);
            var data = buf.Slice(HeaderSize, take * EntrySize);
            return MemoryMarshal.Cast<byte, MetadataTableRef>(data);
        }

        // ---- Add ------------------------------------------------------------

        /// <summary>
        /// Tries to append an entry at the end of the vector.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is resilient to a corrupted or out-of-sync header count being smaller
        /// than the bytes that actually fit: it uses the trimmed <see cref="Items(Span{byte}, out ushort)"/>
        /// length as the logical count if they differ.
        /// </para>
        /// </remarks>
        /// <param name="buf">Writable VALUE buffer (header + entries).</param>
        /// <param name="r">Entry to append.</param>
        /// <param name="valueSizeBytes">Total VALUE size in bytes (used to compute capacity).</param>
        /// <returns>
        /// <see langword="true"/> if the entry was appended; <see langword="false"/> if the vector is full.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAdd(Span<byte> buf, in MetadataTableRef r, int valueSizeBytes)
        {
            int cap = Capacity(valueSizeBytes);
            var items = Items(buf, out var cnt);

            // If header count is smaller than what actually fits, prefer the safe items length.
            int logicalCount = cnt;
            if (logicalCount != items.Length) logicalCount = items.Length;

            if (logicalCount >= cap) return false;

            // Write directly at the next raw slot (do not depend on items.Length for offset math).
            int offset = HeaderSize + logicalCount * EntrySize;
            MemoryMarshal.Write(buf.Slice(offset, EntrySize), r);

            SetCount(buf, (ushort)(logicalCount + 1));
            return true;
        }

        // ---- ReadAt ---------------------------------------------------------

        /// <summary>
        /// Tries to read the entry at a given index.
        /// </summary>
        /// <param name="buf">Read-only VALUE buffer.</param>
        /// <param name="index">Zero-based entry index.</param>
        /// <param name="r">Outputs the entry if found.</param>
        /// <returns>
        /// <see langword="true"/> if the index is within the current logical count and the buffer
        /// is large enough; otherwise <see langword="false"/>.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGet(ReadOnlySpan<byte> buf, int index, out MetadataTableRef r)
        {
            int count = GetCount(buf);
            if ((uint)index >= (uint)count) { r = default; return false; }

            int offset = HeaderSize + index * EntrySize;
            if (offset + EntrySize > buf.Length) { r = default; return false; } // corrupted/too small buffer
            r = MemoryMarshal.Read<MetadataTableRef>(buf.Slice(offset, EntrySize));
            return true;
        }

        /// <summary>
        /// Reads the entry at <paramref name="index"/> or throws if it is out of range or the buffer is too small.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index"/> is not valid.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MetadataTableRef ReadAt(ReadOnlySpan<byte> buf, int index)
        {
            if (!TryGet(buf, index, out var r))
                throw new ArgumentOutOfRangeException(nameof(index));
            return r;
        }

        // ---- RemoveAt (compacts) -------------------------------------------

        /// <summary>
        /// Removes the entry at <paramref name="index"/> and compacts the remaining tail entries to the left.
        /// </summary>
        /// <param name="buf">Writable VALUE buffer.</param>
        /// <param name="index">Zero-based index to remove.</param>
        /// <returns>
        /// <see langword="true"/> if an entry was removed; <see langword="false"/> if
        /// <paramref name="index"/> was out of range.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRemoveAt(Span<byte> buf, int index)
        {
            var items = Items(buf, out _);
            int logicalCount = items.Length;  // trimmed to what the buffer can hold
            if ((uint)index >= (uint)logicalCount) return false;

            // Move tail left in raw bytes
            int tailCount = logicalCount - index - 1;
            if (tailCount > 0)
            {
                int srcOffset = HeaderSize + (index + 1) * EntrySize;
                int dstOffset = HeaderSize + index * EntrySize;
                buf.Slice(srcOffset, tailCount * EntrySize).CopyTo(buf[dstOffset..]);
            }

            // Clear the last slot and decrement count
            int lastOffset = HeaderSize + (logicalCount - 1) * EntrySize;
            buf.Slice(lastOffset, EntrySize).Clear();
            SetCount(buf, (ushort)(logicalCount - 1));
            return true;
        }

        // ---- Find -----------------------------------------------------------

        /// <summary>
        /// Performs a linear search for the first entry matching
        /// (<paramref name="tableId"/>, <paramref name="col"/>, <paramref name="row"/>).
        /// </summary>
        /// <param name="buf">Read-only VALUE buffer.</param>
        /// <param name="tableId">Child table id to match.</param>
        /// <param name="col">Child column index to match.</param>
        /// <param name="row">Child row index to match.</param>
        /// <returns>
        /// The zero-based index of the first match; or <c>-1</c> if no entry matches the triple.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Find(ReadOnlySpan<byte> buf, Guid tableId, uint col, uint row)
        {
            var items = Items(buf, out var _);
            int logicalCount = items.Length; // trimmed to what actually fits

            for (int i = 0; i < logicalCount; i++)
            {
                ref readonly var e = ref items[i];
                if (e.TableId == tableId && e.Column == col && e.Row == row)
                    return i;
            }
            return -1;
        }

    }
}
