using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Extend0.Metadata.Refs
{
    /// <summary>
    /// Compact, blittable reference to a single cell inside a metadata table.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This struct is designed to be stored directly in metadata columns or other
    /// binary structures. It encodes:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>The target table identifier (<see cref="TableId"/>).</description></item>
    ///   <item><description>The zero-based column index (<see cref="Column"/>).</description></item>
    ///   <item><description>The zero-based row index (<see cref="Row"/>).</description></item>
    ///   <item><description>A reserved 64-bit slot (<see cref="Reserved"/>), available for future use.</description></item>
    /// </list>
    /// <para>
    /// With the current layout and packing configuration, each entry occupies exactly
    /// 32 bytes: 16 bytes for the GUID, 8 bytes for row/column indices, and 8 bytes reserved.
    /// </para>
    /// </remarks>
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public readonly struct MetadataTableRef(Guid tableId, uint column, uint row, ulong reserved) : IEquatable<MetadataTableRef>
    {
        /// <summary>
        /// Globally unique identifier of the referenced metadata table.
        /// </summary>
        public readonly Guid TableId = tableId;   // 16

        /// <summary>
        /// Zero-based column index of the referenced cell within the table.
        /// </summary>
        public readonly uint Column = column;    // 4

        /// <summary>
        /// Zero-based row index of the referenced cell within the table.
        /// </summary>
        public readonly uint Row = row;        /// <summary>
        /// Reserved 64-bit field for future extensions (e.g. flags, versioning).
        /// </summary>
        public readonly ulong Reserved = reserved;  // 8

        public readonly bool Equals(MetadataTableRef other) => 
                TableId == other.TableId
             && Column  == other.Column
             && Row     == other.Row
             && Reserved == other.Reserved;

        /// <inheritdoc/>
        public override readonly bool Equals(object? obj) => obj is MetadataTableRef other && Equals(other);
        public static bool operator ==(MetadataTableRef left, MetadataTableRef right) => left.Equals(right);
        public static bool operator !=(MetadataTableRef left, MetadataTableRef right) => !(left==right);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override readonly int GetHashCode() => HashCode.Combine(TableId, Column, Row, Reserved);
    }
}
