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
    public struct MetadataTableRef
    {
        /// <summary>
        /// Globally unique identifier of the referenced metadata table.
        /// </summary>
        public Guid TableId;   // 16

        /// <summary>
        /// Zero-based column index of the referenced cell within the table.
        /// </summary>
        public uint Column;    // 4

        /// <summary>
        /// Zero-based row index of the referenced cell within the table.
        /// </summary>
        public uint Row;       // 4

        /// <summary>
        /// Reserved 64-bit field for future extensions (e.g. flags, versioning).
        /// </summary>
        public ulong Reserved; // 8
    }
}
