using Extend0.Metadata.Storage;
using System.Runtime.InteropServices;

namespace Extend0.Metadata.Storage.Files
{
    /// <summary>
    /// Header stored at the beginning of a metadata table file.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This struct defines the on-disk layout of the file header used by
    /// <see cref="MappedStore"/>. It is read directly from the memory-mapped
    /// file and must therefore remain blittable and layout-stable.
    /// </para>
    /// <para>
    /// The file format is:
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       <see cref="Magic"/>: constant magic number <c>"MTBL"</c> (0x4C42544D)
    ///       that identifies the file as a metadata table.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <see cref="Version"/>: format version. Currently <c>1</c>.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <see cref="ColumnCount"/>: number of column descriptors stored in the
    ///       column table.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <see cref="ColumnsTableOffset"/>: byte offset from the start of the
    ///       file to the first <see cref="ColumnDesc"/> entry.
    ///     </description>
    ///   </item>
    /// </list>
    /// </para>
    /// </remarks>
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct FileHeader
    {
        /// <summary>
        /// Magic constant that identifies the file as a metadata table.
        /// Expected value is <c>0x4C42544D</c> (ASCII "MTBL").
        /// </summary>
        public uint Magic;              // "MTBL" = 0x4C42544D

        /// <summary>
        /// File format version. Currently <c>1</c>.
        /// </summary>
        public ushort Version;          // 1

        /// <summary>
        /// Number of columns described in the column table.
        /// </summary>
        public ushort ColumnCount;

        /// <summary>
        /// Byte offset from the beginning of the file to the column
        /// descriptor table (<see cref="ColumnDesc"/> array).
        /// </summary>
        public long ColumnsTableOffset; // = sizeof(FileHeader)
    }
}
