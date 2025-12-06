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
    internal struct FileHeader : IEquatable<FileHeader>, IComparable<FileHeader>
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

        /// <summary>
        /// Indicates whether this header is equal to another header.
        /// </summary>
        public readonly bool Equals(FileHeader other) =>
            Magic == other.Magic &&
            Version == other.Version &&
            ColumnCount == other.ColumnCount &&
            ColumnsTableOffset == other.ColumnsTableOffset;

        /// <inheritdoc/>
        public override readonly bool Equals(object? obj) => obj is FileHeader other && Equals(other);

        /// <summary>
        /// Compares two <see cref="FileHeader"/> values for equality.
        /// </summary>
        public static bool operator ==(FileHeader left, FileHeader right) => left.Equals(right);

        /// <summary>
        /// Compares two <see cref="FileHeader"/> values for inequality.
        /// </summary>
        public static bool operator !=(FileHeader left, FileHeader right) => !left.Equals(right);

        /// <inheritdoc/>
        public override readonly int GetHashCode() => HashCode.Combine(Magic, Version, ColumnCount, ColumnsTableOffset);

        /// <summary>
        /// Compares this <see cref="FileHeader"/> instance with another one and
        /// returns a value that indicates their relative order.
        /// </summary>
        /// <param name="other">
        /// The other <see cref="FileHeader"/> to compare with this instance.
        /// </param>
        /// <returns>
        /// A signed integer that indicates the relative order of the headers:
        /// <list type="bullet">
        ///   <item><description>
        ///   Less than zero – this instance is less than <paramref name="other"/>.
        ///   </description></item>
        ///   <item><description>
        ///   Zero – this instance is equal to <paramref name="other"/>.
        ///   </description></item>
        ///   <item><description>
        ///   Greater than zero – this instance is greater than <paramref name="other"/>.
        ///   </description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// The comparison is performed field-by-field in the following order:
        /// <see cref="Magic"/>, <see cref="Version"/>, <see cref="ColumnCount"/> and
        /// <see cref="ColumnsTableOffset"/>. The first differing field determines
        /// the result.
        /// </remarks>
        public readonly int CompareTo(FileHeader other)
        {
            int c = Magic.CompareTo(other.Magic);
            if (c != 0) return c;

            c = Version.CompareTo(other.Version);
            if (c != 0) return c;

            c = ColumnCount.CompareTo(other.ColumnCount);
            if (c != 0) return c;

            return ColumnsTableOffset.CompareTo(other.ColumnsTableOffset);
        }
    }
}
