using System.Runtime.InteropServices;

namespace Extend0.Metadata.Storage.Files
{
    /// <summary>
    /// On-disk / mapped descriptor for a single column in a metadata table.
    /// </summary>
    /// <remarks>
    /// This struct is persisted in the memory-mapped file header and used by
    /// <see cref="MappedStore"/> to compute cell locations. It must remain
    /// blittable and layout-compatible across versions that reuse the same file
    /// format.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct ColumnDesc(int keySize, int valueSize, uint rowCapacity, long baseOffset) : IEquatable<ColumnDesc>
    {
        /// <summary>
        /// Size in bytes of the key segment for each entry in this column.
        /// </summary>
        public readonly int KeySize = keySize;

        /// <summary>
        /// Size in bytes of the value segment for each entry in this column.
        /// </summary>
        public readonly int ValueSize = valueSize;

        /// <summary>
        /// Number of rows currently allocated for this column.
        /// </summary>
        public uint RowCapacity = rowCapacity;

        /// <summary>
        /// Byte offset, relative to the beginning of the mapped file,
        /// where the first entry of this column starts.
        /// </summary>
        public long BaseOffset = baseOffset; // inicio del slab de esta columna

        /// <summary>
        /// Total entry size in bytes (key + value) for this column.
        /// </summary>
        public readonly long EntrySizeBytes => KeySize + ValueSize;

        public override bool Equals(object? obj) => obj is ColumnDesc desc&&Equals(desc);

        public bool Equals(ColumnDesc other) => 
                   KeySize==other.KeySize&&
                   ValueSize==other.ValueSize&&
                   RowCapacity==other.RowCapacity&&
                   BaseOffset==other.BaseOffset&&
                   EntrySizeBytes==other.EntrySizeBytes;

        public override readonly int GetHashCode() => HashCode.Combine(KeySize, ValueSize, RowCapacity, BaseOffset, EntrySizeBytes);

        public static bool operator ==(ColumnDesc left, ColumnDesc right) => left.Equals(right);

        public static bool operator !=(ColumnDesc left, ColumnDesc right) => !(left==right);
    }
}
