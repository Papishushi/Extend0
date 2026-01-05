namespace Extend0.Metadata.Storage.Internal
{
    /// <summary>
    /// Describes a contiguous value slice for a single column in a metadata store.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="ColumnBlock"/> exposes raw pointers into the underlying storage for a column,
    /// allowing high–performance access to cell <c>VALUE</c> regions without re-materializing full
    /// <see cref="MetadataCell"/> instances.
    /// </para>
    /// <para>
    /// The layout is assumed to be:
    /// <c>[ KEY (KeySize bytes) ][ VALUE (ValueSize bytes) ]</c> repeated row by row, with
    /// <see cref="Stride"/> bytes per row starting at <see cref="Base"/>.
    /// </para>
    /// <para>
    /// This type is <see langword="unsafe"/> and should typically remain <c>internal</c> to the
    /// metadata assembly; callers are responsible for honoring lifetime and bounds.
    /// </para>
    /// </remarks>
    internal unsafe readonly struct ColumnBlock(byte* @base, int stride, int valueSize, int valueOffset) : IEquatable<ColumnBlock>
    {
        /// <summary>
        /// Base address of the first row in the column (start of the first cell, at offset 0).
        /// </summary>
        internal readonly byte* Base = @base;

        /// <summary>
        /// Total number of bytes occupied by each row (key + value).
        /// </summary>
        internal readonly int Stride = stride;

        /// <summary>
        /// Size in bytes of the <c>VALUE</c> segment for each row.
        /// </summary>
        internal readonly int ValueSize = valueSize;

        /// <summary>
        /// Offset, in bytes from <see cref="Base"/>, at which the <c>VALUE</c> segment starts
        /// within a single row.
        /// </summary>
        /// <remarks>
        /// For a typical layout <c>[KEY][VALUE]</c>, this is equal to the key size.
        /// </remarks>
        internal readonly int ValueOffset = valueOffset;

        public override bool Equals(object? obj) => obj is ColumnBlock block&&Equals(block);

        public bool Equals(ColumnBlock other) =>
            Base == other.Base&&
            Stride == other.Stride&&
            ValueSize == other.ValueSize&&
            ValueOffset == other.ValueOffset;

        public override int GetHashCode() => HashCode.Combine(Stride, ValueSize, ValueOffset);

        /// <summary>
        /// Computes a pointer to the <c>VALUE</c> segment for the specified row.
        /// </summary>
        /// <param name="row">Zero-based row index within the column.</param>
        /// <returns>
        /// A raw <see cref="byte"/> pointer to the beginning of the <c>VALUE</c> bytes
        /// for the given row.
        /// </returns>
        /// <remarks>
        /// No bounds checking is performed. The caller must ensure that
        /// <paramref name="row"/> is within the valid row capacity for the column.
        /// </remarks>
        internal byte* GetValuePtr(uint row) => Base + ValueOffset + Stride * row;

        public static bool operator ==(ColumnBlock left, ColumnBlock right) => left.Equals(right);

        public static bool operator !=(ColumnBlock left, ColumnBlock right) => !left.Equals(right);
    }
}
