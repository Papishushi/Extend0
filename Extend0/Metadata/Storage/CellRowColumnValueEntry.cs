using Extend0.Metadata.CodeGen;

namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Represents a metadata cell together with its logical coordinates
    /// in the table, encapsulated as a <see cref="MetadataCellPointer"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This record struct is the standard payload produced by cell enumeration
    /// in metadata stores. It bundles:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>
    ///     The <see cref="Pointer"/> identifying the cell by row and column.
    ///   </description></item>
    ///   <item><description>
    ///     The <see cref="Cell"/> instance holding the actual data.
    ///   </description></item>
    /// </list>
    /// <para>
    /// The type also provides convenience accessors for <see cref="Row"/> and
    /// <see cref="Col"/>, as well as implicit conversions to and from a tuple in
    /// <c>(Col, Row, Cell)</c> order for ergonomic deconstruction and creation.
    /// </para>
    /// </remarks>
    public readonly record struct CellRowColumnValueEntry(MetadataCellPointer Pointer, MetadataCell Cell) : IEquatable<CellRowColumnValueEntry>, IComparable<CellRowColumnValueEntry>
    {
        /// <summary>
        /// Gets the zero-based row index of the cell, as exposed
        /// by the underlying <see cref="Pointer"/>.
        /// </summary>
        public readonly uint Row => Pointer.Row;
        /// <summary>
        /// Gets the zero-based column index of the cell, as exposed
        /// by the underlying <see cref="Pointer"/>.
        /// </summary>
        public readonly uint Col => Pointer.Column;

        /// <summary>
        /// Compares this entry to another one using the ordering of their
        /// <see cref="Pointer"/> values.
        /// </summary>
        /// <param name="other">The other entry to compare with.</param>
        /// <returns>
        /// A signed integer that indicates whether this entry precedes,
        /// equals, or follows <paramref name="other"/> according to
        /// <see cref="MetadataCellPointer.CompareTo(MetadataCellPointer)"/>.
        /// </returns>
        public readonly int CompareTo(CellRowColumnValueEntry other) => Pointer.CompareTo(other.Pointer);

        /// <summary>
        /// Implicitly converts a <see cref="CellRowColumnValueEntry"/> to a value tuple
        /// in <c>(Col, Row, Cell)</c> order.
        /// </summary>
        /// <param name="value">The entry to convert.</param>
        /// <returns>
        /// A tuple containing the column index, row index and the associated cell.
        /// </returns>
        public static implicit operator (uint Col, uint Row, MetadataCell Cell)(CellRowColumnValueEntry value) => (value.Col, value.Row, value.Cell);

        /// <summary>
        /// Implicitly converts a value tuple in <c>(Col, Row, Cell)</c> order
        /// into a <see cref="CellRowColumnValueEntry"/>.
        /// </summary>
        /// <param name="value">
        /// A tuple whose first element is the column index, second element is
        /// the row index, and third element is the metadata cell.
        /// </param>
        /// <returns>
        /// A new <see cref="CellRowColumnValueEntry"/> with a
        /// <see cref="Pointer"/> created from the supplied row and column.
        /// </returns>
        public static implicit operator CellRowColumnValueEntry((uint Col, uint Row, MetadataCell Cell) value) => new(new MetadataCellPointer(row: value.Row, column: value.Col), value.Cell);
    }
}
