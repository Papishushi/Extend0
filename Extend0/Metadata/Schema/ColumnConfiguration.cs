using Extend0.Metadata.Storage;

namespace Extend0.Metadata.Schema
{
    /// <summary>
    /// Describes the shape and basic behavior of a metadata column.
    /// </summary>
    /// <param name="Size">
    /// Packed entry size for the column (key bits and value bytes), as produced by
    /// <see cref="MetadataEntrySizeExtensions.PackUnchecked(int, int)"/>.
    /// </param>
    /// <param name="Name">
    /// Logical column name, used for lookups and diagnostics. Must be unique within a table.
    /// </param>
    /// <param name="ReadOnly">
    /// Indicates whether the column is intended to be read-only once created.
    /// Implementations may enforce this flag when mutating cells.
    /// </param>
    /// <param name="InitialCapacity">
    /// Initial number of rows allocated for the column. This defines the baseline row capacity
    /// before any growth policy (e.g. <see cref="CapacityPolicy.AutoGrowZeroInit"/>) is applied.
    /// </param>
    public record struct ColumnConfiguration(
        CodeGen.MetadataEntrySize Size,
        string Name,
        bool ReadOnly,
        uint InitialCapacity
    );
}
