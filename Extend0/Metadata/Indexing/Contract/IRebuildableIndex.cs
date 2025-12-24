namespace Extend0.Metadata.Indexing.Contract;

/// <summary>
/// Optional contract for indexes that can rebuild themselves by scanning a table.
/// </summary>
/// <remarks>
/// Implementations typically clear their current state and repopulate it from the table contents.
/// </remarks>
public interface IRebuildableIndex : ITableIndex
{
    /// <summary>
    /// Rebuilds the index by scanning the provided <paramref name="table"/>.
    /// </summary>
    /// <param name="table">The table to scan as the source of truth.</param>
    void Rebuild(MetadataTable table);
}
