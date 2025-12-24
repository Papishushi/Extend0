using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Definitions
{
    public abstract class RebuildableCrossTableIndexDefinition<TKey, TValue>(string name) : CrossTableIndexDefinition<TKey, TValue>(name), IRebuildableIndex where TKey : notnull
    {
        /// <summary>
        /// Rebuilds the index by scanning the provided <paramref name="table"/>.
        /// </summary>
        /// <param name="table">The table to scan and use as the source of truth.</param>
        /// <remarks>
        /// <para>
        /// Implementations typically clear current state and repopulate it based on
        /// <see cref="MetadataTable.EnumerateCells"/> or other table-specific traversal APIs.
        /// </para>
        /// <para>
        /// This method is expected to be safe to call multiple times and should leave the index
        /// in a consistent state even if it was previously populated.
        /// </para>
        /// </remarks>
        public abstract void Rebuild(MetadataTable table);
    }
}
