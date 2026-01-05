namespace Extend0.Metadata.Storage
{
    /// <summary>
    /// Row capacity policy applied when operating on table columns.
    /// </summary>
    public enum CapacityPolicy
    {
        /// <summary>
        /// No automatic capacity handling is performed.
        /// </summary>
        None = 0,

        /// <summary>
        /// The operation fails if the target column does not have enough capacity.
        /// </summary>
        Throw = 1,

        /// <summary>
        /// Attempts to grow the column up to at least the requested row index and
        /// zero-initialize the VALUE area for the newly added rows.
        /// Requires <c>MetaDb.GrowColumnTo</c> to be configured.
        /// </summary>
        AutoGrowZeroInit = 2
    }
}
