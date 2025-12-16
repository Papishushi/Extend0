namespace Extend0.Metadata
{
    public sealed partial class MetaDBManager
    {
        /// <summary>
        /// Represents per-cycle deletion statistics used by the heuristic.
        /// </summary>
        internal readonly struct DeleteCycleStats
        {
            public readonly int Deleted;
            public readonly int Attempts;

            public DeleteCycleStats(int deleted, int attempts)
            {
                Deleted = deleted;
                Attempts = attempts;
            }

            /// <summary>Gets the ratio of successful deletes per attempt.</summary>
            public double SuccessRate => (double)Deleted / Math.Max(1, Attempts);
        }
    }
}