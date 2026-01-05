namespace Extend0.Metadata
{
    internal sealed partial class MetaDBManager
    {
        /// <summary>
        /// Represents per-cycle deletion statistics used by the heuristic.
        /// </summary>
        internal readonly struct DeleteCycleStats(int deleted, int attempts)
        {
            public readonly int Deleted = deleted;
            public readonly int Attempts = attempts;

            /// <summary>Gets the ratio of successful deletes per attempt.</summary>
            public double SuccessRate => (double)Deleted / Math.Max(1, Attempts);
        }
    }
}