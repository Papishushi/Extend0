using System.Diagnostics;

namespace Extend0.Metadata
{
    public sealed partial class MetaDBManager
    {
        /// <summary>
        /// Lightweight value container returned by the <c>PrepareFill</c> helper,
        /// bundling the resolved managed table, an optional stopwatch, the effective
        /// capacity policy and the computed batch size for column operations.
        /// </summary>
        /// <param name="ManagedTable">
        /// The resolved <see cref="ManagedTable"/> instance targeted by the fill operation.
        /// </param>
        /// <param name="StopWatch">
        /// Optional <see cref="Stopwatch"/> used to measure elapsed time when logging is enabled;
        /// <see langword="null"/> when timing is not active.
        /// </param>
        /// <param name="Policy">
        /// Effective <see cref="CapacityPolicy"/> to apply for the current operation, after
        /// normalizing any per-call override against the manager-wide default.
        /// </param>
        /// <param name="BatchSize">
        /// Batch size in rows computed from the column VALUE size and used to drive
        /// cache-friendly inner loops in the fill helpers.
        /// </param>
        private record struct PrepareFillReturnValue(ManagedTable ManagedTable, Stopwatch? StopWatch, CapacityPolicy Policy, int BatchSize) : IEquatable<PrepareFillReturnValue>
        {
            /// <summary>
            /// Implicitly converts a <see cref="PrepareFillReturnValue"/> into a tuple
            /// compatible with deconstruction patterns used by the caller.
            /// </summary>
            public static implicit operator (ManagedTable m, Stopwatch? sw, CapacityPolicy policy, int batchSize)(PrepareFillReturnValue value)
            {
                return (value.ManagedTable, value.StopWatch, value.Policy, value.BatchSize);
            }

            /// <summary>
            /// Implicitly converts a tuple produced by <c>PrepareFill</c> into a
            /// <see cref="PrepareFillReturnValue"/> instance, allowing symmetric usage
            /// between tuple and record forms.
            /// </summary>
            public static implicit operator PrepareFillReturnValue((ManagedTable m, Stopwatch? sw, CapacityPolicy policy, int batchSize) value)
            {
                return new PrepareFillReturnValue(value.m, value.sw, value.policy, value.batchSize);
            }
        }
    }
}