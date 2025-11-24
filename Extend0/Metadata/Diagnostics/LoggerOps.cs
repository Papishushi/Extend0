using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Diagnostics
{
    /// <summary>
    /// Logging extensions for starting timed, traceable MetaDB operations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="BeginOp(ILogger?, string, object?, LogLevel)"/> wraps the call in a
    /// <see cref="OpScope"/>, automatically wiring it to
    /// <see cref="MetaDBManager.ActivitySrc"/> for distributed tracing.
    /// </para>
    /// </remarks>
    public static class LoggerOps
    {
        /// <summary>
        /// Begins a structured, timed logging scope for a named MetaDB operation.
        /// </summary>
        /// <param name="log">Logger instance; can be <see langword="null"/> to disable logging.</param>
        /// <param name="name">Human-readable operation name (e.g. <c>"SyncUsers"</c>).</param>
        /// <param name="state">
        /// Optional structured state attached to the scope (for example <c>new { tenant = "acme" }</c>).
        /// </param>
        /// <param name="level">
        /// Minimum log level required to enable timing and start/end events. Defaults to <see cref="LogLevel.Information"/>.
        /// </param>
        /// <returns>
        /// An <see cref="MetaDBManager.OpScope"/> that logs start/end/fail events and emits an
        /// <see cref="Activity"/> via <see cref="MetaDBManager.ActivitySrc"/>.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static OpScope BeginOp(this ILogger? log, string name, object? state = null, LogLevel level = LogLevel.Information)
            => new(log, name, state, level, MetaDBManager.ActivitySrc);

    }
}
