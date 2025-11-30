using Microsoft.Extensions.Logging;

namespace Extend0.Lifecycle
{
    /// <summary>
    /// Base options for configuring singleton behavior within the current process.
    /// Extended by cross-process variants for IPC scenarios.
    /// </summary>
    /// <remarks>
    /// <para>
    /// These options control local orchestration only (lifecycle, replacement, logging).
    /// For cross-process coordination, use a type that extends this (e.g., <c>CrossProcessSingletonOptions</c>).
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// var opts = new SingletonOptions
    /// {
    ///     Logger = logger,
    ///     Overwrite = false
    /// };
    /// </code>
    /// </example>
    public class SingletonOptions
    {
        /// <summary>
        /// Optional logger used by the singleton infrastructure to emit diagnostics
        /// (e.g., when replacing an existing instance).
        /// </summary>
        public ILogger? Logger { get; init; }

        /// <summary>
        /// When <see langword="true"/>, allows replacing an existing instance of the same concrete singleton type.
        /// When <see langword="false"/>, attempting to construct a second instance will throw.
        /// </summary>
        public bool Overwrite { get; init; }
    }
}
