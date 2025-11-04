namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Options for configuring a singleton that can operate either in-process or as a cross-process singleton.
    /// Extends <see cref="SingletonOptions"/> with cross-process settings.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use these options when constructing a cross-process capable singleton to control whether the
    /// instance should be hosted locally (in-process) or exposed as a machine-wide singleton with
    /// IPC proxies for non-owner processes.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// var opts = new CrossProcessSingletonOptions
    /// {
    ///     Mode = SingletonMode.CrossProcess,
    ///     CrossProcessName = "Extend0.Clock",   // optional logical name
    ///     CrossProcessConnectTimeoutMs = 5000,  // client connect timeout
    ///     Overwrite = false,                    // from SingletonOptions
    ///     Logger = logger                       // from SingletonOptions
    /// };
    /// </code>
    /// </example>
    public class CrossProcessSingletonOptions : SingletonOptions
    {
        /// <summary>
        /// Selects how the singleton is orchestrated.
        /// <list type="bullet">
        ///   <item><description><see cref="SingletonMode.InProcess"/>: a single instance per process (no IPC).</description></item>
        ///   <item><description><see cref="SingletonMode.CrossProcess"/>: one owner process hosts the real service; other processes obtain an IPC proxy.</description></item>
        /// </list>
        /// </summary>
        public SingletonMode Mode { get; init; } = SingletonMode.InProcess;

        /// <summary>
        /// Target server/machine name used by client transports when connecting to the cross-process owner.
        /// Use <c>"."</c> for the local machine.
        /// On Windows you may specify a remote computer name (e.g., <c>"HOST123"</c>) when the chosen transport supports it
        /// (e.g., named pipes). On Linux/macOS this value is typically ignored and connections are local-only.
        /// </summary>
        public string CrossProcessServer { get; init; } = ".";

        /// <summary>
        /// Optional logical name used to derive the cross-process identity (e.g., <c>"Extend0.Clock"</c>).
        /// If omitted, the identity is typically derived from the service contract type and assembly fingerprint.
        /// </summary>
        public string? CrossProcessName { get; init; }  // e.g., "Extend0.Clock"

        /// <summary>
        /// Client connect timeout, in milliseconds, when attaching to an existing cross-process owner.
        /// Ignored when becoming the owner (hosting path).
        /// </summary>
        public int CrossProcessConnectTimeoutMs { get; init; } = 3000;
    }
}
