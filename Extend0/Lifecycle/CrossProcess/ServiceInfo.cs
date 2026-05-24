namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Standard diagnostic/identity information for a hosted cross-process service.
    /// </summary>
    public sealed record ServiceInfo(
        string ContractName,
        string ImplementationName,
        string AssemblyVersion,
        string Fingerprint,
        string MachineName,
        int ProcessId,
        string ProcessName,
        DateTimeOffset StartTimeUtc,
        string? PipeName
    )
    {
        /// <summary>
        /// Transport-neutral endpoint name used by the hosted service, when known.
        /// </summary>
        public string? EndpointName { get; init; } = PipeName;

        /// <summary>
        /// Server or machine name associated with the configured endpoint, when known.
        /// </summary>
        public string? EndpointServerName { get; init; }

        /// <summary>
        /// Logical transport kind used by the service, when known.
        /// </summary>
        public string? TransportKind { get; init; } = PipeName is null ? null : "named-pipe";
    }
}
