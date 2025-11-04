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
    );
}
