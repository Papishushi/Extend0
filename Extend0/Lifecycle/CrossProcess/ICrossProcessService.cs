namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Common contract for all cross-process services.
    /// Derive your service interfaces from this to get standard diagnostics/utilities.
    /// </summary>
    public interface ICrossProcessService
    {
        /// <summary>
        /// The logical contract name reported by <see cref="GetServiceInfoAsync"/>.
        /// Override to return your main interface name if needed.
        /// </summary>
        string ContractName { get; }
        /// <summary>
        /// Lightweight liveness probe.
        /// </summary>
        Task<Heartbeat> PingAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns diagnostic and identity information about the hosted service instance.
        /// </summary>
        Task<ServiceInfo> GetServiceInfoAsync(CancellationToken cancellationToken = default);
    }
}
