using System.Diagnostics;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Convenience base class to implement <see cref="ICrossProcessService"/> once for all services.
    /// Inherit this in your concrete implementation and override virtual properties if needed.
    /// </summary>
    public abstract class CrossProcessServiceBase : ICrossProcessService
    {
        private readonly DateTimeOffset _startUtc = DateTimeOffset.UtcNow;

        /// <summary>
        /// The logical contract name reported by <see cref="GetServiceInfoAsync"/>.
        /// Override to return your main interface name if needed.
        /// </summary>
        public virtual string ContractName =>
            GetType().GetInterfaces().Length > 0
                ? GetType().GetInterfaces()[0].FullName ?? GetType().GetInterfaces()[0].Name
                : GetType().FullName ?? GetType().Name;

        /// <summary>
        /// The pipe name used by the host (if known). Override if you want it surfaced in diagnostics.
        /// </summary>
        protected virtual string? PipeName => null;

        public Task<Heartbeat> PingAsync(CancellationToken cancellationToken = default)
        {
            var now = DateTimeOffset.UtcNow;
            var uptime = (long)(now - _startUtc).TotalSeconds;
            return Task.FromResult(new Heartbeat(now, uptime, CrossProcessUtils.CurrentFingerprint));
        }

        public Task<ServiceInfo> GetServiceInfoAsync(CancellationToken cancellationToken = default)
        {
            var asm = GetType().Assembly.GetName();
            using var proc = Process.GetCurrentProcess();

            var info = new ServiceInfo(
                ContractName,
                GetType().FullName ?? GetType().Name,
                asm.Version?.ToString() ?? "unknown",
                CrossProcessUtils.CurrentFingerprint,
                Environment.MachineName,
                Environment.ProcessId,
                proc.ProcessName,
                _startUtc,
                PipeName
            );

            return Task.FromResult(info);
        }
    }
}
