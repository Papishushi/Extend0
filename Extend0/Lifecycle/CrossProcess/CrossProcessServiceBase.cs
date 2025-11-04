using System.Diagnostics;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>ICrossProcessService
    /// Convenience base class to implement <see cref="ICrossProcessService"/> once for all services.
    /// Inherit this in your concrete implementation and override virtual properties if needed.
    /// </summary>
    public abstract class CrossProcessServiceBase<TService> : ICrossProcessService where TService : class, ICrossProcessService
    {
        private readonly DateTimeOffset _startUtc = DateTimeOffset.UtcNow;

        /// <summary>
        /// Logical contract name reported by <see cref="GetServiceInfoAsync"/>.
        /// Defaults to <c>typeof(TService)</c> full name.
        /// </summary>
        public virtual string ContractName =>
            typeof(TService).FullName ?? typeof(TService).Name;

        /// <summary>
        /// The pipe name used by the host (if known). Override if you want it surfaced in diagnostics.
        /// </summary>
        protected virtual string? PipeName => null;

        public Task<Heartbeat> PingAsync()
        {
            var now = DateTimeOffset.UtcNow;
            var uptime = (long)(now - _startUtc).TotalSeconds;
            return Task.FromResult(new Heartbeat(now, uptime, CrossProcessUtils.CurrentFingerprint));
        }

        public Task<ServiceInfo> GetServiceInfoAsync()
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
