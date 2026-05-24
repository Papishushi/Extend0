using System.Diagnostics;
using System.IO.Pipes;

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
        public virtual string ContractName => typeof(TService).FullName ?? typeof(TService).Name;

        /// <summary>
        /// The named-pipe endpoint used by the host (if known). Override if you want it surfaced in diagnostics.
        /// This remains for compatibility with the current built-in named-pipe transport.
        /// </summary>
        protected virtual string? PipeName => null;

        /// <summary>
        /// The server/machine name used by the host (if known). Override if you want it surfaced in diagnostics.
        /// </summary>
        protected virtual string? ServerName => null;

        /// <summary>
        /// Transport-neutral endpoint name surfaced in diagnostics and connectivity probes.
        /// Defaults to <see cref="PipeName"/> for the current built-in transport.
        /// </summary>
        protected virtual string? EndpointName => PipeName;

        /// <summary>
        /// Transport-neutral endpoint server/machine name surfaced in diagnostics and connectivity probes.
        /// Defaults to <see cref="ServerName"/>.
        /// </summary>
        protected virtual string? EndpointServerName => ServerName;

        /// <summary>
        /// Logical transport kind surfaced in diagnostics.
        /// Defaults to <c>"named-pipe"</c> when an endpoint name is available.
        /// </summary>
        protected virtual string? TransportKind => EndpointName is null ? null : "named-pipe";

        /// <summary>
        /// Returns a lightweight heartbeat snapshot for this service instance.
        /// </summary>
        /// <returns>
        /// A completed <see cref="Task{TResult}"/> whose result contains the current UTC
        /// timestamp, the uptime in whole seconds since the service start, and the
        /// current cross-process fingerprint.
        /// </returns>
        /// <remarks>
        /// This method is intended as a cheap liveness probe that callers can use to
        /// verify connectivity and basic service health without performing any
        /// stateful work.
        /// </remarks>
        public Task<Heartbeat> PingAsync()
        {
            var now = DateTimeOffset.UtcNow;
            var uptime = (long)(now - _startUtc).TotalSeconds;
            return Task.FromResult(new Heartbeat(now, uptime, CrossProcessUtils.CurrentFingerprint));
        }

        /// <summary>
        /// Retrieves static and runtime metadata about the current service instance.
        /// </summary>
        /// <returns>
        /// A completed <see cref="Task{TResult}"/> whose result describes the service
        /// contract, implementation type, assembly version, fingerprint, host machine,
        /// process identity, start time and the associated transport endpoint details.
        /// </returns>
        /// <remarks>
        /// The returned <see cref="ServiceInfo"/> is suitable for diagnostics, logging,
        /// dashboards and troubleshooting cross-process deployments.
        /// </remarks>
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
            )
            {
                EndpointName = EndpointName,
                EndpointServerName = EndpointServerName,
                TransportKind = TransportKind
            };

            return Task.FromResult(info);
        }

        /// <summary>
        /// Probes the configured transport endpoint to determine whether a server is currently listening.
        /// </summary>
        /// <returns>
        /// A task whose result is <c>true</c> if a connection to the configured endpoint
        /// is established within a short timeout;
        /// otherwise, <c>false</c>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The default implementation probes the current built-in named-pipe transport.
        /// Services that expose a different transport should override
        /// <see cref="ProbeConnectivityCoreAsync(CancellationToken)"/>.
        /// </para>
        /// <para>
        /// If <see cref="EndpointName"/> is <see langword="null"/> or empty, the method
        /// returns <c>false</c> without attempting a connection. All exceptions are
        /// swallowed and treated as a negative result.
        /// </para>
        /// </remarks>
        public async Task<bool> CanConnectAsync()
        {
            try
            {
                using var cts = new CancellationTokenSource(200);
                return await ProbeConnectivityCoreAsync(cts.Token).ConfigureAwait(false);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Core transport-specific connectivity probe used by <see cref="CanConnectAsync"/>.
        /// </summary>
        /// <param name="ct">Cancellation token controlling the probe timeout.</param>
        /// <returns>
        /// A task whose result is <c>true</c> when the configured endpoint accepts a connection;
        /// otherwise, <c>false</c>.
        /// </returns>
        /// <remarks>
        /// The default implementation assumes the current built-in named-pipe transport.
        /// Override this method to support other transport kinds without changing the public contract.
        /// </remarks>
        protected virtual async Task<bool> ProbeConnectivityCoreAsync(CancellationToken ct)
        {
            var endpointName = EndpointName;
            if (string.IsNullOrEmpty(endpointName))
                return false;

            using var client = new NamedPipeClientStream(
                EndpointServerName ?? ".",
                endpointName,
                PipeDirection.InOut,
                PipeOptions.Asynchronous);

            await client.ConnectAsync(ct).ConfigureAwait(false);
            return true;
        }
    }
}
