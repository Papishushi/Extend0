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

        // Host state is per TService.
        private static readonly Lock s_hostGate = new();
        private static NamedPipeServer? s_server;
        private static CancellationTokenSource? s_serverCts;
        private static Mutex? s_hostMutex;

        /// <summary>
        /// Logical contract name reported by <see cref="GetServiceInfoAsync"/>.
        /// Defaults to <c>typeof(TService)</c> full name.
        /// </summary>
        public virtual string ContractName => typeof(TService).FullName ?? typeof(TService).Name;

        /// <summary>
        /// The pipe name used by the host (if known). Override if you want it surfaced in diagnostics.
        /// </summary>
        protected virtual string? PipeName => null;

        /// <summary>
        /// The server/machine name used by the host (if known). Override if you want it surfaced in diagnostics.
        /// </summary>
        protected virtual string? ServerName => null;

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
        /// process identity, start time and the associated pipe name.
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
            );

            return Task.FromResult(info);
        }

        /// <summary>
        /// Probes the configured named pipe to determine whether a server is currently listening.
        /// </summary>
        /// <returns>
        /// A task whose result is <c>true</c> if a connection to <see cref="PipeName"/>
        /// on <see cref="ServerName"/> is established within a short timeout;
        /// otherwise, <c>false</c>.
        /// </returns>
        /// <remarks>
        /// If <see cref="PipeName"/> is <see langword="null"/> or empty, the method
        /// returns <c>false</c> without attempting a connection. All exceptions are
        /// swallowed and treated as a negative result.
        /// </remarks>
        public async Task<bool> CanConnectAsync()
        {
            try
            {
                if (string.IsNullOrEmpty(PipeName)) return false;
                using var client = new NamedPipeClientStream(
                    ServerName ?? ".",
                    PipeName,
                    PipeDirection.InOut,
                    PipeOptions.Asynchronous);

                using var cts = new CancellationTokenSource(200);
                await client.ConnectAsync(cts.Token).ConfigureAwait(false);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Stops hosting this service instance over the named pipe and releases all
        /// associated server-side resources.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Cancels the server loop, disposes the underlying <see cref="NamedPipeServer"/>
        /// instance (if any), and releases the host coordination mutex used for cross-process
        /// ownership. It is safe to call multiple times; subsequent calls after the first
        /// will have no effect.
        /// </para>
        /// <para>
        /// The operation is synchronized using <c>s_hostGate</c> so that concurrent calls
        /// cannot interleave server shutdown with new hosting attempts.
        /// </para>
        /// </remarks>
        public void StopHosting()
        {
            lock (s_hostGate)
            {
                CancelAndDisposeServerCts();
                DisposeServerInstance();
                ReleaseAndDisposeHostMutex();
            }
        }

        /// <summary>
        /// Cancels and disposes the server <see cref="CancellationTokenSource"/> if present.
        /// </summary>
        /// <remarks>
        /// Exceptions during cancellation or disposal are ignored because teardown is
        /// best-effort and callers cannot take corrective action here.
        /// </remarks>
        private static void CancelAndDisposeServerCts()
        {
            if (s_serverCts is null) return;

            try
            {
                s_serverCts.Cancel();
            }
            catch
            {
                // Best-effort cancellation during shutdown.
            }

            try
            {
                s_serverCts.Dispose();
            }
            catch
            {
                // Non-critical failure when disposing CTS in teardown.
            }

            s_serverCts = null;
        }

        /// <summary>
        /// Disposes the hosted <see cref="NamedPipeServer"/> instance, using async disposal
        /// when available, and clears the static reference.
        /// </summary>
        /// <remarks>
        /// Any exceptions during disposal are swallowed to keep shutdown idempotent and safe.
        /// </remarks>
        private static void DisposeServerInstance()
        {
            if (s_server is null) return;

            try
            {
                if (s_server is IAsyncDisposable ad)
                {
                    // Synchronous wait is acceptable in a controlled shutdown path.
                    ad.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
                else if (s_server is IDisposable d)
                {
                    d.Dispose();
                }
            }
            catch
            {
                // Server disposal failure is non-recoverable at this point.
            }
            finally
            {
                s_server = null;
            }
        }

        /// <summary>
        /// Releases and disposes the hosting mutex if present, then clears the static reference.
        /// </summary>
        /// <remarks>
        /// Errors when releasing or disposing the OS mutex handle are ignored because the
        /// process is already tearing down hosting and cannot recover from them.
        /// </remarks>
        private static void ReleaseAndDisposeHostMutex()
        {
            if (s_hostMutex is null) return;

            try
            {
                s_hostMutex.ReleaseMutex();
            }
            catch
            {
                // Releasing an invalid/already-released mutex is harmless during shutdown.
            }

            try
            {
                s_hostMutex.Dispose();
            }
            catch
            {
                // Failure to dispose the OS handle is non-actionable for the caller.
            }

            s_hostMutex = null;
        }
    }
}
