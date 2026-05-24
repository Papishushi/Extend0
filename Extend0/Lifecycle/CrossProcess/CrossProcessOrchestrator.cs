using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Cross-process singleton orchestrator for a service contract <typeparamref name="TService"/>.
    /// </summary>
    /// <typeparam name="TService">
    /// The service interface contract. Must implement <see cref="ICrossProcessService"/>.
    /// </typeparam>
    /// <remarks>
    /// <para>
    /// Establishes a single owner process (host) per logical service name and returns a
    /// <see cref="CrossProcessHandle{TService}"/> that either hosts the real implementation (owner)
    /// or connects to it via IPC and exposes a client proxy (non-owner).
    /// </para>
    /// <para>
    /// Uniqueness is determined by <see cref="CrossProcessUtils.BuildNameFor{T}(string)"/>:
    /// it includes the contract type and the assembly MVID so different binaries do not collide.
    /// </para>
    /// </remarks>
    internal static class CrossProcessOrchestrator<TService> where TService : class, ICrossProcessService
    {
        /// <summary>
        /// Shared logger factory used to create <see cref="ILogger"/> instances for this process.
        /// </summary>
        internal static ILoggerFactory? s_LoggerFactory;

        /// <summary>
        /// Starts or connects to a cross-process singleton for <typeparamref name="TService"/>.
        /// </summary>
        public static CrossProcessHandle<TService> GetOrStart(Func<TService> factory, string? name = null, string serverName = ".", int connectTimeoutMs = 3000, bool preferGlobalMutex = true)
        {
            ArgumentNullException.ThrowIfNull(factory);

            var baseName = CrossProcessUtils.BuildNameFor<TService>(name);
            var pipeName = CrossProcessUtils.BuildPipeName(baseName);

            var m = CrossProcessUtils.CreateOwned(baseName, preferGlobalMutex, out bool createdNew, out bool _);

            if (createdNew) return HostBranch(factory, pipeName, m);
            else return ClientBranch(serverName, connectTimeoutMs, pipeName, m);
        }

        private static CrossProcessHandle<TService> ClientBranch(string serverName, int connectTimeoutMs, string pipeName, Mutex m)
        {
            try { m.Dispose(); } catch { }

            var transport = new NamedPipeClientTransport(serverName, pipeName, connectTimeoutMs);
            var proxy = RpcDispatchProxy<TService>.Create(transport, CancellationToken.None);
            if (proxy is null)
            {
                transport.Dispose();
                throw new InvalidOperationException("Failed to create RPC proxy.");
            }

            return new CrossProcessHandle<TService>(
                proxy,
                isOwner: false,
                mutex: null,
                cts: null,
                server: null,
                transport: transport);
        }

        private static CrossProcessHandle<TService> HostBranch(Func<TService> factory, string pipeName, Mutex m)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            NamedPipeServer? server = null;
            try
            {
                var impl = factory();
                var serverLogger = s_LoggerFactory?.CreateLogger<NamedPipeServer>() ?? NullLogger<NamedPipeServer>.Instance;

                server = new NamedPipeServer(pipeName, impl!, serverLogger, cts.Token);

                return new CrossProcessHandle<TService>(
                    impl,
                    isOwner: true,
                    mutex: m,
                    cts: cts,
                    server: server,
                    transport: null);
            }
            catch
            {
                TryDisposeState(m, cts, server);
                throw;
            }
        }

        private static void TryDisposeState(Mutex m, CancellationTokenSource? cts, NamedPipeServer? server)
        {
            try { server?.Dispose(); } catch { }
            try { cts?.Cancel(); cts?.Dispose(); } catch { }
            try { m.ReleaseMutex(); } catch { }
            try { m.Dispose(); } catch { }
        }
    }
}
