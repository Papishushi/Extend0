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
    internal static class CrossProcessOrchestator<TService> where TService : class, ICrossProcessService
    {
        /// <summary>
        /// Shared logger factory used to create <see cref="ILogger"/> instances for this process.
        /// </summary>
        internal static ILoggerFactory? s_LoggerFactory;

        /// <summary>
        /// Starts or connects to a cross-process singleton for <typeparamref name="TService"/>.
        /// </summary>
        /// <param name="factory">
        /// A factory that creates the concrete <typeparamref name="TService"/> implementation.
        /// Used only by the owner process (i.e., when no owner exists yet).
        /// </param>
        /// <param name="name">
        /// Optional logical suffix to distinguish instances (e.g., environment/tenant).
        /// The full identity is built as <see cref="CrossProcessUtils.BuildNameFor{T}(string)"/>.
        /// </param>
        /// <param name="serverName">
        /// The machine name where the named-pipe server is expected to run. Use <c>"."</c> for the local machine.
        /// </param>
        /// <param name="connectTimeoutMs">
        /// Client connect timeout in milliseconds for the IPC channel.
        /// </param>
        /// <param name="preferGlobalMutex">
        /// When <see langword="true"/> on Windows, attempts to acquire a <c>Global\</c> mutex first,
        /// falling back to <c>Local\</c> if access is denied. Ignored on non-Windows platforms.
        /// </param>
        /// <returns>
        /// A <see cref="CrossProcessHandle{TService}"/>. If <see cref="CrossProcessHandle{TService}.IsOwner"/>
        /// is <see langword="true"/>, the handle hosts the service; otherwise it exposes a client proxy.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown if <paramref name="factory"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the proxy cannot be created on the client path.
        /// </exception>
        /// <remarks>
        /// <para>
        /// The method coordinates ownership using a named <see cref="Mutex"/> created via
        /// <see cref="CrossProcessUtils.CreateOwned(string, bool, out bool, out bool, Microsoft.Extensions.Logging.ILogger?)"/>.
        /// If the mutex is newly created, the caller becomes the owner, instantiates the service with
        /// <paramref name="factory"/>, and starts a <c>NamedPipeServer</c>. Otherwise, it connects as a
        /// client using <c>NamedPipeClientTransport</c> and builds a proxy via
        /// <see cref="RpcDispatchProxy{TService}.Create(IClientTransport, CancellationToken)"/>.
        /// </para>
        /// <para>
        /// Disposing the returned handle cleanly shuts down server/proxy resources and releases the mutex
        /// when owned. The handle is idempotent and supports synchronous and asynchronous disposal.
        /// </para>
        /// </remarks>
        /// <example>
        /// <code>
        /// // Contract
        /// public interface IClock : ICrossProcessService
        /// {
        ///     Task&lt;string&gt; NowIsoAsync();
        /// }
        ///
        /// // Implementation
        /// public sealed class ClockService : CrossProcessServiceBase, IClock
        /// {
        ///     public Task&lt;string&gt; NowIsoAsync() =&gt; Task.FromResult(DateTimeOffset.UtcNow.ToString("O"));
        /// }
        ///
        /// // Start or connect
        /// using var handle = CrossProcess&lt;IClock&gt;.GetOrStart(() =&gt; new ClockService(), name: "prod");
        /// var svc = handle.Service; // proxy or real implementation
        /// var now = await svc.NowIsoAsync();
        /// </code>
        /// </example>
        public static CrossProcessHandle<TService> GetOrStart(Func<TService> factory, string? name = null, string serverName = ".", int connectTimeoutMs = 3000, bool preferGlobalMutex = true)
        {
            ArgumentNullException.ThrowIfNull(factory);

            var baseName = CrossProcessUtils.BuildNameFor<TService>(name);
            var pipeName = CrossProcessUtils.BuildPipeName(baseName);

            // Create or open the cross-process mutex (owned if newly created).
            var m = CrossProcessUtils.CreateOwned(baseName, preferGlobalMutex, out bool createdNew, out bool _);

            if (createdNew) return HostBranch(factory, pipeName, m);
            else return ClientBranch(serverName, connectTimeoutMs, pipeName, m);
        }

        /// <summary>
        /// Executes the client branch for an existing cross-process singleton:
        /// connects to the named-pipe server and returns a handle exposing a proxy.
        /// </summary>
        /// <param name="serverName">
        /// Name of the machine where the pipe server is hosted. Use <c>"."</c> for the local machine.
        /// </param>
        /// <param name="connectTimeoutMs">
        /// Connection timeout in milliseconds for the client transport.
        /// </param>
        /// <param name="pipeName">
        /// Logical pipe name built from the service identity.
        /// </param>
        /// <param name="m">
        /// The mutex obtained from <see cref="CrossProcessUtils.CreateOwned(string, bool, out bool, out bool, Microsoft.Extensions.Logging.ILogger?)"/>.
        /// It is disposed in this method because the client does not need to hold ownership.
        /// </param>
        /// <returns>
        /// A <see cref="CrossProcessHandle{TService}"/> configured as a non-owner, with a live
        /// <see cref="IClientTransport"/> and RPC proxy.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the RPC proxy cannot be created for the client transport.
        /// </exception>
        private static CrossProcessHandle<TService> ClientBranch(string serverName, int connectTimeoutMs, string pipeName, Mutex m)
        {
            // Not the creator → act as client (no need to keep the mutex object)
            try { m.Dispose(); } catch { /* client does not rely on the mutex instance */ }

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

        /// <summary>
        /// Executes the owner branch: creates the concrete service implementation, starts
        /// the named-pipe server and returns a handle that owns the cross-process instance.
        /// </summary>
        /// <param name="factory">
        /// Factory used to create the concrete <typeparamref name="TService"/> implementation.
        /// </param>
        /// <param name="pipeName">
        /// Logical name of the IPC pipe that will be used by clients to connect.
        /// </param>
        /// <param name="m">
        /// The mutex representing cross-process ownership. It is kept by the returned handle
        /// and released/disposed during handle teardown.
        /// </param>
        /// <returns>
        /// A <see cref="CrossProcessHandle{TService}"/> configured as owner, holding the real
        /// service implementation, server, mutex and cancellation token source.
        /// </returns>
        /// <remarks>
        /// On failure to host the service (factory exception, server startup error, etc.),
        /// this method cleans up the server, cancellation token source and mutex before
        /// rethrowing the exception to the caller.
        /// </remarks>
        private static CrossProcessHandle<TService> HostBranch(Func<TService> factory, string pipeName, Mutex m)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            NamedPipeServer? server = null;
            try
            {
                var impl = factory(); // if this throws, we release the mutex below
                var serverLogger = s_LoggerFactory?.CreateLogger<NamedPipeServer>() ?? NullLogger<NamedPipeServer>.Instance;

                server = new NamedPipeServer(pipeName, impl!, serverLogger, cts.Token);

                // Return owner handle (keeps the mutex; released on Dispose)
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
                // Clean up on failure to host
                TryDisposeState(m, cts, server);
                throw;
            }
        }

        private static void TryDisposeState(Mutex m, CancellationTokenSource? cts, NamedPipeServer? server)
        {
            try { server?.Dispose(); } catch { /* swallow: hosting failed anyway */ }
            try { cts?.Cancel(); cts?.Dispose(); } catch { /* swallow: teardown best-effort */ }
            try { m.ReleaseMutex(); } catch { /* swallow: just ensure mutex is not held */ }
            try { m.Dispose(); } catch { /* swallow: OS will reclaim handle */ }
        }
    }
}
