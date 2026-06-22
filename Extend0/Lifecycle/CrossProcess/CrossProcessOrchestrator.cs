using Microsoft.Extensions.Logging;

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
        /// <param name="factory">
        /// Creates the concrete service implementation when the current process wins ownership and must host the service.
        /// </param>
        /// <param name="transportKind">
        /// Logical transport used for the cross-process connection. Built-in transports resolve their own endpoint shape.
        /// </param>
        /// <param name="protocolDescriptor">
        /// Optional protocol descriptor override. This is primarily used by custom transports that need to advertise a
        /// protocol id/version different from the built-in defaults.
        /// </param>
        /// <param name="endpointName">
        /// Optional concrete endpoint name. When omitted, the endpoint is derived from the logical service identity and
        /// the selected transport.
        /// </param>
        /// <param name="name">
        /// Optional logical service name suffix appended to the deterministic contract-based identity.
        /// </param>
        /// <param name="serverName">
        /// Target server or machine name used by client transports when connecting to an existing owner.
        /// </param>
        /// <param name="connectTimeoutMs">
        /// Maximum time, in milliseconds, that a non-owner process waits while attaching to the owner transport.
        /// </param>
        /// <param name="preferGlobalMutex">
        /// When <see langword="true"/>, attempts to reserve the ownership mutex in the global namespace when the
        /// platform supports it.
        /// </param>
        /// <param name="clientTransportFactory">
        /// Optional client transport factory for custom transports. When supplied, the orchestrator passes the resolved
        /// endpoint and protocol metadata into the factory instead of using a built-in transport implementation.
        /// </param>
        /// <param name="serverTransportFactory">
        /// Optional owner-side host factory for custom transports. When supplied, the orchestrator delegates server host
        /// creation to the caller while preserving the shared ownership and disposal model.
        /// </param>
        /// <returns>
        /// A <see cref="CrossProcessHandle{TService}"/> that either wraps the hosted implementation for the owner or a
        /// client proxy for non-owner callers.
        /// </returns>
        public static CrossProcessHandle<TService> GetOrStart(
            Func<TService> factory,
            TransportKind transportKind = TransportKind.NamedPipe,
            CrossProcessProtocolDescriptor? protocolDescriptor = null,
            string? endpointName = null,
            string? name = null,
            string serverName = ".",
            int connectTimeoutMs = 3000,
            bool preferGlobalMutex = true,
            CrossProcessAuthenticationOptions? authentication = null,
            CrossProcessTlsOptions? tls = null,
            Func<ClientTransportFactoryContext, IClientTransport>? clientTransportFactory = null,
            Func<ServerTransportFactoryContext, ICrossProcessServerHost>? serverTransportFactory = null)
        {
            ArgumentNullException.ThrowIfNull(factory);

            var baseName = CrossProcessUtils.BuildNameFor<TService>(name);
            var resolvedProtocolDescriptor = CrossProcessTransportFactory.ResolveProtocolDescriptor(
                transportKind,
                protocolDescriptor,
                allowCustom: clientTransportFactory is not null || serverTransportFactory is not null);
            var resolvedEndpointName = CrossProcessTransportFactory.ResolveEndpointName(
                baseName,
                transportKind,
                endpointName,
                allowLogicalFallback: clientTransportFactory is not null || serverTransportFactory is not null);

            var m = CrossProcessUtils.CreateOwned(baseName, preferGlobalMutex, out bool createdNew, out bool isGlobal);

            if (createdNew) return HostBranch(factory, baseName, resolvedEndpointName, serverName, resolvedProtocolDescriptor, transportKind, m, isGlobal, authentication, tls, serverTransportFactory);
            else return ClientBranch(serverName, connectTimeoutMs, resolvedEndpointName, resolvedProtocolDescriptor, transportKind, m, authentication, tls, clientTransportFactory);
        }

        /// <summary>
        /// Connects to an already-owned cross-process instance and returns a client proxy handle.
        /// </summary>
        private static CrossProcessHandle<TService> ClientBranch(
            string serverName,
            int connectTimeoutMs,
            string endpointName,
            CrossProcessProtocolDescriptor protocolDescriptor,
            TransportKind transportKind,
            Mutex m,
            CrossProcessAuthenticationOptions? authentication,
            CrossProcessTlsOptions? tls,
            Func<ClientTransportFactoryContext, IClientTransport>? clientTransportFactory)
        {
            try { m.Dispose(); } catch { }

            var transport = CrossProcessTransportFactory.CreateClientTransport(
                new ClientTransportFactoryContext(transportKind, protocolDescriptor, endpointName, serverName, connectTimeoutMs, authentication, tls),
                clientTransportFactory);
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
        /// Creates the real service implementation, hosts it on the resolved transport, and returns the owner handle.
        /// </summary>
        private static CrossProcessHandle<TService> HostBranch(
            Func<TService> factory,
            string ownershipName,
            string endpointName,
            string serverName,
            CrossProcessProtocolDescriptor protocolDescriptor,
            TransportKind transportKind,
            Mutex m,
            bool isGlobal,
            CrossProcessAuthenticationOptions? authentication,
            CrossProcessTlsOptions? tls,
            Func<ServerTransportFactoryContext, ICrossProcessServerHost>? serverTransportFactory)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            ICrossProcessServerHost? server = null;
            TService? impl = null;
            try
            {
                impl = factory();
                if (impl is CrossProcessServiceBase<TService> serviceBase)
                {
                    serviceBase.ConfigureRuntimeEndpoint(endpointName, serverName, transportKind);
                    serviceBase.ConfigureRuntimeLease(
                        ownershipName,
                        "OSMutex",
                        ownershipName,
                        ResolveCoordinationScope(isGlobal));
                }

                server = CrossProcessTransportFactory.CreateServerHost(
                    new ServerTransportFactoryContext(transportKind, protocolDescriptor, endpointName, impl!, s_LoggerFactory, cts.Token, authentication, tls),
                    serverTransportFactory);

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
                TryDisposeState(m, cts, server, impl);
                throw;
            }
        }

        private static string ResolveCoordinationScope(bool isGlobal) =>
            OperatingSystem.IsWindows()
                ? isGlobal ? "Global" : "LocalOrSession"
                : "System";

        /// <summary>
        /// Best-effort cleanup for partially initialized owner state after a hosting failure.
        /// </summary>
        private static void TryDisposeState(Mutex m, CancellationTokenSource? cts, ICrossProcessServerHost? server, TService? service)
        {
            try { cts?.Cancel(); } catch { }
            try { server?.Dispose(); } catch { }
            TryDisposeService(service);
            try { cts?.Dispose(); } catch { }
            try { m.ReleaseMutex(); } catch { }
            try { m.Dispose(); } catch { }
        }

        /// <summary>
        /// Best-effort disposal for owner implementations created before host startup failed.
        /// </summary>
        private static void TryDisposeService(TService? service)
        {
            try
            {
                if (service is IDisposable disposable)
                {
                    disposable.Dispose();
                }
                else if (service is IAsyncDisposable asyncDisposable)
                {
                    asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            }
            catch
            {
                // Failure-path disposal is best-effort; preserve the original hosting exception.
            }
        }
    }
}
