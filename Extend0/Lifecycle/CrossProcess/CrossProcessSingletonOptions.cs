namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Options for configuring a singleton that can operate either in-process or as a cross-process singleton.
    /// Extends <see cref="SingletonOptions"/> with cross-process settings.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use these options when constructing a cross-process capable singleton to control whether the
    /// instance should be hosted locally (in-process) or exposed through a cross-process singleton
    /// with IPC proxies for non-owner processes.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// var opts = new CrossProcessSingletonOptions
    /// {
    ///     Mode = SingletonMode.CrossProcess,
    ///     TransportKind = TransportKind.NamedPipe,
    ///     CrossProcessName = "Extend0.Clock",   // optional logical name
    ///     CrossProcessConnectTimeoutMs = 5000,  // client connect timeout
    ///     Overwrite = false,                    // from SingletonOptions
    ///     Logger = logger                       // from SingletonOptions
    /// };
    /// </code>
    /// </example>
    public class CrossProcessSingletonOptions : SingletonOptions
    {
        /// <summary>
        /// Selects how the singleton is orchestrated.
        /// <list type="bullet">
        ///   <item><description><see cref="SingletonMode.InProcess"/>: a single instance per process (no IPC).</description></item>
        ///   <item><description><see cref="SingletonMode.CrossProcess"/>: one owner process hosts the real service; other processes obtain an IPC proxy.</description></item>
        /// </list>
        /// </summary>
        public SingletonMode Mode { get; init; } = SingletonMode.InProcess;

        /// <summary>
        /// Selects the transport kind used when <see cref="Mode"/> is <see cref="SingletonMode.CrossProcess"/>.
        /// The current built-in implementation supports <see cref="CrossProcess.TransportKind.NamedPipe"/>,
        /// <see cref="CrossProcess.TransportKind.UnixDomainSocket"/>, and
        /// <see cref="CrossProcess.TransportKind.TcpSocket"/>. TCP requires an explicit
        /// <see cref="CrossProcessEndpointName"/> in host:port form; Unix domain sockets derive a local socket path
        /// from the service identity unless an explicit path is supplied. Other values require compatible custom
        /// client/server factories and an explicit protocol descriptor; unsupported built-in selection fails explicitly.
        /// </summary>
        public TransportKind TransportKind { get; init; } = TransportKind.NamedPipe;

        /// <summary>
        /// Optional explicit wire-protocol descriptor for the selected transport.
        /// Built-in transports use their built-in descriptors when this is omitted.
        /// Custom transport pairs should typically provide this so both client and owner
        /// receive the same protocol identity and version through their factory contexts.
        /// </summary>
        public CrossProcessProtocolDescriptor? ProtocolDescriptor { get; init; }

        /// <summary>
        /// Authentication options applied to the cross-process handshake.
        /// Defaults to unauthenticated compatibility mode. Use
        /// <see cref="CrossProcessAuthenticationOptions.SharedSecretHmac(string)"/> when the owner and clients
        /// should prove knowledge of a shared secret before any RPC call is accepted.
        /// </summary>
        public CrossProcessAuthenticationOptions Authentication { get; init; } = CrossProcessAuthenticationOptions.None;

        /// <summary>
        /// TLS configuration used when <see cref="TransportKind"/> is <see cref="CrossProcess.TransportKind.TlsTcpSocket"/>.
        /// Server-side owners must provide a server certificate. Clients use these options for target-host validation,
        /// custom certificate validation, and optional client certificates for mTLS.
        /// </summary>
        public CrossProcessTlsOptions? Tls { get; init; }

        /// <summary>
        /// Optional explicit endpoint name override for the selected transport.
        /// When omitted, the transport factory derives the endpoint from the logical service identity.
        /// </summary>
        public string? CrossProcessEndpointName { get; init; }

        /// <summary>
        /// Target server/machine name used by client transports when connecting to the cross-process owner.
        /// Use <c>"."</c> for the local machine.
        /// On Windows you may specify a remote computer name (e.g., <c>"HOST123"</c>) when the chosen transport supports it
        /// (for example <see cref="CrossProcess.TransportKind.NamedPipe"/>). On Linux/macOS this value is typically ignored
        /// by local-only transports.
        /// </summary>
        public string CrossProcessServer { get; init; } = ".";

        /// <summary>
        /// Optional logical name used to derive the cross-process identity (e.g., <c>"Extend0.Clock"</c>).
        /// If omitted, the identity is typically derived from the service contract type and assembly fingerprint.
        /// </summary>
        public string? CrossProcessName { get; init; }  // e.g., "Extend0.Clock"

        /// <summary>
        /// Client connect timeout, in milliseconds, when attaching to an existing cross-process owner.
        /// Ignored when becoming the owner (hosting path).
        /// </summary>
        public int CrossProcessConnectTimeoutMs { get; init; } = 3000;

        /// <summary>
        /// Optional user-supplied client transport factory.
        /// When provided, the non-owner attach path uses this factory before falling back to built-in transports.
        /// This allows callers to inject custom <see cref="IClientTransport"/> implementations without changing
        /// singleton orchestration code.
        /// </summary>
        public Func<ClientTransportFactoryContext, IClientTransport>? ClientTransportFactory { get; init; }

        /// <summary>
        /// Optional user-supplied owner-side server host factory.
        /// When provided, the owner hosting path uses this factory before falling back to built-in hosts.
        /// Pair this with <see cref="ClientTransportFactory"/> to fully support custom transports outside the
        /// built-in transport set.
        /// </summary>
        public Func<ServerTransportFactoryContext, ICrossProcessServerHost>? ServerTransportFactory { get; init; }
    }
}
