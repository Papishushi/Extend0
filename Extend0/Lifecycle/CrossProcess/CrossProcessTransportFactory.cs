using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Central factory for built-in cross-process endpoint naming, client transports and server hosts.
    /// </summary>
    public static class CrossProcessTransportFactory
    {
        /// <summary>
        /// Resolves the wire-protocol descriptor for the selected transport.
        /// </summary>
        /// <param name="transportKind">Transport kind chosen for the singleton.</param>
        /// <param name="explicitProtocol">
        /// Optional explicit protocol descriptor supplied by the caller.
        /// </param>
        /// <param name="allowCustom">
        /// When <see langword="true"/>, unsupported built-in kinds may rely on the explicit descriptor
        /// provided by the caller. If no descriptor is supplied, resolution fails explicitly.
        /// </param>
        public static CrossProcessProtocolDescriptor ResolveProtocolDescriptor(
            TransportKind transportKind,
            CrossProcessProtocolDescriptor? explicitProtocol = null,
            bool allowCustom = false)
        {
            if (explicitProtocol is not null)
            {
                if (explicitProtocol.TransportKind != transportKind)
                    throw new ArgumentException(
                        $"The explicit protocol descriptor declares transport '{explicitProtocol.TransportKind}', but the singleton uses '{transportKind}'.",
                        nameof(explicitProtocol));

                return explicitProtocol;
            }

            return transportKind switch
            {
                TransportKind.NamedPipe => NamedPipeTransportProtocol.Descriptor,
                TransportKind.UnixDomainSocket => UnixDomainSocketTransportProtocol.Descriptor,
                TransportKind.TcpSocket => TcpSocketTransportProtocol.Descriptor,
                TransportKind.None => throw new NotSupportedException("Transport kind 'None' cannot be used for cross-process singleton orchestration."),
                _ when allowCustom => throw new InvalidOperationException(
                    $"Transport kind '{transportKind}' requires an explicit protocol descriptor when using custom client/server transport factories."),
                _ => throw new NotSupportedException($"Transport kind '{transportKind}' does not have a built-in protocol descriptor.")
            };
        }

        /// <summary>
        /// Resolves the concrete endpoint name for a cross-process service identity.
        /// </summary>
        /// <param name="baseName">Logical cross-process service identity.</param>
        /// <param name="transportKind">Transport kind that will back the endpoint.</param>
        /// <param name="explicitEndpointName">
        /// Optional endpoint override supplied by the caller. When provided, it is returned as-is.
        /// </param>
        /// <param name="allowLogicalFallback">
        /// When <see langword="true"/>, unsupported built-in kinds fall back to the logical base name so custom
        /// client/server factories can still use a stable endpoint identifier.
        /// </param>
        public static string ResolveEndpointName(
            string baseName,
            TransportKind transportKind,
            string? explicitEndpointName = null,
            bool allowLogicalFallback = false)
        {
            if (!string.IsNullOrWhiteSpace(explicitEndpointName))
                return explicitEndpointName;

            return transportKind switch
            {
                TransportKind.NamedPipe => CrossProcessUtils.BuildPipeName(baseName),
                TransportKind.UnixDomainSocket => UnixDomainSocketEndpointName.BuildPath(baseName),
                TransportKind.TcpSocket => throw new NotSupportedException("Transport kind 'TcpSocket' requires an explicit endpoint name in host:port form."),
                TransportKind.None => throw new NotSupportedException("Transport kind 'None' cannot be used for cross-process singleton orchestration."),
                _ when allowLogicalFallback => baseName,
                _ => throw new NotSupportedException($"Transport kind '{transportKind}' does not have a built-in endpoint naming strategy.")
            };
        }

        /// <summary>
        /// Creates the client transport used to attach to an existing cross-process owner.
        /// </summary>
        /// <param name="context">Resolved client transport context.</param>
        /// <param name="customFactory">
        /// Optional user-supplied factory. When provided, it is used before built-in transport creation.
        /// This enables custom <see cref="IClientTransport"/> injection without modifying the orchestrator.
        /// </param>
        public static IClientTransport CreateClientTransport(
            ClientTransportFactoryContext context,
            Func<ClientTransportFactoryContext, IClientTransport>? customFactory = null)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (customFactory is not null)
                return customFactory(context) ?? throw new InvalidOperationException("The custom client transport factory returned null.");

            return context.TransportKind switch
            {
                TransportKind.NamedPipe => new NamedPipeClientTransport(context.ServerName, context.EndpointName, context.ConnectTimeoutMs, context.Protocol),
                TransportKind.UnixDomainSocket => new UnixDomainSocketClientTransport(context.EndpointName, context.ConnectTimeoutMs, context.Protocol),
                TransportKind.TcpSocket => new TcpSocketClientTransport(context.ServerName, context.EndpointName, context.ConnectTimeoutMs, context.Protocol),
                TransportKind.None => throw new NotSupportedException("Transport kind 'None' cannot create a client transport."),
                _ => throw new NotSupportedException($"Transport kind '{context.TransportKind}' is not yet implemented for cross-process client transport creation.")
            };
        }

        /// <summary>
        /// Creates the owner-side host used to expose the active cross-process owner.
        /// </summary>
        /// <remarks>
        /// Custom server host injection is supported through <paramref name="customFactory"/>.
        /// When no custom factory is provided, only built-in transport kinds can be hosted.
        /// </remarks>
        internal static ICrossProcessServerHost CreateServerHost(
            ServerTransportFactoryContext context,
            Func<ServerTransportFactoryContext, ICrossProcessServerHost>? customFactory = null)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (customFactory is not null)
                return customFactory(context) ?? throw new InvalidOperationException("The custom server transport factory returned null.");

            return context.TransportKind switch
            {
                TransportKind.NamedPipe => new NamedPipeServer(
                    context.EndpointName,
                    context.Implementation,
                    context.LoggerFactory?.CreateLogger<NamedPipeServer>() ?? NullLogger<NamedPipeServer>.Instance,
                    context.CancellationToken,
                    context.Protocol),
                TransportKind.UnixDomainSocket => new UnixDomainSocketServer(
                    context.EndpointName,
                    context.Implementation,
                    context.LoggerFactory?.CreateLogger<UnixDomainSocketServer>() ?? NullLogger<UnixDomainSocketServer>.Instance,
                    context.CancellationToken,
                    context.Protocol),
                TransportKind.TcpSocket => new TcpSocketServer(
                    context.EndpointName,
                    context.Implementation,
                    context.LoggerFactory?.CreateLogger<TcpSocketServer>() ?? NullLogger<TcpSocketServer>.Instance,
                    context.CancellationToken,
                    context.Protocol),
                TransportKind.None => throw new NotSupportedException("Transport kind 'None' cannot host a cross-process service."),
                _ => throw new NotSupportedException($"Transport kind '{context.TransportKind}' is not yet implemented for cross-process server hosting.")
            };
        }
    }
}
