using Microsoft.Extensions.Logging;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Context passed to a custom owner-side host factory when this process becomes the cross-process owner.
    /// </summary>
    /// <param name="TransportKind">Logical transport kind requested by the singleton options.</param>
    /// <param name="Protocol">Wire-protocol descriptor that the owner host is expected to expose.</param>
    /// <param name="EndpointName">Resolved endpoint name the owner host should expose.</param>
    /// <param name="Implementation">Concrete service implementation that must be hosted.</param>
    /// <param name="LoggerFactory">Optional logger factory available to the host implementation.</param>
    /// <param name="CancellationToken">Cancellation token tied to the owner host lifetime.</param>
    /// <param name="Authentication">Authentication options the owner host should enforce during handshake.</param>
    /// <param name="Tls">TLS options used by TLS-capable transports.</param>
    public sealed record ServerTransportFactoryContext(
        TransportKind TransportKind,
        CrossProcessProtocolDescriptor Protocol,
        string EndpointName,
        object Implementation,
        ILoggerFactory? LoggerFactory,
        CancellationToken CancellationToken,
        CrossProcessAuthenticationOptions? Authentication = null,
        CrossProcessTlsOptions? Tls = null);
}
