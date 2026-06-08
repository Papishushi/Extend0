namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Context passed to a custom client transport factory when attaching to a cross-process owner.
    /// </summary>
    /// <param name="TransportKind">Logical transport kind requested by the singleton options.</param>
    /// <param name="Protocol">Wire-protocol descriptor that the client transport is expected to speak.</param>
    /// <param name="EndpointName">Resolved endpoint name the client should target.</param>
    /// <param name="ServerName">Resolved server or machine name for the client transport.</param>
    /// <param name="ConnectTimeoutMs">Connection timeout requested for the client transport.</param>
    public sealed record ClientTransportFactoryContext(
        TransportKind TransportKind,
        CrossProcessProtocolDescriptor Protocol,
        string EndpointName,
        string ServerName,
        int ConnectTimeoutMs);
}
