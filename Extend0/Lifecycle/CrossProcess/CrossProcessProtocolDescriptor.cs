namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Declares the wire-protocol identity expected by a transport pair.
    /// </summary>
    /// <param name="TransportKind">Logical transport kind that carries the protocol.</param>
    /// <param name="ProtocolId">Stable protocol identifier understood by both endpoints.</param>
    /// <param name="ProtocolVersion">Wire-protocol version used for compatibility checks.</param>
    public sealed record CrossProcessProtocolDescriptor(
        TransportKind TransportKind,
        string ProtocolId,
        int ProtocolVersion);
}
