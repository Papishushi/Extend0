namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Wire-protocol profile for the built-in Unix domain socket RPC transport.
    /// </summary>
    internal static class UnixDomainSocketTransportProtocol
    {
        internal static readonly CrossProcessProtocolDescriptor Descriptor =
            new(TransportKind.UnixDomainSocket, "extend0-jsonrpc-ndjson", 1);
    }
}
