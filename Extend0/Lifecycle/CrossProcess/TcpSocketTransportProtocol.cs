namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Wire-protocol profile for the built-in TCP socket RPC transport.
    /// </summary>
    internal static class TcpSocketTransportProtocol
    {
        internal static readonly CrossProcessProtocolDescriptor Descriptor =
            new(TransportKind.TcpSocket, "extend0-jsonrpc-ndjson", 1);
    }
}
