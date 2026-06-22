namespace Extend0.Lifecycle.CrossProcess
{
    internal static class TlsTcpSocketTransportProtocol
    {
        public static CrossProcessProtocolDescriptor Descriptor { get; } =
            new(TransportKind.TlsTcpSocket, "extend0-jsonrpc-ndjson", 1);
    }
}
