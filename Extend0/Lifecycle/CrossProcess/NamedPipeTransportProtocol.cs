namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Wire-protocol profile for the built-in named-pipe RPC transport.
    /// </summary>
    internal static class NamedPipeTransportProtocol
    {
        internal static readonly CrossProcessProtocolDescriptor Descriptor =
            new(TransportKind.NamedPipe, "extend0-jsonrpc-ndjson", 1);
    }
}
