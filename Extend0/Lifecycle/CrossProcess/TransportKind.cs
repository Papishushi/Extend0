namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Logical transport kinds supported or anticipated by the cross-process lifecycle system.
    /// </summary>
    public enum TransportKind
    {
        None = 0,

        /// <summary>
        /// Windows named pipes and their equivalent local/remote pipe semantics.
        /// </summary>
        NamedPipe = 1,

        /// <summary>
        /// Anonymous OS pipes, typically parent/child and local-only.
        /// </summary>
        AnonymousPipe = 2,

        /// <summary>
        /// Unix domain sockets for local machine IPC on Unix-like systems.
        /// </summary>
        UnixDomainSocket = 3,

        /// <summary>
        /// Raw TCP sockets for machine-local or network-reachable communication.
        /// </summary>
        TcpSocket = 4,

        /// <summary>
        /// TCP sockets secured with TLS.
        /// </summary>
        TlsTcpSocket = 5,

        /// <summary>
        /// QUIC-based communication.
        /// </summary>
        Quic = 6,

        /// <summary>
        /// WebSocket transport.
        /// </summary>
        WebSocket = 7,

        /// <summary>
        /// HTTP-based RPC on loopback or private service endpoints.
        /// </summary>
        Http = 8,

        /// <summary>
        /// gRPC transport, commonly over HTTP/2.
        /// </summary>
        Grpc = 9,

        /// <summary>
        /// Shared memory regions coordinated externally.
        /// </summary>
        SharedMemory = 10,

        /// <summary>
        /// Memory-mapped-file backed coordination or exchange.
        /// </summary>
        MemoryMappedFile = 11,

        /// <summary>
        /// Message-queue based transport or rendezvous.
        /// </summary>
        MessageQueue = 12,

        /// <summary>
        /// Custom transport not yet represented by a dedicated enum member.
        /// </summary>
        Custom = 255
    }
}
