using System.Net;
using System.Net.Sockets;
using Extend0.Lifecycle.CrossProcess;
using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class TcpSocketTransportTests
{
    [Fact]
    public async Task TcpSocketServer_AndClientTransport_RoundTrip_EndToEnd()
    {
        var endpointName = AllocateLoopbackEndpoint();
        var implementation = new TcpSocketTestService(endpointName);
        await using var server = LifecycleCrossProcessHarness.CreateTcpSocketServer(
            endpointName,
            implementation,
            CancellationToken.None);

        using var transport = CreateBuiltInTcpSocketTransport(endpointName);
        var proxy = RpcDispatchProxy<ITcpSocketTestService>.Create(transport, CancellationToken.None);

        Assert.Equal(11, proxy.Add(5, 6));
        Assert.Equal("echo:hi", proxy.Echo("hi"));
        Assert.Equal("echo:async", await proxy.EchoAsync("async"));

        var heartbeat = await proxy.PingAsync();
        Assert.False(string.IsNullOrWhiteSpace(heartbeat.Fingerprint));

        var info = await proxy.GetServiceInfoAsync();
        Assert.Equal(typeof(ITcpSocketTestService).FullName, info.ContractName);
        Assert.Equal(endpointName, info.EndpointName);
        Assert.Equal("127.0.0.1", info.EndpointServerName);
        Assert.Equal(TransportKind.TcpSocket, info.TransportKind);
    }

    [Fact]
    public async Task TcpSocketClientTransport_RejectsWrongHandshakeTransport()
    {
        var endpointName = AllocateLoopbackEndpoint();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var serverTask = RunRawTcpServerAsync(
            endpointName,
            async stream =>
            {
                await using var writer = new StreamWriter(stream, new System.Text.UTF8Encoding(false), leaveOpen: true)
                {
                    AutoFlush = true
                };
                await writer.WriteLineAsync(LifecycleCrossProcessHarness.BuildHelloLine(LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor));
            },
            cts.Token);

        var error = Assert.Throws<IOException>(() =>
        {
            using var transport = CreateBuiltInTcpSocketTransport(endpointName);
        });

        Assert.Contains("Invalid server handshake", error.Message, StringComparison.Ordinal);
        await serverTask;
    }

    private static IClientTransport CreateBuiltInTcpSocketTransport(string endpointName) =>
        LifecycleCrossProcessHarness.CreateBuiltInClientTransport(
            new ClientTransportFactoryContext(
                TransportKind.TcpSocket,
                LifecycleCrossProcessHarness.TcpSocketProtocolDescriptor,
                endpointName,
                ".",
                2000));

    private static async Task RunRawTcpServerAsync(string endpointName, Func<NetworkStream, Task> handler, CancellationToken cancellationToken)
    {
        var endpoint = ParseEndpoint(endpointName);
        var listener = new TcpListener(IPAddress.Parse(endpoint.host), endpoint.port);
        listener.Start();
        try
        {
            using var client = await listener.AcceptTcpClientAsync(cancellationToken);
            await using var stream = client.GetStream();
            await handler(stream);
        }
        finally
        {
            listener.Stop();
        }
    }

    private static (string host, int port) ParseEndpoint(string endpointName)
    {
        var separator = endpointName.LastIndexOf(':');
        return (endpointName[..separator], int.Parse(endpointName[(separator + 1)..]));
    }

    private static string AllocateLoopbackEndpoint()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return $"127.0.0.1:{port}";
    }

    private interface ITcpSocketTestService : ICrossProcessService
    {
        int Add(int a, int b);
        string Echo(string value);
        Task<string> EchoAsync(string value);
    }

    private sealed class TcpSocketTestService(string endpointName)
        : CrossProcessServiceBase<ITcpSocketTestService>, ITcpSocketTestService
    {
        protected override string? EndpointName => endpointName;
        protected override string? EndpointServerName => "127.0.0.1";
        protected override TransportKind EndpointTransportKind => TransportKind.TcpSocket;

        public int Add(int a, int b) => a + b;

        public string Echo(string value) => $"echo:{value}";

        public Task<string> EchoAsync(string value) => Task.FromResult($"echo:{value}");
    }
}
