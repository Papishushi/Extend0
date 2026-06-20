using System.Net.Sockets;
using Extend0.Lifecycle.CrossProcess;
using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class UnixDomainSocketTransportTests
{
    [Fact]
    public async Task UnixDomainSocketServer_AndClientTransport_RoundTrip_EndToEnd()
    {
        if (!IsUnixDomainSocketSupported())
            return;

        var endpointName = AllocateUnixDomainSocketEndpoint();
        var implementation = new UnixDomainSocketTestService(endpointName);
        await using var server = LifecycleCrossProcessHarness.CreateUnixDomainSocketServer(
            endpointName,
            implementation,
            CancellationToken.None);

        using var transport = CreateBuiltInUnixDomainSocketTransport(endpointName);
        var proxy = RpcDispatchProxy<IUnixDomainSocketTestService>.Create(transport, CancellationToken.None);

        Assert.Equal(11, proxy.Add(5, 6));
        Assert.Equal("echo:hi", proxy.Echo("hi"));
        Assert.Equal("echo:async", await proxy.EchoAsync("async"));

        var heartbeat = await proxy.PingAsync();
        Assert.False(string.IsNullOrWhiteSpace(heartbeat.Fingerprint));

        var info = await proxy.GetServiceInfoAsync();
        Assert.Equal(typeof(IUnixDomainSocketTestService).FullName, info.ContractName);
        Assert.Equal(endpointName, info.EndpointName);
        Assert.Equal(".", info.EndpointServerName);
        Assert.Equal(TransportKind.UnixDomainSocket, info.TransportKind);
    }

    [Fact]
    public async Task UnixDomainSocketClientTransport_RejectsWrongHandshakeTransport()
    {
        if (!IsUnixDomainSocketSupported())
            return;

        var endpointName = AllocateUnixDomainSocketEndpoint();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var serverTask = RunRawUnixDomainSocketServerAsync(
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
            using var transport = CreateBuiltInUnixDomainSocketTransport(endpointName);
        });

        Assert.Contains("Invalid server handshake", error.Message, StringComparison.Ordinal);
        await serverTask;
    }

    private static IClientTransport CreateBuiltInUnixDomainSocketTransport(string endpointName) =>
        LifecycleCrossProcessHarness.CreateBuiltInClientTransport(
            new ClientTransportFactoryContext(
                TransportKind.UnixDomainSocket,
                LifecycleCrossProcessHarness.UnixDomainSocketProtocolDescriptor,
                endpointName,
                ".",
                2000));

    private static async Task RunRawUnixDomainSocketServerAsync(string endpointName, Func<NetworkStream, Task> handler, CancellationToken cancellationToken)
    {
        var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(new UnixDomainSocketEndPoint(endpointName));
        listener.Listen(backlog: 1);

        try
        {
            using var client = await listener.AcceptAsync(cancellationToken);
            await using var stream = new NetworkStream(client, ownsSocket: false);
            await handler(stream);
        }
        finally
        {
            listener.Dispose();
            TryDelete(endpointName);
        }
    }

    private static string AllocateUnixDomainSocketEndpoint()
    {
        var endpoint = Path.Combine(Path.GetTempPath(), $"extend0-uds-test-{Guid.NewGuid():N}.sock");
        TryDelete(endpoint);
        return endpoint;
    }

    private static bool IsUnixDomainSocketSupported()
    {
        try
        {
            using var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            return true;
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private interface IUnixDomainSocketTestService : ICrossProcessService
    {
        int Add(int a, int b);
        string Echo(string value);
        Task<string> EchoAsync(string value);
    }

    private sealed class UnixDomainSocketTestService(string endpointName)
        : CrossProcessServiceBase<IUnixDomainSocketTestService>, IUnixDomainSocketTestService
    {
        protected override string? EndpointName => endpointName;
        protected override string? EndpointServerName => ".";
        protected override TransportKind EndpointTransportKind => TransportKind.UnixDomainSocket;

        public int Add(int a, int b) => a + b;

        public string Echo(string value) => $"echo:{value}";

        public Task<string> EchoAsync(string value) => Task.FromResult($"echo:{value}");
    }
}
