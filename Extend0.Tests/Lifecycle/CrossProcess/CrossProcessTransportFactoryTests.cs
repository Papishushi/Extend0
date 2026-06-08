using Extend0.Lifecycle.CrossProcess;
using Extend0.Testing.Lifecycle.CrossProcess;
using Microsoft.Extensions.Logging.Abstractions;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessTransportFactoryTests
{
    [Fact]
    public void ResolveProtocolDescriptor_ReturnsBuiltInNamedPipeDescriptor()
    {
        var descriptor = CrossProcessTransportFactory.ResolveProtocolDescriptor(TransportKind.NamedPipe);

        Assert.Equal(LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor, descriptor);
    }

    [Fact]
    public void ResolveProtocolDescriptor_ReturnsExplicitDescriptor_WhenTransportMatches()
    {
        var descriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "custom-rpc", 7);

        var resolved = CrossProcessTransportFactory.ResolveProtocolDescriptor(TransportKind.Custom, descriptor);

        Assert.Equal(descriptor, resolved);
    }

    [Fact]
    public void ResolveProtocolDescriptor_Throws_WhenExplicitDescriptorTransportDoesNotMatch()
    {
        var descriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "custom-rpc", 7);

        var ex = Assert.Throws<ArgumentException>(() =>
            CrossProcessTransportFactory.ResolveProtocolDescriptor(TransportKind.NamedPipe, descriptor));

        Assert.Contains("declares transport", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ResolveEndpointName_ReturnsLogicalName_ForCustomWhenFallbackIsAllowed()
    {
        var endpointName = CrossProcessTransportFactory.ResolveEndpointName(
            "Extend0.Test.Service",
            TransportKind.Custom,
            allowLogicalFallback: true);

        Assert.Equal("Extend0.Test.Service", endpointName);
    }

    [Fact]
    public void ResolveEndpointName_ReturnsExplicitEndpoint_AndRejectsUnsupportedBuiltIns()
    {
        var explicitEndpoint = CrossProcessTransportFactory.ResolveEndpointName(
            "Extend0.Test.Service",
            TransportKind.Custom,
            explicitEndpointName: "explicit-endpoint");

        var namedPipeEndpoint = CrossProcessTransportFactory.ResolveEndpointName(
            "Extend0.Test.Service",
            TransportKind.NamedPipe);

        Assert.Equal("explicit-endpoint", explicitEndpoint);
        Assert.Equal(LifecycleCrossProcessHarness.BuildNamedPipeEndpointName("Extend0.Test.Service"), namedPipeEndpoint);
        Assert.Throws<NotSupportedException>(() => CrossProcessTransportFactory.ResolveEndpointName("none", TransportKind.None));
        Assert.Throws<NotSupportedException>(() => CrossProcessTransportFactory.ResolveEndpointName("custom", TransportKind.Custom));
    }

    [Fact]
    public void ResolveProtocolDescriptor_RejectsUnsupportedKinds_AndMissingCustomDescriptor()
    {
        Assert.Throws<NotSupportedException>(() => CrossProcessTransportFactory.ResolveProtocolDescriptor(TransportKind.None));
        Assert.Throws<NotSupportedException>(() => CrossProcessTransportFactory.ResolveProtocolDescriptor(TransportKind.Custom));
        Assert.Throws<InvalidOperationException>(() =>
            CrossProcessTransportFactory.ResolveProtocolDescriptor(TransportKind.Custom, explicitProtocol: null, allowCustom: true));
    }

    [Fact]
    public void CreateClientTransport_UsesCustomFactory_WhenProvided()
    {
        var descriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "custom-rpc", 1);
        var context = new ClientTransportFactoryContext(TransportKind.Custom, descriptor, "endpoint", ".", 1234);
        var transport = new FakeClientTransport(TransportKind.Custom);

        var created = CrossProcessTransportFactory.CreateClientTransport(context, _ => transport);

        Assert.Same(transport, created);
    }

    [Fact]
    public void CreateClientTransport_ValidatesContextFactoryAndUnsupportedKinds()
    {
        var customDescriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "custom-rpc", 1);
        var customContext = new ClientTransportFactoryContext(TransportKind.Custom, customDescriptor, "endpoint", ".", 1234);
        var noneDescriptor = new CrossProcessProtocolDescriptor(TransportKind.None, "none-rpc", 1);
        var noneContext = new ClientTransportFactoryContext(TransportKind.None, noneDescriptor, "endpoint", ".", 1234);

        Assert.Throws<ArgumentNullException>(() => CrossProcessTransportFactory.CreateClientTransport(null!));
        Assert.Throws<InvalidOperationException>(() => CrossProcessTransportFactory.CreateClientTransport(customContext, _ => null!));
        Assert.Throws<NotSupportedException>(() => CrossProcessTransportFactory.CreateClientTransport(noneContext));
        Assert.Throws<NotSupportedException>(() => CrossProcessTransportFactory.CreateClientTransport(customContext));
    }

    [Fact]
    public void CreateServerHost_UsesCustomFactory_WhenProvided()
    {
        var descriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "custom-rpc", 1);
        var context = new ServerTransportFactoryContext(
            TransportKind.Custom,
            descriptor,
            "endpoint",
            new object(),
            NullLoggerFactory.Instance,
            CancellationToken.None);
        var host = new FakeServerHost();

        var created = LifecycleCrossProcessHarness.CreateBuiltInOrCustomServerHost(context, _ => host);

        Assert.Same(host, created);
    }

    [Fact]
    public void CreateServerHost_ValidatesContextFactoryAndUnsupportedKinds()
    {
        var customDescriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "custom-rpc", 1);
        var customContext = new ServerTransportFactoryContext(
            TransportKind.Custom,
            customDescriptor,
            "endpoint",
            new object(),
            NullLoggerFactory.Instance,
            CancellationToken.None);
        var noneDescriptor = new CrossProcessProtocolDescriptor(TransportKind.None, "none-rpc", 1);
        var noneContext = new ServerTransportFactoryContext(
            TransportKind.None,
            noneDescriptor,
            "endpoint",
            new object(),
            NullLoggerFactory.Instance,
            CancellationToken.None);

        Assert.Throws<ArgumentNullException>(() => LifecycleCrossProcessHarness.CreateBuiltInOrCustomServerHost(null!));
        Assert.Throws<InvalidOperationException>(() => LifecycleCrossProcessHarness.CreateBuiltInOrCustomServerHost(customContext, _ => null!));
        Assert.Throws<NotSupportedException>(() => LifecycleCrossProcessHarness.CreateBuiltInOrCustomServerHost(noneContext));
        Assert.Throws<NotSupportedException>(() => LifecycleCrossProcessHarness.CreateBuiltInOrCustomServerHost(customContext));
    }

    [Fact]
    public async Task CreateClientTransport_CanConnect_ToBuiltInNamedPipeTransport()
    {
        var service = new LifecycleCrossProcessHarness.TestCrossProcessService();
        using var cts = new CancellationTokenSource();
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"test-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(endpointName, service, cts.Token);

        using var transport = LifecycleCrossProcessHarness.CreateBuiltInClientTransport(
            new ClientTransportFactoryContext(
                TransportKind.NamedPipe,
                LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor,
                endpointName,
                ".",
                2000));

        Assert.Equal(TransportKind.NamedPipe, transport.Kind);
    }

    private sealed class FakeClientTransport(TransportKind kind) : IClientTransport
    {
        public TransportKind Kind { get; } = kind;

        public Task<System.Text.Json.JsonDocument> CallAsync(string method, object?[] args, Type[] paramTypes, Type declaredReturnType, CancellationToken ct) =>
            Task.FromResult(System.Text.Json.JsonDocument.Parse("{\"ok\":true,\"r\":null}"));

        public void Dispose()
        {
        }
    }

    private sealed class FakeServerHost : ICrossProcessServerHost
    {
        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
