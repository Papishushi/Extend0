using System.Collections.Concurrent;
using System.Text.Json;
using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class RpcDispatchProxyTests
{
    [Fact]
    public void Create_RejectsConcreteServiceTypes()
    {
        using var transport = new FakeClientTransport();

        var error = Assert.Throws<InvalidOperationException>(() =>
            RpcDispatchProxy<ConcreteRpcService>.Create(transport, CancellationToken.None));

        Assert.Contains("must be an interface", error.Message);
    }

    [Fact]
    public async Task Proxy_CoversVoidSyncTaskAndTaskOfT_MethodShapes()
    {
        using var transport = new FakeClientTransport(
            Success(),
            Success("""{"Sum":3}"""),
            Success(),
            Success("5"),
            Success("""{"UtcTime":"2026-05-25T00:00:00Z","UptimeSeconds":12,"Fingerprint":"fp"}"""),
            Success("""{"ContractName":"ITestRpcService","ImplementationName":"Impl","AssemblyVersion":"1.0.0.0","Fingerprint":"fp","MachineName":"box","ProcessId":7,"ProcessName":"proc","StartTimeUtc":"2026-05-25T00:00:00Z","PipeName":null,"EndpointName":"ep","EndpointServerName":"srv","TransportKind":255}"""),
            Success("true"));

        var proxy = RpcDispatchProxy<ITestRpcService>.Create(transport, CancellationToken.None);

        proxy.Notify("hola");
        var sync = proxy.AddSync(1, 2);
        await proxy.NotifyAsync("adios");
        var asyncSum = await proxy.AddAsync(2, 3);
        var heartbeat = await proxy.PingAsync();
        var info = await proxy.GetServiceInfoAsync();
        var canConnect = await proxy.CanConnectAsync();

        Assert.Equal(3, sync.Sum);
        Assert.Equal(5, asyncSum);
        Assert.Equal("fp", heartbeat.Fingerprint);
        Assert.Equal("ITestRpcService", info.ContractName);
        Assert.True(canConnect);

        Assert.Collection(transport.Calls,
            call =>
            {
                Assert.Equal(nameof(ITestRpcService.Notify), call.Method);
                Assert.Equal(typeof(void), call.DeclaredReturnType);
                Assert.Equal(new[] { typeof(string) }, call.ParamTypes);
            },
            call =>
            {
                Assert.Equal(nameof(ITestRpcService.AddSync), call.Method);
                Assert.Equal(typeof(SumResult), call.DeclaredReturnType);
                Assert.Equal(new[] { typeof(int), typeof(int) }, call.ParamTypes);
            },
            call =>
            {
                Assert.Equal(nameof(ITestRpcService.NotifyAsync), call.Method);
                Assert.Equal(typeof(Task), call.DeclaredReturnType);
            },
            call =>
            {
                Assert.Equal(nameof(ITestRpcService.AddAsync), call.Method);
                Assert.Equal(typeof(int), call.DeclaredReturnType);
            },
            call =>
            {
                Assert.Equal(nameof(ICrossProcessService.PingAsync), call.Method);
                Assert.Equal(typeof(Heartbeat), call.DeclaredReturnType);
            },
            call =>
            {
                Assert.Equal(nameof(ICrossProcessService.GetServiceInfoAsync), call.Method);
                Assert.Equal(typeof(ServiceInfo), call.DeclaredReturnType);
            },
            call =>
            {
                Assert.Equal(nameof(ICrossProcessService.CanConnectAsync), call.Method);
                Assert.Equal(typeof(bool), call.DeclaredReturnType);
            });
    }

    [Fact]
    public void Proxy_TreatsMissingOkAsSuccess_AndPropagatesRemoteErrors()
    {
        using var transport = new FakeClientTransport(
            JsonDocument.Parse("""{"r":{"Sum":9}}"""),
            Error("nope", 409));

        var proxy = RpcDispatchProxy<ITestRpcService>.Create(transport, CancellationToken.None);

        var value = proxy.AddSync(4, 5);
        var error = Assert.Throws<RemoteInvocationException>(() => proxy.Notify("fail"));

        Assert.Equal(9, value.Sum);
        Assert.Equal(409, error.HResult);
        Assert.Contains("nope", error.Message);
    }

    [Fact]
    public async Task Proxy_ErrorEnvelopes_UseProtocolDefaultMessagesWhenFieldsAreMissing()
    {
        using var transport = new FakeClientTransport(
            JsonDocument.Parse("""{"ok":false}"""),
            JsonDocument.Parse("""{"ok":false,"hr":426}"""),
            JsonDocument.Parse("""{"ok":false,"hr":422}"""));

        var proxy = RpcDispatchProxy<ITestRpcService>.Create(transport, CancellationToken.None);
        var previous = RpcDispatchProxy<ITestRpcService>.UpgradeHandler;
        RpcDispatchProxy<ITestRpcService>.UpgradeHandler = null;

        try
        {
            var generic = Assert.Throws<RemoteInvocationException>(() => proxy.Notify("generic"));
            var upgrade = Assert.Throws<RemoteInvocationException>(() => proxy.AddSync(1, 2));
            var corrupted = await Assert.ThrowsAsync<RemoteInvocationException>(() => proxy.AddAsync(1, 2));

            Assert.Equal(0, generic.HResult);
            Assert.Contains("Remote error", generic.Message, StringComparison.Ordinal);
            Assert.Equal(426, upgrade.HResult);
            Assert.Contains("Upgrade required", upgrade.Message, StringComparison.Ordinal);
            Assert.Equal(422, corrupted.HResult);
            Assert.Contains("Corrupted transport messages", corrupted.Message, StringComparison.Ordinal);
        }
        finally
        {
            RpcDispatchProxy<ITestRpcService>.UpgradeHandler = previous;
        }
    }

    [Fact]
    public async Task Proxy_UpgradeHandler_RetriesSyncAndAsyncOnce()
    {
        using var transport = new FakeClientTransport(
            Error("upgrade", 426),
            Success("""{"Sum":11}"""),
            Error("corrupted", 422),
            Success("12"));

        var proxy = RpcDispatchProxy<ITestRpcService>.Create(transport, CancellationToken.None);
        var previous = RpcDispatchProxy<ITestRpcService>.UpgradeHandler;
        var handled = new ConcurrentQueue<int>();

        RpcDispatchProxy<ITestRpcService>.UpgradeHandler = ex =>
        {
            handled.Enqueue(ex.HResult);
            return ValueTask.FromResult(true);
        };

        try
        {
            var sync = proxy.AddSync(5, 6);
            var asyncValue = await proxy.AddAsync(7, 5);

            Assert.Equal(11, sync.Sum);
            Assert.Equal(12, asyncValue);
            Assert.Equal([426, 422], handled.ToArray());
            Assert.Equal(4, transport.Calls.Count);
        }
        finally
        {
            RpcDispatchProxy<ITestRpcService>.UpgradeHandler = previous;
        }
    }

    [Fact]
    public void Proxy_UpgradeHandlerFailure_PropagatesRemoteException()
    {
        using var transport = new FakeClientTransport(Error("upgrade", 426));
        var proxy = RpcDispatchProxy<ITestRpcService>.Create(transport, CancellationToken.None);
        var previous = RpcDispatchProxy<ITestRpcService>.UpgradeHandler;

        RpcDispatchProxy<ITestRpcService>.UpgradeHandler = _ => throw new InvalidOperationException("boom");

        try
        {
            var error = Assert.Throws<RemoteInvocationException>(() => proxy.Notify("x"));
            Assert.Equal(426, error.HResult);
            Assert.Single(transport.Calls);
        }
        finally
        {
            RpcDispatchProxy<ITestRpcService>.UpgradeHandler = previous;
        }
    }

    private static JsonDocument Success(string rawResult = "null") =>
        JsonDocument.Parse($$"""{"ok":true,"r":{{rawResult}}}""");

    private static JsonDocument Error(string message, int hresult) =>
        JsonDocument.Parse($$"""{"ok":false,"e":"{{message}}","hr":{{hresult}}}""");

    private sealed class FakeClientTransport(params JsonDocument[] responses) : IClientTransport
    {
        private readonly Queue<JsonDocument> _responses = new(responses);

        public readonly record struct CallRecord(string Method, object?[] Args, Type[] ParamTypes, Type DeclaredReturnType);

        public List<CallRecord> Calls { get; } = [];

        public TransportKind Kind => TransportKind.Custom;

        public Task<JsonDocument> CallAsync(
            string method,
            object?[] args,
            Type[] paramTypes,
            Type declaredReturnType,
            CancellationToken ct)
        {
            Calls.Add(new CallRecord(method, args, paramTypes, declaredReturnType));

            if (_responses.Count == 0)
                throw new InvalidOperationException("No fake response configured.");

            var next = _responses.Dequeue();
            return Task.FromResult(JsonDocument.Parse(next.RootElement.GetRawText()));
        }

        public void Dispose()
        {
            while (_responses.Count > 0)
                _responses.Dequeue().Dispose();
        }
    }

    private interface ITestRpcService : ICrossProcessService
    {
        void Notify(string message);
        SumResult AddSync(int a, int b);
        Task NotifyAsync(string message);
        Task<int> AddAsync(int a, int b);
    }

    private sealed class ConcreteRpcService : ICrossProcessService
    {
        public string ContractName => "Concrete";
        public Task<bool> CanConnectAsync() => Task.FromResult(true);
        public Task<ServiceInfo> GetServiceInfoAsync() => throw new NotSupportedException();
        public Task<Heartbeat> PingAsync() => throw new NotSupportedException();
    }

    private sealed record SumResult(int Sum);
}
