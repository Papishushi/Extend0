using System.Threading;
using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Testing.Lifecycle.CrossProcess;

public static class CrossProcessHandleHarness
{
    public static HandleBundle CreateOwnerBundle(
        bool ownMutex = true,
        bool serviceThrowsOnDispose = false,
        bool serviceThrowsOnDisposeAsync = false,
        bool serverThrowsOnDispose = false,
        bool serverThrowsOnDisposeAsync = false)
    {
        var service = new TestHandleService(serviceThrowsOnDispose, serviceThrowsOnDisposeAsync);
        var transport = new TestClientTransport();
        var server = new TestServerHost(serverThrowsOnDispose, serverThrowsOnDisposeAsync);
        var mutex = new Mutex(initiallyOwned: ownMutex);
        var cts = new CancellationTokenSource();

        var handle = new CrossProcessHandle<TestHandleService>(
            service,
            isOwner: true,
            mutex,
            cts,
            server,
            transport);

        return new HandleBundle(handle, service, transport, server, mutex, cts);
    }

    public static HandleBundle CreateClientBundle()
    {
        var service = new TestHandleService();
        var transport = new TestClientTransport();
        var handle = new CrossProcessHandle<TestHandleService>(
            service,
            isOwner: false,
            mutex: null,
            cts: null,
            server: null,
            transport);

        return new HandleBundle(handle, service, transport, null, null, null);
    }

    public static SyncOnlyHandleBundle CreateOwnerBundleWithSyncOnlyResources()
    {
        var service = new SyncOnlyTestHandleService();
        var transport = new TestClientTransport();
        var server = new SyncOnlyServerHost();
        var mutex = new Mutex(initiallyOwned: true);
        var cts = new CancellationTokenSource();

        var handle = new CrossProcessHandle<SyncOnlyTestHandleService>(
            service,
            isOwner: true,
            mutex,
            cts,
            server,
            transport);

        return new SyncOnlyHandleBundle(handle, service, transport, server, mutex, cts);
    }

    public sealed record HandleBundle(
        CrossProcessHandle<TestHandleService> Handle,
        TestHandleService Service,
        TestClientTransport Transport,
        TestServerHost? Server,
        Mutex? Mutex,
        CancellationTokenSource? CancellationTokenSource);

    public sealed record SyncOnlyHandleBundle(
        CrossProcessHandle<SyncOnlyTestHandleService> Handle,
        SyncOnlyTestHandleService Service,
        TestClientTransport Transport,
        SyncOnlyServerHost Server,
        Mutex Mutex,
        CancellationTokenSource CancellationTokenSource);

    public sealed class TestHandleService(
        bool throwsOnDispose = false,
        bool throwsOnDisposeAsync = false)
        : ICrossProcessService, IDisposable, IAsyncDisposable
    {
        public string ContractName => "TestHandleService";

        public int DisposeCalls { get; private set; }
        public int DisposeAsyncCalls { get; private set; }

        public Task<Heartbeat> PingAsync() =>
            Task.FromResult(new Heartbeat(DateTimeOffset.UtcNow, 0, "fp"));

        public Task<ServiceInfo> GetServiceInfoAsync() =>
            Task.FromResult(new ServiceInfo("contract", "impl", "1.0", "fp", "machine", 1, "proc", DateTimeOffset.UtcNow, null, null, null, TransportKind.None));

        public Task<Lease> GetLeaseAsync() => Task.FromResult(CreateTestLease(ContractName));

        public Task<bool> CanConnectAsync() => Task.FromResult(true);

        public void Dispose()
        {
            DisposeCalls++;
            if (throwsOnDispose)
                throw new InvalidOperationException("service dispose failed");
        }

        public ValueTask DisposeAsync()
        {
            DisposeAsyncCalls++;
            if (throwsOnDisposeAsync)
                throw new InvalidOperationException("service dispose async failed");

            return ValueTask.CompletedTask;
        }
    }

    public sealed class TestClientTransport : IClientTransport
    {
        public TransportKind Kind => TransportKind.Custom;

        public int DisposeCalls { get; private set; }

        public Task<System.Text.Json.JsonDocument> CallAsync(
            string method,
            object?[] args,
            Type[] paramTypes,
            Type declaredReturnType,
            CancellationToken ct) =>
            Task.FromResult(System.Text.Json.JsonDocument.Parse("{\"ok\":true,\"r\":null}"));

        public void Dispose() => DisposeCalls++;
    }

    public sealed class SyncOnlyTestHandleService : ICrossProcessService, IDisposable
    {
        public string ContractName => "SyncOnlyTestHandleService";
        public int DisposeCalls { get; private set; }

        public Task<Heartbeat> PingAsync() =>
            Task.FromResult(new Heartbeat(DateTimeOffset.UtcNow, 0, "fp"));

        public Task<ServiceInfo> GetServiceInfoAsync() =>
            Task.FromResult(new ServiceInfo("contract", "impl", "1.0", "fp", "machine", 1, "proc", DateTimeOffset.UtcNow, null, null, null, TransportKind.None));

        public Task<Lease> GetLeaseAsync() => Task.FromResult(CreateTestLease(ContractName));

        public Task<bool> CanConnectAsync() => Task.FromResult(true);

        public void Dispose() => DisposeCalls++;
    }

    public sealed class TestServerHost(
        bool throwsOnDispose = false,
        bool throwsOnDisposeAsync = false)
        : ICrossProcessServerHost
    {
        public int DisposeCalls { get; private set; }
        public int DisposeAsyncCalls { get; private set; }

        public void Dispose()
        {
            DisposeCalls++;
            if (throwsOnDispose)
                throw new InvalidOperationException("server dispose failed");
        }

        public ValueTask DisposeAsync()
        {
            DisposeAsyncCalls++;
            if (throwsOnDisposeAsync)
                throw new InvalidOperationException("server dispose async failed");

            return ValueTask.CompletedTask;
        }
    }

    public sealed class SyncOnlyServerHost : ICrossProcessServerHost
    {
        public int DisposeCalls { get; private set; }

        public void Dispose() => DisposeCalls++;

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private static Lease CreateTestLease(string contractName)
    {
        var now = DateTimeOffset.UtcNow;
        return new Lease(
            "test-lease",
            contractName,
            contractName,
            "fp",
            "machine",
            1,
            "proc",
            now,
            now,
            null,
            null,
            null,
            TransportKind.None,
            "Test",
            null,
            "Process",
            true,
            true);
    }
}
