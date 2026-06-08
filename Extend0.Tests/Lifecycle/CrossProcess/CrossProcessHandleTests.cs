using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessHandleTests
{
    [Fact]
    public void Dispose_OwnerHandle_DisposesSyncResources_AndIsIdempotent()
    {
        var bundle = CrossProcessHandleHarness.CreateOwnerBundle();

        bundle.Handle.Dispose();
        bundle.Handle.Dispose();

        Assert.Equal(1, bundle.Transport.DisposeCalls);
        Assert.True(bundle.CancellationTokenSource!.IsCancellationRequested);
        Assert.Equal(1, bundle.Server!.DisposeCalls);
        Assert.Equal(0, bundle.Server.DisposeAsyncCalls);
        Assert.Equal(1, bundle.Service.DisposeCalls);
        Assert.Equal(0, bundle.Service.DisposeAsyncCalls);
    }

    [Fact]
    public async Task DisposeAsync_OwnerHandle_PrefersAsyncDisposal_AndIsIdempotent()
    {
        var bundle = CrossProcessHandleHarness.CreateOwnerBundle();

        await bundle.Handle.DisposeAsync();
        await bundle.Handle.DisposeAsync();

        Assert.Equal(1, bundle.Transport.DisposeCalls);
        Assert.True(bundle.CancellationTokenSource!.IsCancellationRequested);
        Assert.Equal(0, bundle.Server!.DisposeCalls);
        Assert.Equal(1, bundle.Server.DisposeAsyncCalls);
        Assert.Equal(0, bundle.Service.DisposeCalls);
        Assert.Equal(1, bundle.Service.DisposeAsyncCalls);
    }

    [Fact]
    public async Task DisposeAndDisposeAsync_CalledConcurrently_TeardownRunsOnce()
    {
        var bundle = CrossProcessHandleHarness.CreateOwnerBundle(ownMutex: false);

        var syncDispose = Task.Run(bundle.Handle.Dispose);
        var asyncDispose = Task.Run(async () => await bundle.Handle.DisposeAsync());

        await Task.WhenAll(syncDispose, asyncDispose);

        Assert.Equal(1, bundle.Transport.DisposeCalls);
        Assert.True(bundle.CancellationTokenSource!.IsCancellationRequested);
        Assert.Equal(1, bundle.Server!.DisposeCalls + bundle.Server.DisposeAsyncCalls);
        Assert.Equal(1, bundle.Service.DisposeCalls + bundle.Service.DisposeAsyncCalls);
    }

    [Fact]
    public void Dispose_BestEffortSwallowsServerServiceAndMutexTeardownFailures()
    {
        var bundle = CrossProcessHandleHarness.CreateOwnerBundle(
            ownMutex: false,
            serviceThrowsOnDispose: true,
            serverThrowsOnDispose: true);

        bundle.Handle.Dispose();

        Assert.Equal(1, bundle.Transport.DisposeCalls);
        Assert.True(bundle.CancellationTokenSource!.IsCancellationRequested);
        Assert.Equal(1, bundle.Server!.DisposeCalls);
        Assert.Equal(1, bundle.Service.DisposeCalls);
    }

    [Fact]
    public async Task DisposeAsync_BestEffortSwallowsAsyncServerAndServiceFailures()
    {
        var bundle = CrossProcessHandleHarness.CreateOwnerBundle(
            ownMutex: false,
            serviceThrowsOnDisposeAsync: true,
            serverThrowsOnDisposeAsync: true);

        await bundle.Handle.DisposeAsync();

        Assert.Equal(1, bundle.Transport.DisposeCalls);
        Assert.True(bundle.CancellationTokenSource!.IsCancellationRequested);
        Assert.Equal(1, bundle.Server!.DisposeAsyncCalls);
        Assert.Equal(1, bundle.Service.DisposeAsyncCalls);
    }

    [Fact]
    public async Task DisposeAsync_OwnerHandle_FallsBackToSyncServiceDisposal_WhenAsyncServiceIsUnavailable()
    {
        var bundle = CrossProcessHandleHarness.CreateOwnerBundleWithSyncOnlyResources();

        await bundle.Handle.DisposeAsync();

        Assert.Equal(1, bundle.Transport.DisposeCalls);
        Assert.True(bundle.CancellationTokenSource.IsCancellationRequested);
        Assert.Equal(1, bundle.Server.DisposeCalls);
        Assert.Equal(1, bundle.Service.DisposeCalls);
    }

    [Fact]
    public void Dispose_ClientHandle_DoesNotDisposeOwnedService()
    {
        var bundle = CrossProcessHandleHarness.CreateClientBundle();

        bundle.Handle.Dispose();

        Assert.Equal(1, bundle.Transport.DisposeCalls);
        Assert.Equal(0, bundle.Service.DisposeCalls);
        Assert.Equal(0, bundle.Service.DisposeAsyncCalls);
    }
}
