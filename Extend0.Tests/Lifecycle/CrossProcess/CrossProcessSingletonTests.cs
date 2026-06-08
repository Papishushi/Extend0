using Extend0.Lifecycle;
using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessSingletonTests
{
    [Fact]
    public void Service_Throws_WhenSingletonHasNotBeenInitialized()
    {
        Assert.Throws<InvalidOperationException>(() => CrossProcessSingleton<ITestSingletonContract>.Service);
    }

    [Fact]
    public void InProcessMode_InitializesStaticService_AndDisposeClearsIt()
    {
        TestSingletonService? created = null;

        using (var singleton = new CrossProcessSingleton<ITestSingletonContract>(
            () => created = new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true }))
        {
            Assert.True(CrossProcessSingleton<ITestSingletonContract>.IsOwner);
            Assert.Same(created, CrossProcessSingleton<ITestSingletonContract>.Service);
        }

        Assert.NotNull(created);
        Assert.Equal(1, created.DisposeCount);
        Assert.False(CrossProcessSingleton<ITestSingletonContract>.IsOwner);
        Assert.Throws<InvalidOperationException>(() => CrossProcessSingleton<ITestSingletonContract>.Service);
    }

    [Fact]
    public void OverwriteFalse_RejectsSecondInitialization()
    {
        using var first = new CrossProcessSingleton<ITestSingletonContract>(
            () => new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true });

        Assert.Throws<InvalidOperationException>(() => new CrossProcessSingleton<ITestSingletonContract>(
            () => new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = false }));
    }

    [Fact]
    public void OverwriteTrue_DisposesPreviousService_AndReplacesStaticInstance()
    {
        TestSingletonService? firstCreated = null;
        TestSingletonService? secondCreated = null;

        using var first = new CrossProcessSingleton<ITestSingletonContract>(
            () => firstCreated = new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true });

        using var second = new CrossProcessSingleton<ITestSingletonContract>(
            () => secondCreated = new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true });

        Assert.NotNull(firstCreated);
        Assert.NotNull(secondCreated);
        Assert.Equal(1, firstCreated.DisposeCount);
        Assert.Same(secondCreated, CrossProcessSingleton<ITestSingletonContract>.Service);
    }

    [Fact]
    public void OverwriteTrue_FromCrossProcessToInProcess_DisposesPreviousHandleState()
    {
        TestSingletonService? firstCreated = null;
        TestSingletonService? secondCreated = null;
        var serviceName = $"singleton-{Guid.NewGuid():N}";
        var serverDisposeCalls = 0;

        using var first = new CrossProcessSingleton<ITestSingletonContract>(
            () => firstCreated = new TestSingletonService(),
            new CrossProcessSingletonOptions
            {
                Mode = SingletonMode.CrossProcess,
                Overwrite = true,
                TransportKind = TransportKind.Custom,
                ProtocolDescriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "singleton-wire", 1),
                CrossProcessName = serviceName,
                ServerTransportFactory = _ => new TrackingServerHost(() => serverDisposeCalls++)
            });

        using var second = new CrossProcessSingleton<ITestSingletonContract>(
            () => secondCreated = new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true });

        Assert.NotNull(firstCreated);
        Assert.NotNull(secondCreated);
        Assert.Equal(1, firstCreated.DisposeCount);
        Assert.Equal(1, serverDisposeCalls);
        Assert.Same(secondCreated, CrossProcessSingleton<ITestSingletonContract>.Service);
        Assert.True(CrossProcessSingleton<ITestSingletonContract>.IsOwner);
    }

    [Fact]
    public void OverwriteTrue_SwallowsDisposeFailure_FromPreviousInProcessService()
    {
        ThrowingDisposeSingletonService? firstCreated = null;
        TestSingletonService? secondCreated = null;

        using var first = new CrossProcessSingleton<ITestSingletonContract>(
            () => firstCreated = new ThrowingDisposeSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true });

        using var second = new CrossProcessSingleton<ITestSingletonContract>(
            () => secondCreated = new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true });

        Assert.NotNull(firstCreated);
        Assert.NotNull(secondCreated);
        Assert.Equal(1, firstCreated.DisposeAttempts);
        Assert.Same(secondCreated, CrossProcessSingleton<ITestSingletonContract>.Service);
    }

    [Fact]
    public void OverwriteTrue_SwallowsDisposeFailure_FromPreviousCrossProcessHandle()
    {
        var serviceName = $"singleton-throwing-{Guid.NewGuid():N}";
        TestSingletonService? secondCreated = null;

        using var first = new CrossProcessSingleton<ITestSingletonContract>(
            () => new TestSingletonService(),
            new CrossProcessSingletonOptions
            {
                Mode = SingletonMode.CrossProcess,
                Overwrite = true,
                TransportKind = TransportKind.Custom,
                ProtocolDescriptor = new CrossProcessProtocolDescriptor(TransportKind.Custom, "singleton-wire", 1),
                CrossProcessName = serviceName,
                ServerTransportFactory = _ => new ThrowingServerHost()
            });

        using var second = new CrossProcessSingleton<ITestSingletonContract>(
            () => secondCreated = new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true });

        Assert.NotNull(secondCreated);
        Assert.Same(secondCreated, CrossProcessSingleton<ITestSingletonContract>.Service);
    }

    [Fact]
    public void FailedInitialization_DoesNotPoisonSingletonRegistry()
    {
        Assert.Throws<InvalidOperationException>(() => new CrossProcessSingleton<ITestSingletonContract>(
            () => throw new InvalidOperationException("factory-failure"),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = true }));

        Assert.False(Singleton.TryGet<CrossProcessSingleton<ITestSingletonContract>>(out _));
        Assert.Throws<InvalidOperationException>(() => CrossProcessSingleton<ITestSingletonContract>.Service);

        TestSingletonService? created = null;

        using var singleton = new CrossProcessSingleton<ITestSingletonContract>(
            () => created = new TestSingletonService(),
            new CrossProcessSingletonOptions { Mode = SingletonMode.InProcess, Overwrite = false });

        Assert.NotNull(created);
        Assert.Same(created, CrossProcessSingleton<ITestSingletonContract>.Service);
    }

    private interface ITestSingletonContract : ICrossProcessService
    {
        Task<string> EchoAsync(string value);
    }

    private sealed class TestSingletonService
        : CrossProcessServiceBase<ITestSingletonContract>, ITestSingletonContract, IDisposable
    {
        public int DisposeCount { get; private set; }

        public Task<string> EchoAsync(string value) => Task.FromResult(value);

        public void Dispose() => DisposeCount++;
    }

    private sealed class ThrowingDisposeSingletonService
        : CrossProcessServiceBase<ITestSingletonContract>, ITestSingletonContract, IDisposable
    {
        public int DisposeAttempts { get; private set; }

        public Task<string> EchoAsync(string value) => Task.FromResult(value);

        public void Dispose()
        {
            DisposeAttempts++;
            throw new InvalidOperationException("dispose-failure");
        }
    }

    private sealed class TrackingServerHost(Action onDispose) : ICrossProcessServerHost
    {
        public void Dispose() => onDispose();

        public ValueTask DisposeAsync()
        {
            onDispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingServerHost : ICrossProcessServerHost
    {
        public void Dispose() => throw new InvalidOperationException("host-dispose-failure");

        public ValueTask DisposeAsync() => ValueTask.FromException(new InvalidOperationException("host-dispose-failure"));
    }
}
