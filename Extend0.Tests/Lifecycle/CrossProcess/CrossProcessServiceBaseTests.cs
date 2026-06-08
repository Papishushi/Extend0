using Extend0.Lifecycle.CrossProcess;
using Microsoft.Extensions.Logging;
using System.IO.Pipes;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessServiceBaseTests
{
    [Fact]
    public async Task CanConnectAsync_DefaultProbe_ReturnsFalse_WhenEndpointNameIsMissing()
    {
        var service = new DefaultProbeTestService(endpointName: null);

        var canConnect = await service.CanConnectAsync();

        Assert.False(canConnect);
    }

    [Fact]
    public async Task CanConnectAsync_DefaultProbe_ReturnsTrue_WhenNamedPipeServerIsListening()
    {
        var pipeName = $"extend0-probe-{Guid.NewGuid():N}";
        using var server = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var waitServer = server.WaitForConnectionAsync();
        var service = new DefaultProbeTestService(pipeName);

        var canConnect = await service.CanConnectAsync();
        await waitServer;

        Assert.True(canConnect);
    }

    [Fact]
    public async Task CanConnectAsync_ReturnsFalse_WhenProbeIsCanceled()
    {
        var logger = new TestLogger();
        var service = new ProbeTestService(logger, static _ => throw new OperationCanceledException());

        var canConnect = await service.CanConnectAsync();

        Assert.False(canConnect);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Debug && entry.Message.Contains("timed out", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task CanConnectAsync_ReturnsFalse_WhenProbeThrows()
    {
        var logger = new TestLogger();
        var service = new ProbeTestService(logger, static _ => throw new InvalidOperationException("boom"));

        var canConnect = await service.CanConnectAsync();

        Assert.False(canConnect);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Message.Contains("Connectivity probe failed", StringComparison.Ordinal));
    }

    private interface IProbeServiceContract : ICrossProcessService
    {
        Task<string> EchoAsync(string value);
    }

    private sealed class ProbeTestService(ILogger logger, Func<CancellationToken, Task<bool>> probe)
        : CrossProcessServiceBase<IProbeServiceContract>(logger), IProbeServiceContract
    {
        private readonly Func<CancellationToken, Task<bool>> _probe = probe;

        public Task<string> EchoAsync(string value) => Task.FromResult(value);

        protected override string? EndpointName => "probe-endpoint";

        protected override TransportKind EndpointTransportKind => TransportKind.Custom;

        protected override Task<bool> ProbeConnectivityCoreAsync(CancellationToken ct) => _probe(ct);
    }

    private sealed class DefaultProbeTestService(string? endpointName)
        : CrossProcessServiceBase<IProbeServiceContract>, IProbeServiceContract
    {
        private readonly string? _endpointName = endpointName;

        public Task<string> EchoAsync(string value) => Task.FromResult(value);

        protected override string? PipeName => _endpointName;
    }

    private sealed class TestLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
