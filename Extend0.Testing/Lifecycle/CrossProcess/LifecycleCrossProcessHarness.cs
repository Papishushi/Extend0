using Extend0.Lifecycle.CrossProcess;
using Microsoft.Extensions.Logging.Abstractions;
using System.Reflection;
using System.Text.Json;

namespace Extend0.Testing.Lifecycle.CrossProcess;

public static class LifecycleCrossProcessHarness
{
    private static readonly MethodInfo NamedPipeTryResolveMethod =
        typeof(NamedPipeServer).GetMethod("TryResolveMethod", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new MissingMethodException(typeof(NamedPipeServer).FullName, "TryResolveMethod");

    public static CrossProcessProtocolDescriptor NamedPipeProtocolDescriptor => NamedPipeTransportProtocol.Descriptor;

    public static CrossProcessProtocolDescriptor UnixDomainSocketProtocolDescriptor => UnixDomainSocketTransportProtocol.Descriptor;

    public static CrossProcessProtocolDescriptor TcpSocketProtocolDescriptor => TcpSocketTransportProtocol.Descriptor;

    public static string BuildHelloLine(CrossProcessProtocolDescriptor descriptor) =>
        CrossProcessHandshake.BuildHelloLine(descriptor);

    public static bool TryValidateHelloLine(string helloLine, CrossProcessProtocolDescriptor expectedProtocol, out string error) =>
        CrossProcessHandshake.TryValidateHelloLine(helloLine, expectedProtocol, out error);

    public static Mutex CreateOwnedMutex(string baseName, bool preferGlobal, out bool createdNew, out bool isGlobal) =>
        CrossProcessUtils.CreateOwned(baseName, preferGlobal, out createdNew, out isGlobal, NullLogger.Instance);

    public static ICrossProcessServerHost CreateNamedPipeServer(string endpointName, object implementation, CancellationToken ct) =>
        new NamedPipeServer(endpointName, implementation, NullLogger<NamedPipeServer>.Instance, ct);

    public static ICrossProcessServerHost CreateUnixDomainSocketServer(string endpointName, object implementation, CancellationToken ct) =>
        new UnixDomainSocketServer(endpointName, implementation, NullLogger<UnixDomainSocketServer>.Instance, ct);

    public static ICrossProcessServerHost CreateTcpSocketServer(string endpointName, object implementation, CancellationToken ct) =>
        new TcpSocketServer(endpointName, implementation, NullLogger<TcpSocketServer>.Instance, ct);

    public static Task StopNamedPipeServerAsync(ICrossProcessServerHost host) =>
        host is NamedPipeServer server
            ? server.StopAsync()
            : throw new ArgumentException("Expected a NamedPipeServer host.", nameof(host));

    public static string BuildNamedPipeEndpointName(string logicalName) =>
        CrossProcessUtils.BuildPipeName(logicalName);

    public static string BuildUnixDomainSocketEndpointName(string logicalName) =>
        UnixDomainSocketEndpointName.BuildPath(logicalName);

    public static IClientTransport CreateBuiltInClientTransport(ClientTransportFactoryContext context) =>
        CrossProcessTransportFactory.CreateClientTransport(context);

    public static NamedPipeResolveResult ResolveNamedPipeMethod(string json, IReadOnlyDictionary<string, MethodInfo[]> methodsByName)
    {
        using var document = JsonDocument.Parse(json);
        object?[] args = [document.RootElement, methodsByName, null, default(JsonElement), string.Empty];
        var success = (bool)NamedPipeTryResolveMethod.Invoke(null, args)!;
        var target = (MethodInfo?)args[2];
        var argsElement = (JsonElement)args[3]!;
        var error = (string)args[4]!;

        return new NamedPipeResolveResult(
            success,
            target?.Name,
            target?.GetParameters().Length ?? -1,
            argsElement.ValueKind == JsonValueKind.Array ? argsElement.GetArrayLength() : -1,
            error);
    }

    public sealed record NamedPipeResolveResult(bool Success, string? MethodName, int TargetParameterCount, int ArgsLength, string Error);

    public static ICrossProcessServerHost CreateBuiltInOrCustomServerHost(
        ServerTransportFactoryContext context,
        Func<ServerTransportFactoryContext, ICrossProcessServerHost>? factory = null) =>
        CrossProcessTransportFactory.CreateServerHost(context, factory);

    public static CustomTransportOrchestrationResult RunCustomOrchestrationRoundTrip(
        string serviceName,
        CrossProcessProtocolDescriptor protocol)
    {
        ServerTransportFactoryContext? serverContext = null;
        ClientTransportFactoryContext? clientContext = null;

        using var owner = CrossProcessOrchestrator<ITestCrossProcessService>.GetOrStart(
            () => new TestCrossProcessService(),
            transportKind: protocol.TransportKind,
            protocolDescriptor: protocol,
            name: serviceName,
            clientTransportFactory: _ => new FakeClientTransport(protocol.TransportKind),
            serverTransportFactory: ctx =>
            {
                serverContext = ctx;
                return new FakeServerHost();
            });

        using var client = CrossProcessOrchestrator<ITestCrossProcessService>.GetOrStart(
            () => new TestCrossProcessService(),
            transportKind: protocol.TransportKind,
            protocolDescriptor: protocol,
            name: serviceName,
            clientTransportFactory: ctx =>
            {
                clientContext = ctx;
                return new FakeClientTransport(protocol.TransportKind);
            },
            serverTransportFactory: _ => new FakeServerHost());

        return new CustomTransportOrchestrationResult(
            owner.IsOwner,
            client.IsOwner,
            serverContext ?? throw new InvalidOperationException("Server context was not captured."),
            clientContext ?? throw new InvalidOperationException("Client context was not captured."));
    }

    public sealed record CustomTransportOrchestrationResult(
        bool OwnerIsOwner,
        bool ClientIsOwner,
        ServerTransportFactoryContext ServerContext,
        ClientTransportFactoryContext ClientContext);

    public static bool RunHostFailureRecoveryScenario(string serviceName, CrossProcessProtocolDescriptor protocol)
    {
        try
        {
            _ = CrossProcessOrchestrator<ITestCrossProcessService>.GetOrStart(
                () => new TestCrossProcessService(),
                transportKind: protocol.TransportKind,
                protocolDescriptor: protocol,
                name: serviceName,
                serverTransportFactory: _ => throw new InvalidOperationException("boom"));

            return false;
        }
        catch (InvalidOperationException ex) when (ex.Message == "boom")
        {
            using var recovered = CrossProcessOrchestrator<ITestCrossProcessService>.GetOrStart(
                () => new TestCrossProcessService(),
                transportKind: protocol.TransportKind,
                protocolDescriptor: protocol,
                name: serviceName,
                serverTransportFactory: _ => new FakeServerHost());

            return recovered.IsOwner;
        }
    }

    public static int RunHostFailureDisposesCreatedServiceScenario(string serviceName, CrossProcessProtocolDescriptor protocol)
    {
        var service = new DisposableTestCrossProcessService();

        try
        {
            _ = CrossProcessOrchestrator<ITestCrossProcessService>.GetOrStart(
                () => service,
                transportKind: protocol.TransportKind,
                protocolDescriptor: protocol,
                name: serviceName,
                serverTransportFactory: _ => throw new InvalidOperationException("boom"));

            return -1;
        }
        catch (InvalidOperationException ex) when (ex.Message == "boom")
        {
            return service.DisposeCount;
        }
    }

    public static int RunHostFailureDisposesAsyncCreatedServiceScenario(
        string serviceName,
        CrossProcessProtocolDescriptor protocol,
        bool throwOnDisposeAsync)
    {
        var service = new AsyncDisposableTestCrossProcessService(throwOnDisposeAsync);

        try
        {
            _ = CrossProcessOrchestrator<ITestCrossProcessService>.GetOrStart(
                () => service,
                transportKind: protocol.TransportKind,
                protocolDescriptor: protocol,
                name: serviceName,
                serverTransportFactory: _ => throw new InvalidOperationException("boom"));

            return -1;
        }
        catch (InvalidOperationException ex) when (ex.Message == "boom")
        {
            return service.DisposeAsyncCount;
        }
    }

    public interface ITestCrossProcessService : ICrossProcessService
    {
        Task<string> EchoAsync(string value);
    }

    public sealed class TestCrossProcessService : CrossProcessServiceBase<ITestCrossProcessService>, ITestCrossProcessService
    {
        public Task<string> EchoAsync(string value) => Task.FromResult(value);
    }

    private sealed class DisposableTestCrossProcessService : CrossProcessServiceBase<ITestCrossProcessService>, ITestCrossProcessService, IDisposable
    {
        public int DisposeCount { get; private set; }

        public Task<string> EchoAsync(string value) => Task.FromResult(value);

        public void Dispose() => DisposeCount++;
    }

    private sealed class AsyncDisposableTestCrossProcessService(bool throwOnDisposeAsync)
        : CrossProcessServiceBase<ITestCrossProcessService>, ITestCrossProcessService, IAsyncDisposable
    {
        public int DisposeAsyncCount { get; private set; }

        public Task<string> EchoAsync(string value) => Task.FromResult(value);

        public ValueTask DisposeAsync()
        {
            DisposeAsyncCount++;
            if (throwOnDisposeAsync)
                throw new InvalidOperationException("async-dispose-failed");

            return ValueTask.CompletedTask;
        }
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
