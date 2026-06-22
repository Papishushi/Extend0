using Extend0.Lifecycle.CrossProcess;
using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessOrchestratorTests
{
    [Fact]
    public void GetOrStart_PropagatesSameCustomProtocolToOwnerAndClientFactories()
    {
        var serviceName = $"test-{Guid.NewGuid():N}";
        var protocol = new CrossProcessProtocolDescriptor(TransportKind.Custom, "custom-wire", 3);
        var result = LifecycleCrossProcessHarness.RunCustomOrchestrationRoundTrip(serviceName, protocol);

        Assert.True(result.OwnerIsOwner);
        Assert.False(result.ClientIsOwner);
        Assert.Equal(protocol, result.ServerContext.Protocol);
        Assert.Equal(protocol, result.ClientContext.Protocol);
        Assert.Equal(result.ServerContext.EndpointName, result.ClientContext.EndpointName);
        Assert.Equal(TransportKind.Custom, result.ServerContext.TransportKind);
        Assert.Equal(TransportKind.Custom, result.ClientContext.TransportKind);
    }

    [Fact]
    public async Task GetOrStart_ConfiguresOwnerServiceInfoWithResolvedRuntimeEndpoint()
    {
        var serviceName = $"runtime-endpoint-{Guid.NewGuid():N}";
        var result = await LifecycleCrossProcessHarness.RunNamedPipeOwnerRuntimeEndpointScenario(serviceName);
        var expectedEndpoint = CrossProcessTransportFactory.ResolveEndpointNameFor<LifecycleCrossProcessHarness.ITestCrossProcessService>(
            serviceName,
            TransportKind.NamedPipe);

        Assert.True(result.OwnerIsOwner);
        Assert.Equal(expectedEndpoint, result.EndpointName);
        Assert.Equal(".", result.EndpointServerName);
        Assert.Equal(TransportKind.NamedPipe, result.TransportKind);
        Assert.Equal(LifecycleCrossProcessHarness.BuildTestServiceOwnershipName(serviceName), result.OwnershipName);
        Assert.Equal("OSMutex", result.CoordinationKind);
        Assert.False(string.IsNullOrWhiteSpace(result.CoordinationScope));
        Assert.True(result.LeaseIsExclusive);
        Assert.True(result.LeaseIsActive);
        Assert.True(result.CanConnect);
    }

    [Fact]
    public void GetOrStart_WhenServerFactoryThrows_ReleasesState_AndAllowsRetry()
    {
        var serviceName = $"recover-{Guid.NewGuid():N}";
        var protocol = new CrossProcessProtocolDescriptor(TransportKind.Custom, "recover-wire", 1);

        var recovered = LifecycleCrossProcessHarness.RunHostFailureRecoveryScenario(serviceName, protocol);

        Assert.True(recovered);
    }

    [Fact]
    public void GetOrStart_WhenServerFactoryThrows_DisposesCreatedOwnerService()
    {
        var serviceName = $"dispose-created-{Guid.NewGuid():N}";
        var protocol = new CrossProcessProtocolDescriptor(TransportKind.Custom, "dispose-created-wire", 1);

        var disposeCount = LifecycleCrossProcessHarness.RunHostFailureDisposesCreatedServiceScenario(serviceName, protocol);

        Assert.Equal(1, disposeCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GetOrStart_WhenServerFactoryThrows_DisposesAsyncCreatedOwnerService_BestEffort(bool throwOnDisposeAsync)
    {
        var serviceName = $"dispose-async-created-{throwOnDisposeAsync}-{Guid.NewGuid():N}";
        var protocol = new CrossProcessProtocolDescriptor(TransportKind.Custom, "dispose-async-created-wire", 1);

        var disposeAsyncCount = LifecycleCrossProcessHarness.RunHostFailureDisposesAsyncCreatedServiceScenario(
            serviceName,
            protocol,
            throwOnDisposeAsync);

        Assert.Equal(1, disposeAsyncCount);
    }
}
