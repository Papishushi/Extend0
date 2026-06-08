using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.CrossProcess;
using Extend0.Metadata.CrossProcess.Contract;
using Extend0.Metadata.Schema;
using Extend0.Testing.Metadata.Internal;

namespace Extend0.Tests.Metadata.CrossProcess;

public sealed class MetaDBManagerSingletonTests
{
    [Fact]
    public void Singleton_ExposesOwnerService_AndRegistersUpgradeHandler()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using var singleton = new MetaDBManagerSingleton(
                factory: spec => MetadataTableHarness.CreateTable(spec!.Value),
                connectTimeoutMs: 500,
                overwrite: true);

            var service = CrossProcessSingleton<IMetaDBManagerRPCCompatible>.Service;
            var tableId = service.RegisterTable(
                new TableSpec(
                    "Users",
                    Path.Combine(tempRoot, "users.map"),
                    [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)]),
                createNow: true);

            Assert.True(CrossProcessSingleton<IMetaDBManagerRPCCompatible>.IsOwner);
            Assert.NotNull(RpcDispatchProxy<IMetaDBManagerRPCCompatible>.UpgradeHandler);
            Assert.Equal(tableId, service.TryGetIdByName("Users"));
            Assert.True(service.CloseStrict(tableId));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task UpgradeHandler_ReturnsFalse_WhenCurrentConfigurationCannotOverwrite()
    {
        using var singleton = new MetaDBManagerSingleton(connectTimeoutMs: 500, overwrite: false);

        var handler = RpcDispatchProxy<IMetaDBManagerRPCCompatible>.UpgradeHandler;
        var result = await handler!(new RemoteInvocationException("upgrade") { HResult = 426 });

        Assert.False(result);
        Assert.True(CrossProcessSingleton<IMetaDBManagerRPCCompatible>.IsOwner);
    }

    [Fact]
    public async Task UpgradeHandler_ReturnsTrue_WhenRecreationIsAllowed()
    {
        using var singleton = new MetaDBManagerSingleton(connectTimeoutMs: 500, overwrite: true);

        var handler = RpcDispatchProxy<IMetaDBManagerRPCCompatible>.UpgradeHandler;
        var result = await handler!(new RemoteInvocationException("upgrade") { HResult = 426 });

        Assert.True(result);
        Assert.True(CrossProcessSingleton<IMetaDBManagerRPCCompatible>.IsOwner);

        using var cleanup = new MetaDBManagerSingleton(connectTimeoutMs: 500, overwrite: true);
        Assert.NotNull(CrossProcessSingleton<IMetaDBManagerRPCCompatible>.Service);
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
