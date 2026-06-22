using Extend0.Lifecycle.Assurance;
using Extend0.Metadata;
using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Testing.Metadata.Internal;

namespace Extend0.Tests.Metadata;

public sealed class MetaDBTests
{
    [Fact]
    public void CreateManager_ReturnsPublicManagerContract()
    {
        using var manager = MetaDB.CreateManager();

        Assert.NotNull(manager);
        Assert.IsAssignableFrom<IMetaDBManager>(manager);
    }

    [Fact]
    public void RegisterTable_WithProtectedStoragePolicy_FailsClosedWithoutEvidence()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using var manager = MetaDB.CreateManager();
            var spec = new TableSpec(
                "Protected",
                Path.Combine(tempRoot, "protected.meta"),
                [TableSpec.Helpers.Column<int>("Id", 4)])
            {
                Protection = StorageProtectionPolicy.Require(StorageProtectionLevel.ProviderAttestedEncrypted)
            };

            var error = Assert.Throws<InvalidOperationException>(() => manager.RegisterTable(spec, createNow: true));

            Assert.Contains("Protected storage policy is not satisfied", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void RegisterTable_WithProtectedStoragePolicy_AllowsVerifiedManifest()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            StorageProtectionVerifier.SaveManifest(
                Path.Combine(tempRoot, StorageProtectionVerifier.ManifestFileName),
                StorageProtectionManifest.Create(
                    "test-provider",
                    "volume-1",
                    StorageProtectionLevel.ProviderAttestedEncrypted,
                    tempRoot));

            using var manager = MetaDB.CreateManager();
            var spec = new TableSpec(
                "Protected",
                Path.Combine(tempRoot, "protected.meta"),
                [TableSpec.Helpers.Column<int>("Id", 4)])
            {
                Protection = StorageProtectionPolicy.Require(
                    StorageProtectionLevel.ProviderAttestedEncrypted,
                    "test-provider",
                    "volume-1")
            };

            var id = manager.RegisterTable(spec, createNow: true);

            Assert.True(manager.TryGetManaged(id, out var table));
            Assert.NotNull(table);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void RegisterTable_WithStorageContinuityPolicy_FailsClosedWithoutEvidence()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using var manager = MetaDB.CreateManager();
            var spec = new TableSpec(
                "Transferable",
                Path.Combine(tempRoot, "transferable.meta"),
                [TableSpec.Helpers.Column<int>("Id", 4)])
            {
                Continuity = StorageContinuityPolicy.Require(StorageContinuityLevel.SharedBackingStore)
            };

            var error = Assert.Throws<InvalidOperationException>(() => manager.RegisterTable(spec, createNow: true));

            Assert.Contains("Storage continuity policy is not satisfied", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void RegisterTable_WithStorageContinuityPolicy_AllowsSharedBackingEvidence()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            StorageContinuityVerifier.SaveManifest(
                Path.Combine(tempRoot, StorageContinuityVerifier.ManifestFileName),
                StorageContinuityManifest.Create(
                    "test-continuity-provider",
                    "shared-volume-1",
                    StorageContinuityLevel.SharedBackingStore,
                    tempRoot));

            using var manager = MetaDB.CreateManager();
            var spec = new TableSpec(
                "Transferable",
                Path.Combine(tempRoot, "transferable.meta"),
                [TableSpec.Helpers.Column<int>("Id", 4)])
            {
                Continuity = StorageContinuityPolicy.Require(
                    StorageContinuityLevel.SharedBackingStore,
                    "test-continuity-provider",
                    "shared-volume-1")
            };

            var id = manager.RegisterTable(spec, createNow: true);

            Assert.True(manager.TryGetManaged(id, out var table));
            Assert.NotNull(table);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void RegisterTable_WithHardwareAttestationPolicy_FailsClosedWithoutEvidence()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using var manager = MetaDB.CreateManager();
            var spec = new TableSpec(
                "Attested",
                Path.Combine(tempRoot, "attested.meta"),
                [TableSpec.Helpers.Column<int>("Id", 4)])
            {
                Attestation = HardwareAttestationPolicy.Require(HardwareAttestationLevel.ProviderAttested)
            };

            var error = Assert.Throws<InvalidOperationException>(() => manager.RegisterTable(spec, createNow: true));

            Assert.Contains("Hardware attestation policy is not satisfied", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void RegisterTable_WithHardwareAttestationPolicy_AllowsMatchingEvidence()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            HardwareAttestationVerifier.SaveManifest(
                Path.Combine(tempRoot, HardwareAttestationVerifier.ManifestFileName),
                HardwareAttestationManifest.Create(
                    "sgx-provider",
                    "quote-1",
                    HardwareAttestationTechnology.IntelSgx,
                    HardwareAttestationLevel.RemoteAttested,
                    tempRoot,
                    measurement: "mrenclave:abc"));

            using var manager = MetaDB.CreateManager();
            var spec = new TableSpec(
                "Attested",
                Path.Combine(tempRoot, "attested.meta"),
                [TableSpec.Helpers.Column<int>("Id", 4)])
            {
                Attestation = HardwareAttestationPolicy.Require(
                    HardwareAttestationLevel.RemoteAttested,
                    HardwareAttestationTechnology.IntelSgx,
                    "sgx-provider",
                    "quote-1",
                    "mrenclave:abc")
            };

            var id = manager.RegisterTable(spec, createNow: true);

            Assert.True(manager.TryGetManaged(id, out var table));
            Assert.NotNull(table);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void CreateSingleton_ReturnsSingletonAccessSurface()
    {
        using var singleton = MetaDB.CreateSingleton(overwrite: true, connectTimeoutMs: 500);

        Assert.NotNull(singleton);
        Assert.IsType<MetaDBManagerSingleton>(singleton);
    }

    [Fact]
    public void Open_UsesSpecFile_AndCanForceRelocation()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "users.meta");
            var spec = new TableSpec(
                "Users",
                Path.Combine(tempRoot, "original-location.meta"),
                [TableSpec.Helpers.Column("Value", 2, valueBytes: 16)]);
            spec.SaveToFile(inputMapPath + ".tablespec.json");

            TableSpec? seenSpec = null;
            using (var manager = MetaDB.CreateManager(factory: loadedSpec =>
            {
                seenSpec = loadedSpec;
                return MetadataTableHarness.CreateTable(loadedSpec!.Value);
            }))
            {
                var first = manager.Open(inputMapPath, forceRelocation: false);
                Assert.Equal(spec.MapPath, seenSpec!.Value.MapPath);
                Assert.Equal(spec.MapPath, first.Table.Spec.MapPath);
                Assert.True(manager.CloseStrict(first.Id));
            }

            using var relocatedManager = MetaDB.CreateManager(factory: loadedSpec =>
            {
                seenSpec = loadedSpec;
                return MetadataTableHarness.CreateTable(loadedSpec!.Value);
            });

            var relocated = relocatedManager.Open(inputMapPath, forceRelocation: true);
            Assert.Equal(inputMapPath, seenSpec!.Value.MapPath);
            Assert.Equal(inputMapPath, relocated.Table.Spec.MapPath);
            Assert.True(relocatedManager.CloseStrict(relocated.Id));
            Assert.False(relocatedManager.TryGetIdByName("Users", out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void Open_RollsBackRegistration_WhenFactoryThrows()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "broken.meta");
            new TableSpec("Broken", inputMapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)])
                .SaveToFile(inputMapPath + ".tablespec.json");

            using var manager = MetaDB.CreateManager(factory: _ => throw new InvalidOperationException("boom"));

            var ex = Assert.Throws<InvalidOperationException>(() => manager.Open(inputMapPath));

            Assert.Equal("boom", ex.Message);
            Assert.False(manager.TryGetIdByName("Broken", out _));
            Assert.False(manager.CloseStrict("Broken"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void WithTableEphemeral_FromMapPath_ClosesRegistrationAfterCallback()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "ephemeral.meta");
            new TableSpec("EphemeralMap", inputMapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)])
                .SaveToFile(inputMapPath + ".tablespec.json");

            using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));

            Guid openedId = Guid.Empty;
            var nameSeenInsideScope = false;
            var result = manager.WithTableEphemeral(inputMapPath, table =>
            {
                nameSeenInsideScope = manager.TryGetIdByName("EphemeralMap", out openedId);
                return table.Spec.Name;
            });

            Assert.True(nameSeenInsideScope);
            Assert.NotEqual(Guid.Empty, openedId);
            Assert.Equal("EphemeralMap", result);
            Assert.False(manager.TryGetIdByName("EphemeralMap", out _));
            Assert.False(manager.CloseStrict(openedId));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void WithTableEphemeral_FromMapPath_CleansUpWhenCallbackThrows()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "ephemeral-throw.meta");
            new TableSpec("EphemeralThrow", inputMapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)])
                .SaveToFile(inputMapPath + ".tablespec.json");

            using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));

            var ex = Assert.Throws<InvalidOperationException>(() => manager.WithTableEphemeral(inputMapPath, _ => throw new InvalidOperationException("boom")));

            Assert.Equal("boom", ex.Message);
            Assert.False(manager.TryGetIdByName("EphemeralThrow", out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void WithTableEphemeral_FromSpec_ActionAndResult_CleanUpRegistration()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));
            var spec = new TableSpec("EphemeralSpecSync", Path.Combine(tempRoot, "ephemeral-spec-sync.map"), [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)]);

            Guid actionId = Guid.Empty;
            manager.WithTableEphemeral(spec, (id, table) =>
            {
                actionId = id;
                Assert.Equal("EphemeralSpecSync", table.Spec.Name);
                Assert.True(manager.TryGetIdByName("EphemeralSpecSync", out var seenId));
                Assert.Equal(id, seenId);
            }, createNow: true, deleteNow: false, throwIfDeleteFails: true);

            Guid resultId = Guid.Empty;
            var logicalRows = manager.WithTableEphemeral(spec, (id, table) =>
            {
                resultId = id;
                return table.GetLogicalRowCount();
            }, createNow: true, deleteNow: false, throwIfDeleteFails: true);

            Assert.NotEqual(Guid.Empty, actionId);
            Assert.NotEqual(Guid.Empty, resultId);
            Assert.Equal(0u, logicalRows);
            Assert.False(manager.TryGetIdByName("EphemeralSpecSync", out _));
            Assert.False(manager.CloseStrict(actionId));
            Assert.False(manager.CloseStrict(resultId));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task WithTableEphemeralAsync_FromSpec_CleansUpAfterAwaitedCallback()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "ephemeral-spec.map");
        using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));
            var spec = new TableSpec("EphemeralSpec", mapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)]);
        Guid openedId = Guid.Empty;

            var logicalRows = await manager.WithTableEphemeralAsync(spec, async (id, table) =>
            {
                openedId = id;
                await Task.Yield();
                return table.GetLogicalRowCount();
            }, createNow: true, deleteNow: false, throwIfDeleteFails: true);

            Assert.NotEqual(Guid.Empty, openedId);
            Assert.Equal(0u, logicalRows);
            Assert.False(manager.TryGetIdByName("EphemeralSpec", out _));
            Assert.False(manager.CloseStrict(openedId));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task WithTableEphemeralAsync_FromSpec_Action_CleansUpAfterAwaitedCallback()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "ephemeral-spec-action.map");
            using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));
            var spec = new TableSpec("EphemeralSpecAction", mapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)]);
            Guid openedId = Guid.Empty;

            await manager.WithTableEphemeralAsync(spec, async (id, table) =>
            {
                openedId = id;
                await Task.Yield();
                Assert.Equal("EphemeralSpecAction", table.Spec.Name);
                Assert.True(manager.TryGetIdByName("EphemeralSpecAction", out var seenId));
                Assert.Equal(id, seenId);
            }, createNow: true, deleteNow: false, throwIfDeleteFails: true);

            Assert.NotEqual(Guid.Empty, openedId);
            Assert.False(manager.TryGetIdByName("EphemeralSpecAction", out _));
            Assert.False(manager.CloseStrict(openedId));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task WithTableEphemeralAsync_FromMapPath_CleansUpAfterAwaitedCallback()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "ephemeral-async.meta");
            new TableSpec("EphemeralAsyncMap", inputMapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)])
                .SaveToFile(inputMapPath + ".tablespec.json");

            using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));

            var nameSeenInside = false;
            var result = await manager.WithTableEphemeralAsync(inputMapPath, async table =>
            {
                await Task.Yield();
                nameSeenInside = manager.TryGetIdByName("EphemeralAsyncMap", out _);
                return table.Spec.Name;
            });

            Assert.True(nameSeenInside);
            Assert.Equal("EphemeralAsyncMap", result);
            Assert.False(manager.TryGetIdByName("EphemeralAsyncMap", out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task WithTableEphemeralAsync_FromMapPath_CleansUpWhenCallbackThrows()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "ephemeral-async-throw.meta");
            new TableSpec("EphemeralAsyncThrow", inputMapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)])
                .SaveToFile(inputMapPath + ".tablespec.json");

            using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                manager.WithTableEphemeralAsync(inputMapPath, _ => throw new InvalidOperationException("async-boom")));

            Assert.Equal("async-boom", ex.Message);
            Assert.False(manager.TryGetIdByName("EphemeralAsyncThrow", out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void ManagerLookupAndGlobalIndexHelpers_CoverValidationAndHitPaths()
    {
        using var handle = MetaDBManagerHarness.CreateManager(factory: spec => MetadataTableHarness.CreateTable(spec!.Value));
        var manager = handle.Contract;
        var spec = new TableSpec("Users", "users.map", [TableSpec.Helpers.Column("Value", 2, valueBytes: 16)]);
        var id = manager.RegisterTable(spec, createNow: false);
        var key = new byte[] { 0x41, 0x42 };

        Assert.False(manager.TryGetIdByName("", out var emptyId));
        Assert.Equal(Guid.Empty, emptyId);
        Assert.False(manager.TryGetIdByName("   ", out var wsId));
        Assert.Equal(Guid.Empty, wsId);
        Assert.False(manager.TryGetTableIfCreated("Users", out _));
        Assert.True(manager.TryGetIdByName("Users", out var resolvedId));
        Assert.Equal(id, resolvedId);

        Assert.False(handle.TryFindGlobal(key, out _));
        Assert.False(handle.TryFindGlobal(key.AsSpan(), out _));

        handle.SeedGlobalKey(id, "Users", key, col: 1, row: 7);

        Assert.True(handle.TryFindGlobal(key, out var arrayHit));
        Assert.Equal(("Users", 1u, 7u), arrayHit);
        Assert.True(handle.TryFindGlobal(key.AsSpan(), out var spanHit));
        Assert.Equal(("Users", 1u, 7u), spanHit);

        Assert.True(manager.CloseStrict(id));
        Assert.False(manager.CloseStrict(id));
        Assert.False(manager.CloseStrict("missing"));
    }

    [Fact]
    public void Open_RejectsWhitespaceAndMissingSpecFile()
    {
        using var manager = MetaDB.CreateManager(factory: spec => MetadataTableHarness.CreateTable(spec!.Value));
        var tempRoot = CreateTempDirectory();
        try
        {
            var missingMapPath = Path.Combine(tempRoot, "missing.meta");

            Assert.Throws<ArgumentException>(() => manager.Open(" "));
            Assert.Throws<FileNotFoundException>(() => manager.Open(missingMapPath));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void Open_AcceptsDirectSpecPath_AndDerivesMapFilePath()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "direct.meta");
            var specPath = mapPath + ".tablespec.json";
            TableSpec? seenSpec = null;

            new TableSpec("DirectSpec", "ignored-original.meta", [TableSpec.Helpers.Column("Value", 1, valueBytes: 16)])
                .SaveToFile(specPath);

            using var manager = MetaDB.CreateManager(factory: spec =>
            {
                seenSpec = spec;
                return MetadataTableHarness.CreateTable(spec!.Value);
            });

            var opened = manager.Open(specPath, forceRelocation: true);

            Assert.Equal(mapPath, seenSpec!.Value.MapPath);
            Assert.Equal(mapPath, opened.Table.Spec.MapPath);
            Assert.True(manager.CloseStrict(opened.Id));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void RegisterTable_StringOverload_AndGetOrCreate_ValidateArguments()
    {
        using var manager = MetaDB.CreateManager(factory: spec => MetadataTableHarness.CreateTable(spec!.Value));

        var id = manager.RegisterTable("ByArgs", "byargs.map", TableSpec.Helpers.Column("Value", 1, valueBytes: 16));

        Assert.NotEqual(Guid.Empty, id);
        Assert.Throws<ArgumentException>(() => manager.GetOrCreate(Guid.Empty));
        Assert.Throws<KeyNotFoundException>(() => manager.GetOrCreate(Guid.NewGuid()));
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
