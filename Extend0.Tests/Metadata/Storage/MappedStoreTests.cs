using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Diagnostics;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Testing.Metadata.Storage;

namespace Extend0.Tests.Metadata.Storage;

[Collection(MappedStorageCollection.Name)]
public sealed class MappedStoreTests
{
    [Fact]
    public void MappedStore_CreatesLayout_AndCanReloadColumns()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "users.map");
            var spec = new TableSpec("Users", mapPath,
            [
                TableSpec.Helpers.Column("Name", 2, valueBytes: 64),
                TableSpec.Helpers.Column("Blob", 3, valueBytes: 128)
            ]);

            using (var store = MetadataStorageHarness.CreateMappedStore(spec))
            {
                Assert.Equal((uint)2, MetadataStorageHarness.GetMappedColumnCount(store));
                Assert.Equal(2u, MetadataStorageHarness.GetMappedColumnMeta(store, 0).InitialCapacity);
                Assert.Equal(3u, MetadataStorageHarness.GetMappedColumnMeta(store, 1).InitialCapacity);
            }

            Assert.True(MetadataStorageHarness.TryLoadMappedColumns(mapPath, out var loaded));
            Assert.Equal(2, loaded.Length);
            Assert.Equal(2u, loaded[0].InitialCapacity);
            Assert.Equal(3u, loaded[1].InitialCapacity);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_GetOrCreateCell_PopulatesDefaultCompositeKey_AndExposesColumnBlock()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "users.map");
            var spec = new TableSpec("Users", mapPath, [TableSpec.Helpers.Column("Name", 2, valueBytes: 64)]);

            using var store = MetadataStorageHarness.CreateMappedStore(spec);
            var meta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);
            var cell = store.GetOrCreateCell(0, 1, meta);

            Assert.True(cell.TryGetKeyRaw(out var key));
            Assert.StartsWith("Name:1", System.Text.Encoding.UTF8.GetString(key).TrimEnd('\0'), StringComparison.Ordinal);
            Assert.True(MetadataStorageHarness.TryGetMappedColumnBlock(store, 0, out var block));
            Assert.Equal(meta.Size.GetKeySize() + meta.Size.GetValueSize(), block.Stride);
            Assert.Equal(meta.Size.GetValueSize(), block.ValueSize);
            Assert.Equal(meta.Size.GetKeySize(), block.ValueOffset);
            Assert.True(store.TryGetCell(0, 1, out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_GetOrCreateCell_TruncatesCompositeKey_WhenKeySpaceIsTight()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "keys.map");
            var spec = new TableSpec("Keys", mapPath,
            [
                TableSpec.Helpers.Column("VeryLongColumnName", 128, valueBytes: 64, keyBytes: 16)
            ]);

            using var store = MetadataStorageHarness.CreateMappedStore(spec);

            var truncated = store.GetOrCreateCell(0, 99, MetadataStorageHarness.GetMappedColumnMeta(store, 0));
            Assert.True(truncated.TryGetKeyRaw(out var truncatedKey));
            Assert.Equal(16, truncatedKey.Length);
            Assert.Equal("VeryLongColu:99", System.Text.Encoding.UTF8.GetString(truncatedKey).TrimEnd('\0'));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_ValueOnlyPrimitiveColumns_MaterializeSuccessfully()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "cluster-primitives.map");
            var spec = new TableSpec("ClusterNodes", mapPath,
            [
                TableSpec.Helpers.Column<Guid>("node_id", capacity: 2, keyBytes: 0),
                TableSpec.Helpers.Column<int>("connection_count", capacity: 2, keyBytes: 0),
                TableSpec.Helpers.Column<long>("last_heartbeat_utc_ticks", capacity: 2, keyBytes: 0)
            ]);

            using var store = MetadataStorageHarness.CreateMappedStore(spec);

            var nodeId = store.GetOrCreateCell(0, 0, MetadataStorageHarness.GetMappedColumnMeta(store, 0));
            var connCount = store.GetOrCreateCell(1, 0, MetadataStorageHarness.GetMappedColumnMeta(store, 1));
            var heartbeat = store.GetOrCreateCell(2, 0, MetadataStorageHarness.GetMappedColumnMeta(store, 2));

            Assert.False(nodeId.TryGetKeyRaw(out _));
            Assert.True(nodeId.TryGetValueRaw(out var nodeIdRaw));
            Assert.Equal(16, nodeIdRaw.Length);

            Assert.False(connCount.TryGetKeyRaw(out _));
            Assert.True(connCount.TryGetValueRaw(out var connCountRaw));
            Assert.Equal(4, connCountRaw.Length);

            Assert.False(heartbeat.TryGetKeyRaw(out _));
            Assert.True(heartbeat.TryGetValueRaw(out var heartbeatRaw));
            Assert.Equal(8, heartbeatRaw.Length);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_TryGetCell_AndCapacity_ReturnFalse_ForOutOfRangeCoordinates()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "bounds.map");
            var spec = new TableSpec("Bounds", mapPath, [TableSpec.Helpers.Column("Name", 2, valueBytes: 64)]);

            using var store = MetadataStorageHarness.CreateMappedStore(spec);

            Assert.False(store.TryGetCell(1, 0, out _));
            Assert.False(store.TryGetCell(0, 2, out _));
            Assert.False(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 1, out _));
            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 0, out var capacity));
            Assert.Equal((uint)2, capacity);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_GrowCapacity_CoversNoOpSuccessAndMismatchBranches()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "users.map");
            var spec = new TableSpec("Users", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]);

            using var store = MetadataStorageHarness.CreateMappedStore(spec);
            var meta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);

            Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 0, meta, zeroInit: false));
            Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 3, meta, zeroInit: true));
            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 0, out var capacity));
            Assert.Equal((uint)3, capacity);
            Assert.True(store.TryGetCell(0, 2, out var grownCell));
            Assert.True(grownCell.TryGetValueRaw(out var raw));
            Assert.All(raw.ToArray(), static b => Assert.Equal(0, b));

            var wrongMeta = TableSpec.Helpers.Column("Wrong", 1, valueBytes: 128);
            Assert.Throws<InvalidOperationException>(() => MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 4, wrongMeta, zeroInit: true));
            Assert.False(MetadataStorageHarness.TryGrowMappedColumnTo(store, 4, 2, meta, zeroInit: false));
            Assert.False(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 4, out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_GrowCapacity_PreservesExistingContent_AfterReopen()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "persist-grow.map");
            var spec = new TableSpec("Users", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]);

            using (var store = MetadataStorageHarness.CreateMappedStore(spec))
            {
                var meta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);
                var first = store.GetOrCreateCell(0, 0, meta);
                Assert.True(first.TrySetKey("alpha"));
                Assert.True(first.TrySetValue("Alice"));

                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 3, meta, zeroInit: true));
                Assert.True(store.TryGetCell(0, 2, out var grown));
                Assert.True(grown.TryGetValueRaw(out var grownRaw));
                Assert.All(grownRaw.ToArray(), static b => Assert.Equal(0, b));
            }

            using var reopened = MetadataStorageHarness.CreateMappedStore(spec);
            Assert.True(reopened.TryGetCell(0, 0, out var persisted));
            Assert.True(persisted.TryGetKeyRaw(out var key));
            Assert.Equal("alpha", System.Text.Encoding.UTF8.GetString(key).TrimEnd('\0'));
            Assert.True(persisted.TryGetValueRaw(out var value));
            Assert.StartsWith("Alice", System.Text.Encoding.UTF8.GetString(value).TrimEnd('\0'), StringComparison.Ordinal);
            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(reopened, 0, out var reopenedCapacity));
            Assert.Equal((uint)3, reopenedCapacity);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_GrowCapacity_PersistsIndependentCapacities_ForMultipleColumns()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "multi-grow.map");
            var spec = new TableSpec("Users", mapPath,
            [
                TableSpec.Helpers.Column("Name", 1, valueBytes: 64),
                TableSpec.Helpers.Column("Blob", 2, valueBytes: 128)
            ]);

            using (var store = MetadataStorageHarness.CreateMappedStore(spec))
            {
                var nameMeta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);
                var blobMeta = MetadataStorageHarness.GetMappedColumnMeta(store, 1);

                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 3, nameMeta, zeroInit: true));
                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 1, 5, blobMeta, zeroInit: true));
                Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 0, out var nameCapacity));
                Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 1, out var blobCapacity));
                Assert.Equal((uint)3, nameCapacity);
                Assert.Equal((uint)5, blobCapacity);
            }

            Assert.True(MetadataStorageHarness.TryLoadMappedColumns(mapPath, out var loaded));
            Assert.Equal(2, loaded.Length);
            Assert.Equal((uint)3, loaded[0].InitialCapacity);
            Assert.Equal((uint)5, loaded[1].InitialCapacity);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_GrowCapacity_MultipleTimesOnSameColumn_PreservesExistingContent()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "repeat-grow.map");
            var spec = new TableSpec("Users", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]);

            using (var store = MetadataStorageHarness.CreateMappedStore(spec))
            {
                var meta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);
                var row0 = store.GetOrCreateCell(0, 0, meta);
                Assert.True(row0.TrySetKey("alpha"));
                Assert.True(row0.TrySetValue("Alice"));

                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 3, meta, zeroInit: true));

                var row2 = store.GetOrCreateCell(0, 2, meta);
                Assert.True(row2.TrySetKey("omega"));
                Assert.True(row2.TrySetValue("Olivia"));

                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 6, meta, zeroInit: true));
            }

            using var reopened = MetadataStorageHarness.CreateMappedStore(spec);
            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(reopened, 0, out var capacity));
            Assert.Equal((uint)6, capacity);

            Assert.True(reopened.TryGetCell(0, 0, out var first));
            Assert.True(first.TryGetKeyRaw(out var firstKey));
            Assert.Equal("alpha", System.Text.Encoding.UTF8.GetString(firstKey).TrimEnd('\0'));
            Assert.True(first.TryGetValueRaw(out var firstValue));
            Assert.StartsWith("Alice", System.Text.Encoding.UTF8.GetString(firstValue).TrimEnd('\0'), StringComparison.Ordinal);

            Assert.True(reopened.TryGetCell(0, 2, out var third));
            Assert.True(third.TryGetKeyRaw(out var thirdKey));
            Assert.Equal("omega", System.Text.Encoding.UTF8.GetString(thirdKey).TrimEnd('\0'));
            Assert.True(third.TryGetValueRaw(out var thirdValue));
            Assert.StartsWith("Olivia", System.Text.Encoding.UTF8.GetString(thirdValue).TrimEnd('\0'), StringComparison.Ordinal);

            Assert.True(reopened.TryGetCell(0, 5, out var zeroed));
            Assert.True(zeroed.TryGetValueRaw(out var zeroedValue));
            Assert.All(zeroedValue.ToArray(), static b => Assert.Equal(0, b));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task MappedStore_LoadColumnsAndCompactFallback_CoverFailureBranches()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var missing = Path.Combine(tempRoot, "missing.map");
            var garbage = Path.Combine(tempRoot, "garbage.map");
            await File.WriteAllBytesAsync(garbage, [1, 2, 3, 4, 5, 6, 7, 8]);

            Assert.False(MetadataStorageHarness.TryLoadMappedColumns(missing, out var missingColumns));
            Assert.Empty(missingColumns);
            Assert.False(MetadataStorageHarness.TryLoadMappedColumns(garbage, out var garbageColumns));
            Assert.Empty(garbageColumns);

            var mapPath = Path.Combine(tempRoot, "users.map");
            var spec = new TableSpec("Users", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]);

            using var store = MetadataStorageHarness.CreateMappedStore(spec);

            await Assert.ThrowsAsync<NotImplementedException>(() => MetadataStorageHarness.CompactMappedStore(store, strict: false));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_Create_TranslatesRealExclusiveFileLockToMetadataTableLockedException()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "locked-open.map");
            var spec = new TableSpec("LockedOpen", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]);
            File.WriteAllBytes(mapPath, [0]);

            using var exclusiveLock = new FileStream(mapPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);

            var ex = Assert.Throws<MetadataTableLockedException>(() => MetadataStorageHarness.CreateMappedStore(spec));

            Assert.IsAssignableFrom<IOException>(ex.InnerException);
            Assert.Contains("Locked", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Theory]
    [InlineData(unchecked((int)0x80070020))] // HR_SHARING_VIOLATION
    [InlineData(unchecked((int)0x80070021))] // HR_LOCK_VIOLATION
    [InlineData(unchecked((int)0x80070024))] // HR_SHARING_BUF_EXCEEDED
    [InlineData(unchecked((int)0x800704C8))] // HR_USER_MAPPED_FILE
    public void MappedStore_ThrowParsed_MapsKnownLockHResultsToMetadataTableLockedException(int hResult)
    {
        var io = new HResultIOException(hResult);

        var ex = Assert.Throws<MetadataTableLockedException>(() =>
            MetadataStorageHarness.InvokeMappedStoreThrowParsed(() => throw io));

        Assert.Same(io, ex.InnerException);
        Assert.Contains("Locked by another process", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MappedStore_ThrowParsed_MapsAccessDeniedToMetadataTableLockedException()
    {
        var io = new HResultIOException(unchecked((int)0x80070005)); // HR_ACCESS_DENIED

        var ex = Assert.Throws<MetadataTableLockedException>(() =>
            MetadataStorageHarness.InvokeMappedStoreThrowParsed(() => throw io));

        Assert.Same(io, ex.InnerException);
        Assert.Contains("Access denied", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MappedStore_ThrowParsed_RethrowsUnknownIOException()
    {
        var io = new HResultIOException(unchecked((int)0x80070057));

        var ex = Assert.Throws<HResultIOException>(() =>
            MetadataStorageHarness.InvokeMappedStoreThrowParsed(() => throw io));

        Assert.Same(io, ex);
    }

    [Fact]
    public void MappedStore_ThrowParsed_DoesNotWrapNonIOException()
    {
        var expected = new InvalidOperationException("boom");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            MetadataStorageHarness.InvokeMappedStoreThrowParsed(() => throw expected));

        Assert.Same(expected, ex);
    }

    [Fact]
    public void MappedStore_ThrowParsed_ThrowsOnNullAction()
    {
        Assert.Throws<ArgumentNullException>(() => MetadataStorageHarness.InvokeMappedStoreThrowParsed(null));
    }

    [Fact]
    public void MappedStore_ThrowParsed_AllowsSuccessfulAction()
    {
        var called = false;

        MetadataStorageHarness.InvokeMappedStoreThrowParsed(() => called = true);

        Assert.True(called);
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class HResultIOException : IOException
    {
        public HResultIOException(int hResult) =>
            HResult = hResult;
    }
}
