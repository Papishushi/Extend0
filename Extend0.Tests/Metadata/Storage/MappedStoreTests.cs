using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Diagnostics;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Testing.Metadata.Internal;
using Extend0.Testing.Metadata.Storage;
using System.Diagnostics;

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
    public void MappedStore_ChunkSizeRoundsInitialAndGrownCapacity()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "chunk-aligned.map");
            var spec = new TableSpec("ChunkAligned", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)])
            {
                Storage = TableStorageOptions.SingleFile(chunkSize: 256)
            };

            using var store = MetadataStorageHarness.CreateMappedStore(spec);
            var meta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);

            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 0, out var initialCapacity));
            Assert.Equal((uint)2, initialCapacity);

            Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 3, meta, zeroInit: true));
            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 0, out var grownCapacity));
            Assert.Equal((uint)4, grownCapacity);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task SegmentedMappedStore_UsesTableFolder_GrowsReopensAndCompactsChunks()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            const int chunkSize = 256;
            var tableDir = Path.Combine(tempRoot, "segmented-users");
            var spec = new TableSpec("SegmentedUsers", tableDir, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)])
            {
                Storage = TableStorageOptions.Chunked(chunkSize)
            };

            using (var store = MetadataStorageHarness.CreateSegmentedMappedStore(spec))
            {
                Assert.Equal((uint)1, MetadataStorageHarness.GetSegmentedColumnCount(store));
                Assert.True(File.Exists(Path.Combine(tableDir, "tablespec.json")));
                Assert.True(File.Exists(Path.Combine(tableDir, "manifest.json")));

                var meta = MetadataStorageHarness.GetSegmentedColumnMeta(store, 0);
                Assert.True(MetadataStorageHarness.TryGetSegmentedColumnCapacity(store, 0, out var initialCapacity));
                Assert.Equal((uint)2, initialCapacity);

                Assert.True(MetadataStorageHarness.TryGrowSegmentedColumnTo(store, 0, 5, meta, zeroInit: true));
                Assert.True(MetadataStorageHarness.TryGetSegmentedColumnCapacity(store, 0, out var grownCapacity));
                Assert.Equal((uint)6, grownCapacity);
                AssertChunkFilesHaveLength(Path.Combine(tableDir, "chunks"), chunkSize);

                var row4 = store.GetOrCreateCell(0, 4, meta);
                Assert.True(row4.TrySetKey("omega"));
                Assert.True(row4.TrySetValue("Olivia"));
            }

            using (var reopened = MetadataStorageHarness.CreateSegmentedMappedStore(spec))
            {
                Assert.True(MetadataStorageHarness.TryGetSegmentedColumnCapacity(reopened, 0, out var reopenedCapacity));
                Assert.Equal((uint)6, reopenedCapacity);
                AssertCellText(reopened, column: 0, row: 4, key: "omega", value: "Olivia");
                Assert.True(MetadataStorageHarness.TryLoadSegmentedColumns(tableDir, out var loaded));
                Assert.Equal((uint)6, loaded[0].InitialCapacity);
            }

            var compactDir = Path.Combine(tempRoot, "segmented-compact");
            var compactSpec = new TableSpec("SegmentedCompact", compactDir, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)])
            {
                Storage = TableStorageOptions.Chunked(chunkSize)
            };

            using var compactStore = MetadataStorageHarness.CreateSegmentedMappedStore(compactSpec);
            var compactMeta = MetadataStorageHarness.GetSegmentedColumnMeta(compactStore, 0);
            var row0 = compactStore.GetOrCreateCell(0, 0, compactMeta);
            Assert.True(row0.TrySetKey("alpha"));
            Assert.True(row0.TrySetValue("Alice"));
            Assert.True(MetadataStorageHarness.TryGrowSegmentedColumnTo(compactStore, 0, 5, compactMeta, zeroInit: true));
            Assert.Equal(3, Directory.GetFiles(Path.Combine(compactDir, "chunks"), "*.chk").Length);
            AssertChunkFilesHaveLength(Path.Combine(compactDir, "chunks"), chunkSize);

            await MetadataStorageHarness.CompactSegmentedStore(compactStore, strict: true);

            Assert.True(MetadataStorageHarness.TryGetSegmentedColumnCapacity(compactStore, 0, out var compactedCapacity));
            Assert.Equal((uint)2, compactedCapacity);
            Assert.Single(Directory.GetFiles(Path.Combine(compactDir, "chunks"), "*.chk"));
            AssertChunkFilesHaveLength(Path.Combine(compactDir, "chunks"), chunkSize);
            AssertCellText(compactStore, column: 0, row: 0, key: "alpha", value: "Alice");
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void SegmentedMappedStore_RejectsChunkSizeSmallerThanColumnEntry()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var spec = new TableSpec(
                "TooSmallChunk",
                Path.Combine(tempRoot, "too-small-chunk"),
                [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)])
            {
                Storage = TableStorageOptions.Chunked(chunkSize: 16)
            };

            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                MetadataStorageHarness.CreateSegmentedMappedStore(spec));

            Assert.Contains("Chunk size", ex.Message, StringComparison.OrdinalIgnoreCase);
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
    public async Task MappedStore_ReopenedAfterGrowth_UsesActualFileLength_ForFurtherGrowth()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "reopen-grow.map");
            var spec = new TableSpec("Users", mapPath,
            [
                TableSpec.Helpers.Column("Name", 1, valueBytes: 64),
                TableSpec.Helpers.Column("City", 1, valueBytes: 64)
            ]);

            using (var store = MetadataStorageHarness.CreateMappedStore(spec))
            {
                var nameMeta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);
                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 5, nameMeta, zeroInit: true));

                var row4 = store.GetOrCreateCell(0, 4, nameMeta);
                Assert.True(row4.TrySetKey("omega"));
                Assert.True(row4.TrySetValue("Olivia"));
            }

            using (var reopened = MetadataStorageHarness.CreateMappedStore(spec))
            {
                var cityMeta = MetadataStorageHarness.GetMappedColumnMeta(reopened, 1);
                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(reopened, 1, 4, cityMeta, zeroInit: true));

                AssertCellText(reopened, column: 0, row: 4, key: "omega", value: "Olivia");
                Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(reopened, 0, out var nameCapacity));
                Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(reopened, 1, out var cityCapacity));
                Assert.Equal((uint)5, nameCapacity);
                Assert.Equal((uint)4, cityCapacity);
            }
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task MappedStore_Compact_RewritesSlabsShrinksFileAndPreservesOverlappingSourceData()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "compact-overlap.map");
            var spec = new TableSpec("Users", mapPath,
            [
                TableSpec.Helpers.Column("Name", 1, valueBytes: 64),
                TableSpec.Helpers.Column("City", 2, valueBytes: 64)
            ]);

            using (var store = MetadataStorageHarness.CreateMappedStore(spec))
            {
                var nameMeta = MetadataStorageHarness.GetMappedColumnMeta(store, 0);
                var cityMeta = MetadataStorageHarness.GetMappedColumnMeta(store, 1);

                var firstName = store.GetOrCreateCell(0, 0, nameMeta);
                Assert.True(firstName.TrySetKey("alpha"));
                Assert.True(firstName.TrySetValue("Alice"));

                var city = store.GetOrCreateCell(1, 1, cityMeta);
                Assert.True(city.TrySetKey("city-1"));
                Assert.True(city.TrySetValue("Paris"));

                Assert.True(MetadataStorageHarness.TryGrowMappedColumnTo(store, 0, 5, nameMeta, zeroInit: true));

                var lastName = store.GetOrCreateCell(0, 4, nameMeta);
                Assert.True(lastName.TrySetKey("omega"));
                Assert.True(lastName.TrySetValue("Olivia"));

                var lengthBefore = new FileInfo(mapPath).Length;

                await MetadataStorageHarness.CompactMappedStore(store, strict: true);

                var lengthAfter = new FileInfo(mapPath).Length;
                Assert.True(lengthAfter < lengthBefore);
                Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 0, out var nameCapacity));
                Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(store, 1, out var cityCapacity));
                Assert.Equal((uint)5, nameCapacity);
                Assert.Equal((uint)2, cityCapacity);

                AssertCellText(store, column: 0, row: 0, key: "alpha", value: "Alice");
                AssertCellText(store, column: 0, row: 4, key: "omega", value: "Olivia");
                AssertCellText(store, column: 1, row: 1, key: "city-1", value: "Paris");
            }

            using var reopened = MetadataStorageHarness.CreateMappedStore(spec);
            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(reopened, 0, out var reopenedNameCapacity));
            Assert.True(MetadataStorageHarness.TryGetMappedColumnCapacity(reopened, 1, out var reopenedCityCapacity));
            Assert.Equal((uint)5, reopenedNameCapacity);
            Assert.Equal((uint)2, reopenedCityCapacity);
            AssertCellText(reopened, column: 0, row: 0, key: "alpha", value: "Alice");
            AssertCellText(reopened, column: 0, row: 4, key: "omega", value: "Olivia");
            AssertCellText(reopened, column: 1, row: 1, key: "city-1", value: "Paris");
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task MappedStore_LoadColumnsAndCompactCancellation_CoverFailureBranches()
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
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                MetadataStorageHarness.CompactMappedStore(store, strict: false, cts.Token));
            await MetadataStorageHarness.CompactMappedStore(store, strict: false);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedStore_Create_TranslatesPortableStorageLeaseContentionToMetadataTableLockedException()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "locked-open.map");
            var spec = new TableSpec("LockedOpen", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]);
            File.WriteAllBytes(mapPath, [0]);

            using var exclusiveLock = MetadataStorageHarness.AcquireStorageLease(mapPath);

            var ex = Assert.Throws<MetadataTableLockedException>(() => MetadataStorageHarness.CreateMappedStore(spec));

            Assert.IsAssignableFrom<IOException>(ex.InnerException);
            Assert.Contains("Locked", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task MappedStore_CrossProcessLease_BlocksOpenAndDeleteUntilOwnerExits()
    {
        var tempRoot = CreateTempDirectory();
        Process? owner = null;
        try
        {
            var mapPath = Path.Combine(tempRoot, "cross-process.map");
            var readyPath = Path.Combine(tempRoot, "owner.ready");
            var releasePath = Path.Combine(tempRoot, "owner.release");
            var spec = new TableSpec("CrossProcessLease", mapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)]);

            MetadataStorageHarness.CreateMappedStore(spec).Dispose();

            owner = StartMappedStoreOwner(mapPath, readyPath, releasePath);
            await WaitForFileOrProcessExit(owner, readyPath, TimeSpan.FromSeconds(15));

            var openFailure = Assert.Throws<MetadataTableLockedException>(() => MetadataStorageHarness.CreateMappedStore(spec));
            Assert.IsAssignableFrom<IOException>(openFailure.InnerException);

            var deleted = await MetaDBManagerHelpersHarness.TryDeleteWithRetries(mapPath, attempts: 2);
            Assert.False(deleted);
            Assert.True(File.Exists(mapPath));

            File.WriteAllText(releasePath, "release");
            using var exitTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await owner.WaitForExitAsync(exitTimeout.Token);
            Assert.Equal(0, owner.ExitCode);

            using var reopened = MetadataStorageHarness.CreateMappedStore(spec);
        }
        finally
        {
            if (owner is { HasExited: false })
            {
                owner.Kill(entireProcessTree: true);
                await owner.WaitForExitAsync();
            }

            owner?.Dispose();
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static Process StartMappedStoreOwner(string mapPath, string readyPath, string releasePath)
    {
        var hostAssembly = Path.Combine(
            AppContext.BaseDirectory,
            "process-host",
            "Extend0.TestProcessHost.dll");
        Assert.True(File.Exists(hostAssembly), $"Cross-process test host was not found at '{hostAssembly}'.");

        var startInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false
        };
        startInfo.ArgumentList.Add(hostAssembly);
        startInfo.ArgumentList.Add("hold-mapped-store");
        startInfo.ArgumentList.Add(mapPath);
        startInfo.ArgumentList.Add(readyPath);
        startInfo.ArgumentList.Add(releasePath);

        return Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the cross-process lease test host.");
    }

    private static async Task WaitForFileOrProcessExit(Process process, string path, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!File.Exists(path) && !process.HasExited && DateTime.UtcNow < deadline)
            await Task.Delay(20);

        if (File.Exists(path))
            return;

        var standardOutput = await process.StandardOutput.ReadToEndAsync();
        var standardError = await process.StandardError.ReadToEndAsync();
        throw new InvalidOperationException(
            $"Cross-process lease host did not become ready. ExitCode={(process.HasExited ? process.ExitCode : -1)}; stdout={standardOutput}; stderr={standardError}");
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

    private static void AssertCellText(ICellStore store, uint column, uint row, string key, string value)
    {
        Assert.True(store.TryGetCell(column, row, out var cell));
        Assert.True(cell.TryGetKeyRaw(out var actualKey));
        Assert.Equal(key, System.Text.Encoding.UTF8.GetString(actualKey).TrimEnd('\0'));
        Assert.True(cell.TryGetValueRaw(out var actualValue));
        Assert.StartsWith(value, System.Text.Encoding.UTF8.GetString(actualValue).TrimEnd('\0'), StringComparison.Ordinal);
    }

    private static void AssertChunkFilesHaveLength(string chunksDirectory, long expectedLength)
    {
        var files = Directory.GetFiles(chunksDirectory, "*.chk");
        Assert.NotEmpty(files);
        Assert.All(files, path => Assert.Equal(expectedLength, new FileInfo(path).Length));
    }

    private sealed class HResultIOException : IOException
    {
        public HResultIOException(int hResult) =>
            HResult = hResult;
    }
}
