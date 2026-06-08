using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Schema;
using Extend0.Tests.Metadata.Storage;
using Extend0.Testing.Metadata.Internal;

namespace Extend0.Tests.Metadata.Internal;

[Collection(MappedStorageCollection.Name)]
public sealed class MetadataTableTests
{
    [Fact]
    public void Constructor_RejectsTablesWithoutColumns()
    {
        var spec = new TableSpec("Empty", MapPath: null!, []);

        var ex = Assert.Throws<ArgumentException>(() => MetadataTableHarness.CreateTable(spec));

        Assert.Contains("At least one column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InMemoryTable_CanRoundTripCells_ByColumnNameAndIndex()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 2, valueBytes: 64),
            TableSpec.Helpers.Column("City", 2, valueBytes: 64));

        var created = table.GetOrCreateCell("Name", 1);
        Assert.True(created.TrySetKey("user-1"));
        Assert.True(created.TrySetValue("Alice"));

        var byName = table.TryGetCell("Name", 1, out var nameCell);
        var byIndex = table.TryGetCell(0, 1, out var indexCell);

        Assert.True(byName);
        Assert.True(byIndex);
        Assert.Equal(created, nameCell);
        Assert.Equal(created, indexCell);
        Assert.Contains("Name", table.GetColumnNames());
        Assert.Contains("City", table.GetColumnNames());
    }

    [Fact]
    public void Lookups_ReturnFalse_WhenIndexesHaveNotBeenRebuilt()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 2, valueBytes: 64));

        var first = table.GetOrCreateCell(0, 0);
        var second = table.GetOrCreateCell(0, 1);
        Assert.True(first.TrySetKey("alpha"));
        Assert.True(first.TrySetValue("Alice"));
        Assert.True(second.TrySetKey("beta"));
        Assert.True(second.TrySetValue("Bob"));

        Assert.False(table.TryFindRowByKey(0, "alpha"u8.ToArray(), out _));
        Assert.False(table.TryFindRowByKey(0, "beta"u8, out _));
        Assert.False(table.TryFindCellByKey(0, "alpha"u8, out _));
        Assert.False(table.TryFindGlobal("beta"u8.ToArray(), out _));
        Assert.False(table.TryFindGlobal("alpha"u8, out _));
    }

    [Fact]
    public async Task RebuildIndexes_StrictHonorsCancellation()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => table.RebuildIndexes(strict: true, cancellationToken: cts.Token));
    }

    [Fact]
    public async Task RebuildIndexes_Strict_FailsWhenKeyValueCellHasValueButNoKey()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 2, keyBytes: 16, valueBytes: 64));

        MetadataTableHarness.WriteValueBytes(table, column: 0, row: 0, bytes: "orphan-value"u8);
        MetadataTableHarness.ClearKeyBytes(table, column: 0, row: 0);
        Assert.True(table.TryGetCell(0, 0, out var noKeyCell));
        Assert.False(noKeyCell.HasKeyRaw());
        Assert.True(noKeyCell.HasAnyValueRaw());

        var ex = await Assert.ThrowsAsync<AggregateException>(() => table.RebuildIndexes(strict: true, cancellationToken: default));

        Assert.Contains(ex.InnerExceptions, inner =>
            inner is InvalidOperationException ioe &&
            ioe.Message.Contains("has no key", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task RebuildIndexes_EnablesKeyLookups()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 2, valueBytes: 64));

        var first = table.GetOrCreateCell(0, 0);
        var second = table.GetOrCreateCell(0, 1);
        Assert.True(first.TrySetKey("alpha"));
        Assert.True(first.TrySetValue("Alice"));
        Assert.True(second.TrySetKey("beta"));
        Assert.True(second.TrySetValue("Bob"));

        await table.RebuildIndexes(strict: false, cancellationToken: default);

        Assert.True(table.TryFindRowByKey(0, "alpha"u8.ToArray(), out var alphaRow));
        Assert.Equal((uint)0, alphaRow);
        Assert.True(table.TryFindRowByKey(0, "beta"u8, out var betaRow));
        Assert.Equal((uint)1, betaRow);
        Assert.True(table.TryFindCellByKey(0, "alpha"u8.ToArray(), out var alphaCellByBytes));
        Assert.True(alphaCellByBytes.TryGetValueRaw(out var alphaValueByBytes));
        Assert.True(alphaValueByBytes.StartsWith("Alice"u8));
        Assert.True(table.TryFindCellByKey(0, "beta"u8, out var betaCellBySpan));
        Assert.True(betaCellBySpan.TryGetValueRaw(out var betaValueBySpan));
        Assert.True(betaValueBySpan.StartsWith("Bob"u8));
        Assert.True(table.TryFindGlobal("beta"u8.ToArray(), out var globalBytesHit));
        Assert.Equal((uint)0, globalBytesHit.Col);
        Assert.Equal((uint)1, globalBytesHit.Row);
        Assert.True(table.TryFindGlobal("alpha"u8, out var globalSpanHit));
        Assert.Equal((uint)0, globalSpanHit.Col);
        Assert.Equal((uint)0, globalSpanHit.Row);
    }

    [Fact]
    public async Task RebuildIndexes_GlobalKeyIndex_CoversValueOnlyAndMixedKeySizes()
    {
        using IMetadataTable valueOnly = MetadataTableHarness.CreateInMemoryTable(
            "ValueOnly",
            TableSpec.Helpers.Column("Value", 1, keyBytes: 0, valueBytes: 8));

        valueOnly.GetOrCreateCell(0, 0).TrySetValue("v");
        await valueOnly.RebuildIndexes(strict: false, cancellationToken: default);
        Assert.False(valueOnly.TryFindGlobal("v"u8, out _));

        using IMetadataTable mixed = MetadataTableHarness.CreateInMemoryTable(
            "MixedKeys",
            TableSpec.Helpers.Column("SmallKey", 2, keyBytes: 16, valueBytes: 64),
            TableSpec.Helpers.Column("LargeKey", 2, keyBytes: 32, valueBytes: 64));

        var small = mixed.GetOrCreateCell(0, 0);
        Assert.True(small.TrySetKey("small"));
        Assert.True(small.TrySetValue("Small"));

        var large = mixed.GetOrCreateCell(1, 1);
        Assert.True(large.TrySetKey("large"));
        Assert.True(large.TrySetValue("Large"));

        await mixed.RebuildIndexes(strict: false, cancellationToken: default);
        await mixed.RebuildIndexes(strict: false, cancellationToken: default);

        Assert.True(mixed.TryFindGlobal("small"u8, out var smallHit));
        Assert.Equal((uint)0, smallHit.Col);
        Assert.Equal((uint)0, smallHit.Row);

        Assert.True(mixed.TryFindGlobal("large"u8, out var largeHit));
        Assert.Equal((uint)1, largeHit.Col);
        Assert.Equal((uint)1, largeHit.Row);
    }

    [Fact]
    public async Task RebuildIndexes_Strict_AggregatesErrors_WhenARebuildableIndexThrows()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

        table.Indexes.Add(new ThrowingRebuildableIndex("boom-index"));

        var ex = await Assert.ThrowsAsync<AggregateException>(() => table.RebuildIndexes(strict: true, cancellationToken: default));

        Assert.Contains(ex.InnerExceptions, inner =>
            inner is InvalidOperationException ioe &&
            ioe.Message.Contains("rebuild failed", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Growth_LogicalRows_AndCapacities_WorkForInMemoryTables()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

        Assert.True(table.TryGetColumnCapacity(0, out var before));
        Assert.True(table.TryGrowColumnTo(0, 3, zeroInit: true));
        Assert.True(table.TryGetColumnCapacity(0, out var after));
        Assert.Equal((uint)1, before);
        Assert.Equal((uint)3, after);
        Assert.Equal((uint)3, table.GetLogicalRowCount());
        Assert.True(table.TryGetCell(0, 2, out _));
        Assert.True(table.TryGrowColumnTo(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.TryGrowColumnTo(4, 1));
    }

    [Fact]
    public void LogicalRowCount_CoversAllValueOnlyShapes_AndKeyValueSemantics()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "row-shapes.map");
            var spec = new TableSpec(
                "RowShapes",
                mapPath,
                [
                    TableSpec.Helpers.Column("v1", capacity: 1, keyBytes: 0, valueBytes: 1),
                    TableSpec.Helpers.Column("v2", capacity: 2, keyBytes: 0, valueBytes: 2),
                    TableSpec.Helpers.Column("v4", capacity: 3, keyBytes: 0, valueBytes: 4),
                    TableSpec.Helpers.Column("v8", capacity: 4, keyBytes: 0, valueBytes: 8),
                    TableSpec.Helpers.Column("vN", capacity: 5, keyBytes: 0, valueBytes: 16),
                    TableSpec.Helpers.Column("kv", capacity: 6, keyBytes: 16, valueBytes: 16)
                ]);

            using IMetadataTable table = MetadataTableHarness.CreateTable(spec);

            Assert.Equal((uint)0, table.GetLogicalRowCount());

            Assert.True(MetadataTableHarness.TryWriteValueBytes(table, column: 0, row: 0, bytes: [0x01]));
            Assert.Equal((uint)1, table.GetLogicalRowCount());

            Assert.True(MetadataTableHarness.TryWriteValueBytes(table, column: 1, row: 1, bytes: [0x01, 0x00]));
            Assert.Equal((uint)2, table.GetLogicalRowCount());

            Assert.True(MetadataTableHarness.TryWriteValueBytes(table, column: 2, row: 2, bytes: [0x01, 0x00, 0x00, 0x00]));
            Assert.Equal((uint)3, table.GetLogicalRowCount());

            Assert.True(MetadataTableHarness.TryWriteValueBytes(table, column: 3, row: 3, bytes: [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]));
            Assert.Equal((uint)4, table.GetLogicalRowCount());

            Assert.True(MetadataTableHarness.TryWriteValueBytes(table, column: 4, row: 4, bytes: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
            Assert.Equal((uint)5, table.GetLogicalRowCount());

            // Key/value row with value but no key should still count as empty.
            Assert.True(MetadataTableHarness.TryWriteValueBytes(table, column: 5, row: 5, bytes: "payload"u8.ToArray()));
            Assert.Equal((uint)5, table.GetLogicalRowCount());

            var keyValue = table.GetOrCreateCell(5, 5);
            Assert.True(keyValue.TrySetKey("kv-5"));
            Assert.Equal((uint)6, table.GetLogicalRowCount());
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task ToString_AndCompactionFallback_AreUsable()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));
        var cell = table.GetOrCreateCell(0, 0);
        Assert.True(cell.TrySetKey("alpha"));
        Assert.True(cell.TrySetValue("Alice"));

        var preview = table.ToString(maxRows: 1);
        var compacted = await table.TryCompactStore(strict: false, cancellationToken: default);

        Assert.Contains("MetadataTable", preview, StringComparison.Ordinal);
        Assert.Contains("Name", preview, StringComparison.Ordinal);
        Assert.Contains("Alice", preview, StringComparison.Ordinal);
        Assert.False(compacted);
    }

    [Fact]
    public void MissingColumnLookups_ReturnFalseOrThrow()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

        Assert.False(table.TryGetCell("Missing", 0, out _));
        Assert.Throws<KeyNotFoundException>(() => table.GetOrCreateCell("Missing", 0));
    }

    [Fact]
    public void TryGrowColumnTo_UsesFallback_WhenStoreDoesNotImplementGrowable()
    {
        var column = TableSpec.Helpers.Column("Name", 1, valueBytes: 64);

        using IMetadataTable successTable = MetadataTableHarness.CreateInMemoryTable("FallbackGrowSuccess", column);
        MetadataTableHarness.ReplaceStoreWithNonGrowable(successTable, throwOnCreate: false, column);

        Assert.True(successTable.TryGrowColumnTo(0, minRows: 3, zeroInit: true));
        Assert.True(successTable.TryGetCell(0, 2, out _));

        using IMetadataTable failureTable = MetadataTableHarness.CreateInMemoryTable("FallbackGrowFailure", column);
        MetadataTableHarness.ReplaceStoreWithNonGrowable(failureTable, throwOnCreate: true, column);

        Assert.True(failureTable.TryGrowColumnTo(0, minRows: 0, zeroInit: true));
        Assert.False(failureTable.TryGrowColumnTo(0, minRows: 3, zeroInit: true));
    }

    [Fact]
    public void MappedTable_CreatesBackingFile_AndPersistsCapacity()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "users.map");
            var spec = new TableSpec("Users", mapPath, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]);

            using var table = MetadataTableHarness.CreateTable(spec);
            var cell = table.GetOrCreateCell(0, 0);
            Assert.True(cell.TrySetKey("alpha"));
            Assert.True(cell.TrySetValue("Alice"));

            Assert.True(File.Exists(mapPath));
            Assert.True(table.TryGetColumnCapacity(0, out var initialCapacity));
            Assert.True(table.TryGetCell(0, 0, out _));
            Assert.Equal((uint)1, initialCapacity);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedTable_Open_RehydratesExistingStorage()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "users.map");
            var spec = new TableSpec("Users", mapPath, [TableSpec.Helpers.Column("Name", 4, valueBytes: 64)]);

            using (var table = MetadataTableHarness.CreateTable(spec))
            {
                var cell = table.GetOrCreateCell(0, 0);
                Assert.True(cell.TrySetKey("alpha"));

                Assert.ThrowsAny<IOException>(() => table.Open());
            }

            using var reopened = MetadataTableHarness.OpenTable(spec);

            Assert.True(reopened.TryGetColumnCapacity(0, out var capacity));
            Assert.Equal((uint)4, capacity);
            Assert.Equal("Users", reopened.Spec.Name);
            Assert.True(reopened.TryGetCell(0, 0, out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedTable_CanOpenAndMaterializeClusterStylePrimitiveAndGeneratedColumns()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "cluster-nodes.map");
            var spec = new TableSpec("ClusterNodes", mapPath,
            [
                TableSpec.Helpers.Column<Guid>("node_id", capacity: 4, keyBytes: 0),
                TableSpec.Helpers.Column<int>("connection_count", capacity: 4, keyBytes: 0),
                TableSpec.Helpers.Column("node_id_name_pair", capacity: 4, keyBytes: 16, valueBytes: 64)
            ]);

            using var table = MetadataTableHarness.CreateTable(spec);

            Assert.Equal("ClusterNodes", table.Spec.Name);
            Assert.True(File.Exists(mapPath));
            Assert.Null(Record.Exception(() => table.GetOrCreateCell(0, 0)));
            Assert.Null(Record.Exception(() => table.GetOrCreateCell(1, 0)));
            Assert.Null(Record.Exception(() => table.GetOrCreateCell(2, 0)));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class ThrowingRebuildableIndex(string name) : IRebuildableIndex<byte[], int>
    {
        public string Name { get; } = name;
        public Type KeyType => typeof(byte[]);
        public Type ValueType => typeof(int);
        public bool Add(byte[] key, int value) => true;
        public void Clear() { }
        public void Dispose() { }
        public bool Remove(byte[] key) => true;
        public Task Rebuild(IMetadataTable table) => Task.FromException(new InvalidOperationException("boom"));
        public bool TryGetValue(byte[] key, out int value)
        {
            value = 0;
            return false;
        }
    }
}
