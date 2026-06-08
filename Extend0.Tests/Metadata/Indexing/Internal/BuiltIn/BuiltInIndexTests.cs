using Extend0.Metadata.Schema;
using Extend0.Testing.Metadata.Internal;
using Extend0.Testing.Metadata.Indexing.Internal.BuiltIn;

namespace Extend0.Tests.Metadata.Indexing.Internal.BuiltIn;

public sealed class BuiltInIndexTests
{
    [Fact]
    public void ColumnKeyIndex_CoversPartitionSnapshotSetAndRemovePaths()
    {
        var index = new BuiltInIndexHarness.ColumnKeyIndexHandle("column", (0u, 4), (1u, 2));

        Assert.True(index.AddPartition(0,
            ([0x41], 1u),
            ([0x41], 2u),
            ([0x42, 0x42], 3u)));
        Assert.False(index.AddPartition(0, ([0x43], 4u)));
        Assert.False(index.AddPartition(9, ([0x44], 5u)));

        Assert.True(index.TryGetSnapshot(0, out var snapshot));
        Assert.Equal(2, snapshot.Length);
        Assert.All(snapshot, static entry => Assert.True(entry.OwnedKeyMatchesKey));
        Assert.Contains(snapshot, static entry => entry.Row == 2 && entry.Key.SequenceEqual(new byte[] { 0x41, 0, 0, 0 }));
        Assert.Contains(snapshot, static entry => entry.Row == 3 && entry.Key.SequenceEqual(new byte[] { 0x42, 0x42, 0, 0 }));

        Assert.True(index.TryGetRow(0, [0x41], out var row));
        Assert.Equal(2u, row);
        Assert.True(index.Remove(0, [0x41]));
        Assert.False(index.TryGetRow(0, [0x41], out _));

        index.Set(0, [0x43], 9);
        Assert.True(index.TryGetRow(0, [0x43], out row));
        Assert.Equal(9u, row);

        var storedKey = index.GetStoredKey(0, [0x43]);
        Assert.NotNull(storedKey);
        Assert.True(index.RemoveExact(0, storedKey!));
        Assert.False(index.RemoveExact(0, storedKey!));

        Assert.True(index.RemoveColumn(0));
        Assert.False(index.RemoveColumn(0));
    }

    [Fact]
    public void ColumnKeyIndex_RemoveSpan_CoversValidationAndRemovalPaths()
    {
        var index = new BuiltInIndexHarness.ColumnKeyIndexHandle("column-span", (0u, 4));

        Assert.False(index.RemoveSpan(9, "a"u8));
        Assert.False(index.RemoveSpan(0, "toolong"u8));

        Assert.True(index.AddPartition(0, ([0x41], 1u)));
        Assert.True(index.RemoveSpan(0, "A"u8));
        Assert.False(index.RemoveSpan(0, "A"u8));
    }

    [Fact]
    public void ColumnKeyIndex_CoversInvalidInputAndSpanLookupBranches()
    {
        var index = new BuiltInIndexHarness.ColumnKeyIndexHandle("column-invalid", (0u, 2), (1u, 2));

        Assert.False(index.TryGetSnapshot(0, out var missingSnapshot));
        Assert.Empty(missingSnapshot);

        index.Set(9, [0x01], row: 1);
        index.Set(0, [0x01, 0x02, 0x03], row: 1);
        Assert.False(index.TryGetRow(9, [0x01], out _));
        Assert.False(index.TryGetRow(1, [0x01], out _));
        Assert.False(index.TryGetRow(0, [0x01, 0x02, 0x03], out _));

        Assert.True(index.AddPartition(0, ([], 1u), ([0x01, 0x02, 0x03], 2u)));
        Assert.True(index.TryGetSnapshot(0, out var emptySnapshot));
        Assert.Empty(emptySnapshot);

        Assert.False(index.TryGetRowSpan(9, "A"u8, out _));
        Assert.False(index.TryGetRowSpan(1, "A"u8, out _));
        Assert.False(index.TryGetRowSpan(0, "ABC"u8, out _));
        Assert.False(index.TryGetRowSpan(0, "A"u8, out _));
        Assert.False(index.Remove(9, [0x01]));
        Assert.False(index.Remove(0, [0x01, 0x02, 0x03]));
        Assert.False(index.Remove(0, [0x02]));

        index.Set(0, [0x01], row: 4);
        index.Set(0, [0x01], row: 5);

        Assert.False(index.RemoveExact(0, [0x01, 0x00]));
        Assert.True(index.TryGetRowSpan(0, [0x01], out var row));
        Assert.Equal(5u, row);
    }

    [Fact]
    public async Task ColumnKeyIndex_RebuildRefreshesSchemaCacheAndUsesLastWins()
    {
        using var index = new BuiltInIndexHarness.ColumnKeyIndexHandle("column-rebuild", (0u, 4));
        index.Set(0, "old"u8.ToArray(), row: 99);

        using var table = MetadataTableHarness.CreateInMemoryTable(
            "ColumnRebuild",
            TableSpec.Helpers.Column("Keyed", 2, keyBytes: 4, valueBytes: 8),
            TableSpec.Helpers.Column("ValueOnly", 1, keyBytes: 0, valueBytes: 8));

        Assert.True(table.GetOrCreateCell(0, 0).TrySetKey("aa"));
        Assert.True(table.GetOrCreateCell(0, 1).TrySetKey("aa"));
        table.GetOrCreateCell(1, 0).TrySetValue("v");

        await index.Rebuild(table);

        Assert.False(index.TryGetRow(0, "old"u8.ToArray(), out _));
        Assert.True(index.TryGetRow(0, "aa"u8.ToArray(), out var row));
        Assert.Equal(1u, row);

        index.Set(1, "v"u8.ToArray(), row: 7);
        Assert.False(index.TryGetRow(1, "v"u8.ToArray(), out _));
    }

    [Fact]
    public void GlobalKeyIndex_CoversAddSetLookupRemoveAndDisabledSize()
    {
        var index = new BuiltInIndexHarness.GlobalKeyIndexHandle("global", keySize: 4);

        Assert.True(index.Add([0x41], col: 1, row: 2));
        Assert.False(index.Add([0x41], col: 7, row: 8));
        Assert.False(index.Add([1, 2, 3, 4, 5], col: 1, row: 2));

        Assert.True(index.TryGetHit([0x41], out var hit));
        Assert.Equal(1u, hit.Col);
        Assert.Equal(2u, hit.Row);

        index.Set([0x41], col: 6, row: 7);
        Assert.True(index.TryGetHit([0x41], out hit));
        Assert.Equal(6u, hit.Col);
        Assert.Equal(7u, hit.Row);
        Assert.NotNull(index.GetStoredKey([0x41]));

        Assert.True(index.Remove([0x41]));
        Assert.False(index.Remove([0x41]));
        Assert.Equal(0, index.Count);

        var disabled = new BuiltInIndexHarness.GlobalKeyIndexHandle("disabled", keySize: 0);
        Assert.False(disabled.Add([0x42], col: 1, row: 1));
        Assert.False(disabled.TryGetHit([0x42], out _));
        Assert.False(disabled.Remove([0x42]));
    }

    [Fact]
    public void GlobalKeyIndex_CoversSpanLookupAndValidationBranches()
    {
        var disabled = new BuiltInIndexHarness.GlobalKeyIndexHandle("global-span-disabled", keySize: 0);
        Assert.False(disabled.TryGetHitSpan("A"u8, out _));

        var index = new BuiltInIndexHarness.GlobalKeyIndexHandle("global-span", keySize: 4);
        Assert.False(index.TryGetHit([1, 2, 3, 4, 5], out _));
        Assert.False(index.TryGetHitSpan("missing"u8, out _));
        Assert.False(index.Remove([1, 2, 3, 4, 5]));

        index.Set([0x41], col: 2, row: 3);
        Assert.True(index.TryGetHitSpan("A"u8, out var spanHit));
        Assert.Equal(2u, spanHit.Col);
        Assert.Equal(3u, spanHit.Row);

        index.Set([0x41], col: 4, row: 5);
        Assert.True(index.TryGetHit([0x41], out var updated));
        Assert.Equal(4u, updated.Col);
        Assert.Equal(5u, updated.Row);

        index.Set([1, 2, 3, 4, 5], col: 9, row: 9);
        Assert.False(index.TryGetHit([1, 2, 3, 4, 5], out _));
    }

    [Fact]
    public async Task GlobalKeyIndex_RebuildRecomputesGlobalKeySizeAndClearsOldEntries()
    {
        using var index = new BuiltInIndexHarness.GlobalKeyIndexHandle("global-rebuild", keySize: 4);
        index.Set("old"u8.ToArray(), col: 9, row: 9);

        using var table = MetadataTableHarness.CreateInMemoryTable(
            "GlobalRebuild",
            TableSpec.Helpers.Column("SmallKey", 2, keyBytes: 4, valueBytes: 8),
            TableSpec.Helpers.Column("LargeKey", 2, keyBytes: 8, valueBytes: 8));

        Assert.True(table.GetOrCreateCell(0, 0).TrySetKey("aa"));
        Assert.True(table.GetOrCreateCell(1, 1).TrySetKey("large"));

        await index.Rebuild(table);

        Assert.False(index.TryGetHit("old"u8.ToArray(), out _));
        Assert.True(index.TryGetHit("aa"u8.ToArray(), out var smallHit));
        Assert.Equal(0u, smallHit.Col);
        Assert.Equal(0u, smallHit.Row);
        Assert.Equal(8, index.GetStoredKey("aa"u8.ToArray())!.Length);

        Assert.True(index.TryGetHit("large"u8.ToArray(), out var largeHit));
        Assert.Equal(1u, largeHit.Col);
        Assert.Equal(1u, largeHit.Row);
    }

    [Fact]
    public async Task GlobalKeyIndex_RebuildValueOnlyTableDisablesIndex()
    {
        using var index = new BuiltInIndexHarness.GlobalKeyIndexHandle("global-rebuild-value-only", keySize: 4);
        index.Set("old"u8.ToArray(), col: 9, row: 9);

        using var table = MetadataTableHarness.CreateInMemoryTable(
            "GlobalValueOnly",
            TableSpec.Helpers.Column("Value", 1, keyBytes: 0, valueBytes: 8));

        table.GetOrCreateCell(0, 0).TrySetValue("v");

        await index.Rebuild(table);

        Assert.Equal(0, index.Count);
        Assert.False(index.TryGetHit("old"u8.ToArray(), out _));
        Assert.False(index.Add("new"u8.ToArray(), col: 0, row: 0));
    }

    [Fact]
    public void GlobalMultiTableKeyIndex_CoversSetAllMembershipAndRemovals()
    {
        var index = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi", keySize: 4);
        var tableA = Guid.NewGuid();
        var tableB = Guid.NewGuid();

        index.Set(tableA, "UsersA", [0x41], col: 1, row: 2);
        index.Set(tableB, "UsersB", [0x41], col: 3, row: 4);
        Assert.Equal(2, index.PartitionCount);

        Assert.True(index.TryGetHit([0x41], out var anyHit));
        Assert.Contains(anyHit.TableName, new[] { "UsersA", "UsersB" });

        var memberTables = index.GetMemberTables([0x41], out var memberHits);
        Assert.Equal(2, memberTables.Length);
        Assert.NotNull(memberHits);
        Assert.Equal(2, memberHits!.Length);

        index.SetAll("Shared", [0x42], col: 9, row: 10);
        Assert.Equal(2, index.GetMemberTables([0x42]).Length);
        Assert.True(index.TryGetValue(tableA, [0x42], out var hitA));
        Assert.Equal("Shared", hitA.TableName);
        Assert.Equal(9u, hitA.Col);
        Assert.Equal(10u, hitA.Row);

        index.SetAll("SharedUpdated", [0x42], col: 11, row: 12);
        Assert.True(index.TryGetValue(tableA, [0x42], out var updatedA));
        Assert.Equal("SharedUpdated", updatedA.TableName);
        Assert.Equal(11u, updatedA.Col);
        Assert.Equal(12u, updatedA.Row);

        Assert.True(index.Remove(tableA, [0x42]));
        Assert.Single(index.GetMemberTables([0x42]));
        Assert.True(index.Remove([0x42]));
        Assert.Empty(index.GetMemberTables([0x42]));

        Assert.True(index.Remove(tableB));
        Assert.False(index.Remove(tableB));
    }

    [Fact]
    public void GlobalMultiTableKeyIndex_SetAll_CoversNoOpInvalidAndArrayPoolPaths()
    {
        var empty = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-empty", keySize: 4);
        empty.SetAll("NoPartitions", [0x01], col: 1, row: 1);
        Assert.Equal(0, empty.PartitionCount);

        var index = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-large", keySize: 4);
        for (var i = 0; i < 129; i++)
            index.Set(Guid.NewGuid(), $"Table{i}", [(byte)i], col: 0, row: 0);

        index.SetAll("IgnoredEmpty", [], col: 1, row: 1);
        index.SetAll("IgnoredTooLong", [1, 2, 3, 4, 5], col: 1, row: 1);
        Assert.Empty(index.GetMemberTables([0xFE]));

        index.SetAll("LargeShared", [0xFE], col: 7, row: 8);
        Assert.Equal(129, index.GetMemberTables([0xFE]).Length);
        Assert.True(index.TryGetHit([0xFE], out var hit));
        Assert.Equal("LargeShared", hit.TableName);
        Assert.Equal(7u, hit.Col);
        Assert.Equal(8u, hit.Row);

        var exactKey = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        index.SetAll("ExactShared", exactKey, col: 3, row: 4);
        Assert.Equal(129, index.GetMemberTables(exactKey).Length);
        Assert.True(index.TryGetHit(exactKey, out var exactHit));
        Assert.Equal("ExactShared", exactHit.TableName);
        Assert.Equal(3u, exactHit.Col);
        Assert.Equal(4u, exactHit.Row);
    }

    [Fact]
    public void GlobalMultiTableKeyIndex_AddVariantsCoverDuplicateAndValidationPaths()
    {
        var index = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-add", keySize: 4);
        var tableA = Guid.NewGuid();
        var tableB = Guid.NewGuid();
        var tableC = Guid.NewGuid();

        index.Set(tableA, "SeedA", [0x10], col: 0, row: 0);
        index.Set(tableB, "SeedB", [0x20], col: 0, row: 0);

        Assert.True(index.Add([0x33], "All", row: 7, col: 8));
        Assert.True(index.TryGetValue(tableA, [0x33], out _));
        Assert.True(index.TryGetValue(tableB, [0x33], out _));

        Assert.True(index.Add(tableA, [0x44], "OnlyA", row: 1, col: 2));
        Assert.False(index.Add(tableA, [1, 2, 3, 4, 5], "TooLong", row: 1, col: 2));
        Assert.True(index.TryGetValue(tableA, [0x44], out var onlyA));
        Assert.Equal("OnlyA", onlyA.TableName);

        Assert.True(index.AddPartition(tableC,
            ([0x55], "Partition", 1u, 1u),
            ([0x55], "PartitionLastWins", 2u, 3u),
            ([0x56], "Other", 4u, 5u)));
        Assert.False(index.AddPartition(tableC, ([0x77], "DuplicatePartition", 1u, 1u)));
        Assert.True(index.TryGetValue(tableC, [0x55], out var partitionHit));
        Assert.Equal("PartitionLastWins", partitionHit.TableName);
        Assert.Equal(2u, partitionHit.Row);
        Assert.Equal(3u, partitionHit.Col);

        var stored = index.GetStoredKey(tableC, [0x55]);
        Assert.NotNull(stored);
    }

    [Fact]
    public void GlobalMultiTableKeyIndex_CoversSpanGlobalLookupAndInvalidPartitionBranches()
    {
        var index = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-span", keySize: 4);
        var tableA = Guid.NewGuid();

        Assert.False(index.TryGetHitSpan("A"u8, out _));
        index.Set(tableA, "IgnoredEmpty", [], col: 1, row: 1);
        index.Set(tableA, "IgnoredTooLong", [1, 2, 3, 4, 5], col: 1, row: 1);
        Assert.Equal(0, index.PartitionCount);

        index.Set(tableA, "UsersA", [0x41], col: 2, row: 3);

        Assert.True(index.TryGetHitSpan("A"u8, out var spanHit));
        Assert.Equal("UsersA", spanHit.TableName);
        Assert.Equal(2u, spanHit.Col);
        Assert.Equal(3u, spanHit.Row);
        Assert.True(index.TryGetValue([0x41], out var globalHit));
        Assert.Equal("UsersA", globalHit.TableName);
        Assert.False(index.TryGetValue([1, 2, 3, 4, 5], out _));

        var exactKey = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        index.Set(tableA, "UsersExact", exactKey, col: 4, row: 5);
        Assert.True(index.TryGetValue(tableA, exactKey, out var exactHit));
        Assert.Equal("UsersExact", exactHit.TableName);
        Assert.Equal(4u, exactHit.Col);
        Assert.Equal(5u, exactHit.Row);

        Assert.False(index.Add(tableA, [0x41], "DuplicateA", row: 9, col: 9));
        Assert.False(index.Add([0x41], "DuplicateEverywhere", row: 9, col: 9));
        Assert.True(index.TryGetValue(tableA, [0x41], out var unchanged));
        Assert.Equal("UsersA", unchanged.TableName);

        var invalidPartition = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-invalid-partition", keySize: 4);
        Assert.Throws<InvalidOperationException>(() => invalidPartition.AddPartition(
            Guid.NewGuid(),
            ([0x01], "ValidBeforeFailure", 1u, 1u),
            ([1, 2, 3, 4, 5], "TooLong", 2u, 2u)));
        Assert.Equal(0, invalidPartition.PartitionCount);

        var emptyPartition = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-empty-partition", keySize: 4);
        Assert.True(emptyPartition.AddPartition(Guid.NewGuid(), ([], "Skipped", 1u, 1u)));
        Assert.Equal(1, emptyPartition.PartitionCount);
        Assert.False(emptyPartition.TryGetValue([], out _));
    }

    [Fact]
    public void GlobalMultiTableKeyIndex_CoversNegativeLookupAndRemovalPaths()
    {
        var index = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-negative", keySize: 4);
        var tableA = Guid.NewGuid();
        var tableB = Guid.NewGuid();

        index.Set(tableA, "UsersA", [0x10], col: 1, row: 2);
        index.Set(tableB, "UsersB", [0x20], col: 3, row: 4);

        Assert.False(index.TryGetHit([1, 2, 3, 4, 5], out _));
        Assert.False(index.TryGetValue(tableA, [1, 2, 3, 4, 5], out _));
        Assert.Empty(index.GetMemberTables([1, 2, 3, 4, 5]));
        Assert.Empty(index.GetMemberTables([1, 2, 3, 4, 5], out var overlongHits));
        Assert.Null(overlongHits);

        Assert.False(index.Remove(tableA, [0x20]));
        Assert.False(index.Remove([0x99]));
        Assert.False(index.Remove(tableB, [1, 2, 3, 4, 5]));

        index.Set(tableA, "UsersA2", [0x10], col: 9, row: 8);
        Assert.True(index.TryGetValue(tableA, [0x10], out var updated));
        Assert.Equal("UsersA2", updated.TableName);
        Assert.Equal(9u, updated.Col);
        Assert.Equal(8u, updated.Row);
    }

    [Fact]
    public async Task GlobalMultiTableKeyIndex_RebuildScansCreatedTablesAndSkipsLazyTables()
    {
        using var handle = MetaDBManagerHarness.CreateManager(
            factory: spec => MetadataTableHarness.CreateInMemoryTable(spec!.Value.Name, spec.Value.Columns));
        var manager = handle.Contract;

        var alphaId = manager.RegisterTable(
            new TableSpec("Alpha", "alpha.map", [TableSpec.Helpers.Column("Name", 2, keyBytes: 16, valueBytes: 64)]),
            createNow: true);
        var betaId = manager.RegisterTable(
            new TableSpec("Beta", "beta.map", [TableSpec.Helpers.Column("Name", 2, keyBytes: 16, valueBytes: 64)]),
            createNow: true);
        var lazyId = manager.RegisterTable(
            new TableSpec("Lazy", "lazy.map", [TableSpec.Helpers.Column("Name", 2, keyBytes: 16, valueBytes: 64)]),
            createNow: false);

        Assert.True(manager.GetOrCreate(alphaId).GetOrCreateCell(0, 0).TrySetKey("shared"));
        Assert.True(manager.GetOrCreate(betaId).GetOrCreateCell(0, 0).TrySetKey("shared"));

        using var index = new BuiltInIndexHarness.GlobalMultiTableKeyIndexHandle("multi-rebuild", keySize: 16);
        index.Set(Guid.NewGuid(), "Stale", "stale"u8.ToArray(), col: 0, row: 0);
        index.Set(lazyId, "Lazy", "lazy-key"u8.ToArray(), col: 0, row: 0);

        await index.Rebuild(manager);

        Assert.Empty(index.GetMemberTables("stale"u8.ToArray()));
        Assert.Empty(index.GetMemberTables("lazy-key"u8.ToArray()));

        var members = index.GetMemberTables("shared"u8.ToArray(), out var hits);
        Assert.Equal(2, members.Length);
        Assert.NotNull(hits);
        Assert.All(hits!, hit => Assert.Contains(hit.TableName, new[] { "Alpha", "Beta" }));

        Assert.True(manager.GetOrCreate(alphaId).GetOrCreateCell(0, 0).TrySetKey("alpha-only"));

        await index.Rebuild(manager);

        var sharedAfterUpdate = index.GetMemberTables("shared"u8.ToArray(), out var sharedHits);
        Assert.Single(sharedAfterUpdate);
        Assert.NotNull(sharedHits);
        Assert.Equal("Beta", sharedHits![0].TableName);

        Assert.True(index.TryGetValue(alphaId, "alpha-only"u8.ToArray(), out var alphaHit));
        Assert.Equal("Alpha", alphaHit.TableName);
        Assert.Equal(0u, alphaHit.Col);
        Assert.Equal(0u, alphaHit.Row);
    }
}
