using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Definitions;

namespace Extend0.Tests.Metadata.Indexing.Definitions;

public sealed class CrossTableIndexBaseTests
{
    [Fact]
    public void PartitionedOperations_AddLookupRemoveAndClear_Work()
    {
        var index = new TestCrossTableIndex("test");
        var tableA = Guid.NewGuid();
        var tableB = Guid.NewGuid();

        Assert.True(index.Add(tableA, "alpha", 1));
        Assert.True(index.Add(tableB, "alpha", 2));
        Assert.True(index.Add(tableB, "beta", 3));
        Assert.True(index.Add("shared", 9));
        Assert.False(index.Add(tableA, "alpha", 99));

        Assert.True(index.TryGetValue("alpha", out var anyAlpha));
        Assert.Contains(anyAlpha, new[] { 1, 2 });
        Assert.True(index.TryGetValue(tableA, "alpha", out var alphaA));
        Assert.Equal(1, alphaA);
        Assert.False(index.TryGetValue(tableA, "missing", out _));

        var memberTables = index.GetMemberTables("alpha");
        var memberTablesWithValues = index.GetMemberTables("alpha", out var values);
        var allTables = index.GetMemberTables();

        Assert.Equal(2, memberTables.Length);
        Assert.Equal(2, memberTablesWithValues.Length);
        Assert.NotNull(values);
        Assert.Contains(1, values!);
        Assert.Contains(2, values!);
        Assert.Equal(2, allTables.Length);

        Assert.True(index.Remove(tableA, "alpha"));
        Assert.False(index.Remove(tableA, "alpha"));
        Assert.True(index.Remove("shared"));
        Assert.False(index.Remove("shared"));

        index.ClearTable(tableB);
        Assert.Empty(index.GetMemberTables());
    }

    [Fact]
    public void MembershipQueries_GrowBeyondStackThreshold()
    {
        var index = new TestCrossTableIndex("many");
        const int partitions = 140;
        var expectedIds = new HashSet<Guid>();

        for (var i = 0; i < partitions; i++)
        {
            var tableId = Guid.NewGuid();
            expectedIds.Add(tableId);
            Assert.True(index.Add(tableId, "same", i));
        }

        var ids = index.GetMemberTables("same");
        var idsWithValues = index.GetMemberTables("same", out var values);

        Assert.Equal(partitions, ids.Length);
        Assert.Equal(partitions, idsWithValues.Length);
        Assert.NotNull(values);
        Assert.Equal(partitions, values!.Length);
        Assert.True(expectedIds.SetEquals(ids));
        Assert.True(expectedIds.SetEquals(idsWithValues));
    }

    [Fact]
    public void PartitionDictionaryApi_Works_ForWholePartitions()
    {
        var index = new TestCrossTableIndex("snapshot");
        var tableId = Guid.NewGuid();
        IDictionary<string, int> partition = new Dictionary<string, int>
        {
            ["a"] = 1,
            ["b"] = 2
        };

        Assert.True(index.Add(tableId, partition));
        Assert.False(index.Add(tableId, partition));
        Assert.True(index.TryGetValue(tableId, out var stored));
        Assert.Equal(2, stored.Count);
        Assert.True(index.Remove(tableId));
        Assert.False(index.Remove(tableId));
    }

    [Fact]
    public void Dispose_ClearsIndexes_WithoutThrowing()
    {
        var tableIndex = new TestTableIndex("table");
        var crossIndex = new TestCrossTableIndex("cross");
        var tableId = Guid.NewGuid();

        Assert.True(tableIndex.Add("alpha", 1));
        Assert.True(crossIndex.Add(tableId, "alpha", 1));

        tableIndex.Dispose();
        crossIndex.Dispose();
    }

    [Fact]
    public void CrossTableRebuildableHelpers_RentPadAndRecycleKeys()
    {
        var index = new TestCrossTableRebuildableIndex("rebuild", keySize: 4);
        var tableId = Guid.NewGuid();

        Assert.True(index.TryRentKeyBytes([1, 2], out var owned));
        Assert.Equal(new byte[] { 1, 2, 0, 0 }, owned);
        Assert.False(index.TryRentKeyBytes([], out _));
        Assert.False(index.TryRentKeyBytes([1, 2, 3, 4, 5], out _));

        Assert.True(index.Add(tableId, new Dictionary<byte[], int> { [owned] = 7 }));
        index.ClearTable(tableId);

        Assert.True(index.TryRentKeyBytes([9], out var reused));
        Assert.Same(owned, reused);
        index.ReturnRentedKey(reused);

        Assert.Equal(new byte[] { 3, 4, 0, 0 }, index.BuildScratchLookupKey([3, 4]));
    }

    private sealed class TestCrossTableIndex(string name)
        : CrossTableIndexBase<string, int>(name)
    {
    }

    private sealed class TestTableIndex(string name)
        : IndexBase<string, int>(name)
    {
    }

    private sealed class TestCrossTableRebuildableIndex(string name, int keySize)
        : CrossTableRebuildableIndexDefinition<byte[], int>(name, keySize: keySize)
    {
        public bool TryRentKeyBytes(byte[] key, out byte[] owned) => TryRentKey(key, out owned);

        public void ReturnRentedKey(byte[] key) => ReturnPooledKey(key);

        public byte[] BuildScratchLookupKey(ReadOnlySpan<byte> key) => GetScratchLookupKey(key, CachedKeySize);

        public override Task Rebuild(IMetaDBManager manager) => Task.CompletedTask;
    }
}
