using Extend0.Metadata.Indexing.Definitions;

namespace Extend0.Tests.Metadata.Indexing.Definitions;

public sealed class IndexBaseTests
{
    [Fact]
    public void Constructor_RejectsNullName()
    {
        Assert.Throws<ArgumentNullException>(() => new ProbeIndex(null!));
    }

    [Fact]
    public void BaseOperations_ExposeMetadata_AndMutateIndex()
    {
        using var index = new ProbeIndex("users", StringComparer.OrdinalIgnoreCase, capacity: 4);

        Assert.Equal("users", index.Name);
        Assert.Equal(typeof(string), index.KeyType);
        Assert.Equal(typeof(int), index.ValueType);

        Assert.True(index.Add("alice", 1));
        Assert.False(index.Add("alice", 2));
        Assert.True(index.TryGetValue("ALICE", out var value));
        Assert.Equal(1, value);

        Assert.True(index.Remove("alice"));
        Assert.False(index.Remove("alice"));

        Assert.True(index.Add("bob", 2));
        Assert.True(index.Add("carol", 3));
        index.Clear();

        Assert.False(index.TryGetValue("bob", out _));
        Assert.Equal(0, index.Count);
    }

    [Fact]
    public void Dispose_IsIdempotent_AndSubsequentOperationsThrow()
    {
        var index = new ProbeIndex("users");

        index.Add("alice", 1);
        index.Dispose();
        index.Dispose();

        Assert.Throws<ObjectDisposedException>(() => index.Clear());
        Assert.Throws<ObjectDisposedException>(() => index.Add("bob", 2));
        Assert.Throws<ObjectDisposedException>(() => index.Remove("alice"));
        Assert.Throws<ObjectDisposedException>(() => index.TryGetValue("alice", out _));
    }

    [Fact]
    public void CrossTableBaseOperations_ExposeMembership_AndClearPartitions()
    {
        using var index = new ProbeCrossTableIndex("global", StringComparer.OrdinalIgnoreCase, tablesCapacity: 2, perTableCapacity: 2);
        var tableA = Guid.NewGuid();
        var tableB = Guid.NewGuid();

        Assert.Equal("global", index.Name);
        Assert.Equal(typeof(Guid), index.KeyType);
        Assert.Equal(typeof(IDictionary<string, int>), index.ValueType);
        Assert.True(index.Add(tableA, "alice", 1));
        Assert.True(index.Add(tableB, "ALICE", 2));
        Assert.False(index.Add(tableA, "alice", 9));

        Assert.True(index.TryGetValue("alice", out var firstHit));
        Assert.Contains(firstHit, new[] { 1, 2 });

        var memberTables = index.GetMemberTables("alice", out var values);
        Assert.Equal(2, memberTables.Length);
        Assert.NotNull(values);
        Assert.Equal(2, values!.Length);

        Assert.True(index.Add("bob", 3));
        Assert.True(index.Remove("bob"));
        Assert.True(index.TryGetValue(tableB, "alice", out var tableBHit));
        Assert.Equal(2, tableBHit);

        index.ClearTable(tableA);
        Assert.False(index.TryGetValue(tableA, "alice", out _));
        Assert.True(index.TryGetValue(tableB, "alice", out _));

        index.Clear();
        Assert.Equal(0, index.PartitionCount);
        Assert.False(index.TryGetValue("alice", out _));
    }

    private sealed class ProbeIndex(string name, IEqualityComparer<string>? comparer = null, int capacity = 0)
        : IndexBase<string, int>(name, comparer, capacity)
    {
        public int Count => Index.Count;
    }

    private sealed class ProbeCrossTableIndex(
        string name,
        IEqualityComparer<string>? comparer = null,
        int tablesCapacity = 0,
        int perTableCapacity = 0)
        : CrossTableIndexBase<string, int>(name, comparer, tablesCapacity, perTableCapacity)
    {
        public int PartitionCount => Index.Count;
    }
}
