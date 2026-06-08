using Extend0.Metadata.Schema;
using Extend0.Testing.Metadata.Indexing.Registries;
using Extend0.Testing.Metadata.Internal;

namespace Extend0.Tests.Metadata.Indexing.Registries;

public sealed class IndexesRegistryTests
{
    [Fact]
    public void TableRegistry_AddGetTryGetRemoveAndDispose_Work()
    {
        using var registry = new IndexesRegistryHarness.TableRegistryHandle();
        var index = new IndexesRegistryHarness.ProbeTableIndex<string, int>("users-by-name");

        Assert.False(registry.TryGet("missing", out _));

        var added = registry.Add(index);
        Assert.Same(index, added);
        Assert.True(registry.TryGet("users-by-name", out var raw));
        Assert.Same(index, raw);
        Assert.Same(index, registry.Get<string, int>("users-by-name"));
        Assert.Single(registry.Enumerate());

        Assert.Throws<InvalidOperationException>(() => registry.Get<int, int>("users-by-name"));
        Assert.Throws<KeyNotFoundException>(() => registry.Get<string, int>("missing"));
        Assert.Throws<InvalidOperationException>(() => registry.Add(new IndexesRegistryHarness.ProbeTableIndex<string, int>("users-by-name")));

        Assert.True(registry.Remove("users-by-name"));
        Assert.Equal(1, index.DisposeCount);
        Assert.False(registry.Remove("users-by-name"));

        registry.Dispose();
        Assert.Throws<ObjectDisposedException>(() => registry.TryGet("anything", out _));
    }

    [Fact]
    public void TableRegistry_ClearAllAndRebuild_OnlyTouchesMatchingIndexes()
    {
        using var registry = new IndexesRegistryHarness.TableRegistryHandle();
        var regular = new IndexesRegistryHarness.ProbeTableIndex<string, int>("regular");
        var rebuildable = new IndexesRegistryHarness.ProbeRebuildableTableIndex<string, int>("rebuildable");
        var tempRoot = CreateTempDirectory();
        using var table = MetadataTableHarness.CreateTable(new TableSpec(
            "Users",
            Path.Combine(tempRoot, "users.map"),
            [TableSpec.Helpers.Column("Value", 2, valueBytes: 16)]));

        regular.Add("alice", 1);
        rebuildable.Add("bob", 2);
        registry.Add(regular);
        registry.Add(rebuildable);

        registry.ClearAll();

        Assert.Equal(1, regular.ClearCount);
        Assert.Equal(1, rebuildable.ClearCount);

        registry.Rebuild(table);

        Assert.Equal(2, regular.ClearCount);
        Assert.Equal(2, rebuildable.ClearCount);
        Assert.Equal(1, rebuildable.RebuildCount);
        Assert.Same(table, rebuildable.LastTable);
        Assert.Throws<ArgumentNullException>(() => registry.Rebuild(null!));
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    [Fact]
    public void TableRegistry_FactoryAdd_InvokesConstructorAndRejectsDuplicates()
    {
        using var registry = new IndexesRegistryHarness.TableRegistryHandle();
        var invocations = 0;

        var first = registry.Add<string, int>(() =>
        {
            invocations++;
            return new IndexesRegistryHarness.ProbeTableIndex<string, int>("factory-index");
        });

        Assert.Equal(1, invocations);
        Assert.Equal("factory-index", first.Name);

        Assert.Throws<InvalidOperationException>(() => registry.Add<string, int>(() =>
        {
            invocations++;
            return new IndexesRegistryHarness.ProbeTableIndex<string, int>("factory-index");
        }));

        Assert.Equal(2, invocations);
    }

    [Fact]
    public void ProbeTableIndex_DirectOperations_RespectDictionaryAndDisposeSemantics()
    {
        var index = new IndexesRegistryHarness.ProbeTableIndex<string, int>("probe-table");

        Assert.Equal("probe-table", index.Name);
        Assert.Equal(typeof(string), index.KeyType);
        Assert.Equal(typeof(int), index.ValueType);
        Assert.True(index.Add("alice", 1));
        Assert.False(index.Add("alice", 2));
        Assert.True(index.TryGetValue("alice", out var value));
        Assert.Equal(1, value);
        Assert.True(index.Remove("alice"));
        Assert.False(index.Remove("alice"));

        index.Add("bob", 2);
        index.Clear();
        Assert.Equal(1, index.ClearCount);
        Assert.False(index.TryGetValue("bob", out _));

        index.Dispose();
        index.Dispose();
        Assert.Equal(1, index.DisposeCount);
        Assert.Throws<ObjectDisposedException>(() => index.Add("carol", 3));
        Assert.Throws<ObjectDisposedException>(() => index.Remove("carol"));
        Assert.Throws<ObjectDisposedException>(() => index.TryGetValue("carol", out _));
        Assert.Throws<ObjectDisposedException>(() => index.Clear());
    }

    [Fact]
    public void CrossTableRegistry_AddGetTryGetClearForTableAndDispose_Work()
    {
        using var registry = new IndexesRegistryHarness.CrossTableRegistryHandle();
        var index = new IndexesRegistryHarness.ProbeCrossTableIndex<string, int>("global");
        var tableA = Guid.NewGuid();
        var tableB = Guid.NewGuid();

        Assert.False(registry.TryGet("missing", out _));
        Assert.False(registry.TryGet<int, int>("missing", out _));

        index.Add(tableA, "alice", 1);
        index.Add(tableB, "alice", 2);

        var added = registry.Add(index);
        Assert.Same(index, added);
        Assert.True(registry.TryGet("global", out var raw));
        Assert.Same(index, raw);
        Assert.True(registry.TryGet<string, int>("global", out var typed));
        Assert.Same(index, typed);
        Assert.False(registry.TryGet<int, int>("global", out _));
        Assert.Same(index, registry.Get<string, int>("global"));
        Assert.Throws<InvalidOperationException>(() => registry.Get<int, int>("global"));
        Assert.Throws<KeyNotFoundException>(() => registry.Get<string, int>("missing"));

        registry.ClearForTable(tableA);

        Assert.Contains(tableA, index.ClearedTables);
        Assert.False(index.TryGetValue(tableA, "alice", out _));
        Assert.True(index.TryGetValue(tableB, "alice", out _));

        registry.ClearAll();
        Assert.Equal(1, index.ClearCount);

        Assert.True(registry.Remove("global"));
        Assert.Equal(1, index.DisposeCount);
        Assert.False(registry.Remove("global"));

        registry.Dispose();
        Assert.Throws<ObjectDisposedException>(() => registry.TryGet("anything", out _));
    }

    [Fact]
    public void ProbeCrossTableIndex_DirectOperations_RespectPartitionAndDisposeSemantics()
    {
        var index = new IndexesRegistryHarness.ProbeCrossTableIndex<string, int>("probe-cross");
        var tableA = Guid.NewGuid();
        var tableB = Guid.NewGuid();

        Assert.Equal("probe-cross", index.Name);
        Assert.Equal(typeof(string), index.KeyType);
        Assert.Equal(typeof(int), index.ValueType);
        Assert.False(index.Add("orphan", 0));
        Assert.True(index.Add(tableA, "alice", 1));
        Assert.True(index.Add(tableB, "alice", 2));
        Assert.False(index.Add(tableA, "alice", 9));
        Assert.True(index.TryGetValue("alice", out var anyValue));
        Assert.Contains(anyValue, new[] { 1, 2 });
        Assert.True(index.TryGetValue(tableA, "alice", out var tableAValue));
        Assert.Equal(1, tableAValue);

        var memberTables = index.GetMemberTables("alice", out var values);
        Assert.Equal(2, memberTables.Length);
        Assert.NotNull(values);
        Assert.Equal(2, values!.Length);
        Assert.Contains(tableA, index.GetMemberTables());

        Assert.True(index.Add("bob", 3));
        Assert.True(index.Remove("bob"));
        Assert.False(index.Remove("missing"));
        Assert.True(index.Remove(tableA, "alice"));
        Assert.False(index.TryGetValue(tableA, "alice", out _));
        Assert.False(index.Remove(tableA, "alice"));

        index.ClearTable(tableB);
        Assert.Contains(tableB, index.ClearedTables);
        Assert.Contains(tableA, index.GetMemberTables());
        Assert.DoesNotContain(tableB, index.GetMemberTables());

        index.Add(tableA, "carol", 4);
        index.Clear();
        Assert.Equal(1, index.ClearCount);
        Assert.Empty(index.GetMemberTables());

        index.Dispose();
        index.Dispose();
        Assert.Equal(1, index.DisposeCount);
        Assert.Throws<ObjectDisposedException>(() => index.Add("dave", 5));
        Assert.Throws<ObjectDisposedException>(() => index.Add(tableA, "dave", 5));
        Assert.Throws<ObjectDisposedException>(() => index.Remove("dave"));
        Assert.Throws<ObjectDisposedException>(() => index.Remove(tableA, "dave"));
        Assert.Throws<ObjectDisposedException>(() => index.TryGetValue("dave", out _));
        Assert.Throws<ObjectDisposedException>(() => index.TryGetValue(tableA, "dave", out _));
        Assert.Throws<ObjectDisposedException>(() => index.GetMemberTables());
        Assert.Throws<ObjectDisposedException>(() => index.GetMemberTables("dave"));
        Assert.Throws<ObjectDisposedException>(() => index.GetMemberTables("dave", out _));
        Assert.Throws<ObjectDisposedException>(() => index.ClearTable(tableA));
        Assert.Throws<ObjectDisposedException>(() => index.Clear());
    }

    [Fact]
    public void CrossTableRegistry_FactoryAdd_InvokesConstructorAndRejectsDuplicates()
    {
        using var registry = new IndexesRegistryHarness.CrossTableRegistryHandle();
        var invocations = 0;

        var first = registry.Add<string, int>(() =>
        {
            invocations++;
            return new IndexesRegistryHarness.ProbeCrossTableIndex<string, int>("cross-factory");
        });

        Assert.Equal(1, invocations);
        Assert.Equal("cross-factory", first.Name);

        Assert.Throws<InvalidOperationException>(() => registry.Add<string, int>(() =>
        {
            invocations++;
            return new IndexesRegistryHarness.ProbeCrossTableIndex<string, int>("cross-factory");
        }));

        Assert.Equal(2, invocations);
    }
}
