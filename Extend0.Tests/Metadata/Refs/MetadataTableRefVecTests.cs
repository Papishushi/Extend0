using Extend0.Metadata.Refs;

namespace Extend0.Tests.Metadata.Refs;

public sealed class MetadataTableRefVecTests
{
    [Fact]
    public void Init_SetsCountToZero_AndMarksInitialized()
    {
        var buffer = new byte[MetadataTableRefVec.HeaderSize + MetadataTableRefVec.EntrySize * 2];

        MetadataTableRefVec.Init(buffer);

        Assert.Equal((ushort)0, MetadataTableRefVec.GetCount(buffer));
        Assert.True(MetadataTableRefVec.IsInitialized(buffer));
    }

    [Fact]
    public void TryAdd_AndTryGet_RoundTrip()
    {
        var buffer = new byte[MetadataTableRefVec.HeaderSize + MetadataTableRefVec.EntrySize * 2];
        MetadataTableRefVec.Init(buffer);
        var first = new MetadataTableRef(Guid.NewGuid(), 1, 2, 0);
        var second = new MetadataTableRef(Guid.NewGuid(), 3, 4, 0);

        var addedFirst = MetadataTableRefVec.TryAdd(buffer, first, buffer.Length);
        var addedSecond = MetadataTableRefVec.TryAdd(buffer, second, buffer.Length);
        var gotFirst = MetadataTableRefVec.TryGet(buffer, 0, out var readFirst);
        var gotSecond = MetadataTableRefVec.TryGet(buffer, 1, out var readSecond);
        var directSecond = MetadataTableRefVec.ReadAt(buffer, 1);

        Assert.True(addedFirst);
        Assert.True(addedSecond);
        Assert.True(gotFirst);
        Assert.True(gotSecond);
        Assert.Equal(first, readFirst);
        Assert.Equal(second, readSecond);
        Assert.Equal(second, directSecond);
        Assert.Equal((ushort)2, MetadataTableRefVec.GetCount(buffer));
    }

    [Fact]
    public void TryRemoveAt_CompactsTail()
    {
        var buffer = new byte[MetadataTableRefVec.HeaderSize + MetadataTableRefVec.EntrySize * 3];
        MetadataTableRefVec.Init(buffer);
        var first = new MetadataTableRef(Guid.NewGuid(), 1, 1, 0);
        var second = new MetadataTableRef(Guid.NewGuid(), 2, 2, 0);
        var third = new MetadataTableRef(Guid.NewGuid(), 3, 3, 0);
        MetadataTableRefVec.TryAdd(buffer, first, buffer.Length);
        MetadataTableRefVec.TryAdd(buffer, second, buffer.Length);
        MetadataTableRefVec.TryAdd(buffer, third, buffer.Length);

        var removed = MetadataTableRefVec.TryRemoveAt(buffer, 1);
        var gotTail = MetadataTableRefVec.TryGet(buffer, 1, out var tail);

        Assert.True(removed);
        Assert.True(gotTail);
        Assert.Equal(third, tail);
        Assert.Equal((ushort)2, MetadataTableRefVec.GetCount(buffer));
    }

    [Fact]
    public void Find_ReturnsIndex_ForMatchingReference()
    {
        var buffer = new byte[MetadataTableRefVec.HeaderSize + MetadataTableRefVec.EntrySize * 2];
        MetadataTableRefVec.Init(buffer);
        var target = new MetadataTableRef(Guid.NewGuid(), 7, 9, 0);
        MetadataTableRefVec.TryAdd(buffer, new MetadataTableRef(Guid.NewGuid(), 1, 2, 0), buffer.Length);
        MetadataTableRefVec.TryAdd(buffer, target, buffer.Length);

        var index = MetadataTableRefVec.Find(buffer, target.TableId, target.Column, target.Row);

        Assert.Equal(1, index);
    }
}
