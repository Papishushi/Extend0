using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Files;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;
using Extend0.Testing.Metadata.Storage;

namespace Extend0.Tests.Metadata.Storage;

public sealed class StorageStructTests
{
    [Fact]
    public void ColumnDesc_UsesValueSemantics_AndComputesEntrySize()
    {
        var left = new ColumnDesc(32, 64, 5, 128);
        var same = new ColumnDesc(32, 64, 5, 128);
        var different = new ColumnDesc(32, 128, 5, 128);

        Assert.Equal(96, left.EntrySizeBytes);
        Assert.Equal(left, same);
        Assert.True(left == same);
        Assert.True(left != different);
        Assert.NotEqual(left.GetHashCode(), different.GetHashCode());
    }

    [Fact]
    public void FileHeader_UsesEqualityAndFieldOrdering()
    {
        var left = StorageInternalHarness.CreateFileHeader(0x4C42544D, 1, 2, 16);
        var same = StorageInternalHarness.CreateFileHeader(0x4C42544D, 1, 2, 16);
        var later = StorageInternalHarness.CreateFileHeader(0x4C42544D, 1, 3, 16);

        Assert.Equal(left, same);
        Assert.NotEqual(left, later);
        Assert.True(left.CompareTo(later) < 0);
        Assert.True(later.CompareTo(left) > 0);
    }

    [Fact]
    public void ColumnBlock_ComputesValuePointers_ByStrideAndOffset()
    {
        using var handle = StorageInternalHarness.CreateColumnBlock(new byte[64], stride: 8, valueSize: 4, valueOffset: 2);
        var first = handle.GetValuePointer(0);
        var third = handle.GetValuePointer(2);

        Assert.Equal(8, handle.Stride);
        Assert.Equal(4, handle.ValueSize);
        Assert.Equal(2, handle.ValueOffset);
        Assert.Equal(16, third - first);
        Assert.NotEqual(0, handle.HashCode);
    }

    [Fact]
    public void ColumnBlock_UsesEqualityOperatorsAndObjectEquality()
    {
        var shared = new byte[64];
        using var left = StorageInternalHarness.CreateColumnBlock(shared, stride: 8, valueSize: 4, valueOffset: 2);
        using var same = StorageInternalHarness.CreateColumnBlock(shared, stride: 8, valueSize: 4, valueOffset: 2);
        using var different = StorageInternalHarness.CreateColumnBlock(new byte[64], stride: 8, valueSize: 4, valueOffset: 3);

        Assert.True(left.Equals(same));
        Assert.True(left.OperatorsEqual(same));
        Assert.False(left.OperatorsNotEqual(same));
        Assert.False(left.EqualsAsObject(same));
        Assert.True(left.EqualsBlockAsObject(same));
        Assert.False(left.Equals(different));
        Assert.False(left.OperatorsEqual(different));
        Assert.True(left.OperatorsNotEqual(different));
        Assert.False(left.EqualsBlockAsObject(different));
        Assert.False(left.EqualsAsObject("not-a-block"));
    }

    [Fact]
    public void CellRowColumnValueEntry_ExposesPointerOrdering_AndTupleConversions()
    {
        using var store = MetadataStorageHarness.CreateInMemoryStore(TableSpec.Helpers.Column("Name", 1, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);
        var cell = store.GetOrCreateCell(0, 0, meta);
        var left = new CellRowColumnValueEntry(new MetadataCellPointer(0, 0), cell);
        var right = new CellRowColumnValueEntry(new MetadataCellPointer(1, 0), cell);
        var tuple = ((uint Col, uint Row, MetadataCell Cell))left;
        (uint Col, uint Row, MetadataCell Cell) fromTuple = (0u, 0u, cell);
        CellRowColumnValueEntry roundTrip = fromTuple;

        Assert.Equal((uint)0, left.Col);
        Assert.Equal((uint)0, left.Row);
        Assert.True(left.CompareTo(right) < 0);
        Assert.Equal((uint)0, tuple.Col);
        Assert.Equal((uint)0, tuple.Row);
        Assert.Equal(cell, tuple.Cell);
        Assert.Equal(left.Pointer, roundTrip.Pointer);
    }
}
