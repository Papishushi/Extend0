using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Testing.Metadata.Storage;

namespace Extend0.Tests.Metadata.Storage;

public sealed class InMemoryStoreTests
{
    private readonly record struct SamplePair(int Left, int Right);

    [Fact]
    public void CreateStore_ExposesColumnMetadata_AndStartsEmpty()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 2, valueBytes: 64),
            TableSpec.Helpers.Column("Blob", 3, valueBytes: 64));

        Assert.Equal(0, store.Count);
        Assert.Equal((uint)2, MetadataStorageHarness.GetColumnCount(store));
        Assert.Equal("Id", MetadataStorageHarness.GetColumnMeta(store, 0).Name);
        Assert.Equal("Blob", MetadataStorageHarness.GetColumnMeta(store, 1).Name);
    }

    [Fact]
    public void GetOrCreateCell_ReusesExistingCell_AndTracksCapacity()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 4, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);

        var first = store.GetOrCreateCell(0, 2, meta);
        var second = store.GetOrCreateCell(0, 2, meta);
        var got = store.TryGetCell(0, 2, out var existing);
        var capacityOk = MetadataStorageHarness.TryGetColumnCapacity(store, 0, out var capacity);

        Assert.True(got);
        Assert.True(capacityOk);
        Assert.Equal(1, store.Count);
        Assert.Equal(first, second);
        Assert.Equal(first, existing);
        Assert.Equal((uint)3, capacity);
    }

    [Fact]
    public void ColumnOfInt_DefaultKeyBytes_MaterializesSuccessfully()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column<int>("Int32Column", 1));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);

        var cell = store.GetOrCreateCell(0, 0, meta);

        Assert.True(cell.TryGetKeyRaw(out var key));
        Assert.Equal(meta.Size.GetKeySize(), key.Length);
        Assert.Equal("Int32Column"u8.ToArray(), key[.."Int32Column".Length].ToArray());
        Assert.True(cell.TryGetValueRaw(out var raw));
        Assert.Equal(4, raw.Length);
    }

    [Fact]
    public void ColumnOfBlittableStruct_DefaultKeyBytes_MaterializesSuccessfully()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column<SamplePair>("PairColumn", 1));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);

        var cell = store.GetOrCreateCell(0, 0, meta);

        Assert.True(cell.TryGetKeyRaw(out var key));
        Assert.Equal(meta.Size.GetKeySize(), key.Length);
        Assert.Equal("PairColumn"u8.ToArray(), key[.."PairColumn".Length].ToArray());
        Assert.True(cell.TryGetValueRaw(out var raw));
        Assert.Equal(8, raw.Length);
    }

    [Fact]
    public void ColumnOfInt_ValueOnly_MaterializesSuccessfully()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column<int>("ValueOnlyInt32", 1, keyBytes: 0));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);

        var cell = store.GetOrCreateCell(0, 0, meta);

        Assert.False(cell.TryGetKeyRaw(out _));
        Assert.True(cell.TryGetValueRaw(out var raw));
        Assert.Equal(4, raw.Length);
    }

    [Fact]
    public void FixedSizeBlobColumn_WithGeneratedShape_MaterializesSuccessfully()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Blob", 1, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);

        var cell = store.GetOrCreateCell(0, 0, meta);

        Assert.True(cell.TryGetKeyRaw(out var key));
        Assert.Equal(meta.Size.GetKeySize(), key.Length);
        Assert.Equal("Blob"u8.ToArray(), key[.."Blob".Length].ToArray());
        Assert.All(key["Blob".Length..].ToArray(), static b => Assert.Equal(0, b));
        Assert.True(cell.TryGetValueRaw(out var raw));
        Assert.Equal(64, raw.Length);
    }

    [Fact]
    public void TryGrowColumnTo_MaterializesRows_AndUpdatesCapacity()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 1, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);

        var grown = MetadataStorageHarness.TryGrowColumnTo(store, 0, 3, meta, zeroInit: true);
        var got0 = store.TryGetCell(0, 0, out _);
        var got1 = store.TryGetCell(0, 1, out _);
        var got2 = store.TryGetCell(0, 2, out _);
        var capacityOk = MetadataStorageHarness.TryGetColumnCapacity(store, 0, out var capacity);

        Assert.True(grown);
        Assert.True(got0);
        Assert.True(got1);
        Assert.True(got2);
        Assert.True(capacityOk);
        Assert.Equal(3, store.Count);
        Assert.Equal((uint)3, capacity);
    }

    [Fact]
    public void TryGrowColumnTo_ZeroRows_AndAlreadyLargeEnough_AreNoOps()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 1, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);

        var zeroRows = MetadataStorageHarness.TryGrowColumnTo(store, 0, 0, meta, zeroInit: false);
        var firstGrowth = MetadataStorageHarness.TryGrowColumnTo(store, 0, 2, meta, zeroInit: false);
        var secondGrowth = MetadataStorageHarness.TryGrowColumnTo(store, 0, 1, meta, zeroInit: true);
        var capacityOk = MetadataStorageHarness.TryGetColumnCapacity(store, 0, out var capacity);

        Assert.True(zeroRows);
        Assert.True(firstGrowth);
        Assert.True(secondGrowth);
        Assert.True(capacityOk);
        Assert.Equal(2, store.Count);
        Assert.Equal((uint)2, capacity);
    }

    [Fact]
    public void TryGetColumnCapacity_ReturnsFalse_ForOutOfRangeColumn()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 1, valueBytes: 64));

        var ok = MetadataStorageHarness.TryGetColumnCapacity(store, 4, out var capacity);

        Assert.False(ok);
        Assert.Equal((uint)0, capacity);
    }

    [Fact]
    public void TryGetColumnBlock_ReturnsFalse_ForInMemoryLayout()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 1, valueBytes: 64));

        var ok = MetadataStorageHarness.TryGetColumnBlock(store, 0);

        Assert.False(ok);
    }

    [Fact]
    public void Dispose_ClearsCells()
    {
        var store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 1, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);
        store.GetOrCreateCell(0, 0, meta);

        store.Dispose();

        Assert.Equal(0, store.Count);
        Assert.False(store.TryGetCell(0, 0, out _));
    }
}
