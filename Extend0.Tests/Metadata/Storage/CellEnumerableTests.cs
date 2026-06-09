using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Testing.Metadata.Storage;

namespace Extend0.Tests.Metadata.Storage;

public sealed class CellEnumerableTests
{
    [Fact]
    public void DefaultEnumerable_HasStableEquality_AndNoValues()
    {
        var left = default(CellEnumerable);
        var right = default(CellEnumerable);

        Assert.Equal(left, right);
        Assert.True(left == right);
        Assert.False(left != right);
        Assert.Equal(0, left.GetHashCode());
        Assert.Empty(left.ToArray());
    }

    [Fact]
    public void Enumerable_EnumeratesOnlyCreatedCells()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 3, valueBytes: 64),
            TableSpec.Helpers.Column("Score", 2, valueBytes: 64));
        var firstMeta = MetadataStorageHarness.GetColumnMeta(store, 0);
        var secondMeta = MetadataStorageHarness.GetColumnMeta(store, 1);

        store.GetOrCreateCell(0, 1, firstMeta);
        store.GetOrCreateCell(1, 0, secondMeta);

        var cells = store.EnumerateCells().ToArray();

        Assert.Equal(2, cells.Length);
        Assert.Equal((uint)0, cells[0].Col);
        Assert.Equal((uint)1, cells[0].Row);
        Assert.Equal((uint)1, cells[1].Col);
        Assert.Equal((uint)0, cells[1].Row);
    }

    [Fact]
    public void Enumerator_Reset_ReplaysSequence_AndDisposeStopsEnumeration()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 2, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);
        store.GetOrCreateCell(0, 0, meta);

        var enumerator = store.EnumerateCells().GetEnumerator();

        Assert.True(enumerator.MoveNext());
        var first = enumerator.Current;
        Assert.False(enumerator.MoveNext());

        enumerator.Reset();

        Assert.True(enumerator.MoveNext());
        Assert.Equal(first, enumerator.Current);

        enumerator.Dispose();

        Assert.False(enumerator.MoveNext());
    }

    [Fact]
    public async Task AsyncEnumerable_RespectsCancellation_AndCanEnumerate()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 2, valueBytes: 64));
        var meta = MetadataStorageHarness.GetColumnMeta(store, 0);
        store.GetOrCreateCell(0, 0, meta);
        store.GetOrCreateCell(0, 1, meta);

        var cancelled = new CancellationToken(canceled: true);
        var asyncEnumerator = store.EnumerateCells().AsAsync().GetAsyncEnumerator(cancelled);

        await Assert.ThrowsAsync<OperationCanceledException>(async () => await asyncEnumerator.MoveNextAsync());

        var liveEnumerator = store.EnumerateCells().AsAsync().GetAsyncEnumerator();
        var movedFirst = await liveEnumerator.MoveNextAsync();
        var first = liveEnumerator.Current;
        var movedSecond = await liveEnumerator.MoveNextAsync();
        var second = liveEnumerator.Current;
        var movedEnd = await liveEnumerator.MoveNextAsync();
        await liveEnumerator.DisposeAsync();

        Assert.True(movedFirst);
        Assert.Equal((uint)0, first.Col);
        Assert.Equal((uint)0, first.Row);
        Assert.True(movedSecond);
        Assert.Equal((uint)0, second.Col);
        Assert.Equal((uint)1, second.Row);
        Assert.False(movedEnd);
    }

    [Fact]
    public void Enumerable_EqualityTracksBackingStoreIdentity()
    {
        using ICellStore store = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 1, valueBytes: 64));
        using ICellStore otherStore = MetadataStorageHarness.CreateInMemoryStore(
            TableSpec.Helpers.Column("Id", 1, valueBytes: 64));

        var left = store.EnumerateCells();
        var same = store.EnumerateCells();
        var other = otherStore.EnumerateCells();

        Assert.Equal(left, same);
        Assert.NotEqual(left, other);
        Assert.True(left == same);
        Assert.True(left != other);
    }

    [Fact]
    public void Enumerable_DelegatesToAnyCellStoreEnumerator()
    {
        var expected = new CellRowColumnValueEntry(
            new MetadataCellPointer(row: 7, column: 3),
            default);
        using ICellStore store = MetadataStorageHarness.CreateEnumerableOnlyStore(expected);

        var entries = new CellEnumerable(store).ToArray();

        var actual = Assert.Single(entries);
        Assert.Equal(expected, actual);
        Assert.Equal((uint)3, actual.Col);
        Assert.Equal((uint)7, actual.Row);
    }

    [Fact]
    public void MappedEnumerable_UsesMappedIdentityHash_AndNonGenericEnumerator()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "cells.map");
            var spec = new TableSpec("Cells", mapPath, [TableSpec.Helpers.Column("Value", 2, valueBytes: 64)]);

            using ICellStore mapped = MetadataStorageHarness.CreateMappedStore(spec);
            using ICellStore memory = MetadataStorageHarness.CreateInMemoryStore(TableSpec.Helpers.Column("Value", 2, valueBytes: 64));
            var mappedMeta = MetadataStorageHarness.GetMappedColumnMeta(mapped, 0);

            mapped.GetOrCreateCell(0, 1, mappedMeta);

            var mappedEnumerable = mapped.EnumerateCells();
            var memoryEnumerable = memory.EnumerateCells();
            var nonGeneric = ((System.Collections.IEnumerable)mappedEnumerable).GetEnumerator();

            Assert.NotEqual(mappedEnumerable.GetHashCode(), memoryEnumerable.GetHashCode());
            Assert.True(nonGeneric.MoveNext());

            var first = Assert.IsType<CellRowColumnValueEntry>(nonGeneric.Current);
            Assert.Equal((uint)0, first.Col);
            Assert.True(first.Row <= 1);

            var count = 1;
            while (nonGeneric.MoveNext())
            {
                _ = Assert.IsType<CellRowColumnValueEntry>(nonGeneric.Current);
                count++;
            }

            Assert.True(count >= 1);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedEnumerator_ReturnsFalse_WhenOnlyColumnHasZeroCapacity()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "empty-cells.map");
            var spec = new TableSpec("EmptyCells", mapPath, [TableSpec.Helpers.Column("Value", 0, valueBytes: 64)]);

            using ICellStore mapped = MetadataStorageHarness.CreateMappedStore(spec);
            var enumerator = mapped.EnumerateCells().GetEnumerator();

            Assert.Empty(mapped.EnumerateCells().ToArray());
            Assert.False(enumerator.MoveNext());

            enumerator.Reset();

            Assert.False(enumerator.MoveNext());
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void MappedEnumerator_SkipsZeroCapacityColumns_AndExposesNonGenericCurrent()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "mixed-cells.map");
            var spec = new TableSpec("MixedCells", mapPath,
            [
                TableSpec.Helpers.Column("Empty", 0, valueBytes: 64),
                TableSpec.Helpers.Column("Value", 2, valueBytes: 64)
            ]);

            using ICellStore mapped = MetadataStorageHarness.CreateMappedStore(spec);
            var entries = mapped.EnumerateCells().ToArray();

            Assert.Equal(2, entries.Length);
            Assert.All(entries, static entry => Assert.Equal((uint)1, entry.Col));
            Assert.Equal((uint)0, entries[0].Row);
            Assert.Equal((uint)1, entries[1].Row);

            var boxed = (System.Collections.IEnumerator)mapped.EnumerateCells().GetEnumerator();

            Assert.True(boxed.MoveNext());
            var current = Assert.IsType<CellRowColumnValueEntry>(boxed.Current);
            Assert.Equal((uint)1, current.Col);
            Assert.Equal((uint)0, current.Row);
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
}
