using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Metadata.Storage.Internal;
using System.Collections;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Extend0.Testing.Metadata.Storage;

public static class MetadataStorageHarness
{
    private static readonly MethodInfo MappedStoreThrowParsedMethod =
        typeof(MappedStore).GetMethod("ThrowParsed", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new MissingMethodException(typeof(MappedStore).FullName, "ThrowParsed");

    public readonly record struct ColumnBlockSnapshot(int Stride, int ValueSize, int ValueOffset);

    public static ICellStore CreateInMemoryStore(params ColumnConfiguration[] columns) =>
        new InMemoryStore(columns);

    public static ICellStore CreateEnumerableOnlyStore(params CellRowColumnValueEntry[] entries) =>
        new EnumerableOnlyStore(entries);

    public static uint GetColumnCount(ICellStore store) =>
        ((InMemoryStore)store).ColumnCount;

    public static ColumnConfiguration GetColumnMeta(ICellStore store, uint column) =>
        ((InMemoryStore)store).MetaAt(column);

    public static bool TryGrowColumnTo(
        ICellStore store,
        uint column,
        uint minRows,
        in ColumnConfiguration meta,
        bool zeroInit) =>
        ((InMemoryStore)store).TryGrowColumnTo(column, minRows, meta, zeroInit);

    public static bool TryGetColumnCapacity(ICellStore store, uint column, out uint capacity) =>
        ((InMemoryStore)store).TryGetColumnCapacity(column, out capacity);

    public static bool TryGetColumnBlock(ICellStore store, uint column) =>
        ((InMemoryStore)store).TryGetColumnBlock(column, out _);

    public static ICellStore CreateMappedStore(TableSpec spec) =>
        new MappedStore(spec);

    public static bool TryLoadMappedColumns(string path, out ColumnConfiguration[] columns) =>
        MappedStore.TryLoadColumns(path, out columns);

    public static uint GetMappedColumnCount(ICellStore store) =>
        ((MappedStore)store).ColumnCount;

    public static ColumnConfiguration GetMappedColumnMeta(ICellStore store, uint column) =>
        ((MappedStore)store).GetColumnConfiguration(column);

    public static bool TryGetMappedColumnBlock(ICellStore store, uint column, out ColumnBlockSnapshot block)
    {
        var ok = ((MappedStore)store).TryGetColumnBlock(column, out var raw);
        block = new ColumnBlockSnapshot(raw.Stride, raw.ValueSize, raw.ValueOffset);
        return ok;
    }

    public static bool TryGrowMappedColumnTo(ICellStore store, uint column, uint minRows, in ColumnConfiguration meta, bool zeroInit) =>
        ((MappedStore)store).TryGrowColumnTo(column, minRows, meta, zeroInit);

    public static bool TryGetMappedColumnCapacity(ICellStore store, uint column, out uint capacity) =>
        ((ITryGrowableStore)((MappedStore)store)).TryGetColumnCapacity(column, out capacity);

    public static Task CompactMappedStore(ICellStore store, bool strict, CancellationToken cancellationToken = default) =>
        ((ICompactableStore)((MappedStore)store)).Compact(strict, cancellationToken);

    public static ICellStore CreateSegmentedMappedStore(TableSpec spec) =>
        new SegmentedMappedStore(spec);

    public static bool TryLoadSegmentedColumns(string path, out ColumnConfiguration[] columns) =>
        SegmentedMappedStore.TryLoadColumns(path, out columns);

    public static uint GetSegmentedColumnCount(ICellStore store) =>
        ((SegmentedMappedStore)store).ColumnCount;

    public static ColumnConfiguration GetSegmentedColumnMeta(ICellStore store, uint column) =>
        ((SegmentedMappedStore)store).GetColumnConfiguration(column);

    public static bool TryGrowSegmentedColumnTo(ICellStore store, uint column, uint minRows, in ColumnConfiguration meta, bool zeroInit) =>
        ((SegmentedMappedStore)store).TryGrowColumnTo(column, minRows, meta, zeroInit);

    public static bool TryGetSegmentedColumnCapacity(ICellStore store, uint column, out uint capacity) =>
        ((ITryGrowableStore)((SegmentedMappedStore)store)).TryGetColumnCapacity(column, out capacity);

    public static Task CompactSegmentedStore(ICellStore store, bool strict, CancellationToken cancellationToken = default) =>
        ((ICompactableStore)((SegmentedMappedStore)store)).Compact(strict, cancellationToken);

    public static void InvokeMappedStoreThrowParsed(Action? action)
    {
        try
        {
            MappedStoreThrowParsedMethod.Invoke(null, [action]);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    private sealed class EnumerableOnlyStore(IEnumerable<CellRowColumnValueEntry> entries) : ICellStore
    {
        private readonly CellRowColumnValueEntry[] _entries = entries.ToArray();

        public int Count => _entries.Length;

        public bool TryGetColumnBlock(uint column, out ColumnBlock block)
        {
            block = default;
            return false;
        }

        public bool TryGetCell(uint col, uint row, out MetadataCell cell)
        {
            foreach (var entry in _entries)
            {
                if (entry.Col == col && entry.Row == row)
                {
                    cell = entry.Cell;
                    return true;
                }
            }

            cell = default;
            return false;
        }

        public MetadataCell GetOrCreateCell(uint col, uint row, in ColumnConfiguration meta) =>
            TryGetCell(col, row, out var cell)
                ? cell
                : throw new InvalidOperationException("This test store only exposes its own enumerable.");

        public CellEnumerable EnumerateCells() => new(this);

        public IEnumerator<CellRowColumnValueEntry> GetEnumerator() =>
            ((IEnumerable<CellRowColumnValueEntry>)_entries).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Dispose()
        {
        }
    }
}
