using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Metadata.Storage.Internal;
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
}
