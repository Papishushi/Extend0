using Extend0.Metadata.Contract;
using Extend0.Metadata.Internal;
using Extend0.Metadata.Refs;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata;
using System.Runtime.CompilerServices;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Extend0.Testing.Metadata.Internal;

public static unsafe class MetaDBManagerHelpersHarness
{
    private static readonly MethodInfo EnsureRowCapacityMethod =
        typeof(MetaDBManagerHelpers).GetMethod("EnsureRowCapacity", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new MissingMethodException(typeof(MetaDBManagerHelpers).FullName, "EnsureRowCapacity");

    public readonly record struct DeleteCycleStatsSnapshot(int Deleted, int Attempts, double SuccessRate);
    public readonly record struct CycleBudget(int MaxDeletesPerCycle, int MaxAttemptsPerCycle);
    public readonly record struct CooldownState(int StormScore, int CompactCooldownMs);

    public static void ValidateTableSpec(TableSpec spec) =>
        MetaDBManagerHelpers.ValidateTableSpec(spec);

    public static int ComputeBatchFromValueSize(uint valueSize) =>
        MetaDBManagerHelpers.ComputeBatchFromValueSize(valueSize);

    public static uint GetColumnValueSize(IMetadataTable table, uint column) =>
        MetaDBManagerHelpers.GetColumnValueSize(table, column);

    public static Task<bool> TryDeleteWithRetries(string path, int attempts = 8) =>
        MetaDBManagerHelpers.TryDeleteWithRetries(path, attempts);

    public static string? TryMoveAside(string path) =>
        MetaDBManagerHelpers.TryMoveAside(path);

    public static void EnsureDirForFile(string path) =>
        MetaDBManagerHelpers.EnsureDirForFile(path);

    public static CycleBudget ComputeCycleBudget(int backlog, int baseDeletes, int baseAttempts)
    {
        MetaDBManagerHelpers.ComputeCycleBudget(backlog, baseDeletes, baseAttempts, out var maxDeletesPerCycle, out var maxAttemptsPerCycle);
        return new CycleBudget(maxDeletesPerCycle, maxAttemptsPerCycle);
    }

    public static int EstimateArrivals(int backlogBefore, int backlogAfter, int deletedThisCycle) =>
        MetaDBManagerHelpers.EstimateArrivals(backlogBefore, backlogAfter, deletedThisCycle);

    public static bool IsStorm(int backlogBefore, int backlogAfter, int deletedThisCycle, int arrivalsThisCycle) =>
        MetaDBManagerHelpers.IsStorm(backlogBefore, backlogAfter, deletedThisCycle, arrivalsThisCycle);

    public static CooldownState UpdateCompactionCooldown(bool storm, int stormScore, int compactCooldownMs, int minCooldownMs, int maxCooldownMs)
    {
        MetaDBManagerHelpers.UpdateCompactionCooldown(storm, ref stormScore, ref compactCooldownMs, minCooldownMs, maxCooldownMs);
        return new CooldownState(stormScore, compactCooldownMs);
    }

    public static int ComputeNextDelayMs(int deleted, int attempts, int busyDelayMs, int idleDelayMs) =>
        MetaDBManagerHelpers.ComputeNextDelayMs(new MetaDBManager.DeleteCycleStats(deleted, attempts), busyDelayMs, idleDelayMs);

    public static DeleteCycleStatsSnapshot CreateDeleteCycleStats(int deleted, int attempts)
    {
        var stats = new MetaDBManager.DeleteCycleStats(deleted, attempts);
        return new DeleteCycleStatsSnapshot(stats.Deleted, stats.Attempts, stats.SuccessRate);
    }

    public static Task DelaySafe(int ms, CancellationToken cancellationToken) =>
        MetaDBManagerHelpers.DelaySafe(ms, cancellationToken);

    public static bool EnsureRefVec(IMetadataTable table, uint refsCol, uint parentRow, CapacityPolicy policy) =>
        MetaDBManagerHelpers.EnsureRefVec(table, refsCol, parentRow, policy);

    public static void LinkRef(IMetadataTable parent, uint refsCol, uint parentRow, in MetadataTableRef reference) =>
        MetaDBManagerHelpers.LinkRef(parent, refsCol, parentRow, reference);

    public static bool TryHasRef(IMetadataTable table, uint refsCol, uint parentRow, in MetadataTableRef reference) =>
        MetaDBManagerHelpers.TryHasRef(table, refsCol, parentRow, reference);

    public static int GetRefCount(IMetadataTable table, uint refsCol, uint parentRow)
    {
        var cell = table.GetOrCreateCell(refsCol, parentRow);
        var buffer = new ReadOnlySpan<byte>(cell.GetValuePointer(), cell.ValueSize);
        return MetadataTableRefVec.GetCount(buffer);
    }

    public static void FillUInt32Column(IMetadataTable table, uint column, uint rows, uint seed, CapacityPolicy policy, int batchSize = 512) =>
        MetaDBManagerHelpers.FillColumn(table, column, rows, row => seed + row, policy, batchSize);

    public static void FillRawUInt64Column(IMetadataTable table, uint column, uint rows, ulong seed, CapacityPolicy policy, int batchSize = 512) =>
        MetaDBManagerHelpers.FillColumn(table, column, rows, (row, ptr, valueSize) =>
        {
            new Span<byte>((void*)ptr, checked((int)valueSize)).Clear();
            Unsafe.WriteUnaligned((void*)ptr, seed + row);
        }, policy, batchSize);

    public static void CopyColumn(IMetadataTable source, uint sourceColumn, IMetadataTable destination, uint destinationColumn, uint rows, CapacityPolicy policy, int batchSize = 512) =>
        MetaDBManagerHelpers.CopyColumn(source, sourceColumn, destination, destinationColumn, rows, policy, batchSize);

    public static void EnsureRowCapacity(IMetadataTable table, uint column, uint neededRows, CapacityPolicy policy)
    {
        try
        {
            EnsureRowCapacityMethod.Invoke(null, [table, column, neededRows, policy]);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    public static byte[] ReadValueBytes(IMetadataTable table, uint column, uint row)
    {
        var cell = table.GetOrCreateCell(column, row);
        var bytes = new byte[cell.ValueSize];
        new ReadOnlySpan<byte>(cell.GetValuePointer(), cell.ValueSize).CopyTo(bytes);
        return bytes;
    }

    public static uint ReadUInt32(IMetadataTable table, uint column, uint row)
    {
        var cell = table.GetOrCreateCell(column, row);
        return Unsafe.ReadUnaligned<uint>(cell.GetValuePointer());
    }

    public static ulong ReadUInt64(IMetadataTable table, uint column, uint row)
    {
        var cell = table.GetOrCreateCell(column, row);
        return Unsafe.ReadUnaligned<ulong>(cell.GetValuePointer());
    }
}
