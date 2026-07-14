using Extend0.Metadata.Contract;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Indexing.Registries.Contract;
using Extend0.Metadata.Refs;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Testing.Metadata.Internal;
using Extend0.Testing.Metadata.Storage;

namespace Extend0.Tests.Metadata.Internal;

[Collection(Extend0.Tests.Metadata.Storage.MappedStorageCollection.Name)]
public sealed class MetaDBManagerHelpersTests
{
    [Fact]
    public void ValidateTableSpec_RejectsMissingRequiredFields_AndAcceptsValidSpec()
    {
        Assert.Throws<ArgumentException>(() => MetaDBManagerHelpersHarness.ValidateTableSpec(new TableSpec("", "users.map", [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)])));
        Assert.Throws<ArgumentException>(() => MetaDBManagerHelpersHarness.ValidateTableSpec(new TableSpec("Users", "", [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)])));
        Assert.Throws<ArgumentException>(() => MetaDBManagerHelpersHarness.ValidateTableSpec(new TableSpec("Users", "users.map", [])));

        MetaDBManagerHelpersHarness.ValidateTableSpec(new TableSpec("Users", "users.map", [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]));
    }

    [Fact]
    public void TextRenderingHelpers_CoverBorderPaddingUtf8AndHexFallbacks()
    {
        var border = MetadataTableHelpersHarness.Border(2, [3, 4, 5]);
        var padded = MetadataTableHelpersHarness.Pad("abc", 5);
        var unchanged = MetadataTableHelpersHarness.Pad("abcdef", 3);
        var emptyPreview = MetadataTableHelpersHarness.Preview([], maxChars: 5);
        var utf8Preview = MetadataTableHelpersHarness.Preview("Alice\0ignored"u8.ToArray(), maxChars: 10);
        var utf8Ellipsis = MetadataTableHelpersHarness.Preview("Hello world"u8.ToArray(), maxChars: 6);
        var utf8NoEllipsisWhenItFits = MetadataTableHelpersHarness.Preview("Hello"u8.ToArray(), maxChars: 5);
        var utf8EmptyWhenWidthIsZero = MetadataTableHelpersHarness.Preview("Hello"u8.ToArray(), maxChars: 0);
        var allowedControlUtf8 = MetadataTableHelpersHarness.Preview("A\tB\n"u8.ToArray(), maxChars: 8);
        var surrogateText = "A" + char.ConvertFromUtf32(0x1F642) + "B";
        var surrogateSafeEllipsis = MetadataTableHelpersHarness.Preview(System.Text.Encoding.UTF8.GetBytes(surrogateText), maxChars: 3);
        var controlFallback = MetadataTableHelpersHarness.Preview([0x01, 0x02, 0x03], maxChars: 5);
        var controlFallbackEmptyWhenWidthIsZero = MetadataTableHelpersHarness.Preview([0x01, 0x02], maxChars: 0);
        var invalidUtf8Fallback = MetadataTableHelpersHarness.Preview([0xF0, 0x28, 0x8C, 0x28], maxChars: 8);

        Assert.Equal("+-----+------+-------+\n", border);
        Assert.Equal("abc  ", padded);
        Assert.Equal("abcdef", unchanged);
        Assert.Equal(string.Empty, emptyPreview);
        Assert.Equal("Alice", utf8Preview);
        Assert.Equal("Hello…", utf8Ellipsis);
        Assert.Equal("Hello", utf8NoEllipsisWhenItFits);
        Assert.Equal(string.Empty, utf8EmptyWhenWidthIsZero);
        Assert.Equal("A\tB\n", allowedControlUtf8);
        Assert.Equal("A…", surrogateSafeEllipsis);
        Assert.Equal("0102…", controlFallback);
        Assert.Equal(string.Empty, controlFallbackEmptyWhenWidthIsZero);
        Assert.Equal("F0288C28", invalidUtf8Fallback);
    }

    [Fact]
    public void ComputeBatchFromValueSize_UsesDefaultClampAndAlignmentRules()
    {
        Assert.Equal(512, MetaDBManagerHelpersHarness.ComputeBatchFromValueSize(0));
        Assert.Equal(4096, MetaDBManagerHelpersHarness.ComputeBatchFromValueSize(1));
        Assert.Equal(132, MetaDBManagerHelpersHarness.ComputeBatchFromValueSize(1000));
        Assert.Equal(64, MetaDBManagerHelpersHarness.ComputeBatchFromValueSize(10_000));
    }

    [Fact]
    public void GetColumnValueSize_UsesBlocksOrExistingCells()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using IMetadataTable emptyInMemory = MetadataTableHarness.CreateInMemoryTable(
                "Users",
                TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

            using IMetadataTable mapped = MetadataTableHarness.CreateTable(new TableSpec(
                "MappedUsers",
                Path.Combine(tempRoot, "users.map"),
                [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]));

            using IMetadataTable populatedInMemory = MetadataTableHarness.CreateInMemoryTable(
                "UsersWithData",
                TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

            populatedInMemory.GetOrCreateCell(0, 0);

            Assert.Equal((uint)64, MetaDBManagerHelpersHarness.GetColumnValueSize(emptyInMemory, 0));
            Assert.Equal((uint)64, MetaDBManagerHelpersHarness.GetColumnValueSize(mapped, 0));
            Assert.Equal((uint)64, MetaDBManagerHelpersHarness.GetColumnValueSize(populatedInMemory, 0));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteAndMoveHelpers_HandleExistingAndMissingFiles()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var sourcePath = Path.Combine(tempRoot, "source.bin");
            var deletePath = Path.Combine(tempRoot, "delete.bin");
            var missingPath = Path.Combine(tempRoot, "missing.bin");

            File.WriteAllText(sourcePath, "source");
            File.WriteAllText(deletePath, "delete");

            var movedPath = MetaDBManagerHelpersHarness.TryMoveAside(sourcePath);
            var deleted = await MetaDBManagerHelpersHarness.TryDeleteWithRetries(deletePath, attempts: 1);
            var deletedMissing = await MetaDBManagerHelpersHarness.TryDeleteWithRetries(missingPath, attempts: 1);
            var movedMissing = MetaDBManagerHelpersHarness.TryMoveAside(missingPath);

            Assert.NotNull(movedPath);
            Assert.False(File.Exists(sourcePath));
            Assert.True(File.Exists(movedPath));
            Assert.True(deleted);
            Assert.True(deletedMissing);
            Assert.False(File.Exists(deletePath));
            Assert.Null(movedMissing);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteHelpers_CreateDirectories_AndHandleZeroAttemptFallback()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var nestedFile = Path.Combine(tempRoot, "a", "b", "c", "queue.log");
            var deletePath = Path.Combine(tempRoot, "zero-attempt.bin");

            MetaDBManagerHelpersHarness.EnsureDirForFile(nestedFile);
            File.WriteAllText(deletePath, "delete me");

            var deleted = await MetaDBManagerHelpersHarness.TryDeleteWithRetries(deletePath, attempts: 0);

            Assert.True(Directory.Exists(Path.GetDirectoryName(nestedFile)!));
            Assert.True(deleted);
            Assert.False(File.Exists(deletePath));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task TryDeleteWithRetries_ReturnsFalse_ForLockedFile_InsteadOfThrowing()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var lockedPath = Path.Combine(tempRoot, "locked.bin");
            File.WriteAllText(lockedPath, "locked");

            using var lockHandle = MetadataStorageHarness.AcquireStorageLease(lockedPath);
            var deleted = await MetaDBManagerHelpersHarness.TryDeleteWithRetries(lockedPath, attempts: 2);

            Assert.False(deleted);
            Assert.True(File.Exists(lockedPath));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Theory]
    [InlineData(12, 3, 5, 3, 5)]
    [InlineData(300, 3, 5, 6, 10)]
    [InlineData(1500, 3, 5, 12, 20)]
    [InlineData(12000, 3, 5, 24, 40)]
    public void ComputeCycleBudget_ScalesByBacklogThresholds(int backlog, int baseDeletes, int baseAttempts, int expectedDeletes, int expectedAttempts)
    {
        var budget = MetaDBManagerHelpersHarness.ComputeCycleBudget(backlog, baseDeletes, baseAttempts);

        Assert.Equal(expectedDeletes, budget.MaxDeletesPerCycle);
        Assert.Equal(expectedAttempts, budget.MaxAttemptsPerCycle);
    }

    [Fact]
    public void DeleteWorkerHeuristics_CoverStormAndCooldownBranches()
    {
        var stats = MetaDBManagerHelpersHarness.CreateDeleteCycleStats(deleted: 1, attempts: 4);
        var arrivals = MetaDBManagerHelpersHarness.EstimateArrivals(backlogBefore: 10, backlogAfter: 11, deletedThisCycle: 3);
        var noArrivals = MetaDBManagerHelpersHarness.EstimateArrivals(backlogBefore: 10, backlogAfter: 5, deletedThisCycle: 3);

        var trendStorm = MetaDBManagerHelpersHarness.IsStorm(backlogBefore: 10, backlogAfter: 12, deletedThisCycle: 1, arrivalsThisCycle: 0);
        var arrivalsStorm = MetaDBManagerHelpersHarness.IsStorm(backlogBefore: 10, backlogAfter: 8, deletedThisCycle: 2, arrivalsThisCycle: 2);
        var backlogStorm = MetaDBManagerHelpersHarness.IsStorm(backlogBefore: 2000, backlogAfter: 1500, deletedThisCycle: 10, arrivalsThisCycle: 1);
        var calm = MetaDBManagerHelpersHarness.IsStorm(backlogBefore: 10, backlogAfter: 5, deletedThisCycle: 4, arrivalsThisCycle: 1);

        var stormCooldown = MetaDBManagerHelpersHarness.UpdateCompactionCooldown(storm: true, stormScore: 2, compactCooldownMs: 100, minCooldownMs: 25, maxCooldownMs: 1000);
        var drainCooldown = MetaDBManagerHelpersHarness.UpdateCompactionCooldown(storm: false, stormScore: 2, compactCooldownMs: 100, minCooldownMs: 80, maxCooldownMs: 1000);

        Assert.Equal(1, stats.Deleted);
        Assert.Equal(4, stats.Attempts);
        Assert.Equal(0.25d, stats.SuccessRate, precision: 6);
        Assert.Equal(4, arrivals);
        Assert.Equal(0, noArrivals);
        Assert.True(trendStorm);
        Assert.True(arrivalsStorm);
        Assert.True(backlogStorm);
        Assert.False(calm);
        Assert.Equal(3, stormCooldown.StormScore);
        Assert.Equal(300, stormCooldown.CompactCooldownMs);
        Assert.Equal(0, drainCooldown.StormScore);
        Assert.Equal(80, drainCooldown.CompactCooldownMs);
    }

    [Fact]
    public async Task DelayAndBackoffHelpers_CoverBusyIdleAndCanceledCases()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var busy = MetaDBManagerHelpersHarness.ComputeNextDelayMs(deleted: 1, attempts: 10, busyDelayMs: 20, idleDelayMs: 100);
        var moderate = MetaDBManagerHelpersHarness.ComputeNextDelayMs(deleted: 0, attempts: 0, busyDelayMs: 20, idleDelayMs: 100);
        var idle = MetaDBManagerHelpersHarness.ComputeNextDelayMs(deleted: 0, attempts: 100, busyDelayMs: 20, idleDelayMs: 100);

        await MetaDBManagerHelpersHarness.DelaySafe(10, cts.Token);

        Assert.Equal(20, busy);
        Assert.Equal(50, moderate);
        Assert.Equal(100, idle);
    }

    [Fact]
    public void RefVectorHelpers_InitializeLinkAndQueryReferences()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Parents",
            TableSpec.Helpers.RefsColumn(2));

        var reference = new MetadataTableRef(Guid.NewGuid(), 1, 7, 0);

        var firstInit = MetaDBManagerHelpersHarness.EnsureRefVec(table, refsCol: 0, parentRow: 0, CapacityPolicy.Throw);
        var secondInit = MetaDBManagerHelpersHarness.EnsureRefVec(table, refsCol: 0, parentRow: 0, CapacityPolicy.Throw);

        MetaDBManagerHelpersHarness.LinkRef(table, refsCol: 0, parentRow: 0, reference);

        Assert.True(firstInit);
        Assert.False(secondInit);
        Assert.True(MetaDBManagerHelpersHarness.TryHasRef(table, refsCol: 0, parentRow: 0, reference));
        Assert.Equal(1, MetaDBManagerHelpersHarness.GetRefCount(table, refsCol: 0, parentRow: 0));
    }

    [Fact]
    public void TryHasRef_ReturnsFalse_WhenReferenceVectorHasNotBeenInitialized()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Parents",
            TableSpec.Helpers.RefsColumn(2));

        var reference = new MetadataTableRef(Guid.NewGuid(), 1, 7, 0);

        Assert.False(MetaDBManagerHelpersHarness.TryHasRef(table, refsCol: 0, parentRow: 0, reference));
    }

    [Fact]
    public void LinkRef_ThrowsWhenReferenceVectorIsFull()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Parents",
            TableSpec.Helpers.RefsColumn(1));

        Assert.True(MetaDBManagerHelpersHarness.EnsureRefVec(table, refsCol: 0, parentRow: 0, CapacityPolicy.Throw));

        MetaDBManagerHelpersHarness.LinkRef(table, 0, 0, new MetadataTableRef(Guid.NewGuid(), 0, 0, 0));
        MetaDBManagerHelpersHarness.LinkRef(table, 0, 0, new MetadataTableRef(Guid.NewGuid(), 0, 1, 0));
        MetaDBManagerHelpersHarness.LinkRef(table, 0, 0, new MetadataTableRef(Guid.NewGuid(), 0, 2, 0));

        Assert.Throws<InvalidOperationException>(() => MetaDBManagerHelpersHarness.LinkRef(table, 0, 0, new MetadataTableRef(Guid.NewGuid(), 0, 3, 0)));
    }

    [Fact]
    public void EnsureRefVec_ThrowsWhenValueAreaCannotFitHeaderAndEntry()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "TinyRefs",
            TableSpec.Helpers.Column("Tiny", 1, valueBytes: 8));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            MetaDBManagerHelpersHarness.EnsureRefVec(table, refsCol: 0, parentRow: 0, CapacityPolicy.Throw));

        Assert.Contains("insufficient", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FillAndCopyHelpers_PopulateColumnsAcrossFastAndFallbackPaths()
    {
        using IMetadataTable source = MetadataTableHarness.CreateInMemoryTable(
            "Source",
            TableSpec.Helpers.Column("Numbers", 3, valueBytes: 64),
            TableSpec.Helpers.Column("Raw", 3, valueBytes: 64));
        using IMetadataTable destination = MetadataTableHarness.CreateInMemoryTable(
            "Destination",
            TableSpec.Helpers.Column("Numbers", 1, valueBytes: 64),
            TableSpec.Helpers.Column("Raw", 1, valueBytes: 64));

        MetaDBManagerHelpersHarness.FillUInt32Column(source, column: 0, rows: 3, seed: 10, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
        MetaDBManagerHelpersHarness.FillRawUInt64Column(source, column: 1, rows: 3, seed: 100, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
        MetaDBManagerHelpersHarness.CopyColumn(source, 0, destination, 0, rows: 3, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
        MetaDBManagerHelpersHarness.CopyColumn(source, 1, destination, 1, rows: 3, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);

        Assert.Equal((uint)10, MetaDBManagerHelpersHarness.ReadUInt32(source, 0, 0));
        Assert.Equal((uint)12, MetaDBManagerHelpersHarness.ReadUInt32(source, 0, 2));
        Assert.Equal((ulong)100, MetaDBManagerHelpersHarness.ReadUInt64(source, 1, 0));
        Assert.Equal((ulong)102, MetaDBManagerHelpersHarness.ReadUInt64(source, 1, 2));

        Assert.Equal((uint)10, MetaDBManagerHelpersHarness.ReadUInt32(destination, 0, 0));
        Assert.Equal((uint)12, MetaDBManagerHelpersHarness.ReadUInt32(destination, 0, 2));
        Assert.Equal((ulong)100, MetaDBManagerHelpersHarness.ReadUInt64(destination, 1, 0));
        Assert.Equal((ulong)102, MetaDBManagerHelpersHarness.ReadUInt64(destination, 1, 2));

        Assert.Equal(64, MetaDBManagerHelpersHarness.ReadValueBytes(destination, 0, 1).Length);
        Assert.Equal(64, MetaDBManagerHelpersHarness.ReadValueBytes(destination, 1, 1).Length);
    }

    [Fact]
    public void FillAndCopyHelpers_ThrowForInsufficientCapacityAndMismatchedValueSizes()
    {
        using IMetadataTable source = MetadataTableHarness.CreateInMemoryTable(
            "Source",
            TableSpec.Helpers.Column("Numbers", 2, valueBytes: 64));
        using IMetadataTable destination = MetadataTableHarness.CreateInMemoryTable(
            "Destination",
            TableSpec.Helpers.Column("Numbers", 1, valueBytes: 16));

        var fillEx = Assert.Throws<InvalidOperationException>(() =>
            MetaDBManagerHelpersHarness.FillUInt32Column(source, column: 0, rows: 3, seed: 10, CapacityPolicy.Throw, batchSize: 3));

        var copyEx = Assert.Throws<InvalidOperationException>(() =>
            MetaDBManagerHelpersHarness.CopyColumn(source, 0, destination, 0, rows: 1, CapacityPolicy.AutoGrowZeroInit, batchSize: 1));

        Assert.Contains("insufficient row capacity", fillEx.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUE sizes differ", copyEx.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FillAndCopyHelpers_OnMappedTables_ExerciseStridedBlockFastPaths()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using IMetadataTable source = MetadataTableHarness.CreateTable(new TableSpec(
                "MappedSource",
                Path.Combine(tempRoot, "source.map"),
                [
                    TableSpec.Helpers.Column("Typed64", 3, keyBytes: 16, valueBytes: 64),
                    TableSpec.Helpers.Column("Raw128", 3, keyBytes: 16, valueBytes: 128),
                    TableSpec.Helpers.Column("Raw256", 3, keyBytes: 16, valueBytes: 256),
                    TableSpec.Helpers.Column("Raw20", 3, keyBytes: 16, valueBytes: 20)
                ]));

            using IMetadataTable destination = MetadataTableHarness.CreateTable(new TableSpec(
                "MappedDestination",
                Path.Combine(tempRoot, "destination.map"),
                [
                    TableSpec.Helpers.Column("Typed64", 1, keyBytes: 16, valueBytes: 64),
                    TableSpec.Helpers.Column("Raw128", 1, keyBytes: 16, valueBytes: 128),
                    TableSpec.Helpers.Column("Raw256", 1, keyBytes: 16, valueBytes: 256),
                    TableSpec.Helpers.Column("Raw20", 1, keyBytes: 16, valueBytes: 20)
                ]));

            MetaDBManagerHelpersHarness.FillUInt32Column(source, column: 0, rows: 3, seed: 10, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
            MetaDBManagerHelpersHarness.FillRawUInt64Column(source, column: 1, rows: 3, seed: 100, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
            MetaDBManagerHelpersHarness.FillRawUInt64Column(source, column: 2, rows: 3, seed: 200, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
            MetaDBManagerHelpersHarness.FillRawUInt64Column(source, column: 3, rows: 3, seed: 300, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);

            MetaDBManagerHelpersHarness.CopyColumn(source, 0, destination, 0, rows: 3, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
            MetaDBManagerHelpersHarness.CopyColumn(source, 1, destination, 1, rows: 3, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
            MetaDBManagerHelpersHarness.CopyColumn(source, 2, destination, 2, rows: 3, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);
            MetaDBManagerHelpersHarness.CopyColumn(source, 3, destination, 3, rows: 3, CapacityPolicy.AutoGrowZeroInit, batchSize: 3);

            Assert.Equal((uint)10, MetaDBManagerHelpersHarness.ReadUInt32(destination, 0, 0));
            Assert.Equal((uint)12, MetaDBManagerHelpersHarness.ReadUInt32(destination, 0, 2));
            Assert.Equal((ulong)100, MetaDBManagerHelpersHarness.ReadUInt64(destination, 1, 0));
            Assert.Equal((ulong)102, MetaDBManagerHelpersHarness.ReadUInt64(destination, 1, 2));
            Assert.Equal((ulong)200, MetaDBManagerHelpersHarness.ReadUInt64(destination, 2, 0));
            Assert.Equal((ulong)202, MetaDBManagerHelpersHarness.ReadUInt64(destination, 2, 2));
            Assert.Equal((ulong)300, MetaDBManagerHelpersHarness.ReadUInt64(destination, 3, 0));
            Assert.Equal((ulong)302, MetaDBManagerHelpersHarness.ReadUInt64(destination, 3, 2));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void EnsureRowCapacity_ZeroRows_ReturnsWithoutTouchingTable()
    {
        using IMetadataTable inner = MetadataTableHarness.CreateInMemoryTable(
            "Probe",
            TableSpec.Helpers.Column("Value", 1, valueBytes: 64));
        using var probe = new CapacityProbeTable(inner);

        MetaDBManagerHelpersHarness.EnsureRowCapacity(probe, column: 0, neededRows: 0, policy: CapacityPolicy.Throw);

        Assert.Equal(0, probe.TryGetColumnCapacityCalls);
        Assert.Equal(0, probe.TryGrowColumnToCalls);
        Assert.Equal(0, probe.GetOrCreateCellCalls);
    }

    [Fact]
    public void EnsureRowCapacity_DeterministicCapacityPath_CoversNoOpAndThrow()
    {
        using IMetadataTable inner = MetadataTableHarness.CreateInMemoryTable(
            "Probe",
            TableSpec.Helpers.Column("Value", 1, valueBytes: 64));

        using (var ok = new CapacityProbeTable(inner))
        {
            ok.EnqueueCapacityResponse(hasCapacity: true, capacity: 4);
            MetaDBManagerHelpersHarness.EnsureRowCapacity(ok, column: 0, neededRows: 3, policy: CapacityPolicy.Throw);
            Assert.Equal(1, ok.TryGetColumnCapacityCalls);
            Assert.Equal(0, ok.TryGrowColumnToCalls);
        }

        using var insufficient = new CapacityProbeTable(inner);
        insufficient.EnqueueCapacityResponse(hasCapacity: true, capacity: 1);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            MetaDBManagerHelpersHarness.EnsureRowCapacity(insufficient, column: 0, neededRows: 3, policy: CapacityPolicy.Throw));

        Assert.Contains("insufficient row capacity", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, insufficient.TryGrowColumnToCalls);
    }

    [Fact]
    public void EnsureRowCapacity_DeterministicGrowPath_CoversGrowFailureAndPostValidation()
    {
        using IMetadataTable inner = MetadataTableHarness.CreateInMemoryTable(
            "Probe",
            TableSpec.Helpers.Column("Value", 1, valueBytes: 64));

        using (var growFails = new CapacityProbeTable(inner))
        {
            growFails.EnqueueCapacityResponse(hasCapacity: true, capacity: 1);
            growFails.EnqueueGrowResponse(result: false);

            var ex = Assert.Throws<InvalidOperationException>(() =>
                MetaDBManagerHelpersHarness.EnsureRowCapacity(growFails, column: 0, neededRows: 3, policy: CapacityPolicy.AutoGrowZeroInit));

            Assert.Contains("could not grow", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        using (var reportedButMissing = new CapacityProbeTable(inner))
        {
            reportedButMissing.EnqueueCapacityResponse(hasCapacity: true, capacity: 1);
            reportedButMissing.EnqueueGrowResponse(result: true);
            reportedButMissing.EnqueueCapacityResponse(hasCapacity: false, capacity: 0);

            var ex = Assert.Throws<InvalidOperationException>(() =>
                MetaDBManagerHelpersHarness.EnsureRowCapacity(reportedButMissing, column: 0, neededRows: 3, policy: CapacityPolicy.AutoGrowZeroInit));

            Assert.Contains("reported success", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        using (var reportedButSmall = new CapacityProbeTable(inner))
        {
            reportedButSmall.EnqueueCapacityResponse(hasCapacity: true, capacity: 1);
            reportedButSmall.EnqueueGrowResponse(result: true);
            reportedButSmall.EnqueueCapacityResponse(hasCapacity: true, capacity: 2);

            var ex = Assert.Throws<InvalidOperationException>(() =>
                MetaDBManagerHelpersHarness.EnsureRowCapacity(reportedButSmall, column: 0, neededRows: 3, policy: CapacityPolicy.AutoGrowZeroInit));

            Assert.Contains("reported success", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        using var success = new CapacityProbeTable(inner);
        success.EnqueueCapacityResponse(hasCapacity: true, capacity: 1);
        success.EnqueueGrowResponse(result: true);
        success.EnqueueCapacityResponse(hasCapacity: true, capacity: 3);

        MetaDBManagerHelpersHarness.EnsureRowCapacity(success, column: 0, neededRows: 3, policy: CapacityPolicy.AutoGrowZeroInit);

        Assert.Equal(2, success.TryGetColumnCapacityCalls);
        Assert.Equal(1, success.TryGrowColumnToCalls);
    }

    [Fact]
    public void EnsureRowCapacity_ProbePath_CoversThrowAndGrowVariants()
    {
        using IMetadataTable inner = MetadataTableHarness.CreateInMemoryTable(
            "Probe",
            TableSpec.Helpers.Column("Value", 1, valueBytes: 64));

        using (var throwPolicy = new CapacityProbeTable(inner))
        {
            throwPolicy.EnqueueCapacityResponse(hasCapacity: false, capacity: 0);
            throwPolicy.ThrowOnGetOrCreateCell = true;

            var ex = Assert.Throws<InvalidOperationException>(() =>
                MetaDBManagerHelpersHarness.EnsureRowCapacity(throwPolicy, column: 0, neededRows: 3, policy: CapacityPolicy.Throw));

            Assert.Contains("probe failed", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal((uint)2, throwPolicy.LastProbedRow);
            Assert.Equal(0, throwPolicy.TryGrowColumnToCalls);
        }

        using (var growFails = new CapacityProbeTable(inner))
        {
            growFails.EnqueueCapacityResponse(hasCapacity: false, capacity: 0);
            growFails.EnqueueGrowResponse(result: false);

            var ex = Assert.Throws<InvalidOperationException>(() =>
                MetaDBManagerHelpersHarness.EnsureRowCapacity(growFails, column: 0, neededRows: 3, policy: CapacityPolicy.AutoGrowZeroInit));

            Assert.Contains("could not grow", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        using var growAndProbe = new CapacityProbeTable(inner);
        growAndProbe.EnqueueCapacityResponse(hasCapacity: false, capacity: 0);
        growAndProbe.EnqueueGrowResponse(result: true);

        MetaDBManagerHelpersHarness.EnsureRowCapacity(growAndProbe, column: 0, neededRows: 3, policy: CapacityPolicy.AutoGrowZeroInit);

        Assert.Equal(1, growAndProbe.TryGrowColumnToCalls);
        Assert.Equal((uint)2, growAndProbe.LastProbedRow);
        Assert.Equal(1, growAndProbe.GetOrCreateCellCalls);
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class CapacityProbeTable(IMetadataTable inner) : IMetadataTable
    {
        private readonly Queue<(bool hasCapacity, uint capacity)> _capacityResponses = [];
        private readonly Queue<bool> _growResponses = [];

        public int TryGetColumnCapacityCalls { get; private set; }
        public int TryGrowColumnToCalls { get; private set; }
        public int GetOrCreateCellCalls { get; private set; }
        public uint LastProbedRow { get; private set; }

        public bool ThrowOnGetOrCreateCell { get; set; }

        public void EnqueueCapacityResponse(bool hasCapacity, uint capacity) =>
            _capacityResponses.Enqueue((hasCapacity, capacity));

        public void EnqueueGrowResponse(bool result) =>
            _growResponses.Enqueue(result);

        public int ColumnCount => inner.ColumnCount;
        public ITableIndexesRegistry Indexes => inner.Indexes;
        public TableSpec Spec => inner.Spec;
        public ICellStore CellStore { get => inner.CellStore; set => inner.CellStore = value; }

        public CellEnumerable EnumerateCells() => inner.EnumerateCells();
        public IEnumerable<string> GetColumnNames() => inner.GetColumnNames();
        public uint GetLogicalRowCount() => inner.GetLogicalRowCount();

        public MetadataCell GetOrCreateCell(string columnName, uint row)
        {
            if (ThrowOnGetOrCreateCell)
                throw new InvalidOperationException("probe failed");

            return inner.GetOrCreateCell(columnName, row);
        }

        public MetadataCell GetOrCreateCell(uint column, uint row)
        {
            GetOrCreateCellCalls++;
            LastProbedRow = row;

            if (ThrowOnGetOrCreateCell)
                throw new InvalidOperationException("probe failed");

            return inner.GetOrCreateCell(column, row);
        }

        public IMetadataTable Open() => inner.Open();
        public Task RebuildIndexes(bool strict = false, CancellationToken cancellationToken = default) => inner.RebuildIndexes(strict, cancellationToken);
        public override string ToString() => inner.ToString();
        public string ToString(uint maxRows) => inner.ToString(maxRows);
        public Task<bool> TryCompactStore(bool strict, CancellationToken cancellationToken) => inner.TryCompactStore(strict, cancellationToken);
        public bool TryFindCellByKey(uint column, byte[] keyUtf8, out MetadataCell cell) => inner.TryFindCellByKey(column, keyUtf8, out cell);
        public bool TryFindCellByKey(uint column, ReadOnlySpan<byte> keyUtf8, out MetadataCell cell) => inner.TryFindCellByKey(column, keyUtf8, out cell);
        public bool TryFindGlobal(byte[] keyUtf8, out TryFindGlobalHit hit) => inner.TryFindGlobal(keyUtf8, out hit);
        public bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out TryFindGlobalHit hit) => inner.TryFindGlobal(keyUtf8, out hit);
        public bool TryFindRowByKey(uint column, byte[] keyUtf8, out uint row) => inner.TryFindRowByKey(column, keyUtf8, out row);
        public bool TryFindRowByKey(uint column, ReadOnlySpan<byte> keyUtf8, out uint row) => inner.TryFindRowByKey(column, keyUtf8, out row);
        public bool TryGetCell(string columnName, uint row, out MetadataCell cell) => inner.TryGetCell(columnName, row, out cell);
        public bool TryGetCell(uint column, uint row, out MetadataCell cell) => inner.TryGetCell(column, row, out cell);

        public bool TryGetColumnCapacity(uint column, out uint rowCapacity)
        {
            TryGetColumnCapacityCalls++;

            if (_capacityResponses.TryDequeue(out var response))
            {
                rowCapacity = response.capacity;
                return response.hasCapacity;
            }

            return inner.TryGetColumnCapacity(column, out rowCapacity);
        }

        public bool TryGrowColumnTo(uint column, uint minRows, bool zeroInit = true)
        {
            TryGrowColumnToCalls++;

            if (_growResponses.TryDequeue(out var response))
                return response;

            return inner.TryGrowColumnTo(column, minRows, zeroInit);
        }

        public void Dispose() => inner.Dispose();
    }
}
