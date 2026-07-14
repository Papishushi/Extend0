using Extend0.Metadata;
using Extend0.Metadata.Contract;
using Extend0.Metadata.Diagnostics;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Testing.Metadata.Indexing.Registries;
using Extend0.Testing.Metadata.Internal;
using Extend0.Testing.Metadata.Storage;
using Extend0.Tests.Metadata.Storage;
using Extend0.Tests.TestUtilities;
using Microsoft.Extensions.Logging;

namespace Extend0.Tests.Metadata;

[Collection(MappedStorageCollection.Name)]
public sealed class MetaDBManagerBehaviorTests
{
    [Fact]
    public void Register_GetOrCreate_AndClose_Work()
    {
        using var handle = MetaDBManagerHarness.CreateManager(factory: spec =>
        {
            var effective = spec is null
                ? CreateSupportedSpec("Default", capacity: 1)
                : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

            return MetadataTableHarness.CreateTable(effective);
        });
        var manager = handle.Contract;
        var spec = CreateSupportedSpec("Users", capacity: 2);

        var id = manager.RegisterTable(spec, createNow: false);
        var resolvedByName = manager.TryGetIdByName("Users", out var nameId);
        var createdBefore = manager.TryGetTableIfCreated("Users", out _);
        var publicTableIds = manager.TableIds.ToArray();
        var idRegistry = handle.GetRegisteredIdsFromIdRegistry();
        var nameRegistry = handle.GetRegisteredIdsFromNameRegistry();
        var table = manager.GetOrCreate(id);
        var managedNow = manager.TryGetManaged(id, out var managed);
        var createdNow = manager.TryGetTableIfCreated("Users", out var created);
        var closedByName = manager.CloseStrict("Users");

        Assert.True(resolvedByName);
        Assert.Equal(id, nameId);
        Assert.Contains(id, idRegistry);
        Assert.Contains(id, nameRegistry);
        Assert.Contains(id, publicTableIds);
        Assert.False(createdBefore);
        Assert.NotNull(table);
        Assert.True(managedNow);
        Assert.True(createdNow);
        Assert.Same(table, managed);
        Assert.Same(table, created);
        Assert.True(closedByName);
    }

    [Fact]
    public async Task ConcurrentLazyMaterialization_DisposesUnpublishedTable()
    {
        using var publishRace = new Barrier(2);
        var created = 0;
        var disposed = 0;

        using var handle = MetaDBManagerHarness.CreateManager(factory: spec =>
        {
            var effective = spec ?? CreateSupportedSpec("Default", capacity: 1);
            Interlocked.Increment(ref created);

            var table = MetadataTableHarness.CreateDisposeCountingTable(
                effective.Name,
                () => Interlocked.Increment(ref disposed),
                [.. effective.Columns]);

            Assert.True(publishRace.SignalAndWait(TimeSpan.FromSeconds(5)));
            return table;
        });

        var manager = handle.Contract;
        var id = manager.RegisterTable(CreateSupportedSpec("Race", capacity: 1), createNow: false);

        var first = Task.Run(() => manager.GetOrCreate(id));
        var second = Task.Run(() => manager.GetOrCreate(id));
        var tables = await Task.WhenAll(first, second);

        Assert.Same(tables[0], tables[1]);
        Assert.Equal(2, Volatile.Read(ref created));
        Assert.Equal(1, Volatile.Read(ref disposed));
    }

    [Fact]
    public async Task Run_WithTable_AndAsyncOverloads_Work()
    {
        using var manager = CreateInMemoryManager();
        var id = manager.RegisterTable(CreateSupportedSpec("Orders", capacity: 3), createNow: true);
        var syncVisited = false;
        var asyncVisited = false;

        manager.Run("sync-op", m =>
        {
            syncVisited = true;
            m.WithTable(id, table => Assert.Equal(1, table.ColumnCount));
        });

        var logicalRows = manager.WithTable(id, table => table.GetLogicalRowCount());

        await manager.RunAsync("async-op", async m =>
        {
            await m.WithTableAsync(id, table =>
            {
                asyncVisited = true;
                Assert.Equal("Orders", table.Spec.Name);
                return Task.CompletedTask;
            });
        });

        var asyncLogicalRows = await manager.WithTableAsync(id, table => Task.FromResult(table.GetLogicalRowCount()));

        Assert.True(syncVisited);
        Assert.True(asyncVisited);
        Assert.Equal((uint)3, logicalRows);
        Assert.Equal(logicalRows, asyncLogicalRows);
    }

    [Fact]
    public async Task WithTable_BlocksConcurrentCompactionUntilCallbackCompletes()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            using var manager = MetaDB.CreateManager(factory: spec => MetadataTableHarness.CreateTable(spec!.Value));
            var mapPath = Path.Combine(tempRoot, "manager-exclusive-compact.map");
            var id = manager.RegisterTable(new TableSpec("ManagerExclusiveCompact", mapPath,
            [
                TableSpec.Helpers.Column("Name", 1, valueBytes: 64),
                TableSpec.Helpers.Column("City", 1, valueBytes: 64)
            ]), createNow: true);

            manager.WithTable(id, table => Assert.True(table.TryGrowColumnTo(0, minRows: 4, zeroInit: true)));

            using var entered = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);

            var holder = Task.Run(() =>
            {
                manager.WithTable(id, _ =>
                {
                    entered.Set();
                    Assert.True(release.Wait(TimeSpan.FromSeconds(5)));
                });
            });

            Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));

            var compact = Task.Run(() => manager.TryCompactTable(id, strict: true, cancellationToken: default));

            await Task.Delay(150);
            Assert.False(compact.IsCompleted);

            release.Set();
            await holder.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(await compact.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void Open_LoadsChunkedTableFromDirectory()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var tableDir = Path.Combine(tempRoot, "chunked-open");
            var spec = new TableSpec("ChunkedOpen", tableDir, [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)])
            {
                Storage = TableStorageOptions.Chunked(chunkSize: 256)
            };

            using (var table = MetadataTableHarness.CreateTable(spec))
            {
                Assert.True(table.TryGrowColumnTo(0, minRows: 5, zeroInit: true));
                var row4 = table.GetOrCreateCell(0, 4);
                Assert.True(row4.TrySetKey("omega"));
                Assert.True(row4.TrySetValue("Olivia"));
            }

            using var manager = MetaDB.CreateManager(factory: loadedSpec => MetadataTableHarness.CreateTable(loadedSpec!.Value));
            var opened = manager.Open(tableDir, forceRelocation: false);

            Assert.Equal("ChunkedOpen", opened.Table.Spec.Name);
            Assert.Equal(TableStorageLayout.Chunked, opened.Table.Spec.Storage.Layout);
            Assert.True(opened.Table.TryGetCell(0, 4, out var cell));
            Assert.True(cell.TryGetValueRaw(out var value));
            Assert.StartsWith("Olivia", System.Text.Encoding.UTF8.GetString(value).TrimEnd('\0'), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void EnsureRefVec_LinkRef_AndGetOrCreateAndLinkChild_Work()
    {
        using var manager = CreateInMemoryManager();
        var parentId = manager.RegisterTable(CreateRefParentSpec("Parents", capacity: 1), createNow: true);
        var childId = manager.RegisterTable(CreateSupportedSpec("Children", capacity: 1), createNow: true);

        manager.EnsureRefVec(parentId, refsCol: 0, parentRow: 0, policy: CapacityPolicy.Throw);
        manager.LinkRef(parentId, refsCol: 0, parentRow: 0, childId, childCol: 0, childRow: 0, policy: CapacityPolicy.Throw);
        manager.LinkRef(parentId, refsCol: 0, parentRow: 0, childId, childCol: 0, childRow: 0, policy: CapacityPolicy.Throw);

        var linkedChildId = manager.GetOrCreateAndLinkChild(
            parentId,
            refsCol: 0,
            parentRow: 0,
            childKey: 7,
            childSpecFactory: key => CreateSupportedSpec($"Child-{key}", capacity: 1),
            childCol: 0,
            childRow: 0);

        var linkedChildIdAgain = manager.GetOrCreateAndLinkChild(
            parentId,
            refsCol: 0,
            parentRow: 0,
            childKey: 7,
            childSpecFactory: key => CreateSupportedSpec($"Child-{key}", capacity: 1),
            childCol: 0,
            childRow: 0);

        var defaultKeyChildId = manager.GetOrCreateAndLinkChild(
            parentId,
            refsCol: 0,
            parentRow: 0,
            childSpecFactory: key => CreateSupportedSpec($"DefaultChild-{key}", capacity: 1),
            childCol: 0,
            childRow: 0);

        Assert.NotEqual(Guid.Empty, linkedChildId);
        Assert.Equal(linkedChildId, linkedChildIdAgain);
        Assert.NotEqual(Guid.Empty, defaultKeyChildId);
        Assert.NotEqual(linkedChildId, defaultKeyChildId);
        Assert.True(manager.TryGetManaged(linkedChildId, out _));
        Assert.True(manager.TryGetManaged(defaultKeyChildId, out _));
        Assert.Throws<ArgumentNullException>(() =>
            manager.GetOrCreateAndLinkChild(parentId, refsCol: 0, parentRow: 0, childSpecFactory: null!));
    }

    [Fact]
    public async Task EphemeralAndCompactionFlows_AreCallable()
    {
        using var manager = CreateInMemoryManager();
        var spec = CreateSupportedSpec("Ephemeral", capacity: 1);
        Guid callbackId = Guid.Empty;

        manager.WithTableEphemeral(spec, (id, table) =>
        {
            callbackId = id;
            Assert.Equal("Ephemeral", table.Spec.Name);
        }, createNow: true, deleteNow: false, throwIfDeleteFails: false);

        var result = await manager.TryCompactAllTables(strict: false, cancellationToken: default);

        Assert.NotEqual(Guid.Empty, callbackId);
        Assert.DoesNotContain(callbackId, manager.TableIds);
        Assert.True(result.Success);
        Assert.Null(result.FailedTableIds);
    }

    [Fact]
    public void FillColumn_Primitives_Work_OnMappedTables_ThroughManager()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "cluster-values.map");
            using var manager = MetaDB.CreateManager(factory: spec => MetadataTableHarness.CreateTable(spec!.Value));
            var spec = new TableSpec("ClusterValues", mapPath,
            [
                TableSpec.Helpers.Column<int>("connection_count", capacity: 4, keyBytes: 0),
                TableSpec.Helpers.Column<long>("last_heartbeat_utc_ticks", capacity: 4, keyBytes: 0)
            ]);

            var id = manager.RegisterTable(spec, createNow: true);
            manager.FillColumn<int>(id, 0, rows: 4, row => (int)(100 + row), CapacityPolicy.None);
            manager.FillColumn<long>(id, 1, rows: 4, row => 1000L + row, CapacityPolicy.None);

            manager.WithTable(id, table =>
            {
                Assert.Equal((uint)4, table.GetLogicalRowCount());

                Assert.True(table.TryGetCell(0, 2, out var countCell));
                Assert.True(countCell.TryGetValueRaw(out var countRaw));
                Assert.Equal(102, BitConverter.ToInt32(countRaw.ToArray(), 0));

                Assert.True(table.TryGetCell(1, 3, out var heartbeatCell));
                Assert.True(heartbeatCell.TryGetValueRaw(out var heartbeatRaw));
                Assert.Equal(1003L, BitConverter.ToInt64(heartbeatRaw.ToArray(), 0));
            });
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void FillColumn_AutoGrowZeroInit_Works_OnMappedValueOnlyTable()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "autogrow-values.map");
            using var manager = MetaDB.CreateManager(factory: spec => MetadataTableHarness.CreateTable(spec!.Value));
            var spec = new TableSpec("AutoGrowValues", mapPath,
            [
                TableSpec.Helpers.Column<int>("connection_count", capacity: 1, keyBytes: 0)
            ]);

            var id = manager.RegisterTable(spec, createNow: true);
            manager.FillColumn<int>(id, 0, rows: 4, row => (int)(10 + row), CapacityPolicy.AutoGrowZeroInit);

            manager.WithTable(id, table =>
            {
                Assert.True(table.TryGetColumnCapacity(0, out var capacity));
                Assert.Equal((uint)4, capacity);
                Assert.Equal((uint)4, table.GetLogicalRowCount());
                Assert.True(table.TryGetCell(0, 3, out var cell));
                Assert.True(cell.TryGetValueRaw(out var raw));
                Assert.Equal(13, BitConverter.ToInt32(raw.ToArray(), 0));
            });
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task RestartDeleteWorker_AndCloseAll_AreCallable()
    {
        using var manager = CreateInMemoryManager();
        manager.RegisterTable(CreateSupportedSpec("A", capacity: 1), createNow: true);
        manager.RegisterTable(CreateSupportedSpec("B", capacity: 1), createNow: true);

        await manager.RestartDeleteWorker();
        manager.CloseAll();
        manager.CloseAllStrict();

        Assert.Empty(manager.TableIds);
    }

    [Fact]
    public async Task RebuildIndexes_StrictRequiresCreatedTable_AndBestEffortCancellationIsObservable()
    {
        using var manager = CreateInMemoryManager();
        var lazyId = manager.RegisterTable(CreateSupportedSpec("Lazy", capacity: 1), createNow: false);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<InvalidOperationException>(() => manager.RebuildIndexes(lazyId, strict: true));
        await Assert.ThrowsAsync<OperationCanceledException>(() => manager.RebuildIndexes(lazyId, strict: false, cancellationToken: cts.Token));
    }

    [Fact]
    public async Task RebuildAllIndexes_HonorsCancellationBeforeEnteringRebuildLoop()
    {
        using var manager = CreateInMemoryManager();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => manager.RebuildAllIndexes(strict: true, cancellationToken: cts.Token));
    }

    [Fact]
    public async Task RebuildAllIndexes_BestEffort_SkipsLazyTables_AndIndexesCreatedOnes()
    {
        using var handle = MetaDBManagerHarness.CreateManager(factory: spec =>
        {
            var effective = spec is null
                ? CreateSupportedSpec("Default", capacity: 1)
                : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

            return MetadataTableHarness.CreateTable(effective);
        });
        var manager = handle.Contract;
        var readyId = manager.RegisterTable(CreateSupportedSpec("Ready", capacity: 2), createNow: true);
        var lazyId = manager.RegisterTable(CreateSupportedSpec("LazyRebuildAll", capacity: 2), createNow: false);

        manager.WithTable(readyId, table =>
        {
            var first = table.GetOrCreateCell(0, 0);
            Assert.True(first.TrySetKey("alpha"));
            Assert.True(first.TrySetValue("Alice"));
        });

        Assert.False(manager.TryGetTableIfCreated("LazyRebuildAll", out _));

        await manager.RebuildAllIndexes(strict: false, cancellationToken: default);

        Assert.False(manager.TryGetTableIfCreated("LazyRebuildAll", out _));
        Assert.True(handle.TryFindGlobal("alpha"u8.ToArray(), out var hit));
        Assert.Equal("Ready", hit.TableName);
        Assert.Equal((uint)0, hit.Col);
        Assert.Equal((uint)0, hit.Row);
        Assert.Contains(lazyId, manager.TableIds);
    }

    [Fact]
    public async Task RebuildAllIndexes_Strict_FailsForLazyTables_WithoutMaterializingThem()
    {
        using var manager = CreateInMemoryManager();
        manager.RegisterTable(CreateSupportedSpec("LazyStrict", capacity: 1), createNow: false);

        var ex = await Assert.ThrowsAsync<AggregateException>(() => manager.RebuildAllIndexes(strict: true, cancellationToken: default));

        Assert.Contains(ex.InnerExceptions, inner => inner is InvalidOperationException ioe &&
            ioe.Message.Contains("not created", StringComparison.OrdinalIgnoreCase));
        Assert.False(manager.TryGetTableIfCreated("LazyStrict", out _));
    }

    [Fact]
    public async Task RebuildAllIndexes_BestEffortSkipsNonRebuildableManagerIndexes_AndStrictAggregatesThem()
    {
        using var manager = CreateInMemoryManager();
        manager.RegisterTable(CreateSupportedSpec("ReadyWithProbe", capacity: 1), createNow: true);
        var probe = manager.Indexes.Add(new IndexesRegistryHarness.ProbeCrossTableIndex<string, int>("probe-cross"));

        await manager.RebuildAllIndexes(strict: false, cancellationToken: default);

        var ex = await Assert.ThrowsAsync<AggregateException>(() =>
            manager.RebuildAllIndexes(strict: true, cancellationToken: default));

        Assert.True(probe.ClearCount >= 2);
        Assert.Contains(ex.InnerExceptions, inner =>
            inner is InvalidOperationException ioe &&
            ioe.Message.Contains("probe-cross", StringComparison.Ordinal) &&
            ioe.Message.Contains("does not support cross-table rebuilds", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void FillColumnRaw_WritesRowsThroughManagerActionOverload()
    {
        using var manager = CreateInMemoryManager();
        var tableId = manager.RegisterTable(CreateSupportedSpec("RawWrite", capacity: 3), createNow: true);

        manager.FillColumn(
            tableId,
            column: 0,
            rows: 3,
            writer: static (row, ptr, valueSize) =>
            {
                for (var i = 0; i < valueSize; i++)
                    System.Runtime.InteropServices.Marshal.WriteByte(ptr, checked((int)i), 0);

                System.Runtime.InteropServices.Marshal.WriteByte(ptr, 0, (byte)(0x41 + row));
            },
            policy: CapacityPolicy.AutoGrowZeroInit);

        var table = manager.GetOrCreate(tableId);

        Assert.Equal<byte>(0x41, MetaDBManagerHelpersHarness.ReadValueBytes(table, 0, 0)[0]);
        Assert.Equal<byte>(0x43, MetaDBManagerHelpersHarness.ReadValueBytes(table, 0, 2)[0]);
    }

    [Fact]
    public void FillColumn_GenericAndZeroRowPaths_UseManagerPreparationSafely()
    {
        using var manager = CreateInMemoryManager();
        var tableId = manager.RegisterTable(CreateSupportedSpec("GenericWrite", capacity: 1), createNow: true);
        var genericFactoryCalls = 0;
        var rawWriterCalls = 0;

        manager.FillColumn<uint>(
            tableId,
            column: 0,
            rows: 3,
            factory: row =>
            {
                genericFactoryCalls++;
                return 100u + row;
            },
            policy: CapacityPolicy.AutoGrowZeroInit);

        manager.FillColumn<uint>(
            tableId,
            column: 0,
            rows: 0,
            factory: row =>
            {
                genericFactoryCalls++;
                return row;
            },
            policy: CapacityPolicy.Throw);

        manager.FillColumn(
            tableId,
            column: 0,
            rows: 0,
            writer: (_, _, _) => rawWriterCalls++,
            policy: CapacityPolicy.Throw);

        var table = manager.GetOrCreate(tableId);

        Assert.Equal(3, genericFactoryCalls);
        Assert.Equal(0, rawWriterCalls);
        Assert.Equal((uint)100, MetaDBManagerHelpersHarness.ReadUInt32(table, 0, 0));
        Assert.Equal((uint)102, MetaDBManagerHelpersHarness.ReadUInt32(table, 0, 2));
    }

    [Fact]
    public async Task RunWithReindexHelpers_PropagateStrictAndCancellationPaths()
    {
        using var manager = CreateInMemoryManager();
        var lazyId = manager.RegisterTable(CreateSupportedSpec("LazyRun", capacity: 1), createNow: false);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            manager.RunWithReindexAllAsync("async-reindex", _ => Task.CompletedTask, strict: false, cancellationToken: cts.Token));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            manager.RunWithReindexTableAsync("async-table-reindex", lazyId, _ => Task.CompletedTask, state: null, strict: true));
    }

    [Fact]
    public async Task RunWithReindexHelpers_SyncOverloads_EventuallyRebuildIndexes()
    {
        using var handle = MetaDBManagerHarness.CreateManager(factory: spec =>
        {
            var effective = spec is null
                ? CreateSupportedSpec("Default", capacity: 1)
                : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

            return MetadataTableHarness.CreateTable(effective);
        });
        var manager = handle.Contract;
        var tableId = manager.RegisterTable(CreateSupportedSpec("SyncReindex", capacity: 2), createNow: true);

        manager.WithTable(tableId, table =>
        {
            var first = table.GetOrCreateCell(0, 0);
            Assert.True(first.TrySetKey("alpha"));
            Assert.True(first.TrySetValue("Alice"));
        });

        manager.RunWithReindexTable("sync-table-reindex", tableId, _ => { }, state: new { kind = "single" }, strict: true);
        await WaitUntilAsync(() => manager.WithTable(tableId, table => table.TryFindRowByKey(0, "alpha"u8.ToArray(), out _)));

        manager.RunWithReindexAll("sync-all-reindex", _ => { }, state: new { kind = "all" }, strict: false);
        await WaitUntilAsync(() => handle.TryFindGlobal("alpha"u8.ToArray(), out _));
    }

    [Fact]
    public async Task RunHelpers_ValidateInputs_AndLogFailures()
    {
        var logger = new ListLogger();
        using var handle = MetaDBManagerHarness.CreateManager(
            logger,
            spec =>
            {
                var effective = spec is null
                    ? CreateSupportedSpec("Default", capacity: 1)
                    : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

                return MetadataTableHarness.CreateTable(effective);
            });
        var manager = handle.Contract;
        var expected = new InvalidOperationException("pipeline failed");

        Assert.Throws<ArgumentNullException>(() => manager.Run(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => manager.Run("missing-action", null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => manager.RunAsync(null!, _ => Task.CompletedTask));
        await Assert.ThrowsAsync<ArgumentNullException>(() => manager.RunAsync("missing-action", null!));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            manager.Run("failing-op", _ => throw expected, state: new { stage = "test" }));

        Assert.Same(expected, ex);
        Assert.Contains(logger.Entries, entry =>
            entry.Level == LogLevel.Information &&
            entry.Message.Contains("START", StringComparison.Ordinal) &&
            entry.Message.Contains("failing-op", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry =>
            entry.Level == LogLevel.Error &&
            entry.Message.Contains("FAILED", StringComparison.Ordinal) &&
            ReferenceEquals(entry.Exception, expected));
    }

    [Fact]
    public async Task TryCompactTable_CoversNoOpPath_ForLazyTables()
    {
        using var manager = CreateInMemoryManager();
        var lazyId = manager.RegisterTable(CreateSupportedSpec("LazyCompact", capacity: 1), createNow: false);

        var lazyCompact = await manager.TryCompactTable(lazyId, strict: true, cancellationToken: default);

        Assert.True(lazyCompact);
    }

    [Fact]
    public async Task TryCompactAllTables_CollectsFalseAndExceptionFailures_WhenBestEffort()
    {
        var compactFailure = new InvalidOperationException("compact failed");
        using var manager = MetaDB.CreateManager(factory: spec =>
            spec?.Name switch
            {
                "CompactFalse" => MetadataTableHarness.CreateCompactBehaviorTable(
                    "CompactFalse",
                    static (_, _) => Task.FromResult(false),
                    TableSpec.Helpers.Column("Value", 1, valueBytes: 64)),
                "CompactThrow" => MetadataTableHarness.CreateCompactBehaviorTable(
                    "CompactThrow",
                    (_, _) => Task.FromException<bool>(compactFailure),
                    TableSpec.Helpers.Column("Value", 1, valueBytes: 64)),
                _ => MetadataTableHarness.CreateTable(spec!.Value)
            });

        var falseId = manager.RegisterTable(CreateSupportedSpec("CompactFalse", capacity: 1), createNow: true);
        var throwId = manager.RegisterTable(CreateSupportedSpec("CompactThrow", capacity: 1), createNow: true);

        var result = await manager.TryCompactAllTables(strict: false, cancellationToken: default);

        Assert.False(result.Success);
        Assert.NotNull(result.FailedTableIds);
        Assert.Contains(falseId, result.FailedTableIds);
        Assert.Contains(throwId, result.FailedTableIds);
    }

    [Fact]
    public async Task TryCompactTable_HandlesCreatedTableFailurePaths()
    {
        var compactFailure = new InvalidOperationException("single compact failed");
        using var manager = MetaDB.CreateManager(factory: spec =>
            MetadataTableHarness.CreateCompactBehaviorTable(
                spec!.Value.Name,
                (_, _) => Task.FromException<bool>(compactFailure),
                spec.Value.Columns));
        var tableId = manager.RegisterTable(CreateSupportedSpec("CompactSingleThrow", capacity: 1), createNow: true);

        var bestEffort = await manager.TryCompactTable(tableId, strict: false, cancellationToken: default);
        var strictFailure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            manager.TryCompactTable(tableId, strict: true, cancellationToken: default));

        Assert.False(bestEffort);
        Assert.Same(compactFailure, strictFailure);
    }

    [Fact]
    public async Task DisposeAsync_IsIdempotent()
    {
        var manager = CreateInMemoryManager();
        manager.RegisterTable(CreateSupportedSpec("Disposable", capacity: 1), createNow: false);

        await manager.DisposeAsync();
        await manager.DisposeAsync();
    }

    [Fact]
    public void DeleteQueue_LoadEnqueueRewriteAndCompact_AreIdempotent()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var queuePath = Path.Combine(tempRoot, "meta", "deletes.log");
            Directory.CreateDirectory(Path.GetDirectoryName(queuePath)!);
            var firstPath = Path.Combine(tempRoot, "first.map");
            var secondPath = Path.Combine(tempRoot, "second.map");
            File.WriteAllText(firstPath, "first");
            File.WriteAllText(secondPath, "second");
            using var firstLock = MetadataStorageHarness.AcquireStorageLease(firstPath);
            using var secondLock = MetadataStorageHarness.AcquireStorageLease(secondPath);
            File.WriteAllText(queuePath, "  " + firstPath + "  " + Environment.NewLine + Environment.NewLine + secondPath + Environment.NewLine);

            using var loaded = MetaDBManagerHarness.CreateManager(
                factory: _ => MetadataTableHarness.CreateTable(CreateSupportedSpec("Loaded", 1)),
                deleteQueuePath: queuePath,
                startDeleteWorker: false);
            Assert.Equal([firstPath, secondPath], loaded.GetPendingDeletePaths());

            var thirdPath = Path.Combine(tempRoot, "third.map");
            loaded.EnqueueDelete(firstPath);
            loaded.EnqueueDelete(thirdPath);
            loaded.EnqueueDelete(" ");

            var persistedLines = File.ReadAllLines(queuePath)
                .Select(static line => line.Trim())
                .Where(static line => !string.IsNullOrWhiteSpace(line))
                .ToArray();

            Assert.Contains(thirdPath, loaded.GetPendingDeletePaths());
            Assert.Equal(3, persistedLines.Length);

            File.AppendAllLines(queuePath, ["stale-one", "stale-two"]);
            loaded.TryRewriteDeleteQueueFile();

            var rewritten = File.ReadAllLines(queuePath)
                .Select(static line => line.Trim())
                .Where(static line => !string.IsNullOrWhiteSpace(line))
                .OrderBy(static line => line, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            Assert.Equal(loaded.GetPendingDeletePaths(), rewritten);

            var compactedAt = loaded.MaybeCompactDeleteQueueFile(
                storm: false,
                backlogAfter: 1,
                deletedThisCycle: 1,
                lastCompactMs: 0,
                compactCooldownMs: 0);
            Assert.NotEqual(0L, compactedAt);

            var unchangedNoDeletes = loaded.MaybeCompactDeleteQueueFile(
                storm: false,
                backlogAfter: 1,
                deletedThisCycle: 0,
                lastCompactMs: compactedAt,
                compactCooldownMs: 0);
            var unchangedStorm = loaded.MaybeCompactDeleteQueueFile(
                storm: true,
                backlogAfter: 256,
                deletedThisCycle: 1,
                lastCompactMs: compactedAt,
                compactCooldownMs: 0);

            Assert.Equal(compactedAt, unchangedNoDeletes);
            Assert.Equal(compactedAt, unchangedStorm);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteQueue_TryDeleteNow_DeletesUnlockedFiles_AndEnqueuesLockedOnes()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var queuePath = Path.Combine(tempRoot, "deletes.log");
            using var handle = MetaDBManagerHarness.CreateManager(factory: _ => MetadataTableHarness.CreateTable(CreateSupportedSpec("DeleteQueue", 1)), deleteQueuePath: queuePath);

            var mapPath = Path.Combine(tempRoot, "users.map");
            var specPath = mapPath + ".tablespec.json";
            await File.WriteAllTextAsync(mapPath, "map");
            await File.WriteAllTextAsync(specPath, "spec");

            var deletedNow = await handle.TryDeleteNow(mapPath, specPath, Guid.NewGuid());

            Assert.True(deletedNow);
            Assert.False(File.Exists(mapPath));
            Assert.False(File.Exists(specPath));

            var lockedMapPath = Path.Combine(tempRoot, "locked.map");
            var lockedSpecPath = lockedMapPath + ".tablespec.json";
            await File.WriteAllTextAsync(lockedMapPath, "map");
            await File.WriteAllTextAsync(lockedSpecPath, "spec");

            using var mapLock = MetadataStorageHarness.AcquireStorageLease(lockedMapPath);
            using var specLock = MetadataStorageHarness.AcquireStorageLease(lockedSpecPath);

            var lockedDelete = await handle.TryDeleteNow(lockedMapPath, lockedSpecPath, Guid.NewGuid());

            Assert.False(lockedDelete);
            Assert.Contains(lockedMapPath, handle.GetPendingDeletePaths());
            Assert.Contains(lockedSpecPath, handle.GetPendingDeletePaths());
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void DeleteQueue_EnqueueDelete_KeepsInMemoryWhenPersistenceDisabledOrFails()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var queuePath = Path.Combine(tempRoot, "deletes.log");
            using var handle = MetaDBManagerHarness.CreateManager(
                factory: _ => MetadataTableHarness.CreateTable(CreateSupportedSpec("QueueFallback", 1)),
                deleteQueuePath: queuePath,
                startDeleteWorker: false);

            var disabledPath = Path.Combine(tempRoot, "disabled.map");
            File.WriteAllText(disabledPath, "locked");
            handle.SetDeleteQueuePath(string.Empty);
            handle.EnqueueDelete(disabledPath);

            Assert.Contains(disabledPath, handle.GetPendingDeletePaths());

            var queueAsDirectory = Path.Combine(tempRoot, "queue-as-directory");
            Directory.CreateDirectory(queueAsDirectory);
            var persistenceFailurePath = Path.Combine(tempRoot, "persist-failure.map");
            File.WriteAllText(persistenceFailurePath, "locked");
            handle.SetDeleteQueuePath(queueAsDirectory);
            handle.EnqueueDelete(persistenceFailurePath);
            handle.EnqueueDelete(persistenceFailurePath);
            handle.TryRewriteDeleteQueueFile();

            var pending = handle.GetPendingDeletePaths();
            Assert.Contains(disabledPath, pending);
            Assert.Contains(persistenceFailurePath, pending);
            Assert.True(Directory.Exists(queueAsDirectory));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteQueue_TryDeleteNow_MoveAsideReadOnlyFile_EnqueuesMovedPath()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var queuePath = Path.Combine(tempRoot, "deletes.log");
            using var handle = MetaDBManagerHarness.CreateManager(factory: _ => MetadataTableHarness.CreateTable(CreateSupportedSpec("MoveAside", 1)), deleteQueuePath: queuePath);

            var mapPath = Path.Combine(tempRoot, "readonly.map");
            var specPath = mapPath + ".missing.tablespec.json";
            File.WriteAllText(mapPath, "readonly");
            File.SetAttributes(mapPath, File.GetAttributes(mapPath) | FileAttributes.ReadOnly);

            var deleted = await handle.TryDeleteNow(mapPath, specPath, Guid.NewGuid());
            var pending = handle.GetPendingDeletePaths();
            Assert.False(File.Exists(mapPath));
            if (OperatingSystem.IsWindows())
            {
                var movedPath = Assert.Single(pending, path => path.Contains(".deleting.", StringComparison.Ordinal));
                Assert.False(deleted);
                Assert.StartsWith(mapPath + ".deleting.", movedPath, StringComparison.Ordinal);
                Assert.True(File.Exists(movedPath));
                File.SetAttributes(movedPath, FileAttributes.Normal);
            }
            else
            {
                // Unix deletion is controlled by directory permissions, not the file's read-only bit.
                Assert.True(deleted);
                Assert.Empty(pending);
            }
        }
        finally
        {
            foreach (var file in Directory.Exists(tempRoot) ? Directory.EnumerateFiles(tempRoot, "*", SearchOption.AllDirectories) : Array.Empty<string>())
                File.SetAttributes(file, FileAttributes.Normal);

            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public async Task CleanupEphemeralDeleteAsync_ThrowsOnlyWhenConfigured()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var queuePath = Path.Combine(tempRoot, "deletes.log");
            using var handle = MetaDBManagerHarness.CreateManager(factory: _ => MetadataTableHarness.CreateTable(CreateSupportedSpec("Cleanup", 1)), deleteQueuePath: queuePath);

            var mapPath = Path.Combine(tempRoot, "cleanup.map");
            var specPath = mapPath + ".tablespec.json";
            await File.WriteAllTextAsync(mapPath, "map");
            await File.WriteAllTextAsync(specPath, "spec");

            using var mapLock = MetadataStorageHarness.AcquireStorageLease(mapPath);
            using var specLock = MetadataStorageHarness.AcquireStorageLease(specPath);

            await handle.CleanupEphemeralDeleteAsync(throwIfDeleteFails: false, Guid.NewGuid(), mapPath, specPath);
            Assert.Contains(mapPath, handle.GetPendingDeletePaths());

            var ex = await Assert.ThrowsAsync<IOException>(async () =>
                await handle.CleanupEphemeralDeleteAsync(throwIfDeleteFails: true, Guid.NewGuid(), mapPath, specPath));
            Assert.Contains("deleteNow requested", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void ResolveCrossGlobalKeySize_UsesFallbackAndLargestRegisteredKey()
    {
        using var handle = MetaDBManagerHarness.CreateManager(factory: spec =>
        {
            var effective = spec is null
                ? CreateSupportedSpec("Default", capacity: 1)
                : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

            return MetadataTableHarness.CreateTable(effective);
        });
        var manager = handle.Contract;

        Assert.Equal(32, handle.ResolveCrossGlobalKeySize());

        manager.RegisterTable(new TableSpec("Small", "small.map", [TableSpec.Helpers.Column("Value", 1, keyBytes: 16, valueBytes: 64)]), createNow: false);
        Assert.Equal(32, handle.ResolveCrossGlobalKeySize());

        manager.RegisterTable(new TableSpec("Large", "large.map", [TableSpec.Helpers.Column("Value", 1, keyBytes: 64, valueBytes: 64)]), createNow: false);
        Assert.Equal(64, handle.ResolveCrossGlobalKeySize());
    }

    [Fact]
    public void Open_RespectsForceRelocation_AndAcceptsDirectSpecPath()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "input.map");
            var storedMapPath = Path.Combine(tempRoot, "stored", "users.map");
            var spec = new TableSpec("OpenRelocation", storedMapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)]);
            spec.SaveToFile(inputMapPath + ".tablespec.json");

            TableSpec? captured = null;
            using var handle = MetaDBManagerHarness.CreateManager(factory: loadedSpec =>
            {
                captured = loadedSpec;
                return MetadataTableHarness.CreateInMemoryTable(loadedSpec!.Value.Name, loadedSpec.Value.Columns);
            });
            var manager = handle.Contract;

            var openedWithoutRelocation = manager.Open(inputMapPath, forceRelocation: false);
            Assert.Equal(storedMapPath, captured!.Value.MapPath);

            Assert.True(manager.CloseStrict(openedWithoutRelocation.Id));

            var openedWithRelocation = manager.Open(inputMapPath + ".tablespec.json", forceRelocation: true);
            Assert.Equal(inputMapPath, captured!.Value.MapPath);
            Assert.True(manager.CloseStrict(openedWithRelocation.Id));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void Open_LogsMapPathMismatchAndSuccessfulOpen_WhenLoggerIsConfigured()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var inputMapPath = Path.Combine(tempRoot, "input-log.map");
            var storedMapPath = Path.Combine(tempRoot, "stored", "users-log.map");
            var spec = new TableSpec("OpenLogging", storedMapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)]);
            spec.SaveToFile(inputMapPath + ".tablespec.json");

            var logger = new ListLogger();
            using var handle = MetaDBManagerHarness.CreateManager(
                logger,
                loadedSpec => MetadataTableHarness.CreateInMemoryTable(loadedSpec!.Value.Name, loadedSpec.Value.Columns));
            var manager = handle.Contract;

            var opened = manager.Open(inputMapPath, forceRelocation: false);

            Assert.Contains(logger.Entries, entry =>
                entry.Level == LogLevel.Debug &&
                entry.Message.Contains("TableSpec MapPath differs", StringComparison.Ordinal) &&
                entry.Message.Contains(storedMapPath, StringComparison.Ordinal) &&
                entry.Message.Contains(inputMapPath, StringComparison.Ordinal));
            Assert.Contains(logger.Entries, entry =>
                entry.Level == LogLevel.Information &&
                entry.Message.Contains("OpenLogging", StringComparison.Ordinal));
            Assert.True(manager.CloseStrict(opened.Id));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void Open_RollsBackRegistries_WhenMaterializationFails()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "broken.map");
            var spec = new TableSpec("BrokenOpen", mapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)]);
            spec.SaveToFile(mapPath + ".tablespec.json");

            using var handle = MetaDBManagerHarness.CreateManager(factory: _ => throw new InvalidOperationException("open-failure"));
            var manager = handle.Contract;

            var ex = Assert.Throws<InvalidOperationException>(() => manager.Open(mapPath));
            Assert.Contains("open-failure", ex.Message, StringComparison.Ordinal);
            Assert.Empty(manager.TableIds);
            Assert.False(manager.TryGetIdByName("BrokenOpen", out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void Open_RollsBackRegistries_WhenMappedFileIsLockedByAnotherOwner()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(tempRoot, "locked-open.map");
            var spec = new TableSpec("LockedOpen", mapPath, [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)]);
            spec.SaveToFile(mapPath + ".tablespec.json");
            File.WriteAllBytes(mapPath, [0]);

            using var mapLock = MetadataStorageHarness.AcquireStorageLease(mapPath);
            using var handle = MetaDBManagerHarness.CreateManager();
            var manager = handle.Contract;

            var ex = Assert.Throws<MetadataTableLockedException>(() => manager.Open(mapPath));

            Assert.IsAssignableFrom<IOException>(ex.InnerException);
            Assert.Empty(manager.TableIds);
            Assert.False(manager.TryGetIdByName("LockedOpen", out _));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void CloseAndCloseAll_CoverBestEffortAndAggregateFailurePaths()
    {
        var disposeFailure = new InvalidOperationException("dispose failed");
        using var handle = MetaDBManagerHarness.CreateManager(factory: spec =>
        {
            if (spec?.Name == "Boom")
                return MetadataTableHarness.CreateDisposeThrowingTable("Boom", disposeFailure, TableSpec.Helpers.Column("Value", 1, valueBytes: 64));

            return MetadataTableHarness.CreateTable(spec!.Value);
        });
        var manager = handle.Contract;

        var okId = manager.RegisterTable(CreateSupportedSpec("Ok", capacity: 1), createNow: true);
        var boomId = manager.RegisterTable(CreateSupportedSpec("Boom", capacity: 1), createNow: true);

        Assert.False(handle.Close(Guid.Empty));
        Assert.False(handle.Close("missing"));
        Assert.True(handle.Close("Ok"));
        Assert.False(handle.Close("Ok"));

        var aggregate = Assert.Throws<AggregateException>(() => handle.CloseAll());
        var inner = Assert.Single(aggregate.InnerExceptions);
        Assert.Same(disposeFailure, inner);
        Assert.False(handle.Close(boomId));
    }

    [Fact]
    public void CloseStrict_LogsSuccessAndDisposeFailure_WhenLoggerIsConfigured()
    {
        var disposeFailure = new InvalidOperationException("dispose failed with logger");
        var logger = new ListLogger();
        using var handle = MetaDBManagerHarness.CreateManager(
            logger,
            spec => spec?.Name == "BoomLog"
                ? MetadataTableHarness.CreateDisposeThrowingTable("BoomLog", disposeFailure, TableSpec.Helpers.Column("Value", 1, valueBytes: 64))
                : MetadataTableHarness.CreateTable(spec!.Value));
        var manager = handle.Contract;

        var okId = manager.RegisterTable(CreateSupportedSpec("OkLog", capacity: 1), createNow: true);
        var boomId = manager.RegisterTable(CreateSupportedSpec("BoomLog", capacity: 1), createNow: true);

        Assert.True(manager.CloseStrict(okId));
        var ex = Assert.Throws<InvalidOperationException>(() => manager.CloseStrict(boomId));

        Assert.Same(disposeFailure, ex);
        Assert.Contains(logger.Entries, entry =>
            entry.Level == LogLevel.Debug &&
            entry.Message.Contains("Closed metadata table OkLog", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry =>
            entry.Level == LogLevel.Error &&
            ReferenceEquals(entry.Exception, disposeFailure) &&
            entry.Message.Contains("Error disposing metadata table BoomLog", StringComparison.Ordinal));
    }

    [Fact]
    public void CloseAllStrict_PropagatesFirstDisposeFailure()
    {
        var disposeFailure = new InvalidOperationException("dispose failed strictly");
        using var handle = MetaDBManagerHarness.CreateManager(factory: spec =>
            spec?.Name == "BoomStrict"
                ? MetadataTableHarness.CreateDisposeThrowingTable("BoomStrict", disposeFailure, TableSpec.Helpers.Column("Value", 1, valueBytes: 64))
                : MetadataTableHarness.CreateTable(spec!.Value));
        var manager = handle.Contract;

        manager.RegisterTable(CreateSupportedSpec("BoomStrict", capacity: 1), createNow: true);

        var ex = Assert.Throws<InvalidOperationException>(() => handle.CloseAllStrict());

        Assert.Same(disposeFailure, ex);
    }

    [Fact]
    public void Manager_Methods_ThrowAfterDispose()
    {
        var manager = CreateInMemoryManager();
        manager.Dispose();

        Assert.Throws<ObjectDisposedException>(() => manager.RegisterTable(CreateSupportedSpec("Disposed", capacity: 1), createNow: false));
        Assert.Throws<ObjectDisposedException>(() => manager.Open("disposed.meta"));
        Assert.Throws<ObjectDisposedException>(() => manager.WithTableEphemeral(CreateSupportedSpec("DisposedEphemeral", capacity: 1), static (_, _) => { }));
    }

    private static IMetaDBManager CreateInMemoryManager() =>
        MetaDB.CreateManager(factory: spec =>
        {
            var effective = spec is null
                ? CreateSupportedSpec("Default", capacity: 1)
                : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

            return MetadataTableHarness.CreateTable(effective);
        });

    private static TableSpec CreateSupportedSpec(string name, uint capacity) =>
        new(name, $"{name.ToLowerInvariant()}.map", [TableSpec.Helpers.Column("Value", capacity, valueBytes: 64)]);

    private static TableSpec CreateRefParentSpec(string name, uint capacity) =>
        new(name, $"{name.ToLowerInvariant()}.map", [TableSpec.Helpers.RefsColumn(capacity)]);

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int attempts = 50, int delayMs = 20)
    {
        for (var i = 0; i < attempts; i++)
        {
            if (condition())
                return;

            await Task.Delay(delayMs);
        }

        Assert.True(condition(), "Condition was not met within the expected time.");
    }
}
