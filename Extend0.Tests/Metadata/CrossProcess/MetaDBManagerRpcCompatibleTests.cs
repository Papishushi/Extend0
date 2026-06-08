using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.Contract;
using Extend0.Metadata.CrossProcess.DTO;
using Extend0.Metadata.CrossProcess.HResult;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Testing.Metadata.CrossProcess;
using Extend0.Testing.Metadata.Indexing.Registries;
using InternalRpcHarness = Extend0.Testing.Metadata.CrossProcess.Internal.MetaDBManagerRpcServiceHarness;

namespace Extend0.Tests.Metadata.CrossProcess;

public sealed class MetaDBManagerRpcCompatibleTests
{
    [Fact]
    public void RegisterByName_CloseByName_AndCloseAll_AreCallable()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();

        var id = service.RegisterTable(
            name: "UsersByName",
            mapPath: "users-by-name.map",
            TableSpec.Helpers.Column("Name", 2, valueBytes: 64));

        Assert.Equal(id, service.TryGetIdByName("UsersByName"));
        Assert.True(service.CloseStrict("UsersByName"));
        Assert.False(service.CloseStrict("UsersByName"));

        service.RegisterTable(CreateSupportedSpec("UsersAgain", 1), createNow: true);
        service.CloseAll();
        service.CloseAllStrict();
    }

    [Fact]
    public void RegisterResolveReadAndClose_Flow_WorksThroughRpcFacade()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var spec = new TableSpec("Users", "users.map", [TableSpec.Helpers.Column("Name", 2, valueBytes: 64)]);

        var tableId = service.RegisterTable(spec, createNow: true);
        var resolvedId = service.TryGetIdByName("Users");

        service.FillColumn(
            tableId,
            column: 0,
            startRow: 0,
            values:
            [
                MakeCell("user-1", "Alice"),
                MakeCell("user-2", "Bob")
            ],
            policy: CapacityPolicy.AutoGrowZeroInit);

        var rowCount = service.GetRowCount(tableId);
        var columnCount = service.GetColumnCount(tableId);
        var columnNames = service.GetColumnNames(tableId);
        var preview = service.PreviewTable(tableId, maxRows: 2);
        var cell = service.ReadCell(tableId, 0, 0);
        var raw = service.ReadCellRaw(tableId, 0, 1);
        var row = service.ReadRow(tableId, 0);
        var column = service.ReadColumn(tableId, 0, 0, 2);
        var block = service.ReadBlock(tableId, [0], 0, 2);
        var closed = service.CloseStrict(tableId);

        Assert.Equal(tableId, resolvedId);
        Assert.Equal((uint)2, rowCount);
        Assert.Equal(1, columnCount);
        Assert.Equal(["Name"], columnNames);
        Assert.Contains("Name", preview, StringComparison.Ordinal);
        Assert.NotNull(cell);
        Assert.Equal("Alice", cell.Value.ValueUtf8);
        Assert.NotNull(raw);
        Assert.NotEmpty(raw);
        Assert.NotNull(row["Name"]);
        Assert.Equal("Alice", row["Name"]!.Value.ValueUtf8);
        Assert.Equal(2, column.Length);
        Assert.NotNull(column[1]);
        Assert.Equal("Bob", column[1]!.Value.ValueUtf8);
        Assert.Equal(2, block.Length);
        Assert.Single(block[0]);
        Assert.True(closed);
    }

    [Fact]
    public void FillColumnRaw_ReadRowRaw_ReadColumnRaw_AndReadBlockRaw_Work()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var tableId = service.RegisterTable(new TableSpec("RawUsers", "raw.map", [TableSpec.Helpers.Column("Blob", 2, valueBytes: 64)]), createNow: true);

        service.FillColumnRaw(
            tableId,
            column: 0,
            startRow: 0,
            valuesRaw:
            [
                "hello"u8.ToArray(),
                "world"u8.ToArray()
            ],
            policy: CapacityPolicy.AutoGrowZeroInit);

        var row = service.ReadRowRaw(tableId, 1);
        var column = service.ReadColumnRaw(tableId, 0, 0, 2);
        var block = service.ReadBlockRaw(tableId, [0], 0, 2);

        Assert.NotNull(row["Blob"]);
        Assert.NotEmpty(row["Blob"]!);
        Assert.Equal(2, column.Length);
        Assert.NotNull(column[0]);
        Assert.NotNull(column[1]);
        Assert.Equal(2, block.Length);
        Assert.Single(block[0]);
        Assert.Single(block[1]);
    }

    [Fact]
    public void IndexAndLookupApis_ReturnStructuredStatuses()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var tableId = service.RegisterTable(new TableSpec("Users", "users.map", [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]), createNow: true);

        service.FillColumn(tableId, 0, 0, [MakeCell("user-1", "Alice")], CapacityPolicy.AutoGrowZeroInit);

        var indexes = service.GetIndexes(tableId);
        var invalidStringLookup = service.FindRowByKey(tableId, 0, "");
        var invalidColumnLookup = service.FindRowByKey(tableId, 4, "user-1"u8.ToArray());
        var notBuiltLookup = service.FindRowByKey(tableId, 0, "user-1");
        var invalidGlobalLookup = service.FindGlobal(tableId, "");
        var missingManagerGlobal = service.FindGlobal("user-1"u8.ToArray());
        var addCustom = service.AddIndex(tableId, new AddIndexRequestDTO("custom", IndexKindDTO.Custom_InTable, EmptyJson()));
        var removeMissing = service.RemoveIndex(tableId, "missing");
        var removeBuiltIn = service.RemoveIndex(tableId, indexes[0].Name);

        Assert.NotEmpty(indexes);
        Assert.Equal(IndexLookupStatusDTO.InvalidKey, invalidStringLookup.Status);
        Assert.Equal(IndexLookupStatusDTO.InvalidColumn, invalidColumnLookup.Status);
        Assert.Equal(IndexLookupStatusDTO.NotFound, notBuiltLookup.Status);
        Assert.Equal(IndexLookupStatusDTO.InvalidKey, invalidGlobalLookup.Status);
        Assert.Equal(IndexLookupStatusDTO.NotFound, missingManagerGlobal.Status);
        Assert.Equal(IndexMutationStatusDTO.NotSupported, addCustom.Status);
        Assert.Equal(IndexMutationStatusDTO.NotFound, removeMissing.Status);
        Assert.Equal(IndexMutationStatusDTO.BuiltInProtected, removeBuiltIn.Status);
    }

    [Fact]
    public void AddIndex_TableLevel_ValidatesGuards_AndProtectsBuiltInCollisions()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var tableId = service.RegisterTable(CreateSupportedSpec("UsersAddIndexGuards", 1), createNow: true);

        var emptyTable = service.AddIndex(Guid.Empty, new AddIndexRequestDTO("x", IndexKindDTO.Custom_InTable, EmptyJson()));
        var missingTable = service.AddIndex(Guid.NewGuid(), new AddIndexRequestDTO("x", IndexKindDTO.Custom_InTable, EmptyJson()));
        var invalidName = service.AddIndex(tableId, new AddIndexRequestDTO(" ", IndexKindDTO.Custom_InTable, EmptyJson()));
        var invalidKind = service.AddIndex(tableId, new AddIndexRequestDTO("x", IndexKindDTO.Unknown, EmptyJson()));
        var builtInKind = service.AddIndex(tableId, new AddIndexRequestDTO("x", IndexKindDTO.BuiltIn_ColumnKey, EmptyJson()));

        var builtInName = service.GetIndexes(tableId).First(i => i.IsBuiltIn).Name;
        var builtInCollision = service.AddIndex(
            tableId,
            new AddIndexRequestDTO(
                Name: builtInName,
                Kind: IndexKindDTO.Custom_InTable,
                IndexInputPayload: EmptyJson(),
                ReplaceIfExists: true));

        Assert.Equal(IndexMutationStatusDTO.TableNotOpen, emptyTable.Status);
        Assert.Equal(IndexMutationStatusDTO.TableNotOpen, missingTable.Status);
        Assert.Equal(IndexMutationStatusDTO.InvalidName, invalidName.Status);
        Assert.Equal(IndexMutationStatusDTO.InvalidKind, invalidKind.Status);
        Assert.Equal(IndexMutationStatusDTO.BuiltInProtected, builtInKind.Status);
        Assert.Equal(IndexMutationStatusDTO.BuiltInProtected, builtInCollision.Status);
    }

    [Fact]
    public void AddIndex_ReturnsAlreadyExists_ForInjectedCustomIndexes_WhenReplaceIsDisabled()
    {
        using var handle = InternalRpcHarness.CreateInMemoryService();
        var service = handle.RpcService;

        var tableId = service.RegisterTable(CreateSupportedSpec("UsersInjectedIndex", 1), createNow: true);

        handle.AddTableIndex(tableId, new IndexesRegistryHarness.ProbeTableIndex<byte[], int>("custom-table"));
        handle.AddManagerIndex(new IndexesRegistryHarness.ProbeCrossTableIndex<byte[], int>("custom-manager"));

        var tableAlreadyExists = service.AddIndex(
            tableId,
            new AddIndexRequestDTO(
                Name: "custom-table",
                Kind: IndexKindDTO.Custom_InTable,
                IndexInputPayload: EmptyJson(),
                ReplaceIfExists: false,
                Notes: "table-exists"));

        var managerAlreadyExists = service.AddIndex(
            new AddIndexRequestDTO(
                Name: "custom-manager",
                Kind: IndexKindDTO.Custom_CrossTable,
                IndexInputPayload: EmptyJson(),
                ReplaceIfExists: false,
                Notes: "manager-exists"));

        Assert.Equal(IndexMutationStatusDTO.AlreadyExists, tableAlreadyExists.Status);
        Assert.Equal("table-exists", tableAlreadyExists.Notes);
        Assert.Equal(IndexMutationStatusDTO.AlreadyExists, managerAlreadyExists.Status);
        Assert.Equal("manager-exists", managerAlreadyExists.Notes);
    }

    [Fact]
    public void AddIndex_ReplaceTrue_KeepsExistingCustomIndexes_WhenCreationIsNotSupported()
    {
        using var handle = InternalRpcHarness.CreateInMemoryService();
        var service = handle.RpcService;

        var tableId = service.RegisterTable(CreateSupportedSpec("UsersReplaceNotSupported", 1), createNow: true);
        handle.AddTableIndex(tableId, new IndexesRegistryHarness.ProbeTableIndex<byte[], int>("custom-table"));
        handle.AddManagerIndex(new IndexesRegistryHarness.ProbeCrossTableIndex<byte[], int>("custom-manager"));

        var replaceTable = service.AddIndex(
            tableId,
            new AddIndexRequestDTO(
                Name: "custom-table",
                Kind: IndexKindDTO.Custom_InTable,
                IndexInputPayload: EmptyJson(),
                ReplaceIfExists: true,
                Notes: "replace-table"));

        var replaceManager = service.AddIndex(
            new AddIndexRequestDTO(
                Name: "custom-manager",
                Kind: IndexKindDTO.Custom_CrossTable,
                IndexInputPayload: EmptyJson(),
                ReplaceIfExists: true,
                Notes: "replace-manager"));

        Assert.Equal(IndexMutationStatusDTO.NotSupported, replaceTable.Status);
        Assert.Equal(IndexMutationStatusDTO.NotSupported, replaceManager.Status);
        Assert.Contains(service.GetIndexes(tableId), idx => idx.Name == "custom-table");
        Assert.Contains(service.GetIndexes(), idx => idx.Name == "custom-manager");
    }

    [Fact]
    public void AddIndex_ManagerLevel_BuiltInNameCollision_IsProtectedEvenForCustomKind()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        _ = service.FindGlobal("seed-key");
        var builtInName = service.GetIndexes().First(idx => idx.IsBuiltIn).Name;

        var collision = service.AddIndex(
            new AddIndexRequestDTO(
                Name: builtInName,
                Kind: IndexKindDTO.Custom_CrossTable,
                IndexInputPayload: EmptyJson(),
                ReplaceIfExists: true));

        Assert.Equal(IndexMutationStatusDTO.BuiltInProtected, collision.Status);
    }

    [Fact]
    public async Task ManagerLevelIndexes_AndGlobalLookups_ReturnStructuredStatuses()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var tableId = service.RegisterTable(CreateSupportedSpec("Users", 2), createNow: true);

        service.FillColumn(
            tableId,
            column: 0,
            startRow: 0,
            values: [MakeCell("alpha", "Alice"), MakeCell("beta", "Bob")],
            policy: CapacityPolicy.AutoGrowZeroInit);

        var invalidName = service.AddIndex(new AddIndexRequestDTO("", IndexKindDTO.Custom_CrossTable, EmptyJson()));
        var invalidKind = service.AddIndex(new AddIndexRequestDTO("x", IndexKindDTO.Unknown, EmptyJson()));
        var builtInProtected = service.AddIndex(new AddIndexRequestDTO("global", IndexKindDTO.BuiltIn_GlobalMultiTableKey, EmptyJson()));
        var notSupported = service.AddIndex(new AddIndexRequestDTO("custom-global", IndexKindDTO.Custom_CrossTable, EmptyJson()));
        var removeInvalid = service.RemoveIndex("");
        var removeMissing = service.RemoveIndex("missing");

        var beforeRebuild = service.FindGlobal("alpha");
        var rebuildCallId = service.RebuildAllIndexesBegin(strict: true);
        await service.Await(rebuildCallId);
        var managerIndexes = service.GetIndexes();
        var afterRebuild = service.FindGlobal("alpha");
        var missing = service.FindGlobal("missing");
        var removeBuiltIn = service.RemoveIndex(managerIndexes[0].Name);

        Assert.Equal(IndexMutationStatusDTO.InvalidName, invalidName.Status);
        Assert.Equal(IndexMutationStatusDTO.InvalidKind, invalidKind.Status);
        Assert.Equal(IndexMutationStatusDTO.BuiltInProtected, builtInProtected.Status);
        Assert.Equal(IndexMutationStatusDTO.NotSupported, notSupported.Status);
        Assert.Equal(IndexMutationStatusDTO.InvalidName, removeInvalid.Status);
        Assert.Equal(IndexMutationStatusDTO.NotFound, removeMissing.Status);
        Assert.Equal(IndexLookupStatusDTO.NotFound, beforeRebuild.Status);
        Assert.Contains(managerIndexes, idx => idx.Kind == IndexKindDTO.BuiltIn_GlobalMultiTableKey);
        Assert.Equal(IndexLookupStatusDTO.Ok, afterRebuild.Status);
        Assert.True(afterRebuild.Hit.Found);
        Assert.Equal("Users", afterRebuild.Hit.TableName);
        Assert.Equal(IndexLookupStatusDTO.NotFound, missing.Status);
        Assert.Equal(IndexMutationStatusDTO.BuiltInProtected, removeBuiltIn.Status);
    }

    [Fact]
    public async Task RebuildIndexesBegin_RebuildsTableIndexes_AndCanBeAwaited()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var tableId = service.RegisterTable(CreateSupportedSpec("UsersTableRebuildBegin", 2), createNow: true);

        service.FillColumn(
            tableId,
            column: 0,
            startRow: 0,
            values: [MakeCell("alpha", "Alice")],
            policy: CapacityPolicy.AutoGrowZeroInit);

        var beforeRebuild = service.FindRowByKey(tableId, column: 0, keyUtf8: "alpha");
        var callId = service.RebuildIndexesBegin(tableId, strict: true);
        await service.Await(callId);
        var afterRebuild = service.FindRowByKey(tableId, column: 0, keyUtf8: "alpha");

        Assert.True(callId > 0);
        Assert.Equal(IndexLookupStatusDTO.NotFound, beforeRebuild.Status);
        Assert.Equal(IndexLookupStatusDTO.Ok, afterRebuild.Status);
        Assert.True(afterRebuild.Hit.Found);
        Assert.Equal((uint)0, afterRebuild.Hit.Row);
        Assert.Equal((uint)0, afterRebuild.Hit.Col);
    }

    [Fact]
    public async Task FindGlobal_TableBytes_ReturnsStructuredStatuses()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var tableId = service.RegisterTable(CreateSupportedSpec("UsersBytesLookup", 2), createNow: true);

        service.FillColumn(
            tableId,
            column: 0,
            startRow: 0,
            values: [MakeCell("alpha", "Alice")],
            policy: CapacityPolicy.AutoGrowZeroInit);

        var emptyTable = service.FindGlobal(Guid.Empty, "alpha"u8.ToArray());
        var beforeRebuild = service.FindGlobal(tableId, "alpha"u8.ToArray());

        var callId = service.RebuildAllIndexesBegin(strict: true);
        await service.Await(callId);

        var afterRebuild = service.FindGlobal(tableId, "alpha"u8.ToArray());
        var missing = service.FindGlobal(tableId, "missing"u8.ToArray());

        Assert.Equal(IndexLookupStatusDTO.TableNotOpen, emptyTable.Status);
        Assert.Equal(IndexLookupStatusDTO.NotFound, beforeRebuild.Status);
        Assert.Equal(IndexLookupStatusDTO.Ok, afterRebuild.Status);
        Assert.True(afterRebuild.Hit.Found);
        Assert.Equal((uint)0, afterRebuild.Hit.Col);
        Assert.Equal((uint)0, afterRebuild.Hit.Row);
        Assert.Equal(IndexLookupStatusDTO.NotFound, missing.Status);
    }

    [Fact]
    public void CopyColumn_Refs_AndChildLinking_WorkThroughRpcFacade()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var sourceId = service.RegisterTable(CreateSupportedSpec("Source", 2), createNow: true);
        var destinationId = service.RegisterTable(CreateSupportedSpec("Destination", 1), createNow: true);
        var parentId = service.RegisterTable(CreateRefParentSpec("Parents", 1), createNow: true);
        var childId = service.RegisterTable(CreateSupportedSpec("Children", 1), createNow: true);

        service.FillColumn(
            sourceId,
            column: 0,
            startRow: 0,
            values: [MakeCell("alpha", "Alice"), MakeCell("beta", "Bob")],
            policy: CapacityPolicy.AutoGrowZeroInit);

        service.CopyColumn(sourceId, 0, destinationId, 0, rows: 2, dstPolicy: CapacityPolicy.AutoGrowZeroInit);

        var copied = service.ReadColumn(destinationId, 0, 0, 2);
        Assert.Equal("Alice", copied[0]!.Value.ValueUtf8);
        Assert.Equal("Bob", copied[1]!.Value.ValueUtf8);

        service.EnsureRefVec(parentId, refsCol: 0, parentRow: 0, policy: CapacityPolicy.Throw);
        service.LinkRef(parentId, refsCol: 0, parentRow: 0, childId, childCol: 0, childRow: 0, policy: CapacityPolicy.Throw);

        var linkedChildId = service.GetOrCreateAndLinkChild(parentId, refsCol: 0, parentRow: 0, CreateSupportedSpec("ChildNoKey", 1));
        var keyedChildId = service.GetOrCreateAndLinkChild(parentId, refsCol: 0, parentRow: 0, childKey: 7, CreateSupportedSpec("ChildKeyed", 1));
        var keyedChildIdAgain = service.GetOrCreateAndLinkChild(parentId, refsCol: 0, parentRow: 0, childKey: 7, CreateSupportedSpec("ChildKeyed", 1));

        Assert.NotEqual(Guid.Empty, linkedChildId);
        Assert.NotEqual(Guid.Empty, keyedChildId);
        Assert.Equal(keyedChildId, keyedChildIdAgain);
        Assert.Equal(keyedChildId, service.TryGetIdByName("ChildKeyed"));
    }

    [Fact]
    public async Task BeginAwaitCancelAndCompaction_Apis_Work()
    {
        using IMetaDBManagerRPCCompatible service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var tableId = service.RegisterTable(new TableSpec("Users", "users.map", [TableSpec.Helpers.Column("Name", 1, valueBytes: 64)]), createNow: true);

        await service.RestartDeleteWorker();

        var compactCallId = service.TryCompactTableBegin(tableId, strict: false);
        var compactAllCallId = service.TryCompactAllTablesBegin(strict: false);
        var compacted = await service.Await<bool>(compactCallId);
        var compactAll = await service.Await<TryCompactAllTablesResult>(compactAllCallId);

        await service.CancelAll();

        var missingAwait = await Assert.ThrowsAsync<RemoteInvocationException>(() => service.Await(-1));
        var missingCancel = await Assert.ThrowsAsync<RemoteInvocationException>(() => service.CancelByCallId(-1));

        Assert.False(compacted);
        Assert.False(compactAll.Success);
        Assert.Contains(tableId, compactAll.FailedTableIds ?? []);
        Assert.True(missingAwait.HResult == 404 || MetaDBHResult.IsMetaDBHResult(missingAwait.HResult));

        if (MetaDBHResult.TryDecode(missingCancel.HResult, out var op, out var err))
        {
            Assert.Equal(RpcOp.Cancel_CallId, op);
            Assert.Equal(RpcErr.NotFound, err);
        }
        else
        {
            Assert.Equal(404, missingCancel.HResult);
        }
    }

    [Fact]
    public async Task DisposeAsync_TransitionsServiceToDisposedState()
    {
        var service = MetaDBManagerRpcServiceHarness.CreateInMemoryService();
        var asyncDisposable = Assert.IsAssignableFrom<IAsyncDisposable>(service);

        await asyncDisposable.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => service.CloseAll());
    }

    [Fact]
    public void InternalCallTrackingHelpers_CoverCollisionAndCleanupPaths()
    {
        using var handle = InternalRpcHarness.CreateInMemoryService();
        using var cts = new CancellationTokenSource();

        handle.AddCtsAndCheckCallIdCollision(7, cts);
        handle.AddTaskAndCheckCallIdCollision(7, Task.CompletedTask);
        handle.MarkCleanup(7, DateTime.UtcNow.Ticks);

        Assert.Equal(1, handle.CtsCount);
        Assert.Equal(1, handle.TaskCount);
        Assert.Equal(1, handle.CleanupCount);

        var ctsCollision = Assert.Throws<InvalidOperationException>(() =>
            handle.AddCtsAndCheckCallIdCollision(7, new CancellationTokenSource()));
        Assert.Contains("collision", ctsCollision.Message, StringComparison.OrdinalIgnoreCase);

        var taskCollision = Assert.Throws<InvalidOperationException>(() =>
            handle.AddTaskAndCheckCallIdCollision(7, Task.CompletedTask));
        Assert.Contains("collision", taskCollision.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, handle.CtsCount);

        handle.CleanupCall(7);

        Assert.Equal(0, handle.TaskCount);
        Assert.Equal(0, handle.CleanupCount);
    }

    [Fact]
    public async Task AwaitGeneric_AndDisposeAsync_InternalState_AreCallable()
    {
        await using var handle = InternalRpcHarness.CreateInMemoryService();

        var rebuildCallId = handle.RebuildAllIndexesBegin(strict: false);
        await handle.Await(rebuildCallId);

        var compactCallId = handle.TryCompactAllTablesBegin(strict: false);
        var compactResult = await handle.Await<TryCompactAllTablesResult>(compactCallId);

        Assert.True(rebuildCallId > 0);
        Assert.True(compactResult.Success);
        Assert.True(handle.CtsCount >= 0);
    }

    private static CellResultDTO MakeCell(string keyUtf8, string valueUtf8) =>
        new(
            HasCell: true,
            EntrySize: default,
            KeyCapacity: 32,
            ValueCapacity: 64,
            IsKeyValue: true,
            HasKey: true,
            HasAnyValue: true,
            KeyUtf8LengthHint: keyUtf8.Length,
            ValueUtf8LengthHint: valueUtf8.Length,
            Mode: CellPayloadModeDTO.Both,
            KeyUtf8: keyUtf8,
            ValueUtf8: valueUtf8,
            KeyRaw: null,
            ValueRaw: null,
            Preview: valueUtf8);

    private static System.Text.Json.JsonElement EmptyJson() =>
        System.Text.Json.JsonDocument.Parse("{}").RootElement.Clone();

    private static TableSpec CreateSupportedSpec(string name, uint capacity) =>
        new(name, $"{name.ToLowerInvariant()}.map", [TableSpec.Helpers.Column("Value", capacity, valueBytes: 64)]);

    private static TableSpec CreateRefParentSpec(string name, uint capacity) =>
        new(name, $"{name.ToLowerInvariant()}.map", [TableSpec.Helpers.RefsColumn(capacity)]);
}
