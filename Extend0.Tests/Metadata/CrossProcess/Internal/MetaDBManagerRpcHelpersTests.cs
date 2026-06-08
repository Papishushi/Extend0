using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.DTO;
using Extend0.Metadata.CrossProcess.HResult;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Testing.Metadata.CrossProcess.Internal;
using Extend0.Testing.Metadata.Internal;

namespace Extend0.Tests.Metadata.CrossProcess.Internal;

public sealed class MetaDBManagerRpcHelpersTests
{
    [Fact]
    public void WithUtf8_EncodesExactUtf8Bytes()
    {
        var encoded = MetaDBManagerRpcHelpersHarness.EncodeUtf8("hola-ñ");
        var largeText = new string('x', 600) + "ñ";
        var largeEncoded = MetaDBManagerRpcHelpersHarness.EncodeUtf8(largeText);

        Assert.Equal("hola-ñ"u8.ToArray(), encoded);
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(largeText), largeEncoded);
    }

    [Fact]
    public void BuildCellDto_ReturnsMissingSnapshot_ForAbsentCell()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

        var dto = MetaDBManagerRpcHelpersHarness.BuildCellDto(table, column: 0, row: 5, CellPayloadModeDTO.Both);

        Assert.NotNull(dto);
        Assert.False(dto.Value.HasCell);
        Assert.False(dto.Value.HasKey);
        Assert.False(dto.Value.HasAnyValue);
        Assert.Null(dto.Value.KeyUtf8);
        Assert.Null(dto.Value.ValueUtf8);
    }

    [Fact]
    public void BuildCellDto_HandlesKeyValueCells()
    {
        using IMetadataTable keyValue = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

        var kvCell = keyValue.GetOrCreateCell(0, 0);
        Assert.True(kvCell.TrySetKey("user-1"));
        Assert.True(kvCell.TrySetValue("Alice"));

        var keyValueDto = MetaDBManagerRpcHelpersHarness.BuildCellDto(keyValue, 0, 0, CellPayloadModeDTO.Both);

        Assert.NotNull(keyValueDto);
        Assert.True(keyValueDto.Value.HasCell);
        Assert.True(keyValueDto.Value.IsKeyValue);
        Assert.True(keyValueDto.Value.HasKey);
        Assert.True(keyValueDto.Value.HasAnyValue);
        Assert.Equal("user-1", keyValueDto.Value.KeyUtf8);
        Assert.Equal("Alice", keyValueDto.Value.ValueUtf8);
        Assert.NotNull(keyValueDto.Value.KeyRaw);
        Assert.NotNull(keyValueDto.Value.ValueRaw);
        Assert.Equal("Alice", keyValueDto.Value.Preview);
    }

    [Fact]
    public void EnsureCapacityBestEffort_CoversSuccessFalseAndThrowModes()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));
        using IMetadataTable retrySuccess = MetadataTableHarness.CreateCapacityRetryTable(
            MetadataTableHarness.CreateInMemoryTable("RetrySuccess", TableSpec.Helpers.Column("Name", 1, valueBytes: 64)),
            targetColumn: 0,
            targetRow: 3,
            growResult: true);
        using IMetadataTable retryFailure = MetadataTableHarness.CreateCapacityRetryTable(
            MetadataTableHarness.CreateInMemoryTable("RetryFailure", TableSpec.Helpers.Column("Name", 1, valueBytes: 64)),
            targetColumn: 0,
            targetRow: 3,
            growResult: true,
            retryThrows: true);
        using IMetadataTable growFailure = MetadataTableHarness.CreateCapacityRetryTable(
            MetadataTableHarness.CreateInMemoryTable("GrowFailure", TableSpec.Helpers.Column("Name", 1, valueBytes: 64)),
            targetColumn: 0,
            targetRow: 3,
            growResult: false);
        using IMetadataTable growFailureThrow = MetadataTableHarness.CreateCapacityRetryTable(
            MetadataTableHarness.CreateInMemoryTable("GrowFailureThrow", TableSpec.Helpers.Column("Name", 1, valueBytes: 64)),
            targetColumn: 0,
            targetRow: 3,
            growResult: false);

        var success = MetaDBManagerRpcHelpersHarness.EnsureCapacityBestEffort(table, column: 0, row: 3, CapacityPolicy.AutoGrowZeroInit);
        var softFailure = MetaDBManagerRpcHelpersHarness.EnsureCapacityBestEffort(table, column: 4, row: 0, CapacityPolicy.None);
        var retrySuccessResult = MetaDBManagerRpcHelpersHarness.EnsureCapacityBestEffort(retrySuccess, column: 0, row: 3, CapacityPolicy.AutoGrowZeroInit);
        var retryFailureResult = MetaDBManagerRpcHelpersHarness.EnsureCapacityBestEffort(retryFailure, column: 0, row: 3, CapacityPolicy.AutoGrowZeroInit);
        var growFailureResult = MetaDBManagerRpcHelpersHarness.EnsureCapacityBestEffort(growFailure, column: 0, row: 3, CapacityPolicy.AutoGrowZeroInit);

        Assert.True(success);
        Assert.False(softFailure);
        Assert.True(retrySuccessResult);
        Assert.False(retryFailureResult);
        Assert.False(growFailureResult);
        Assert.Throws<InvalidOperationException>(() => MetaDBManagerRpcHelpersHarness.EnsureCapacityBestEffort(growFailureThrow, column: 0, row: 3, CapacityPolicy.Throw));
        Assert.Throws<ArgumentOutOfRangeException>(() => MetaDBManagerRpcHelpersHarness.EnsureCapacityBestEffort(table, column: 4, row: 0, CapacityPolicy.Throw));
    }

    [Fact]
    public void IndexHelpers_DescribeBuiltInAndCustomIndexes()
    {
        using IMetadataTable table = MetadataTableHarness.CreateInMemoryTable(
            "Users",
            TableSpec.Helpers.Column("Name", 1, valueBytes: 64));

        var builtIn = table.Indexes.Enumerate().First();
        var custom = new FakeRebuildableIndex("custom");

        var builtInDto = MetaDBManagerRpcHelpersHarness.ToIndexInfoDto(builtIn);
        var customDto = MetaDBManagerRpcHelpersHarness.ToIndexInfoDto(custom);

        Assert.True(MetaDBManagerRpcHelpersHarness.IsBuiltIn(builtIn));
        Assert.False(MetaDBManagerRpcHelpersHarness.IsBuiltIn(custom));
        Assert.True(builtInDto.IsBuiltIn);
        Assert.True(builtInDto.Kind is IndexKindDTO.BuiltIn_ColumnKey or IndexKindDTO.BuiltIn_GlobalKey or IndexKindDTO.BuiltIn_GlobalMultiTableKey);
        Assert.False(customDto.IsBuiltIn);
        Assert.True(customDto.IsRebuildable);
        Assert.Equal(IndexKindDTO.Unknown, customDto.Kind);
    }

    [Fact]
    public void TryCreateCustomIndex_ReturnsNotSupported_ForCurrentBackend()
    {
        var customRequest = new AddIndexRequestDTO("custom", IndexKindDTO.Custom_InTable, MetaDBManagerRpcHelpersHarness.EmptyJsonObject());
        var invalidKindRequest = new AddIndexRequestDTO("builtin", IndexKindDTO.BuiltIn_ColumnKey, MetaDBManagerRpcHelpersHarness.EmptyJsonObject());

        var customStatus = MetaDBManagerRpcHelpersHarness.TryCreateCustomIndex(customRequest, out var customNotes);
        var invalidKindStatus = MetaDBManagerRpcHelpersHarness.TryCreateCustomIndex(invalidKindRequest, out var invalidKindNotes);

        Assert.Equal(IndexMutationStatusDTO.NotSupported, customStatus);
        Assert.Equal(IndexMutationStatusDTO.NotSupported, invalidKindStatus);
        Assert.NotNull(customNotes);
        Assert.NotNull(invalidKindNotes);
    }

    [Fact]
    public void FixedSegmentWriters_RespectModeAndZeroFill()
    {
        var fixedBytes = MetaDBManagerRpcHelpersHarness.WriteFixed(6, [1, 2, 3, 4, 5, 6, 7]);
        var keyRaw = MetaDBManagerRpcHelpersHarness.WriteKeySegment(8, [1, 2, 3], "ignored", CellPayloadModeDTO.Both);
        var keyUtf8 = MetaDBManagerRpcHelpersHarness.WriteKeySegment(8, null, "abc", CellPayloadModeDTO.Utf8Only);
        var valueUtf8 = MetaDBManagerRpcHelpersHarness.WriteValueSegment(8, null, "xy", CellPayloadModeDTO.Both);
        var zeroFilled = MetaDBManagerRpcHelpersHarness.ZeroFill([9, 9, 9, 9], 3);

        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, fixedBytes);
        Assert.Equal(new byte[] { 1, 2, 3, 0, 0, 0, 0, 0 }, keyRaw);
        Assert.Equal(new byte[] { (byte)'a', (byte)'b', (byte)'c', 0, 0, 0, 0, 0 }, keyUtf8);
        Assert.Equal(new byte[] { (byte)'x', (byte)'y', 0, 0, 0, 0, 0, 0 }, valueUtf8);
        Assert.Equal(new byte[] { 0, 0, 0, 9 }, zeroFilled);
    }

    [Fact]
    public void MakePreview_CoversPrintableAndHexFormattingBranches()
    {
        var printable = MetaDBManagerRpcHelpersHarness.MakePreview("abcdefghijklmnopqrstuvwxyz"u8.ToArray(), lenHint: 26, maxChars: 8);
        var hexWithEllipsis = MetaDBManagerRpcHelpersHarness.MakePreview([0xDE, 0xAD, 0xBE, 0xEF], lenHint: 4, maxChars: 5);
        var hexEmptyWhenDisabled = MetaDBManagerRpcHelpersHarness.MakePreview([0xDE, 0xAD], lenHint: 2, maxChars: 0);

        Assert.StartsWith("abcdefg", printable, StringComparison.Ordinal);
        Assert.EndsWith("…", printable, StringComparison.Ordinal);
        Assert.Equal("DEAD…", hexWithEllipsis);
        Assert.Equal(string.Empty, hexEmptyWhenDisabled);
    }

    [Fact]
    public async Task RpcHelpers_ClassifyAndStampExceptions()
    {
        Assert.Equal(RpcErr.InvalidArg, MetaDBManagerRpcHelpersHarness.ClassifyErr(new ArgumentException("bad")));
        Assert.Equal(RpcErr.NotFound, MetaDBManagerRpcHelpersHarness.ClassifyErr(new KeyNotFoundException()));
        Assert.Equal(RpcErr.AlreadyExists, MetaDBManagerRpcHelpersHarness.ClassifyErr(new InvalidOperationException("already registered")));
        Assert.Equal(RpcErr.Unexpected, MetaDBManagerRpcHelpersHarness.ClassifyErr(new InvalidOperationException("other")));

        var syncHResult = MetaDBManagerRpcHelpersHarness.CaptureRpcHResult(RpcOp.FillColumn, new ArgumentException("bad"));
        var voidHResult = MetaDBManagerRpcHelpersHarness.CaptureRpcVoidHResult(RpcOp.LinkRef, new UnauthorizedAccessException("blocked"));
        var asyncHResult = await MetaDBManagerRpcHelpersHarness.CaptureRpcAsyncHResult(RpcOp.ReadCell, new ObjectDisposedException("svc"));
        var asyncValueHResult = await MetaDBManagerRpcHelpersHarness.CaptureRpcAsyncValueHResult(RpcOp.FindGlobal_Manager_Bytes, new NotSupportedException("nope"));

        Assert.Equal((RpcOp.FillColumn, RpcErr.InvalidArg), MetaDBHResult.Decode(syncHResult));
        Assert.Equal((RpcOp.LinkRef, RpcErr.Protected), MetaDBHResult.Decode(voidHResult));
        Assert.Equal((RpcOp.ReadCell, RpcErr.Disposed), MetaDBHResult.Decode(asyncHResult));
        Assert.Equal((RpcOp.FindGlobal_Manager_Bytes, RpcErr.NotSupported), MetaDBHResult.Decode(asyncValueHResult));
    }

    [Fact]
    public void MetaDBHResult_RejectsNonRpcValues_AndFlagsUndefinedEnums()
    {
        var plain = 42;
        var encoded = MetaDBHResult.MakeRpcHResult((RpcOp)250, (RpcErr)251);

        Assert.False(MetaDBHResult.IsMetaDBHResult(plain));
        Assert.False(MetaDBHResult.TryDecode(plain, out _, out _));
        Assert.False(MetaDBHResult.TryDecodeDefined(encoded, out var op, out var err));
        Assert.Equal((RpcOp)250, op);
        Assert.Equal((RpcErr)251, err);

        var ex = Assert.Throws<ArgumentException>(() => MetaDBHResult.Decode(plain));
        Assert.Contains("HRESULT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class FakeRebuildableIndex(string name) : IRebuildableIndex<byte[], int>
    {
        public string Name { get; } = name;
        public Type KeyType => typeof(byte[]);
        public Type ValueType => typeof(int);
        public bool Add(byte[] key, int value) => true;
        public void Clear() { }
        public void Dispose() { }
        public bool Remove(byte[] key) => true;
        public Task Rebuild(IMetadataTable table) => Task.CompletedTask;
        public bool TryGetValue(byte[] key, out int value)
        {
            value = 0;
            return false;
        }
    }
}
