using System.Text.Json;
using Extend0.Metadata.CrossProcess.DTO;

namespace Extend0.Tests.Metadata.CrossProcess;

public sealed class MetaDBCrossProcessDtoTests
{
    [Fact]
    public void Dtos_UseRecordValueSemantics_AndPreservePayloads()
    {
        using var payloadDoc = JsonDocument.Parse("""{"column":"users.name","mode":"btree"}""");
        var payload = payloadDoc.RootElement.Clone();
        var request = new AddIndexRequestDTO(
            Name: "users-by-name",
            Kind: IndexKindDTO.Custom_InTable,
            IndexInputPayload: payload,
            ProgramBytes: [1, 2, 3],
            ProgramHashSha256: "abc123",
            ReplaceIfExists: true,
            Notes: "seed");

        var hit = new IndexHitDTO(true, 2u, 7u, "Users");
        var lookup = new IndexLookupResultDTO(IndexLookupStatusDTO.Ok, hit, "found");
        var indexInfo = new IndexInfoDTO("users-by-name", IndexKindDTO.Custom_InTable, IsRebuildable: true, IsBuiltIn: false, Notes: "ready");
        var mutation = new IndexMutationResultDTO(IndexMutationStatusDTO.Ok, indexInfo, "created");

        var cell = new CellResultDTO(
            HasCell: true,
            EntrySize: default,
            KeyCapacity: 16,
            ValueCapacity: 64,
            IsKeyValue: true,
            HasKey: true,
            HasAnyValue: true,
            KeyUtf8LengthHint: 5,
            ValueUtf8LengthHint: 5,
            Mode: CellPayloadModeDTO.Both,
            KeyUtf8: "alpha",
            ValueUtf8: "Alice",
            KeyRaw: [97, 108, 112, 104, 97],
            ValueRaw: [65, 108, 105, 99, 101],
            Preview: "alpha=Alice");

        Assert.Equal("users-by-name", request.Name);
        Assert.True(request.ReplaceIfExists);
        Assert.Equal("abc123", request.ProgramHashSha256);
        Assert.Equal("users.name", request.IndexInputPayload.GetProperty("column").GetString());
        Assert.Equal("found", lookup.Notes);
        Assert.Equal("ready", indexInfo.Notes);
        Assert.Equal(hit, lookup.Hit);
        Assert.Equal(indexInfo, mutation.Index);
        Assert.Equal("created", mutation.Notes);
        Assert.Equal(default, cell.EntrySize);
        Assert.Equal(16, cell.KeyCapacity);
        Assert.Equal(64, cell.ValueCapacity);
        Assert.Equal(5, cell.KeyUtf8LengthHint);
        Assert.Equal(5, cell.ValueUtf8LengthHint);
        Assert.Equal("alpha", cell.KeyUtf8);
        Assert.Equal("Alice", cell.ValueUtf8);
        Assert.Equal("alpha=Alice", cell.Preview);

        Assert.Equal(request, request with { });
        Assert.Equal(hit, hit with { });
        Assert.Equal(lookup, lookup with { });
        Assert.Equal(mutation, mutation with { });
        Assert.Equal(cell, cell with { });
    }

    [Fact]
    public void Dtos_DifferentPayloads_ProduceDifferentEqualityAndStableHashCodes()
    {
        using var leftDoc = JsonDocument.Parse("""{"column":"users.name","mode":"btree"}""");
        using var rightDoc = JsonDocument.Parse("""{"column":"users.email","mode":"hash"}""");
        var sharedProgramBytes = new byte[] { 1, 2, 3 };
        var sharedPayload = leftDoc.RootElement.Clone();

        var left = new AddIndexRequestDTO(
            "users-by-name",
            IndexKindDTO.Custom_InTable,
            sharedPayload,
            ProgramBytes: sharedProgramBytes,
            ProgramHashSha256: "hash-a",
            ReplaceIfExists: false,
            Notes: "a");
        var same = new AddIndexRequestDTO(
            "users-by-name",
            IndexKindDTO.Custom_InTable,
            sharedPayload,
            ProgramBytes: sharedProgramBytes,
            ProgramHashSha256: "hash-a",
            ReplaceIfExists: false,
            Notes: "a");
        var different = new AddIndexRequestDTO(
            "users-by-name",
            IndexKindDTO.Custom_InTable,
            rightDoc.RootElement.Clone(),
            ProgramBytes: [9],
            ProgramHashSha256: "hash-b",
            ReplaceIfExists: true,
            Notes: "b");

        var hit = new IndexHitDTO(false, 0, 0, string.Empty);
        var lookup = new IndexLookupResultDTO(IndexLookupStatusDTO.NotFound, hit);
        var info = new IndexInfoDTO("users-by-name", IndexKindDTO.BuiltIn_GlobalKey, IsRebuildable: false, IsBuiltIn: true);
        var mutation = new IndexMutationResultDTO(IndexMutationStatusDTO.AlreadyExists);
        var emptyCell = new CellResultDTO(
            HasCell: false,
            EntrySize: default,
            KeyCapacity: 0,
            ValueCapacity: 0,
            IsKeyValue: false,
            HasKey: false,
            HasAnyValue: false,
            KeyUtf8LengthHint: 0,
            ValueUtf8LengthHint: 0,
            Mode: CellPayloadModeDTO.RawOnly,
            KeyUtf8: null,
            ValueUtf8: null,
            KeyRaw: null,
            ValueRaw: null,
            Preview: null);

        Assert.Equal(left, same);
        Assert.Equal(left.GetHashCode(), same.GetHashCode());
        Assert.NotEqual(left, different);
        Assert.NotEqual(left.GetHashCode(), different.GetHashCode());
        Assert.Equal("users.name", left.IndexInputPayload.GetProperty("column").GetString());
        Assert.Equal(new byte[] { 1, 2, 3 }, left.ProgramBytes);

        Assert.False(lookup.Hit.Found);
        Assert.True(info.IsBuiltIn);
        Assert.False(info.IsRebuildable);
        Assert.Null(mutation.Index);
        Assert.False(emptyCell.HasCell);
        Assert.Equal(CellPayloadModeDTO.RawOnly, emptyCell.Mode);
        Assert.Null(emptyCell.Preview);
    }
}
