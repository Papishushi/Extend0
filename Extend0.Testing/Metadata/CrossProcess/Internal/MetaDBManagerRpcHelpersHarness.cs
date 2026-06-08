using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.DTO;
using Extend0.Metadata.CrossProcess.HResult;
using Extend0.Metadata.CrossProcess.Internal;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Storage;
using System.Text.Json;

namespace Extend0.Testing.Metadata.CrossProcess.Internal;

public static class MetaDBManagerRpcHelpersHarness
{
    public static byte[] EncodeUtf8(string value)
    {
        byte[]? encoded = null;
        MetaDBManagerRPCCompatibleHelpers.WithUtf8(value, bytes =>
        {
            encoded = bytes;
            return new IndexLookupResultDTO(default, default);
        });
        return encoded ?? [];
    }

    public static CellResultDTO? BuildCellDto(IMetadataTable table, uint column, uint row, CellPayloadModeDTO mode) =>
        MetaDBManagerRPCCompatibleHelpers.BuildCellDto(table, column, row, mode);

    public static bool EnsureCapacityBestEffort(IMetadataTable table, uint column, uint row, CapacityPolicy policy) =>
        MetaDBManagerRPCCompatibleHelpers.EnsureCapacityBestEffort(table, column, row, policy);

    public static bool IsBuiltIn(ITableIndex index) =>
        MetaDBManagerRPCCompatibleHelpers.IsBuiltIn(index);

    public static IndexInfoDTO ToIndexInfoDto(ITableIndex index) =>
        MetaDBManagerRPCCompatibleHelpers.ToIndexInfoDTO(index);

    public static IndexMutationStatusDTO TryCreateCustomIndex(AddIndexRequestDTO request, out string? notes)
    {
        var status = MetaDBManagerRPCCompatibleHelpers.TryCreateCustomIndex<FakeTableIndex>(request, out _, out notes);
        return status;
    }

    public static unsafe byte[] WriteFixed(int capacity, byte[] source)
    {
        var buffer = new byte[capacity];
        fixed (byte* ptr = buffer)
            MetaDBManagerRPCCompatibleHelpers.WriteFixed(ptr, capacity, source);
        return buffer;
    }

    public static unsafe byte[] WriteKeySegment(int capacity, byte[]? keyRaw, string? keyUtf8, CellPayloadModeDTO mode)
    {
        var buffer = new byte[capacity];
        fixed (byte* ptr = buffer)
            MetaDBManagerRPCCompatibleHelpers.WriteKeySegment(ptr, capacity, keyRaw, keyUtf8, mode);
        return buffer;
    }

    public static unsafe byte[] WriteValueSegment(int capacity, byte[]? valueRaw, string? valueUtf8, CellPayloadModeDTO mode)
    {
        var buffer = new byte[capacity];
        fixed (byte* ptr = buffer)
            MetaDBManagerRPCCompatibleHelpers.WriteValueSegment(ptr, capacity, valueRaw, valueUtf8, mode);
        return buffer;
    }

    public static unsafe byte[] ZeroFill(byte[] buffer, int bytes)
    {
        fixed (byte* ptr = buffer)
            MetaDBManagerRPCCompatibleHelpers.ZeroFill(ptr, bytes);
        return buffer;
    }

    public static RpcErr ClassifyErr(Exception exception) =>
        MetaDBManagerRPCCompatibleHelpers.ClassifyErr(exception);

    public static string MakePreview(byte[] data, int lenHint, int maxChars) =>
        MetaDBManagerRPCCompatibleHelpers.MakePreviewForTests(data, lenHint, maxChars);

    public static int CaptureRpcHResult(RpcOp op, Exception exception)
    {
        try
        {
            MetaDBManagerRPCCompatibleHelpers.Rpc<int>(op, () => throw exception, static () => { });
            return 0;
        }
        catch (Exception ex)
        {
            return ex.HResult;
        }
    }

    public static int CaptureRpcVoidHResult(RpcOp op, Exception exception)
    {
        try
        {
            MetaDBManagerRPCCompatibleHelpers.RpcVoid(op, () => throw exception, static () => { });
            return 0;
        }
        catch (Exception ex)
        {
            return ex.HResult;
        }
    }

    public static async Task<int> CaptureRpcAsyncHResult(RpcOp op, Exception exception)
    {
        try
        {
            await MetaDBManagerRPCCompatibleHelpers.RpcAsync(op, () => Task.FromException(exception), static () => { });
            return 0;
        }
        catch (Exception ex)
        {
            return ex.HResult;
        }
    }

    public static async Task<int> CaptureRpcAsyncValueHResult(RpcOp op, Exception exception)
    {
        try
        {
            await MetaDBManagerRPCCompatibleHelpers.RpcAsync<int>(op, () => Task.FromException<int>(exception), static () => { });
            return 0;
        }
        catch (Exception ex)
        {
            return ex.HResult;
        }
    }

    public static JsonElement EmptyJsonObject() =>
        JsonDocument.Parse("{}").RootElement.Clone();

    private sealed class FakeTableIndex : ITableIndex
    {
        public string Name => "fake";
        public Type KeyType => typeof(byte[]);
        public Type ValueType => typeof(int);
        public void Clear() { }
        public void Dispose() { }
    }
}
