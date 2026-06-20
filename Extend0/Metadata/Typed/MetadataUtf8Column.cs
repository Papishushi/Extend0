using System.Text;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;

namespace Extend0.Metadata.Typed;

/// <summary>
/// UTF-8 string view over a fixed-size MetaDB value segment.
/// </summary>
public sealed class MetadataUtf8Column : MetadataTypedColumnBase
{
    public MetadataUtf8Column(IMetadataTable table, uint column, string name, int keySize, int valueSize)
        : base(table, column, name, keySize, valueSize)
    {
        ValidateColumn();
    }

    public MetadataCell GetOrCreateCell(uint row) => Table.GetOrCreateCell(Column, row);

    public bool TryGetCell(uint row, out MetadataCell cell) => Table.TryGetCell(Column, row, out cell);

    public bool TrySet(uint row, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return GetOrCreateCell(row).TrySetValue(value);
    }

    public void Set(uint row, string value)
    {
        if (!TrySet(row, value))
            throw new InvalidOperationException($"Could not write UTF-8 value into column '{Name}'.");
    }

    public bool TryGet(uint row, out string? value)
    {
        value = null;
        if (!TryGetCell(row, out var cell))
            return false;

        if (!cell.TryGetValueRaw(out var raw))
            return false;

        var end = raw.IndexOf((byte)0);
        var trimmed = end < 0 ? raw : raw[..end];
        value = Encoding.UTF8.GetString(trimmed);
        return true;
    }

    public string Get(uint row) =>
        TryGet(row, out var value)
            ? value ?? string.Empty
            : throw new InvalidOperationException($"Could not read row {row} from column '{Name}'.");

    public bool TrySetKey(uint row, ReadOnlySpan<byte> keyUtf8) =>
        KeySize > 0 && GetOrCreateCell(row).TrySetKey(keyUtf8);

    public bool TrySetKey(uint row, string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return KeySize > 0 && GetOrCreateCell(row).TrySetKey(key);
    }

    public bool TryFindRowByKey(ReadOnlySpan<byte> keyUtf8, out uint row) =>
        Table.TryFindRowByKey(Column, keyUtf8, out row);

    public bool TryFindRowByKey(string key, out uint row)
    {
        ArgumentNullException.ThrowIfNull(key);
        var byteCount = Encoding.UTF8.GetByteCount(key);
        Span<byte> buffer = byteCount <= 256 ? stackalloc byte[byteCount] : new byte[byteCount];
        Encoding.UTF8.GetBytes(key, buffer);
        return TryFindRowByKey(buffer, out row);
    }
}
