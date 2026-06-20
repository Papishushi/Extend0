using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;

namespace Extend0.Metadata.Typed;

/// <summary>
/// Raw byte-oriented view over a MetaDB column when no stronger semantic value type is declared.
/// </summary>
public sealed class MetadataRawColumn : MetadataTypedColumnBase
{
    public MetadataRawColumn(IMetadataTable table, uint column, string name, int keySize, int valueSize)
        : base(table, column, name, keySize, valueSize)
    {
        ValidateColumn();
    }

    public MetadataCell GetOrCreateCell(uint row) => Table.GetOrCreateCell(Column, row);

    public bool TryGetCell(uint row, out MetadataCell cell) => Table.TryGetCell(Column, row, out cell);

    public bool TrySetValue(uint row, ReadOnlySpan<byte> value, bool clearRemainder = true)
    {
        var cell = GetOrCreateCell(row);
        if (value.Length > cell.ValueSize)
            return false;

        unsafe
        {
            var dst = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);
            if (clearRemainder)
                dst.Clear();

            value.CopyTo(dst);
            return true;
        }
    }

    public bool TryGetValue(uint row, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (!TryGetCell(row, out var cell))
            return false;

        return cell.TryGetValueRaw(out value);
    }

    public bool TrySetKey(uint row, ReadOnlySpan<byte> keyUtf8) =>
        KeySize > 0 && GetOrCreateCell(row).TrySetKey(keyUtf8);

    public bool TrySetKey(uint row, string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return KeySize > 0 && GetOrCreateCell(row).TrySetKey(key);
    }
}
