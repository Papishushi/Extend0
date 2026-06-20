using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;
using Extend0.Metadata.Refs;

namespace Extend0.Metadata.Typed;

/// <summary>
/// Strongly named helper over a MetaDB reference-vector column.
/// </summary>
public sealed class MetadataRefsColumn : MetadataTypedColumnBase
{
    public MetadataRefsColumn(IMetadataTable table, uint column, string name, int keySize, int valueSize)
        : base(table, column, name, keySize, valueSize)
    {
        if (MetadataTableRefVec.Capacity(valueSize) <= 0)
            throw new ArgumentException($"Column '{name}' does not have enough value bytes for a reference vector.", nameof(valueSize));

        ValidateColumn();
    }

    public MetadataCell GetOrCreateCell(uint row) => Table.GetOrCreateCell(Column, row);

    public bool TryGetCell(uint row, out MetadataCell cell) => Table.TryGetCell(Column, row, out cell);

    public int Capacity => MetadataTableRefVec.Capacity(ValueSize);

    public bool Init(uint row, bool markInitialized = true)
    {
        var cell = GetOrCreateCell(row);
        if (cell.ValueSize < MetadataTableRefVec.HeaderSize)
            return false;

        unsafe
        {
            MetadataTableRefVec.Init(new Span<byte>(cell.GetValuePointer(), cell.ValueSize), markInitialized);
            return true;
        }
    }

    public ushort Count(uint row)
    {
        if (!TryGetCell(row, out var cell) || !cell.TryGetValueRaw(out var raw) || raw.Length < MetadataTableRefVec.HeaderSize)
            return 0;

        return MetadataTableRefVec.GetCount(raw);
    }

    public bool TryAdd(uint row, Guid childTableId, uint childColumn = 0, uint childRow = 0, ulong reserved = 0)
    {
        var reference = new MetadataTableRef(childTableId, childColumn, childRow, reserved);
        return TryAdd(row, reference);
    }

    public bool TryAdd(uint row, in MetadataTableRef reference)
    {
        var cell = GetOrCreateCell(row);
        if (cell.ValueSize < MetadataTableRefVec.HeaderSize)
            return false;

        unsafe
        {
            var value = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);
            if (!MetadataTableRefVec.IsInitialized(value))
                MetadataTableRefVec.Init(value);

            return MetadataTableRefVec.TryAdd(value, reference, cell.ValueSize);
        }
    }

    public bool TryGet(uint row, int index, out MetadataTableRef reference)
    {
        reference = default;
        if (!TryGetCell(row, out var cell) || !cell.TryGetValueRaw(out var raw))
            return false;

        return MetadataTableRefVec.TryGet(raw, index, out reference);
    }

    public MetadataTableRef Get(uint row, int index) =>
        TryGet(row, index, out var reference)
            ? reference
            : throw new ArgumentOutOfRangeException(nameof(index));

    public int Find(uint row, Guid childTableId, uint childColumn = 0, uint childRow = 0)
    {
        if (!TryGetCell(row, out var cell) || !cell.TryGetValueRaw(out var raw))
            return -1;

        return MetadataTableRefVec.Find(raw, childTableId, childColumn, childRow);
    }

    public bool TryRemoveAt(uint row, int index)
    {
        if (!TryGetCell(row, out var cell))
            return false;

        unsafe
        {
            return MetadataTableRefVec.TryRemoveAt(new Span<byte>(cell.GetValuePointer(), cell.ValueSize), index);
        }
    }
}
