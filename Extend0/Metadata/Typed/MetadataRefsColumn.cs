using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;
using Extend0.Metadata.Refs;

namespace Extend0.Metadata.Typed;

/// <summary>
/// Strongly named helper over a MetaDB reference-vector column.
/// </summary>
public sealed class MetadataRefsColumn : MetadataTypedColumnBase
{
    /// <summary>
    /// Initializes a reference-vector typed column wrapper.
    /// </summary>
    /// <param name="table">Underlying dynamic MetaDB table.</param>
    /// <param name="column">Zero-based column index in the underlying table.</param>
    /// <param name="name">Expected schema column name.</param>
    /// <param name="keySize">Fixed key segment size in bytes.</param>
    /// <param name="valueSize">Fixed value segment size in bytes available for the reference vector.</param>
    public MetadataRefsColumn(IMetadataTable table, uint column, string name, int keySize, int valueSize)
        : base(table, column, name, keySize, valueSize)
    {
        if (MetadataTableRefVec.Capacity(valueSize) <= 0)
            throw new ArgumentException($"Column '{name}' does not have enough value bytes for a reference vector.", nameof(valueSize));

        ValidateColumn();
    }

    /// <summary>
    /// Gets an existing cell or creates it when the row is within growable capacity.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <returns>The resolved metadata cell.</returns>
    public MetadataCell GetOrCreateCell(uint row) => Table.GetOrCreateCell(Column, row);

    /// <summary>
    /// Attempts to get an existing cell without creating storage.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="cell">Receives the resolved cell when the method succeeds.</param>
    /// <returns><see langword="true"/> when the cell exists; otherwise <see langword="false"/>.</returns>
    public bool TryGetCell(uint row, out MetadataCell cell) => Table.TryGetCell(Column, row, out cell);

    /// <summary>
    /// Gets the maximum number of references that fit in one value cell.
    /// </summary>
    public int Capacity => MetadataTableRefVec.Capacity(ValueSize);

    /// <summary>
    /// Initializes a row's reference vector to an empty state.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="markInitialized">Whether to set the initialized flag in the vector header.</param>
    /// <returns><see langword="true"/> when the vector header fits and was initialized; otherwise <see langword="false"/>.</returns>
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

    /// <summary>
    /// Reads the number of references currently stored in a row's vector.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <returns>The stored reference count, or zero when the row cannot be read.</returns>
    public ushort Count(uint row)
    {
        if (!TryGetCell(row, out var cell) || !cell.TryGetValueRaw(out var raw) || raw.Length < MetadataTableRefVec.HeaderSize)
            return 0;

        return MetadataTableRefVec.GetCount(raw);
    }

    /// <summary>
    /// Attempts to append a child table reference to a row's vector.
    /// </summary>
    /// <param name="row">Zero-based parent row index.</param>
    /// <param name="childTableId">Identifier of the referenced child table.</param>
    /// <param name="childColumn">Referenced child column index.</param>
    /// <param name="childRow">Referenced child row index.</param>
    /// <param name="reserved">Reserved payload carried by the reference record.</param>
    /// <returns><see langword="true"/> when the reference was appended; otherwise <see langword="false"/>.</returns>
    public bool TryAdd(uint row, Guid childTableId, uint childColumn = 0, uint childRow = 0, ulong reserved = 0)
    {
        var reference = new MetadataTableRef(childTableId, childColumn, childRow, reserved);
        return TryAdd(row, reference);
    }

    /// <summary>
    /// Attempts to append a prebuilt reference to a row's vector.
    /// </summary>
    /// <param name="row">Zero-based parent row index.</param>
    /// <param name="reference">Reference record to append.</param>
    /// <returns><see langword="true"/> when the reference was appended; otherwise <see langword="false"/>.</returns>
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

    /// <summary>
    /// Attempts to read a reference at a specific vector position.
    /// </summary>
    /// <param name="row">Zero-based parent row index.</param>
    /// <param name="index">Zero-based reference index within the row vector.</param>
    /// <param name="reference">Receives the reference when the method succeeds.</param>
    /// <returns><see langword="true"/> when the reference exists; otherwise <see langword="false"/>.</returns>
    public bool TryGet(uint row, int index, out MetadataTableRef reference)
    {
        reference = default;
        if (!TryGetCell(row, out var cell) || !cell.TryGetValueRaw(out var raw))
            return false;

        return MetadataTableRefVec.TryGet(raw, index, out reference);
    }

    /// <summary>
    /// Reads a reference at a specific vector position or throws when it does not exist.
    /// </summary>
    /// <param name="row">Zero-based parent row index.</param>
    /// <param name="index">Zero-based reference index within the row vector.</param>
    /// <returns>The requested table reference.</returns>
    public MetadataTableRef Get(uint row, int index) =>
        TryGet(row, index, out var reference)
            ? reference
            : throw new ArgumentOutOfRangeException(nameof(index));

    /// <summary>
    /// Finds the index of a child reference in a row's vector.
    /// </summary>
    /// <param name="row">Zero-based parent row index.</param>
    /// <param name="childTableId">Identifier of the referenced child table.</param>
    /// <param name="childColumn">Referenced child column index.</param>
    /// <param name="childRow">Referenced child row index.</param>
    /// <returns>The zero-based reference index, or <c>-1</c> when no match exists.</returns>
    public int Find(uint row, Guid childTableId, uint childColumn = 0, uint childRow = 0)
    {
        if (!TryGetCell(row, out var cell) || !cell.TryGetValueRaw(out var raw))
            return -1;

        return MetadataTableRefVec.Find(raw, childTableId, childColumn, childRow);
    }

    /// <summary>
    /// Attempts to remove a reference by index from a row's vector.
    /// </summary>
    /// <param name="row">Zero-based parent row index.</param>
    /// <param name="index">Zero-based reference index within the row vector.</param>
    /// <returns><see langword="true"/> when the reference was removed; otherwise <see langword="false"/>.</returns>
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
