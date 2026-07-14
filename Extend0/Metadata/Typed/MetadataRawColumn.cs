using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;

namespace Extend0.Metadata.Typed;

/// <summary>
/// Raw byte-oriented view over a MetaDB column when no stronger semantic value type is declared.
/// </summary>
public sealed class MetadataRawColumn : MetadataTypedColumnBase
{
    /// <summary>
    /// Initializes a raw byte-oriented typed column wrapper.
    /// </summary>
    /// <param name="table">Underlying dynamic MetaDB table.</param>
    /// <param name="column">Zero-based column index in the underlying table.</param>
    /// <param name="name">Expected schema column name.</param>
    /// <param name="keySize">Fixed key segment size in bytes.</param>
    /// <param name="valueSize">Fixed value segment size in bytes.</param>
    public MetadataRawColumn(IMetadataTable table, uint column, string name, int keySize, int valueSize)
        : base(table, column, name, keySize, valueSize)
    {
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
    /// Attempts to write raw bytes into the cell value segment.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="value">Raw bytes to copy into the value segment.</param>
    /// <param name="clearRemainder">Whether to clear unused bytes before copying the value.</param>
    /// <returns><see langword="true"/> when the value fits and was written; otherwise <see langword="false"/>.</returns>
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

    /// <summary>
    /// Attempts to read the raw value bytes for a row.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="value">Receives a span over the value segment when the method succeeds.</param>
    /// <returns><see langword="true"/> when the value segment can be read; otherwise <see langword="false"/>.</returns>
    public bool TryGetValue(uint row, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (!TryGetCell(row, out var cell))
            return false;

        return cell.TryGetValueRaw(out value);
    }

    /// <summary>
    /// Attempts to write raw key bytes for a key/value column.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="keyUtf8">Key bytes to copy into the fixed key segment.</param>
    /// <returns><see langword="true"/> when the column has a key segment and the key fits; otherwise <see langword="false"/>.</returns>
    public bool TrySetKey(uint row, ReadOnlySpan<byte> keyUtf8) =>
        KeySize > 0 && GetOrCreateCell(row).TrySetKey(keyUtf8);

    /// <summary>
    /// Attempts to write a UTF-8 encoded string key for a key/value column.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="key">Key text to encode as UTF-8.</param>
    /// <returns><see langword="true"/> when the column has a key segment and the key fits; otherwise <see langword="false"/>.</returns>
    public bool TrySetKey(uint row, string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return KeySize > 0 && GetOrCreateCell(row).TrySetKey(key);
    }
}
