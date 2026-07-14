using System.Text;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;

namespace Extend0.Metadata.Typed;

/// <summary>
/// UTF-8 string view over a fixed-size MetaDB value segment.
/// </summary>
public sealed class MetadataUtf8Column : MetadataTypedColumnBase
{
    /// <summary>
    /// Initializes a UTF-8 string typed column wrapper.
    /// </summary>
    /// <param name="table">Underlying dynamic MetaDB table.</param>
    /// <param name="column">Zero-based column index in the underlying table.</param>
    /// <param name="name">Expected schema column name.</param>
    /// <param name="keySize">Fixed key segment size in bytes.</param>
    /// <param name="valueSize">Fixed value segment size in bytes.</param>
    public MetadataUtf8Column(IMetadataTable table, uint column, string name, int keySize, int valueSize)
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
    /// Attempts to encode and write a UTF-8 string into the value segment.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="value">String value to encode as UTF-8.</param>
    /// <returns><see langword="true"/> when the encoded value fits; otherwise <see langword="false"/>.</returns>
    public bool TrySet(uint row, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return GetOrCreateCell(row).TrySetValue(value);
    }

    /// <summary>
    /// Writes a UTF-8 string into the value segment or throws when it cannot be written.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="value">String value to encode as UTF-8.</param>
    public void Set(uint row, string value)
    {
        if (!TrySet(row, value))
            throw new InvalidOperationException($"Could not write UTF-8 value into column '{Name}'.");
    }

    /// <summary>
    /// Attempts to read and decode a UTF-8 string from the value segment.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="value">Receives the decoded string when the method succeeds.</param>
    /// <returns><see langword="true"/> when the value can be read and decoded; otherwise <see langword="false"/>.</returns>
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

    /// <summary>
    /// Reads and decodes a UTF-8 string from the value segment or throws when it cannot be read.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <returns>The decoded string value, or an empty string for a null decoded value.</returns>
    public string Get(uint row) =>
        TryGet(row, out var value)
            ? value ?? string.Empty
            : throw new InvalidOperationException($"Could not read row {row} from column '{Name}'.");

    /// <summary>
    /// Attempts to write raw UTF-8 key bytes for a key/value column.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="keyUtf8">UTF-8 key bytes to copy into the fixed key segment.</param>
    /// <returns><see langword="true"/> when the column has a key segment and the key fits; otherwise <see langword="false"/>.</returns>
    public bool TrySetKey(uint row, ReadOnlySpan<byte> keyUtf8) =>
        KeySize > 0 && GetOrCreateCell(row).TrySetKey(keyUtf8);

    /// <summary>
    /// Attempts to encode and write a string key for a key/value column.
    /// </summary>
    /// <param name="row">Zero-based row index.</param>
    /// <param name="key">Key text to encode as UTF-8.</param>
    /// <returns><see langword="true"/> when the column has a key segment and the key fits; otherwise <see langword="false"/>.</returns>
    public bool TrySetKey(uint row, string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return KeySize > 0 && GetOrCreateCell(row).TrySetKey(key);
    }

    /// <summary>
    /// Attempts to find a row by raw UTF-8 key bytes.
    /// </summary>
    /// <param name="keyUtf8">UTF-8 key bytes to look up.</param>
    /// <param name="row">Receives the matching row when the method succeeds.</param>
    /// <returns><see langword="true"/> when a row with the key exists; otherwise <see langword="false"/>.</returns>
    public bool TryFindRowByKey(ReadOnlySpan<byte> keyUtf8, out uint row) =>
        Table.TryFindRowByKey(Column, keyUtf8, out row);

    /// <summary>
    /// Attempts to find a row by string key.
    /// </summary>
    /// <param name="key">Key text to encode as UTF-8 before lookup.</param>
    /// <param name="row">Receives the matching row when the method succeeds.</param>
    /// <returns><see langword="true"/> when a row with the key exists; otherwise <see langword="false"/>.</returns>
    public bool TryFindRowByKey(string key, out uint row)
    {
        ArgumentNullException.ThrowIfNull(key);
        var byteCount = Encoding.UTF8.GetByteCount(key);
        Span<byte> buffer = byteCount <= 256 ? stackalloc byte[byteCount] : new byte[byteCount];
        Encoding.UTF8.GetBytes(key, buffer);
        return TryFindRowByKey(buffer, out row);
    }
}
