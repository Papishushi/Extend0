using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;

namespace Extend0.Metadata.Typed;

/// <summary>
/// Strongly typed unmanaged-value view over a MetaDB column.
/// </summary>
/// <typeparam name="T">Unmanaged value stored in the fixed value segment.</typeparam>
public sealed class MetadataValueColumn<T> : MetadataTypedColumnBase where T : unmanaged
{
    public MetadataValueColumn(IMetadataTable table, uint column, string name, int keySize, int valueSize)
        : base(table, column, name, keySize, valueSize)
    {
        if (Unsafe.SizeOf<T>() > valueSize)
            throw new ArgumentException($"Value type '{typeof(T).FullName}' requires {Unsafe.SizeOf<T>()} bytes, but column '{name}' only has {valueSize} value bytes.", nameof(valueSize));

        ValidateColumn();
    }

    /// <summary>
    /// Gets or creates the raw cell for <paramref name="row"/>.
    /// </summary>
    public MetadataCell GetOrCreateCell(uint row) => Table.GetOrCreateCell(Column, row);

    /// <summary>
    /// Attempts to get an existing raw cell for <paramref name="row"/>.
    /// </summary>
    public bool TryGetCell(uint row, out MetadataCell cell) => Table.TryGetCell(Column, row, out cell);

    /// <summary>
    /// Writes <paramref name="value"/> into the row value segment.
    /// </summary>
    public bool TrySet(uint row, T value)
    {
        var cell = GetOrCreateCell(row);
        if (cell.ValueSize < Unsafe.SizeOf<T>())
            return false;

        unsafe
        {
            var dst = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);
            dst.Clear();
            MemoryMarshal.Write(dst, value);
            return true;
        }
    }

    /// <summary>
    /// Writes <paramref name="value"/> or throws when the value cannot fit.
    /// </summary>
    public void Set(uint row, T value)
    {
        if (!TrySet(row, value))
            throw new InvalidOperationException($"Could not write value of type '{typeof(T).FullName}' into column '{Name}'.");
    }

    /// <summary>
    /// Reads the row value as <typeparamref name="T"/>.
    /// </summary>
    public bool TryGet(uint row, out T value)
    {
        value = default;
        if (!TryGetCell(row, out var cell))
            return false;

        if (!cell.TryGetValueRaw(out var raw) || raw.Length < Unsafe.SizeOf<T>())
            return false;

        value = MemoryMarshal.Read<T>(raw);
        return true;
    }

    /// <summary>
    /// Reads the row value or throws when it is missing.
    /// </summary>
    public T Get(uint row) =>
        TryGet(row, out var value)
            ? value
            : throw new InvalidOperationException($"Could not read row {row} from column '{Name}'.");

    /// <summary>
    /// Writes the cell key as UTF-8 bytes.
    /// </summary>
    public bool TrySetKey(uint row, ReadOnlySpan<byte> keyUtf8) =>
        KeySize > 0 && GetOrCreateCell(row).TrySetKey(keyUtf8);

    /// <summary>
    /// Writes the cell key as a UTF-8 string.
    /// </summary>
    public bool TrySetKey(uint row, string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return KeySize > 0 && GetOrCreateCell(row).TrySetKey(key);
    }

    /// <summary>
    /// Finds a row by a UTF-8 key in this column.
    /// </summary>
    public bool TryFindRowByKey(ReadOnlySpan<byte> keyUtf8, out uint row) =>
        Table.TryFindRowByKey(Column, keyUtf8, out row);

    /// <summary>
    /// Finds a row by a string key in this column.
    /// </summary>
    public bool TryFindRowByKey(string key, out uint row)
    {
        ArgumentNullException.ThrowIfNull(key);
        var byteCount = Encoding.UTF8.GetByteCount(key);
        Span<byte> buffer = byteCount <= 256 ? stackalloc byte[byteCount] : new byte[byteCount];
        Encoding.UTF8.GetBytes(key, buffer);
        return TryFindRowByKey(buffer, out row);
    }
}
