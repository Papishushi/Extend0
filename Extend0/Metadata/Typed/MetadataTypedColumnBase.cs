using Extend0.Metadata.Contract;

namespace Extend0.Metadata.Typed;

/// <summary>
/// Base state shared by typed column wrappers generated from a <see cref="Schema.TableSpec"/>.
/// </summary>
public abstract class MetadataTypedColumnBase
{
    protected MetadataTypedColumnBase(IMetadataTable table, uint column, string name, int keySize, int valueSize)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
        Column = column;
        Name = string.IsNullOrWhiteSpace(name) ? throw new ArgumentException("Column name cannot be empty.", nameof(name)) : name;
        KeySize = keySize < 0 ? throw new ArgumentOutOfRangeException(nameof(keySize)) : keySize;
        ValueSize = valueSize < 0 ? throw new ArgumentOutOfRangeException(nameof(valueSize)) : valueSize;
    }

    /// <summary>
    /// Underlying dynamic MetaDB table.
    /// </summary>
    public IMetadataTable Table { get; }

    /// <summary>
    /// Zero-based column index in the underlying table.
    /// </summary>
    public uint Column { get; }

    /// <summary>
    /// Schema column name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Fixed key segment size in bytes.
    /// </summary>
    public int KeySize { get; }

    /// <summary>
    /// Fixed value segment size in bytes.
    /// </summary>
    public int ValueSize { get; }

    /// <summary>
    /// Ensures the column has at least <paramref name="minRows"/> physical rows.
    /// </summary>
    public bool TryGrowTo(uint minRows, bool zeroInit = true) =>
        Table.TryGrowColumnTo(Column, minRows, zeroInit);

    protected void ValidateColumn()
    {
        if (Column >= Table.Spec.Columns.Length)
            throw new InvalidOperationException($"Column index {Column} is outside table '{Table.Spec.Name}'.");

        var column = Table.Spec.Columns[(int)Column];
        if (!string.Equals(column.Name, Name, StringComparison.Ordinal))
            throw new InvalidOperationException($"Column {Column} is '{column.Name}', but typed wrapper expected '{Name}'.");
    }
}
