using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Indexing.Registries.Contract;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;

namespace Extend0.Metadata
{
    public interface IMetadataTable : IDisposable
    {
        int ColumnCount { get; }
        ITableIndexesRegistry Indexes { get; }
        TableSpec Spec { get; }

        CellEnumerable EnumerateCells();
        IEnumerable<string> GetColumnNames();
        uint GetLogicalRowCount();
        MetadataCell GetOrCreateCell(string columnName, uint row);
        MetadataCell GetOrCreateCell(uint column, uint row);
        IMetadataTable Open();
        void RebuildIndexes(bool strict = false);
        string ToString();
        string ToString(uint maxRows);
        bool TryFindCellByKey(uint column, byte[] keyUtf8, out MetadataCell cell);
        bool TryFindCellByKey(uint column, ReadOnlySpan<byte> keyUtf8, out MetadataCell cell);
        bool TryFindGlobal(byte[] keyUtf8, out (uint col, uint row) hit);
        bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out (uint col, uint row) hit);
        bool TryFindRowByKey(uint column, byte[] keyUtf8, out uint row);
        bool TryFindRowByKey(uint column, ReadOnlySpan<byte> keyUtf8, out uint row);
        bool TryGetCell(string columnName, uint row, out MetadataCell cell);
        bool TryGetCell(uint column, uint row, out MetadataCell cell);
        bool TryGetColumnCapacity(uint column, out uint rowCapacity);
        bool TryGrowColumnTo(uint column, uint minRows, bool zeroInit = true);
    }
}