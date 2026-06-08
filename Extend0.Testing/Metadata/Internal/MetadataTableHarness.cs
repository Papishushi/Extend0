using Extend0.Metadata.Contract;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Internal;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata.Storage.Contract;
using Extend0.Metadata.Indexing.Registries.Contract;
using Extend0.Metadata.Storage.Internal;
using System.Collections;

namespace Extend0.Testing.Metadata.Internal;

public static class MetadataTableHarness
{
    public static IMetadataTable CreateTable(TableSpec spec) =>
        new MetadataTable(spec);

    public static IMetadataTable OpenTable(TableSpec spec) =>
        MetadataTable.Open(spec);

    public static IMetadataTable CreateInMemoryTable(string name, params ColumnConfiguration[] columns) =>
        new MetadataTable(new TableSpec(name, MapPath: null!, columns));

    public static IMetadataTable CreateDisposeThrowingTable(string name, Exception exception, params ColumnConfiguration[] columns) =>
        new DisposeThrowingMetadataTable(CreateInMemoryTable(name, columns), exception);

    public static IMetadataTable CreateDisposeCountingTable(string name, Action disposed, params ColumnConfiguration[] columns) =>
        new DisposeCountingMetadataTable(CreateInMemoryTable(name, columns), disposed);

    public static IMetadataTable CreateCompactBehaviorTable(
        string name,
        Func<bool, CancellationToken, Task<bool>> compact,
        params ColumnConfiguration[] columns) =>
        new CompactBehaviorMetadataTable(CreateInMemoryTable(name, columns), compact);

    public static IMetadataTable CreateCapacityRetryTable(
        IMetadataTable inner,
        uint targetColumn,
        uint targetRow,
        bool growResult,
        bool retryThrows = false) =>
        new CapacityRetryMetadataTable(inner, targetColumn, targetRow, growResult, retryThrows);

    public static void ReplaceStoreWithNonGrowable(IMetadataTable table, bool throwOnCreate, params ColumnConfiguration[] columns)
    {
        var original = table.CellStore;
        table.CellStore = new NonGrowableCellStore(columns, throwOnCreate);
        original.Dispose();
    }

    public static unsafe void WriteValueBytes(IMetadataTable table, uint column, uint row, ReadOnlySpan<byte> bytes)
    {
        var cell = table.GetOrCreateCell(column, row);
        var destination = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);
        destination.Clear();
        bytes[..Math.Min(bytes.Length, destination.Length)].CopyTo(destination);
    }

    public static unsafe bool TryWriteValueBytes(IMetadataTable table, uint column, uint row, ReadOnlySpan<byte> bytes)
    {
        if (!table.TryGetCell(column, row, out var cell))
            return false;

        var destination = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);
        destination.Clear();
        bytes[..Math.Min(bytes.Length, destination.Length)].CopyTo(destination);
        return true;
    }

    public static unsafe void ClearKeyBytes(IMetadataTable table, uint column, uint row)
    {
        var cell = table.GetOrCreateCell(column, row);
        if (cell.KeySize <= 0)
            return;

        var keyDestination = new Span<byte>(cell.GetValuePointer() - cell.KeySize, cell.KeySize);
        keyDestination.Clear();
    }

    private abstract class DelegatingMetadataTable(IMetadataTable inner) : IMetadataTable
    {
        public int ColumnCount => inner.ColumnCount;
        public ITableIndexesRegistry Indexes => inner.Indexes;
        public TableSpec Spec => inner.Spec;
        public ICellStore CellStore { get => inner.CellStore; set => inner.CellStore = value; }

        public CellEnumerable EnumerateCells() => inner.EnumerateCells();
        public IEnumerable<string> GetColumnNames() => inner.GetColumnNames();
        public uint GetLogicalRowCount() => inner.GetLogicalRowCount();
        public virtual MetadataCell GetOrCreateCell(string columnName, uint row) => inner.GetOrCreateCell(columnName, row);
        public virtual MetadataCell GetOrCreateCell(uint column, uint row) => inner.GetOrCreateCell(column, row);
        public IMetadataTable Open() => inner.Open();
        public Task RebuildIndexes(bool strict = false, CancellationToken cancellationToken = default) => inner.RebuildIndexes(strict, cancellationToken);
        public virtual Task<bool> TryCompactStore(bool strict, CancellationToken cancellationToken) => inner.TryCompactStore(strict, cancellationToken);
        public bool TryFindCellByKey(uint column, byte[] keyUtf8, out MetadataCell cell) => inner.TryFindCellByKey(column, keyUtf8, out cell);
        public bool TryFindCellByKey(uint column, ReadOnlySpan<byte> keyUtf8, out MetadataCell cell) => inner.TryFindCellByKey(column, keyUtf8, out cell);
        public bool TryFindGlobal(byte[] keyUtf8, out TryFindGlobalHit hit) => inner.TryFindGlobal(keyUtf8, out hit);
        public bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out TryFindGlobalHit hit) => inner.TryFindGlobal(keyUtf8, out hit);
        public bool TryFindRowByKey(uint column, byte[] keyUtf8, out uint row) => inner.TryFindRowByKey(column, keyUtf8, out row);
        public bool TryFindRowByKey(uint column, ReadOnlySpan<byte> keyUtf8, out uint row) => inner.TryFindRowByKey(column, keyUtf8, out row);
        public bool TryGetCell(string columnName, uint row, out MetadataCell cell) => inner.TryGetCell(columnName, row, out cell);
        public bool TryGetCell(uint column, uint row, out MetadataCell cell) => inner.TryGetCell(column, row, out cell);
        public bool TryGetColumnCapacity(uint column, out uint rowCapacity) => inner.TryGetColumnCapacity(column, out rowCapacity);
        public virtual bool TryGrowColumnTo(uint column, uint minRows, bool zeroInit = true) => inner.TryGrowColumnTo(column, minRows, zeroInit);
        public override string ToString() => inner.ToString();
        public string ToString(uint maxRows) => inner.ToString(maxRows);
        public virtual void Dispose() => inner.Dispose();
    }

    private sealed class DisposeThrowingMetadataTable(IMetadataTable inner, Exception exception) : DelegatingMetadataTable(inner)
    {
        public override void Dispose()
        {
            base.Dispose();
            throw exception;
        }
    }

    private sealed class DisposeCountingMetadataTable(IMetadataTable inner, Action disposed) : DelegatingMetadataTable(inner)
    {
        public override void Dispose()
        {
            try
            {
                base.Dispose();
            }
            finally
            {
                disposed();
            }
        }
    }

    private sealed class CompactBehaviorMetadataTable(IMetadataTable inner, Func<bool, CancellationToken, Task<bool>> compact) : DelegatingMetadataTable(inner)
    {
        public override Task<bool> TryCompactStore(bool strict, CancellationToken cancellationToken) => compact(strict, cancellationToken);
    }

    private sealed class CapacityRetryMetadataTable(
        IMetadataTable inner,
        uint targetColumn,
        uint targetRow,
        bool growResult,
        bool retryThrows) : DelegatingMetadataTable(inner)
    {
        private int _targetGetCalls;

        public override MetadataCell GetOrCreateCell(uint column, uint row)
        {
            if (column == targetColumn && row == targetRow)
            {
                _targetGetCalls++;
                if (_targetGetCalls == 1 || retryThrows)
                    throw new InvalidOperationException("capacity-probe-failed");
            }

            return base.GetOrCreateCell(column, row);
        }

        public override bool TryGrowColumnTo(uint column, uint minRows, bool zeroInit = true) =>
            column == targetColumn && minRows >= targetRow + 1
                ? growResult
                : base.TryGrowColumnTo(column, minRows, zeroInit);
    }

    private sealed class NonGrowableCellStore(IEnumerable<ColumnConfiguration> columns, bool throwOnCreate) : ICellStore
    {
        private readonly InMemoryStore _inner = new(columns);

        public int Count => _inner.Count;

        public bool TryGetColumnBlock(uint column, out ColumnBlock block) =>
            _inner.TryGetColumnBlock(column, out block);

        public bool TryGetCell(uint col, uint row, out MetadataCell cell) =>
            _inner.TryGetCell(col, row, out cell);

        public MetadataCell GetOrCreateCell(uint col, uint row, in ColumnConfiguration meta)
        {
            if (throwOnCreate)
                throw new InvalidOperationException("fallback-create-failed");

            return _inner.GetOrCreateCell(col, row, meta);
        }

        public CellEnumerable EnumerateCells() => _inner.EnumerateCells();

        public IEnumerator<CellRowColumnValueEntry> GetEnumerator() => _inner.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Dispose() => _inner.Dispose();
    }
}
