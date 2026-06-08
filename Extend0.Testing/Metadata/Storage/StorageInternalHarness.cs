using Extend0.Metadata.Storage.Files;
using Extend0.Metadata.Storage.Internal;
using System.Runtime.InteropServices;

namespace Extend0.Testing.Metadata.Storage;

public static unsafe class StorageInternalHarness
{
    public static FileHeaderHandle CreateFileHeader(uint magic, ushort version, ushort columnCount, long columnsTableOffset) =>
        new(magic, version, columnCount, columnsTableOffset);

    public static ColumnBlockHandle CreateColumnBlock(byte[] buffer, int stride, int valueSize, int valueOffset) =>
        new(buffer, stride, valueSize, valueOffset);

    public sealed class FileHeaderHandle(uint magic, ushort version, ushort columnCount, long columnsTableOffset)
    {
        private readonly FileHeader _header = new()
        {
            Magic = magic,
            Version = version,
            ColumnCount = columnCount,
            ColumnsTableOffset = columnsTableOffset
        };

        public uint Magic => _header.Magic;
        public ushort Version => _header.Version;
        public ushort ColumnCount => _header.ColumnCount;
        public long ColumnsTableOffset => _header.ColumnsTableOffset;

        public override bool Equals(object? obj) =>
            obj is FileHeaderHandle other && _header.Equals(other._header);

        public override int GetHashCode() => _header.GetHashCode();

        public int CompareTo(FileHeaderHandle other) => _header.CompareTo(other._header);
    }

    public sealed class ColumnBlockHandle : IDisposable
    {
        private readonly GCHandle _handle;
        private readonly ColumnBlock _block;

        public ColumnBlockHandle(byte[] buffer, int stride, int valueSize, int valueOffset)
        {
            _handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            _block = new ColumnBlock((byte*)_handle.AddrOfPinnedObject(), stride, valueSize, valueOffset);
        }

        public int Stride => _block.Stride;
        public int ValueSize => _block.ValueSize;
        public int ValueOffset => _block.ValueOffset;

        public bool Equals(ColumnBlockHandle other) => _block.Equals(other._block);

        public bool EqualsAsObject(object? other) => _block.Equals(other);

        public bool EqualsBlockAsObject(ColumnBlockHandle other) => _block.Equals((object)other._block);

        public bool OperatorsEqual(ColumnBlockHandle other) => _block == other._block;

        public bool OperatorsNotEqual(ColumnBlockHandle other) => _block != other._block;

        public int HashCode => _block.GetHashCode();

        public nint GetValuePointer(uint row) => (nint)_block.GetValuePtr(row);

        public void Dispose()
        {
            if (_handle.IsAllocated)
                _handle.Free();
        }
    }
}
