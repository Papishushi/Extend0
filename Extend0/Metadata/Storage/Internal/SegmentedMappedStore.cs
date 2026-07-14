using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Diagnostics;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage.Contract;
using System.Buffers.Text;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Extend0.Metadata.Storage.Internal
{
    /// <summary>
    /// Chunked memory-mapped store. A table is a directory containing a manifest and one file per column chunk.
    /// </summary>
    internal sealed unsafe class SegmentedMappedStore : ITryGrowableStore, ICompactableStore
    {
        private const int MutationLockTimeoutMilliseconds = 30_000;
        private const string ManifestFileName = "manifest.json";
        private const string SpecFileName = "tablespec.json";
        private const string ChunksDirectoryName = "chunks";

        private static readonly JsonSerializerOptions Json = new()
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true
        };

        private readonly string _directory;
        private readonly MetadataStorageLease _storageLease;
        private readonly string _chunksDirectory;
        private readonly int _chunkSize;
        private readonly ColumnState[] _columns;
        private readonly byte[][] _colNameUtf8;
        private bool _disposed;

        public SegmentedMappedStore(TableSpec spec)
        {
            var storage = spec.Storage.Normalize();
            if (storage.Layout != TableStorageLayout.Chunked)
                throw new ArgumentException("SegmentedMappedStore requires chunked table storage.", nameof(spec));

            _directory = Path.GetFullPath(spec.MapPath);
            _storageLease = MetadataStorageLease.Acquire(_directory);
            _chunksDirectory = Path.Combine(_directory, ChunksDirectoryName);
            _chunkSize = storage.ChunkSize;
            _colNameUtf8 = new byte[spec.Columns.Length][];

            try
            {
                using var mutationLock = AcquireMutationLock(_directory);
                Directory.CreateDirectory(_chunksDirectory);
                spec.SaveToFile(Path.Combine(_directory, SpecFileName));

                var manifestPath = GetManifestPath(_directory);
                if (File.Exists(manifestPath))
                {
                    var manifest = LoadManifest(manifestPath);
                    _columns = CreateStatesFromManifest(spec, manifest);
                }
                else
                {
                    _columns = CreateInitialStates(spec.Columns);
                    SaveManifest();
                }

                for (int i = 0; i < _columns.Length; i++)
                {
                    var keySize = _columns[i].KeySize;
                    var max = Math.Max(0, keySize - 1);
                    var bytes = Encoding.UTF8.GetBytes(spec.Columns[i].Name);
                    _colNameUtf8[i] = bytes.Length > max ? bytes.AsSpan(0, max).ToArray() : bytes;

                    EnsureChunkCount(_columns[i], GetRequiredChunkCount(_columns[i]));
                }
            }
            catch
            {
                _storageLease.Dispose();
                throw;
            }
        }

        public int Count
        {
            get
            {
                long count = 0;
                foreach (var column in _columns)
                    count += column.RowCapacity;
                return count > int.MaxValue ? int.MaxValue : (int)count;
            }
        }

        internal uint ColumnCount => (uint)_columns.Length;

        internal uint GetRowCapacity(uint c) => _columns[(int)c].RowCapacity;

        internal ColumnConfiguration GetColumnConfiguration(uint c)
        {
            var column = _columns[(int)c];
            return new(
                MetadataEntrySizeExtensions.PackUnchecked(column.KeySize, column.ValueSize),
                $"c{c}",
                ReadOnly: false,
                InitialCapacity: column.RowCapacity);
        }

        public bool TryGetCell(uint col, uint row, out MetadataCell cell)
        {
            if (col >= (uint)_columns.Length)
            {
                cell = default;
                return false;
            }

            var column = _columns[(int)col];
            if (row >= column.RowCapacity)
            {
                cell = default;
                return false;
            }

            cell = CreateCell(column, row);
            return true;
        }

        public MetadataCell GetOrCreateCell(uint col, uint row, in ColumnConfiguration meta)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (col >= (uint)_columns.Length)
                throw new ArgumentOutOfRangeException(nameof(col));

            var column = _columns[(int)col];
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(row, column.RowCapacity);

            var keySize = meta.Size.GetKeySize();
            var valueSize = meta.Size.GetValueSize();
            if (keySize != column.KeySize || valueSize != column.ValueSize)
                throw new InvalidOperationException($"Segmented cell size mismatch. meta=({keySize},{valueSize}) mapped=({column.KeySize},{column.ValueSize})");

            var cell = CreateCell(column, row);
            PopulateDefaultKey(col, row, column);
            return cell;
        }

        public bool TryGetColumnBlock(uint column, out ColumnBlock block)
        {
            if (column >= (uint)_columns.Length)
            {
                block = default;
                return false;
            }

            var state = _columns[(int)column];
            if (state.Chunks.Count != 1)
            {
                block = default;
                return false;
            }

            block = new ColumnBlock(
                @base: state.Chunks[0].Base,
                stride: state.EntrySizeBytes,
                valueSize: state.ValueSize,
                valueOffset: state.KeySize);
            return true;
        }

        public bool TryGrowColumnTo(uint column, uint minRows, in ColumnConfiguration meta, bool zeroInit)
        {
            if (minRows == 0) return true;
            using var mutationLock = AcquireMutationLock(_directory);

            if (column >= (uint)_columns.Length)
                return false;

            var state = _columns[(int)column];
            var keySize = meta.Size.GetKeySize();
            var valueSize = meta.Size.GetValueSize();
            if (keySize != state.KeySize || valueSize != state.ValueSize)
                throw new InvalidOperationException($"GrowColumnTo: size mismatch. meta=({keySize},{valueSize}) mapped=({state.KeySize},{state.ValueSize})");

            if (minRows <= state.RowCapacity)
                return true;

            var newCapacity = RoundRowsToChunks(state, minRows);
            var oldChunkCount = state.Chunks.Count;
            state.RowCapacity = newCapacity;
            EnsureChunkCount(state, GetRequiredChunkCount(state));

            if (zeroInit)
            {
                for (int i = oldChunkCount; i < state.Chunks.Count; i++)
                    new Span<byte>(state.Chunks[i].Base, state.ChunkByteLength).Clear();
            }

            SaveManifest();
            return true;
        }

        public bool TryGetColumnCapacity(uint column, out uint rowCapacity)
        {
            if (column >= (uint)_columns.Length)
            {
                rowCapacity = 0;
                return false;
            }

            rowCapacity = _columns[(int)column].RowCapacity;
            return true;
        }

        public CellEnumerable EnumerateCells() => new(this);

        public Task Compact(bool strict, CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();

            using var mutationLock = AcquireMutationLock(_directory, cancellationToken);
            var changed = false;

            foreach (var column in _columns)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var activeRows = FindActiveRows(column, cancellationToken);
                var compactedCapacity = RoundRowsToChunks(column, activeRows);
                var keepChunks = compactedCapacity == 0 ? 0 : checked((int)((compactedCapacity + column.RowsPerChunk - 1) / column.RowsPerChunk));

                if (keepChunks < column.Chunks.Count)
                {
                    RemoveChunksFrom(column, keepChunks);
                    changed = true;
                }

                if (column.RowCapacity != compactedCapacity)
                {
                    column.RowCapacity = compactedCapacity;
                    changed = true;
                }
            }

            if (changed)
                SaveManifest();

            return Task.CompletedTask;
        }

        public IEnumerator<CellRowColumnValueEntry> GetEnumerator() => EnumerateCells().GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        public void Dispose()
        {
            if (_disposed) return;
            try
            {
                foreach (var column in _columns)
                    foreach (var chunk in column.Chunks)
                        chunk.Dispose();
            }
            finally
            {
                _storageLease.Dispose();
                _disposed = true;
            }
        }

        public static bool TryLoadColumns(string path, out ColumnConfiguration[] columns)
        {
            columns = [];
            var directory = Path.GetFullPath(path);
            var manifestPath = GetManifestPath(directory);
            if (!File.Exists(manifestPath))
                return false;

            var manifest = LoadManifest(manifestPath);
            columns = new ColumnConfiguration[manifest.Columns.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                var c = manifest.Columns[i];
                columns[i] = new ColumnConfiguration(
                    MetadataEntrySizeExtensions.PackUnchecked(c.KeySize, c.ValueSize),
                    $"c{i}",
                    ReadOnly: false,
                    InitialCapacity: c.RowCapacity);
            }

            return true;
        }

        private ColumnState[] CreateInitialStates(ColumnConfiguration[] columns)
        {
            var states = new ColumnState[columns.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                var keySize = columns[i].Size.GetKeySize();
                var valueSize = columns[i].Size.GetValueSize();
                var state = new ColumnState(i, keySize, valueSize, _chunkSize);
                state.RowCapacity = RoundRowsToChunks(state, columns[i].InitialCapacity);
                states[i] = state;
            }

            return states;
        }

        private ColumnState[] CreateStatesFromManifest(TableSpec spec, SegmentedManifest manifest)
        {
            if (manifest.Version != 1)
                throw new InvalidDataException($"Unsupported segmented table manifest version: {manifest.Version}");

            if (manifest.ChunkSize != _chunkSize)
                throw new InvalidDataException($"Segmented table chunk size mismatch. spec={_chunkSize}, manifest={manifest.ChunkSize}");

            if (manifest.Columns.Length != spec.Columns.Length)
                throw new InvalidDataException("Segmented table column count does not match the TableSpec.");

            var states = new ColumnState[manifest.Columns.Length];
            for (int i = 0; i < states.Length; i++)
            {
                var expectedKeySize = spec.Columns[i].Size.GetKeySize();
                var expectedValueSize = spec.Columns[i].Size.GetValueSize();
                var c = manifest.Columns[i];

                if (c.KeySize != expectedKeySize || c.ValueSize != expectedValueSize)
                    throw new InvalidDataException($"Segmented table column {i} size does not match the TableSpec.");

                var state = new ColumnState(i, c.KeySize, c.ValueSize, manifest.ChunkSize)
                {
                    RowCapacity = c.RowCapacity
                };
                states[i] = state;
            }

            return states;
        }

        private MetadataCell CreateCell(ColumnState column, uint row)
        {
            var ptr = CellPtr(column, row);
            var size = MetadataEntrySizeExtensions.PackUnchecked(column.KeySize, column.ValueSize);
            return MetadataCell.FromPointer(size, ptr, owns: false);
        }

        private byte* CellPtr(ColumnState column, uint row)
        {
            var chunkIndex = checked((int)(row / column.RowsPerChunk));
            var rowInChunk = row % column.RowsPerChunk;
            return column.Chunks[chunkIndex].Base + rowInChunk * column.EntrySizeBytes;
        }

        private void PopulateDefaultKey(uint col, uint row, ColumnState column)
        {
            if (column.KeySize <= 0)
                return;

            var name = _colNameUtf8[(int)col];
            int cap = column.KeySize;

            Span<byte> rowBuf = stackalloc byte[20];
            if (!Utf8Formatter.TryFormat(row, rowBuf, out int rowLen))
                rowLen = 0;

            int needed = name.Length + 1 + rowLen;
            var dst = new Span<byte>(CellPtr(column, row), cap);
            dst.Clear();

            if (needed >= cap)
            {
                int maxName = Math.Max(0, cap - 2 - rowLen);
                needed = maxName + 1 + rowLen;
                if (needed >= cap) return;

                name.AsSpan(0, maxName).CopyTo(dst);
                dst[maxName] = (byte)':';
                rowBuf[..rowLen].CopyTo(dst[(maxName + 1)..]);
                return;
            }

            name.CopyTo(dst);
            int w = name.Length;
            dst[w++] = (byte)':';
            rowBuf[..rowLen].CopyTo(dst[w..]);
        }

        private uint FindActiveRows(ColumnState column, CancellationToken cancellationToken)
        {
            uint activeRows = 0;
            for (uint row = 0; row < column.RowCapacity; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (RowHasData(column, row))
                    activeRows = row + 1;
            }

            return activeRows;
        }

        private bool RowHasData(ColumnState column, uint row)
        {
            var ptr = CellPtr(column, row);
            if (column.KeySize > 0)
            {
                var key = new ReadOnlySpan<byte>(ptr, column.KeySize);
                for (int i = 0; i < key.Length; i++)
                    if (key[i] != 0)
                        return true;
                return false;
            }

            var value = new ReadOnlySpan<byte>(ptr + column.KeySize, column.ValueSize);
            for (int i = 0; i < value.Length; i++)
                if (value[i] != 0)
                    return true;

            return false;
        }

        private void EnsureChunkCount(ColumnState column, int targetChunkCount)
        {
            for (int i = column.Chunks.Count; i < targetChunkCount; i++)
                column.Chunks.Add(OpenOrCreateChunk(column, i));
        }

        private ChunkMapping OpenOrCreateChunk(ColumnState column, int chunkIndex)
        {
            var path = GetChunkPath(column.Index, chunkIndex);
            using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
            {
                if (fs.Length != column.ChunkByteLength)
                    fs.SetLength(column.ChunkByteLength);
            }

            var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.ReadWrite);
            var view = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
            byte* basePtr = null;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
            return new ChunkMapping(path, mmf, view, basePtr);
        }

        private void RemoveChunksFrom(ColumnState column, int keepChunks)
        {
            for (int i = column.Chunks.Count - 1; i >= keepChunks; i--)
            {
                var chunk = column.Chunks[i];
                column.Chunks.RemoveAt(i);
                var path = chunk.Path;
                chunk.Dispose();
                if (File.Exists(path))
                    File.Delete(path);
            }
        }

        private int GetRequiredChunkCount(ColumnState column)
        {
            if (column.RowCapacity == 0)
                return 0;

            return checked((int)((column.RowCapacity + column.RowsPerChunk - 1) / column.RowsPerChunk));
        }

        private static uint RoundRowsToChunks(ColumnState column, uint minRows)
        {
            if (minRows == 0)
                return 0;

            var chunks = (minRows + column.RowsPerChunk - 1) / column.RowsPerChunk;
            return checked(chunks * column.RowsPerChunk);
        }

        private string GetChunkPath(int column, int chunk) =>
            Path.Combine(_chunksDirectory, $"c{column:D4}_{chunk:D6}.chk");

        private void SaveManifest()
        {
            var manifest = new SegmentedManifest
            {
                Version = 1,
                ChunkSize = _chunkSize,
                Columns = _columns
                    .Select(static c => new SegmentedColumnManifest
                    {
                        KeySize = c.KeySize,
                        ValueSize = c.ValueSize,
                        RowCapacity = c.RowCapacity
                    })
                    .ToArray()
            };

            var json = JsonSerializer.Serialize(manifest, Json);
            File.WriteAllText(GetManifestPath(_directory), json, Encoding.UTF8);
        }

        private static string GetManifestPath(string directory) =>
            Path.Combine(directory, ManifestFileName);

        private static SegmentedManifest LoadManifest(string manifestPath)
        {
            var json = File.ReadAllText(manifestPath, Encoding.UTF8);
            return JsonSerializer.Deserialize<SegmentedManifest>(json, Json)
                   ?? throw new InvalidDataException("Invalid segmented table manifest.");
        }

        private static MutationLock AcquireMutationLock(string path, CancellationToken cancellationToken = default)
        {
            var mutexName = CreateMutexName(path);
            var mutex = new Mutex(initiallyOwned: false, name: mutexName);
            var sw = Stopwatch.StartNew();

            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        if (mutex.WaitOne(millisecondsTimeout: 100))
                            return new MutationLock(mutex);
                    }
                    catch (AbandonedMutexException)
                    {
                        return new MutationLock(mutex);
                    }

                    if (sw.ElapsedMilliseconds >= MutationLockTimeoutMilliseconds)
                    {
                        throw new MetadataTableLockedException(
                            $"Timed out waiting for segmented table mutation lock after {MutationLockTimeoutMilliseconds}ms. Path='{Path.GetFullPath(path)}'.",
                            null);
                    }
                }
            }
            catch
            {
                mutex.Dispose();
                throw;
            }
        }

        private static string CreateMutexName(string path)
        {
            var normalized = Path.GetFullPath(path);
            if (OperatingSystem.IsWindows())
                normalized = normalized.ToUpperInvariant();

            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(normalized)));
            var name = $"Extend0.Metadata.SegmentedMappedStore.{hash}";
            return OperatingSystem.IsWindows() ? $@"Local\{name}" : name;
        }

        private sealed class ColumnState
        {
            public ColumnState(int index, int keySize, int valueSize, int chunkSize)
            {
                Index = index;
                KeySize = keySize;
                ValueSize = valueSize;
                EntrySizeBytes = checked(keySize + valueSize);
                if (EntrySizeBytes <= 0)
                    throw new ArgumentOutOfRangeException(nameof(valueSize), "Column entry size must be greater than zero.");
                if (chunkSize < EntrySizeBytes)
                    throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be at least the column entry size.");

                RowsPerChunk = checked((uint)(chunkSize / EntrySizeBytes));
                ChunkByteLength = chunkSize;
            }

            public int Index { get; }
            public int KeySize { get; }
            public int ValueSize { get; }
            public int EntrySizeBytes { get; }
            public uint RowsPerChunk { get; }
            public int ChunkByteLength { get; }
            public uint RowCapacity { get; set; }
            public List<ChunkMapping> Chunks { get; } = [];
        }

        private sealed class ChunkMapping : IDisposable
        {
            private readonly MemoryMappedFile _mmf;
            private readonly MemoryMappedViewAccessor _view;
            private bool _disposed;

            public ChunkMapping(string path, MemoryMappedFile mmf, MemoryMappedViewAccessor view, byte* basePtr)
            {
                Path = path;
                _mmf = mmf;
                _view = view;
                Base = basePtr;
            }

            public string Path { get; }
            public byte* Base { get; private set; }

            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;

                if (Base != null)
                {
                    _view.Flush();
                    _view.SafeMemoryMappedViewHandle.ReleasePointer();
                    Base = null;
                }

                _view.Dispose();
                _mmf.Dispose();
            }
        }

        private sealed class SegmentedManifest
        {
            public int Version { get; set; }
            public int ChunkSize { get; set; }
            public SegmentedColumnManifest[] Columns { get; set; } = [];
        }

        private sealed class SegmentedColumnManifest
        {
            public int KeySize { get; set; }
            public int ValueSize { get; set; }
            public uint RowCapacity { get; set; }
        }

        private sealed class MutationLock : IDisposable
        {
            private readonly Mutex _mutex;
            private bool _disposed;

            public MutationLock(Mutex mutex) => _mutex = mutex;

            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;

                try { _mutex.ReleaseMutex(); }
                finally { _mutex.Dispose(); }
            }
        }
    }
}
