using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Indexing.Registries;
using Extend0.Metadata.Indexing.Registries.Contract;

namespace Extend0.Testing.Metadata.Indexing.Registries;

public static class IndexesRegistryHarness
{
    public sealed class TableRegistryHandle : IDisposable
    {
        private readonly TableIndexesRegistry _registry = new();

        public ITableIndexesRegistry Registry => _registry;

        public bool TryGet(string name, out ITableIndex? index) => _registry.TryGet(name, out index);

        public TIndex Add<TIndex>(TIndex index) where TIndex : class, ITableIndex => _registry.Add(index);

        public ITableIndex Add<TKey, TValue>(Func<ITableIndex> indexConstructor) where TKey : notnull =>
            _registry.Add<TKey, TValue>(indexConstructor);

        public ITableIndex<TKey, TValue> Get<TKey, TValue>(string name) where TKey : notnull =>
            _registry.Get<TKey, TValue>(name);

        public void Rebuild(IMetadataTable table) => _registry.Rebuild(table);

        public bool Remove(string name) => _registry.Remove(name);

        public void ClearAll() => _registry.ClearAll();

        public ITableIndex[] Enumerate() => _registry.Enumerate().ToArray();

        public void Dispose() => _registry.Dispose();
    }

    public sealed class CrossTableRegistryHandle : IDisposable
    {
        private readonly CrossTableIndexesRegistry _registry = new();

        public ICrossTableIndexesRegistry Registry => _registry;

        public bool TryGet(string name, out ICrossTableIndex? index) => _registry.TryGet(name, out index);

        public bool TryGet<TKey, TValue>(string name, out ICrossTableIndex<TKey, TValue>? index) where TKey : notnull =>
            _registry.TryGet(name, out index);

        public TIndex Add<TIndex>(TIndex index) where TIndex : class, ICrossTableIndex => _registry.Add(index);

        public ICrossTableIndex Add<TKey, TValue>(Func<ICrossTableIndex> indexConstructor) where TKey : notnull =>
            _registry.Add<TKey, TValue>(indexConstructor);

        public ICrossTableIndex<TKey, TValue> Get<TKey, TValue>(string name) where TKey : notnull =>
            _registry.Get<TKey, TValue>(name);

        public void ClearForTable(Guid tableId) => _registry.ClearForTable(tableId);

        public bool Remove(string name) => _registry.Remove(name);

        public void ClearAll() => _registry.ClearAll();

        public ITableIndex[] Enumerate() => _registry.Enumerate().ToArray();

        public void Dispose() => _registry.Dispose();
    }

    public class ProbeTableIndex<TKey, TValue>(string name) : ITableIndex<TKey, TValue> where TKey : notnull
    {
        private readonly Dictionary<TKey, TValue> _entries = new();
        private bool _disposed;

        public string Name { get; } = name;
        public Type KeyType => typeof(TKey);
        public Type ValueType => typeof(TValue);
        public int ClearCount { get; private set; }
        public int DisposeCount { get; private set; }

        public bool Add(TKey key, TValue value)
        {
            ThrowIfDisposed();
            return _entries.TryAdd(key, value);
        }

        public bool Remove(TKey key)
        {
            ThrowIfDisposed();
            return _entries.Remove(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            ThrowIfDisposed();
            return _entries.TryGetValue(key, out value!);
        }

        public void Clear()
        {
            ThrowIfDisposed();
            ClearCount++;
            _entries.Clear();
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            DisposeCount++;
        }

        protected void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, Name);
    }

    public sealed class ProbeRebuildableTableIndex<TKey, TValue>(string name) : ProbeTableIndex<TKey, TValue>(name), IRebuildableIndex where TKey : notnull
    {
        public int RebuildCount { get; private set; }
        public IMetadataTable? LastTable { get; private set; }

        public Task Rebuild(IMetadataTable table)
        {
            LastTable = table;
            RebuildCount++;
            return Task.CompletedTask;
        }
    }

    public class ProbeCrossTableIndex<TKey, TValue>(string name) : ICrossTableIndex<TKey, TValue> where TKey : notnull
    {
        private readonly Dictionary<Guid, Dictionary<TKey, TValue>> _partitions = new();
        private bool _disposed;

        public string Name { get; } = name;
        public Type KeyType => typeof(TKey);
        public Type ValueType => typeof(TValue);
        public int ClearCount { get; private set; }
        public int DisposeCount { get; private set; }
        public List<Guid> ClearedTables { get; } = [];

        public bool Add(TKey key, TValue value)
        {
            ThrowIfDisposed();

            var addedAny = false;
            foreach (var partition in _partitions.Values)
                addedAny |= partition.TryAdd(key, value);

            return addedAny;
        }

        public bool Remove(TKey key)
        {
            ThrowIfDisposed();

            var removedAny = false;
            foreach (var partition in _partitions.Values)
                removedAny |= partition.Remove(key);

            return removedAny;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            ThrowIfDisposed();

            foreach (var partition in _partitions.Values)
            {
                if (partition.TryGetValue(key, out value!))
                    return true;
            }

            value = default!;
            return false;
        }

        public bool Add(Guid tableId, TKey key, TValue value)
        {
            ThrowIfDisposed();
            return GetPartition(tableId).TryAdd(key, value);
        }

        public bool Remove(Guid tableId, TKey key)
        {
            ThrowIfDisposed();
            return _partitions.TryGetValue(tableId, out var partition) && partition.Remove(key);
        }

        public bool TryGetValue(Guid tableId, TKey key, out TValue value)
        {
            ThrowIfDisposed();
            if (_partitions.TryGetValue(tableId, out var partition) && partition.TryGetValue(key, out value!))
                return true;

            value = default!;
            return false;
        }

        public Guid[] GetMemberTables()
        {
            ThrowIfDisposed();
            return _partitions.Keys.ToArray();
        }

        public Guid[] GetMemberTables(TKey key)
        {
            ThrowIfDisposed();
            return _partitions.Where(kv => kv.Value.ContainsKey(key)).Select(kv => kv.Key).ToArray();
        }

        public Guid[] GetMemberTables(TKey key, out TValue[]? value)
        {
            ThrowIfDisposed();
            var matches = _partitions
                .Where(kv => kv.Value.TryGetValue(key, out _))
                .Select(kv => (kv.Key, Value: kv.Value[key]))
                .ToArray();

            value = matches.Select(static m => m.Value).ToArray();
            return matches.Select(static m => m.Key).ToArray();
        }

        public void ClearTable(Guid tableId)
        {
            ThrowIfDisposed();
            ClearedTables.Add(tableId);
            _partitions.Remove(tableId);
        }

        public void Clear()
        {
            ThrowIfDisposed();
            ClearCount++;
            _partitions.Clear();
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            DisposeCount++;
        }

        protected void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, Name);

        private Dictionary<TKey, TValue> GetPartition(Guid tableId)
        {
            if (!_partitions.TryGetValue(tableId, out var partition))
            {
                partition = [];
                _partitions[tableId] = partition;
            }

            return partition;
        }
    }
}
