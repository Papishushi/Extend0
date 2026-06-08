using Extend0.Metadata.Indexing.Definitions;
using Extend0.Metadata.Indexing.Internal.BuiltIn;
using Extend0.Metadata.Contract;
using System.Reflection;

namespace Extend0.Testing.Metadata.Indexing.Internal.BuiltIn;

public static class BuiltInIndexHarness
{
    public readonly record struct ColumnKeyEntrySnapshot(byte[] Key, uint Row, bool OwnedKeyMatchesKey);
    public readonly record struct GlobalKeyHitSnapshot(uint Col, uint Row, byte[] OwnedKey);
    public readonly record struct GlobalMultiTableHitSnapshot(string TableName, uint Row, uint Col, byte[] OwnedKey);

    public sealed class ColumnKeyIndexHandle : IDisposable
    {
        private readonly ColumnKeyIndex _index;

        public ColumnKeyIndexHandle(string name, params (uint Column, int KeySize)[] keySizes)
        {
            _index = new ColumnKeyIndex(name);

            var cache = GetField<Dictionary<uint, int>>(_index, "_cachedKeySizes");
            foreach (var (column, keySize) in keySizes)
                cache[column] = keySize;
        }

        public bool AddPartition(uint column, params (byte[] Key, uint Row)[] entries)
        {
            var partition = new Dictionary<byte[], ColumnKeyIndex.Hit>();
            foreach (var (key, row) in entries)
                partition.Add(key, new ColumnKeyIndex.Hit(row, key));

            return _index.Add(column, partition);
        }

        public bool RemoveColumn(uint column) => _index.Remove(column);

        public bool Remove(uint column, byte[] key) => _index.Remove(column, key);

        public bool RemoveSpan(uint column, ReadOnlySpan<byte> keyUtf8) => _index.Remove(column, keyUtf8);

        public bool RemoveExact(uint column, byte[] storedKey) => _index.RemoveExact(column, storedKey);

        public bool TryGetRow(uint column, byte[] key, out uint row) => _index.TryGetRow(column, key, out row);

        public bool TryGetRowSpan(uint column, ReadOnlySpan<byte> keyUtf8, out uint row) => _index.TryGetRow(column, keyUtf8, out row);

        public void Set(uint column, byte[] key, uint row) => _index.Set(column, key, row);

        public bool TryGetSnapshot(uint column, out ColumnKeyEntrySnapshot[] snapshot)
        {
            var ok = _index.TryGetValue(column, out var copy);
            snapshot = copy
                .Select(static kv => new ColumnKeyEntrySnapshot(kv.Key, kv.Value.Row, ReferenceEquals(kv.Key, kv.Value.OwnedKey)))
                .ToArray();
            return ok;
        }

        public byte[]? GetStoredKey(uint column, byte[] key)
        {
            var live = GetProtectedIndex<uint, Dictionary<byte[], ColumnKeyIndex.Hit>>(_index);
            if (!live.TryGetValue(column, out var dict))
                return null;

            var lookup = NormalizeKey(key, GetField<Dictionary<uint, int>>(_index, "_cachedKeySizes")[column]);
            return dict.TryGetValue(lookup, out var hit) ? hit.OwnedKey : null;
        }

        public void Clear() => _index.Clear();

        public Task Rebuild(IMetadataTable table) => _index.Rebuild(table);

        public void Dispose() => _index.Dispose();
    }

    public sealed class GlobalKeyIndexHandle : IDisposable
    {
        private readonly GlobalKeyIndex _index;

        public GlobalKeyIndexHandle(string name, int keySize)
        {
            _index = new GlobalKeyIndex(name);
            SetField(_index, "_keySize", keySize);
        }

        public bool Add(byte[] key, uint col, uint row) =>
            _index.Add(key, new GlobalKeyIndex.Hit(col, row, NormalizeKey(key, Math.Max(1, GetField<int>(_index, "_keySize")))));

        public void Set(byte[] key, uint col, uint row) => _index.Set(key, col, row);

        public bool Remove(byte[] key) => _index.Remove(key);

        public bool TryGetHit(byte[] key, out GlobalKeyHitSnapshot hit)
        {
            var ok = _index.TryGetHit(key, out var raw);
            hit = new GlobalKeyHitSnapshot(raw.Col, raw.Row, raw.OwnedKey);
            return ok;
        }

        public bool TryGetHitSpan(ReadOnlySpan<byte> keyUtf8, out GlobalKeyHitSnapshot hit)
        {
            var ok = _index.TryGetHit(keyUtf8, out var raw);
            hit = new GlobalKeyHitSnapshot(raw.Col, raw.Row, raw.OwnedKey);
            return ok;
        }

        public byte[]? GetStoredKey(byte[] key)
        {
            var live = GetProtectedIndex<byte[], GlobalKeyIndex.Hit>(_index);
            var keySize = GetField<int>(_index, "_keySize");
            if (keySize <= 0)
                return null;

            var lookup = NormalizeKey(key, keySize);
            return live.TryGetValue(lookup, out var hit) ? hit.OwnedKey : null;
        }

        public int Count => GetProtectedIndex<byte[], GlobalKeyIndex.Hit>(_index).Count;

        public Task Rebuild(IMetadataTable table) => _index.Rebuild(table);

        public void Clear() => _index.Clear();

        public void Dispose() => _index.Dispose();
    }

    public sealed class GlobalMultiTableKeyIndexHandle : IDisposable
    {
        private readonly GlobalMultiTableKeyIndex _index;

        public GlobalMultiTableKeyIndexHandle(string name, int keySize)
        {
            _index = new GlobalMultiTableKeyIndex(name, keySize: keySize);
        }

        public void Set(Guid tableId, string tableName, byte[] key, uint col, uint row) =>
            _index.Set(tableId, tableName, key, col, row);

        public void SetAll(string tableName, byte[] key, uint col, uint row) =>
            _index.SetAll(tableName, key, col, row);

        public bool Add(byte[] key, string tableName, uint row, uint col) =>
            _index.Add(key, new GlobalMultiTableKeyIndex.Hit(tableName, row, col, NormalizeKey(key, KeySize)));

        public bool Add(Guid tableId, byte[] key, string tableName, uint row, uint col) =>
            _index.Add(tableId, key, new GlobalMultiTableKeyIndex.Hit(tableName, row, col, NormalizeKey(key, KeySize)));

        public bool AddPartition(Guid tableId, params (byte[] Key, string TableName, uint Row, uint Col)[] entries)
        {
            var partition = new Dictionary<byte[], GlobalMultiTableKeyIndex.Hit>();
            foreach (var (key, tableName, row, col) in entries)
                partition.Add(key, new GlobalMultiTableKeyIndex.Hit(tableName, row, col, NormalizeKey(key, KeySize)));

            return _index.Add(tableId, partition);
        }

        public bool TryGetHit(byte[] key, out GlobalMultiTableHitSnapshot hit)
        {
            var ok = _index.TryGetHit(key, out var raw);
            hit = new GlobalMultiTableHitSnapshot(raw.TableName, raw.Row, raw.Col, raw.OwnedKey);
            return ok;
        }

        public bool TryGetHitSpan(ReadOnlySpan<byte> keyUtf8, out GlobalMultiTableHitSnapshot hit)
        {
            var ok = _index.TryGetHit(keyUtf8, out var raw);
            hit = new GlobalMultiTableHitSnapshot(raw.TableName, raw.Row, raw.Col, raw.OwnedKey);
            return ok;
        }

        public bool TryGetValue(byte[] key, out GlobalMultiTableHitSnapshot hit)
        {
            var ok = _index.TryGetValue(key, out var raw);
            hit = new GlobalMultiTableHitSnapshot(raw.TableName, raw.Row, raw.Col, raw.OwnedKey);
            return ok;
        }

        public bool TryGetValue(Guid tableId, byte[] key, out GlobalMultiTableHitSnapshot hit)
        {
            var ok = _index.TryGetValue(tableId, key, out var raw);
            hit = new GlobalMultiTableHitSnapshot(raw.TableName, raw.Row, raw.Col, raw.OwnedKey);
            return ok;
        }

        public Guid[] GetMemberTables(byte[] key) => _index.GetMemberTables(key);

        public Guid[] GetMemberTables(byte[] key, out GlobalMultiTableHitSnapshot[]? hits)
        {
            var ids = _index.GetMemberTables(key, out var raw);
            hits = raw?.Select(static h => new GlobalMultiTableHitSnapshot(h.TableName, h.Row, h.Col, h.OwnedKey)).ToArray();
            return ids;
        }

        public bool Remove(Guid tableId) => _index.Remove(tableId);

        public bool Remove(Guid tableId, byte[] key) => _index.Remove(tableId, key);

        public bool Remove(byte[] key) => _index.Remove(key);

        public byte[]? GetStoredKey(Guid tableId, byte[] key)
        {
            var live = GetProtectedIndex<Guid, IDictionary<byte[], GlobalMultiTableKeyIndex.Hit>>(_index);
            if (!live.TryGetValue(tableId, out var dict))
                return null;

            var lookup = NormalizeKey(key, KeySize);
            return dict.TryGetValue(lookup, out var hit) ? hit.OwnedKey : null;
        }

        public int KeySize => GetField<int>(_index, "_cachedKeySize");

        public int PartitionCount => GetProtectedIndex<Guid, IDictionary<byte[], GlobalMultiTableKeyIndex.Hit>>(_index).Count;

        public Task Rebuild(IMetaDBManager manager) => _index.Rebuild(manager);

        public void Clear() => _index.Clear();

        public void Dispose() => _index.Dispose();
    }

    private static byte[] NormalizeKey(byte[] key, int keySize)
    {
        var buffer = new byte[keySize];
        Buffer.BlockCopy(key, 0, buffer, 0, Math.Min(key.Length, buffer.Length));
        return buffer;
    }

    private static TField GetField<TField>(object target, string name)
    {
        var field = FindField(target.GetType(), name);
        return (TField)field.GetValue(target)!;
    }

    private static void SetField<TValue>(object target, string name, TValue value)
    {
        var field = FindField(target.GetType(), name);
        field.SetValue(target, value);
    }

    private static Dictionary<TKey, TValue> GetProtectedIndex<TKey, TValue>(object target)
        where TKey : notnull
    {
        var property = FindProperty(target.GetType(), "Index");
        return (Dictionary<TKey, TValue>)property.GetValue(target)!;
    }

    private static FieldInfo FindField(Type type, string name)
    {
        for (var current = type; current is not null; current = current.BaseType)
        {
            var field = current.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
            if (field is not null)
                return field;
        }

        throw new MissingFieldException(type.FullName, name);
    }

    private static PropertyInfo FindProperty(Type type, string name)
    {
        for (var current = type; current is not null; current = current.BaseType)
        {
            var property = current.GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic);
            if (property is not null)
                return property;
        }

        throw new MissingMemberException(type.FullName, name);
    }
}
