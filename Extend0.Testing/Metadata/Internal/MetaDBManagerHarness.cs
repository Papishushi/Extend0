using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Internal.BuiltIn;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Metadata;
using Microsoft.Extensions.Logging;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;

namespace Extend0.Testing.Metadata.Internal;

public static class MetaDBManagerHarness
{
    public sealed class ManagerHandle : IDisposable, IAsyncDisposable
    {
        private readonly MetaDBManager _manager;

        internal ManagerHandle(MetaDBManager manager)
        {
            _manager = manager;
        }

        public IMetaDBManager Contract => _manager;

        public bool TryFindGlobal(byte[] keyUtf8, out (string TableName, uint Col, uint Row) hit) =>
            _manager.TryFindGlobal(keyUtf8, out hit);

        public bool TryFindGlobal(ReadOnlySpan<byte> keyUtf8, out (string TableName, uint Col, uint Row) hit) =>
            _manager.TryFindGlobal(keyUtf8, out hit);

        public bool Close(Guid tableId) => _manager.Close(tableId);

        public bool Close(string name) => _manager.Close(name);

        public Guid[] GetRegisteredIdsFromIdRegistry() =>
            GetPrivateEntries("_byId").Select(entry => (Guid)entry.Key).ToArray();

        public Guid[] GetRegisteredIdsFromNameRegistry() =>
            GetPrivateEntries("_byName").Select(entry => (Guid)entry.Value!).ToArray();

        public void CloseAll() => _manager.CloseAll();

        public void CloseAllStrict() => _manager.CloseAllStrict();

        public void SeedGlobalKey(Guid tableId, string tableName, byte[] keyUtf8, uint col, uint row, int keySize = 16)
        {
            if (!_manager.Indexes.TryGet("__builtIn:cross:globalKey", out var existing) || existing is not GlobalMultiTableKeyIndex index)
            {
                index = new GlobalMultiTableKeyIndex("__builtIn:cross:globalKey", keySize: keySize);
                _manager.Indexes.Add(index);
            }

            index.Set(tableId, tableName, keyUtf8, col, row);
        }

        public string[] GetPendingDeletePaths() =>
            GetField<ConcurrentDictionary<string, byte>>("_pendingDeletes").Keys.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase).ToArray();

        public void SetDeleteQueuePath(string path) => SetField("_deleteQueuePath", path);

        public string GetDeleteQueuePath() => GetField<string>("_deleteQueuePath");

        public int ResolveCrossGlobalKeySize() => InvokePrivate<int>("ResolveCrossGlobalKeySize");

        public void EnqueueDelete(string path) => InvokePrivate("EnqueueDelete", path);

        public void LoadDeleteQueueFromDisk() => InvokePrivate("LoadDeleteQueueFromDisk");

        public void TryRewriteDeleteQueueFile() => InvokePrivate("TryRewriteDeleteQueueFile");

        public long MaybeCompactDeleteQueueFile(bool storm, int backlogAfter, int deletedThisCycle, long lastCompactMs, int compactCooldownMs)
        {
            object?[] args = [storm, backlogAfter, deletedThisCycle, lastCompactMs, compactCooldownMs];
            InvokePrivate("MaybeCompactDeleteQueueFile", args);
            return (long)args[3]!;
        }

        public Task<bool> TryDeleteNow(string mapPath, string specPath, Guid id) =>
            InvokePrivate<Task<bool>>("TryDeleteNow", mapPath, specPath, id);

        public ValueTask CleanupEphemeralDeleteAsync(bool throwIfDeleteFails, Guid id, string mapPath, string specPath) =>
            InvokePrivate<ValueTask>("CleanupEphemeralDeleteAsync", throwIfDeleteFails, id, mapPath, specPath);

        public void Dispose() => _manager.Dispose();

        public ValueTask DisposeAsync() => _manager.DisposeAsync();

        private IEnumerable<DictionaryEntry> GetPrivateEntries(string fieldName)
        {
            var field = typeof(MetaDBManager).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingFieldException(typeof(MetaDBManager).FullName, fieldName);

            var dictionary = field.GetValue(_manager)
                ?? throw new InvalidOperationException($"Field '{fieldName}' was null.");

            var enumerable = dictionary as IEnumerable
                ?? throw new InvalidOperationException($"Field '{fieldName}' was not enumerable.");

            foreach (var item in enumerable)
            {
                var itemType = item?.GetType() ?? throw new InvalidOperationException($"Field '{fieldName}' contained a null entry.");
                var key = itemType.GetProperty("Key", BindingFlags.Instance | BindingFlags.Public)?.GetValue(item)
                    ?? throw new InvalidOperationException($"Entry type '{itemType.FullName}' did not expose a Key.");
                var value = itemType.GetProperty("Value", BindingFlags.Instance | BindingFlags.Public)?.GetValue(item)
                    ?? throw new InvalidOperationException($"Entry type '{itemType.FullName}' did not expose a Value.");

                yield return new DictionaryEntry(key, value);
            }
        }

        private TField GetField<TField>(string fieldName)
        {
            var field = typeof(MetaDBManager).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingFieldException(typeof(MetaDBManager).FullName, fieldName);

            return (TField)field.GetValue(_manager)!;
        }

        private void SetField<TField>(string fieldName, TField value)
        {
            var field = typeof(MetaDBManager).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingFieldException(typeof(MetaDBManager).FullName, fieldName);

            field.SetValue(_manager, value);
        }

        private void InvokePrivate(string methodName, params object?[] args) =>
            _ = InvokePrivate<object?>(methodName, args);

        private TResult InvokePrivate<TResult>(string methodName, params object?[] args)
        {
            var method = typeof(MetaDBManager).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingMethodException(typeof(MetaDBManager).FullName, methodName);

            var result = method.Invoke(_manager, args);
            return (TResult)result!;
        }
    }

    public static ManagerHandle CreateManager(
        ILogger? logger = null,
        Func<TableSpec?, IMetadataTable>? factory = null,
        CapacityPolicy capacityPolicy = CapacityPolicy.Throw,
        string? deleteQueuePath = null,
        bool startDeleteWorker = true) =>
        new(new MetaDBManager(
            logger,
            factory ?? (spec => MetadataTableHarness.CreateTable(spec!.Value)),
            capacityPolicy,
            deleteQueuePath,
            startDeleteWorker));
}
