using Extend0.Metadata.CrossProcess.Internal;
using Extend0.Metadata.CrossProcess.Contract;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata;
using Extend0.Metadata.Internal;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Extend0.Testing.Metadata.CrossProcess.Internal;

public static class MetaDBManagerRpcServiceHarness
{
    public sealed class ServiceHandle : IDisposable, IAsyncDisposable
    {
        private readonly MetaDBManagerRPCCompatible _service;

        internal ServiceHandle(MetaDBManagerRPCCompatible service)
        {
            _service = service;
        }

        public void AddTaskAndCheckCallIdCollision(long callId, Task task) =>
            InvokePrivate("AddTaskAndCheckCallIdCollision", callId, task);

        public void AddCtsAndCheckCallIdCollision(long callId, CancellationTokenSource cts) =>
            InvokePrivate("AddCTSAndCheckCallIdCollision", callId, cts);

        public void CleanupCall(long callId) => InvokePrivate("CleanupCall", callId);

        public void MarkCleanup(long callId, long ticks) =>
            GetField<ConcurrentDictionary<long, long>>("_cleanupAtUtcTicks")[callId] = ticks;

        public int TaskCount => GetField<ConcurrentDictionary<long, Task>>("_tasksByCallId").Count;

        public int CtsCount => GetField<ConcurrentDictionary<long, CancellationTokenSource>>("_ctsByCallId").Count;

        public int CleanupCount => GetField<ConcurrentDictionary<long, long>>("_cleanupAtUtcTicks").Count;

        public bool IsDisposed => GetField<bool>("_disposed");

        public long RebuildAllIndexesBegin(bool strict = true) => _service.RebuildAllIndexesBegin(strict);

        public Task Await(long callId) => _service.Await(callId);

        public Task<T> Await<T>(long callId) => _service.Await<T>(callId);

        public long TryCompactTableBegin(Guid tableId, bool strict) => _service.TryCompactTableBegin(tableId, strict);

        public long TryCompactAllTablesBegin(bool strict) => _service.TryCompactAllTablesBegin(strict);

        public IMetaDBManagerRPCCompatible RpcService => _service;

        public void AddManagerIndex(ICrossTableIndex index) =>
            GetField<MetaDBManager>("_innerManager").Indexes.Add(index);

        public void AddTableIndex(Guid tableId, ITableIndex index) =>
            GetField<MetaDBManager>("_innerManager").WithTable(tableId, t => t.Indexes.Add(index));

        public void Dispose() => _service.Dispose();

        public ValueTask DisposeAsync() => _service.DisposeAsync();

        private TField GetField<TField>(string fieldName)
        {
            var field = typeof(MetaDBManagerRPCCompatible).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingFieldException(typeof(MetaDBManagerRPCCompatible).FullName, fieldName);

            return (TField)field.GetValue(_service)!;
        }

        private void InvokePrivate(string methodName, params object?[] args)
        {
            var method = typeof(MetaDBManagerRPCCompatible).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingMethodException(typeof(MetaDBManagerRPCCompatible).FullName, methodName);

            try
            {
                _ = method.Invoke(_service, args);
            }
            catch (TargetInvocationException ex) when (ex.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            }
        }
    }

    public static ServiceHandle CreateInMemoryService(CapacityPolicy capacityPolicy = CapacityPolicy.Throw) =>
        new(new MetaDBManagerRPCCompatible(
            logger: null,
            factory: spec =>
            {
                var effective = spec is null
                    ? new TableSpec("Default", MapPath: null!, [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)])
                    : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

                return new MetadataTable(effective);
            },
            capacityPolicy: capacityPolicy));
}
