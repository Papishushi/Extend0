using Extend0.Metadata.Contract;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Internal;

internal interface IMetadataTableConcurrency
{
    MetadataTableConcurrencyGate ConcurrencyGate { get; }
}

internal sealed class MetadataTableConcurrencyGate
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly AsyncLocal<int> _depth = new();

    public MetadataTableConcurrencyLease Enter(CancellationToken cancellationToken = default)
    {
        if (_depth.Value > 0)
        {
            _depth.Value++;
            return new MetadataTableConcurrencyLease(this);
        }

        _gate.Wait(cancellationToken);
        _depth.Value = 1;
        return new MetadataTableConcurrencyLease(this);
    }

    public MetadataTableConcurrencyAwaitable EnterAsync(CancellationToken cancellationToken = default) =>
        new(this, cancellationToken);

    internal MetadataTableConcurrencyAwaiter CreateAsyncAwaiter(CancellationToken cancellationToken)
    {
        if (_depth.Value > 0)
        {
            _depth.Value++;
            return new MetadataTableConcurrencyAwaiter(this, waitTask: null, isReentrant: true);
        }

        return new MetadataTableConcurrencyAwaiter(this, _gate.WaitAsync(cancellationToken), isReentrant: false);
    }

    internal void MarkAcquired()
    {
        if (_depth.Value != 0)
            throw new SynchronizationLockException("The metadata table concurrency gate is already held by this execution flow.");

        _depth.Value = 1;
    }

    internal void Exit()
    {
        var depth = _depth.Value;
        if (depth <= 0)
            throw new SynchronizationLockException("The metadata table concurrency gate is not held by this execution flow.");

        if (depth == 1)
        {
            _depth.Value = 0;
            _gate.Release();
            return;
        }

        _depth.Value = depth - 1;
    }
}

internal readonly struct MetadataTableConcurrencyLease : IDisposable, IAsyncDisposable
{
    private readonly MetadataTableConcurrencyGate? _gate;

    internal MetadataTableConcurrencyLease(MetadataTableConcurrencyGate? gate)
    {
        _gate = gate;
    }

    public void Dispose() => _gate?.Exit();

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}

internal readonly struct MetadataTableConcurrencyAwaitable
{
    private readonly MetadataTableConcurrencyGate? _gate;
    private readonly CancellationToken _cancellationToken;

    internal MetadataTableConcurrencyAwaitable(MetadataTableConcurrencyGate? gate, CancellationToken cancellationToken)
    {
        _gate = gate;
        _cancellationToken = cancellationToken;
    }

    public MetadataTableConcurrencyAwaiter GetAwaiter() =>
        _gate is null
            ? default
            : _gate.CreateAsyncAwaiter(_cancellationToken);
}

internal readonly struct MetadataTableConcurrencyAwaiter : ICriticalNotifyCompletion
{
    private readonly MetadataTableConcurrencyGate? _gate;
    private readonly Task? _waitTask;
    private readonly bool _isReentrant;

    internal MetadataTableConcurrencyAwaiter(MetadataTableConcurrencyGate gate, Task? waitTask, bool isReentrant)
    {
        _gate = gate;
        _waitTask = waitTask;
        _isReentrant = isReentrant;
    }

    public bool IsCompleted => _gate is null || _isReentrant || _waitTask!.IsCompleted;

    public MetadataTableConcurrencyLease GetResult()
    {
        if (_gate is null)
            return default;

        if (_isReentrant)
            return new MetadataTableConcurrencyLease(_gate);

        _waitTask!.GetAwaiter().GetResult();
        _gate.MarkAcquired();
        return new MetadataTableConcurrencyLease(_gate);
    }

    public void OnCompleted(Action continuation) =>
        _waitTask!.GetAwaiter().OnCompleted(continuation);

    public void UnsafeOnCompleted(Action continuation) =>
        _waitTask!.GetAwaiter().UnsafeOnCompleted(continuation);
}

internal static class MetadataTableConcurrency
{
    public static MetadataTableConcurrencyLease EnterExclusive(IMetadataTable table, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        return table is IMetadataTableConcurrency concurrency
            ? concurrency.ConcurrencyGate.Enter(cancellationToken)
            : default;
    }

    public static MetadataTableConcurrencyAwaitable EnterExclusiveAsync(IMetadataTable table, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        return table is IMetadataTableConcurrency concurrency
            ? concurrency.ConcurrencyGate.EnterAsync(cancellationToken)
            : new MetadataTableConcurrencyAwaitable(null, cancellationToken);
    }
}
