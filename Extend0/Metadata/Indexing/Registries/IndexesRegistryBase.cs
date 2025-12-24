using Extend0.Metadata.Indexing.Contract;
using System.Collections.Concurrent;

namespace Extend0.Metadata.Indexing.Registries;

/// <summary>
/// Shared base class for index registries (per-table and cross-table).
/// </summary>
/// <remarks>
/// <para>
/// This type centralizes the common registry concerns to avoid duplication:
/// </para>
/// <list type="bullet">
///   <item><description>Thread-safe storage of indexes by name.</description></item>
///   <item><description>Removal with best-effort disposal of the removed index.</description></item>
///   <item><description>Clearing all indexes.</description></item>
///   <item><description>Deterministic disposal of the registry itself.</description></item>
///   <item><description>Disposed-guard via <see cref="ThrowIfDisposed"/>.</description></item>
/// </list>
/// <para>
/// The registry stores values as <see cref="ITableIndex"/> because both per-table and cross-table
/// indexes derive from that base contract. Derived types can add typed helpers (e.g. generics),
/// and can filter/cast during enumeration when needed.
/// </para>
/// </remarks>
public abstract class IndexesRegistryBase : IDisposable
{
    /// <summary>
    /// Underlying index registry keyed by index name (ordinal).
    /// </summary>
    /// <remarks>
    /// Values are stored as <see cref="ITableIndex"/> to support heterogeneous index instances.
    /// Thread-safety is provided by the <see cref="ConcurrentDictionary{TKey,TValue}"/> itself.
    /// Individual index implementations are responsible for their own internal synchronization.
    /// </remarks>
    protected readonly ConcurrentDictionary<string, ITableIndex> _indexes =
        new(StringComparer.Ordinal);

    private int _disposed; // 0 = alive, 1 = disposed

    /// <summary>
    /// Removes an index by name.
    /// </summary>
    /// <param name="name">Index name.</param>
    /// <returns><see langword="true"/> if removed; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// If the removed index also implements <see cref="IDisposable"/>, it is disposed.
    /// </remarks>
    public bool Remove(string name)
    {
        ThrowIfDisposed();

        if (_indexes.TryRemove(name, out var removed))
        {
            (removed as IDisposable)?.Dispose();
            return true;
        }

        return false;
    }

    /// <summary>
    /// Clears every index currently registered in this registry.
    /// </summary>
    /// <remarks>
    /// Calls <see cref="ITableIndex.Clear"/> on each registered index.
    /// </remarks>
    public void ClearAll()
    {
        ThrowIfDisposed();

        foreach (var kv in _indexes)
            kv.Value.Clear();
    }

    /// <summary>
    /// Disposes the registry and best-effort disposes any registered indexes that implement <see cref="IDisposable"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method is idempotent. Subsequent calls are no-ops.
    /// </para>
    /// <para>
    /// Disposing the registry does not guarantee that no other thread still holds a reference
    /// to a previously retrieved index instance; it only disposes what is currently registered.
    /// Callers should treat disposal as a shutdown-only operation.
    /// </para>
    /// </remarks>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var kv in _indexes)
            (kv.Value as IDisposable)?.Dispose();

        _indexes.Clear();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Throws <see cref="ObjectDisposedException"/> if this registry has been disposed.
    /// </summary>
    /// <remarks>
    /// Intended to be called at the beginning of public methods in derived registries.
    /// </remarks>
    protected void ThrowIfDisposed()
        => ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            GetType().Name);

    public IEnumerable<ITableIndex> Enumerate()
    {
        ThrowIfDisposed();
        return _indexes.Values;
    }
}
