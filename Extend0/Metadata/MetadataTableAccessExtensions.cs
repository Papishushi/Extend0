using Extend0.Metadata.Contract;
using Extend0.Metadata.Internal;

namespace Extend0.Metadata;

/// <summary>
/// Callback-based access helpers for callers that need to keep table operations serialized with
/// layout-changing work such as growth or compaction.
/// </summary>
public static class MetadataTableAccessExtensions
{
    /// <summary>
    /// Executes <paramref name="action"/> while holding the table's exclusive access gate when the
    /// table is an Extend0 built-in implementation.
    /// </summary>
    public static void WithExclusiveAccess(this IMetadataTable table, Action<IMetadataTable> action, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(action);

        using var _ = MetadataTableConcurrency.EnterExclusive(table, cancellationToken);
        action(table);
    }

    /// <summary>
    /// Executes <paramref name="func"/> while holding the table's exclusive access gate when the
    /// table is an Extend0 built-in implementation.
    /// </summary>
    public static TResult WithExclusiveAccess<TResult>(this IMetadataTable table, Func<IMetadataTable, TResult> func, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(func);

        using var _ = MetadataTableConcurrency.EnterExclusive(table, cancellationToken);
        return func(table);
    }

    /// <summary>
    /// Executes <paramref name="action"/> asynchronously while holding the table's exclusive access gate when the
    /// table is an Extend0 built-in implementation.
    /// </summary>
    public static async Task WithExclusiveAccessAsync(this IMetadataTable table, Func<IMetadataTable, Task> action, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(action);

        await using var _ = await MetadataTableConcurrency.EnterExclusiveAsync(table, cancellationToken);
        await action(table).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes <paramref name="func"/> asynchronously while holding the table's exclusive access gate when the
    /// table is an Extend0 built-in implementation.
    /// </summary>
    public static async Task<TResult> WithExclusiveAccessAsync<TResult>(this IMetadataTable table, Func<IMetadataTable, Task<TResult>> func, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(func);

        await using var _ = await MetadataTableConcurrency.EnterExclusiveAsync(table, cancellationToken);
        return await func(table).ConfigureAwait(false);
    }
}
