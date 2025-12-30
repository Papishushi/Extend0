using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Registries.Contract;

/// <summary>
/// Common registry surface for both table-local and cross-table index registries.
/// </summary>
/// <remarks>
/// Implementations typically own the lifetime of contained indexes and must dispose them
/// when removed/cleared and when the registry itself is disposed.
/// </remarks>
public interface IIndexesRegistryBase : IDisposable
{
    /// <summary>
    /// Removes all registered indexes and clears any associated state.
    /// </summary>
    /// <remarks>
    /// Implementations should dispose removed indexes as part of the operation.
    /// </remarks>
    void ClearAll();

    /// <summary>
    /// Enumerates all indexes currently registered.
    /// </summary>
    /// <returns>A snapshot or live enumeration of registered indexes.</returns>
    /// <remarks>
    /// If the underlying registry is mutable concurrently, implementations should document whether the enumeration is
    /// thread-safe, snapshot-based, or may throw due to concurrent mutation.
    /// </remarks>
    IEnumerable<ITableIndex> Enumerate();

    /// <summary>
    /// Removes an index by name.
    /// </summary>
    /// <param name="name">Logical index name.</param>
    /// <returns><see langword="true"/> if an index was removed; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Implementations should dispose the removed index if the registry owns its lifetime.
    /// </remarks>
    bool Remove(string name);
}
