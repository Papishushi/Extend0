using Extend0.Metadata.Contract;
using Extend0.Metadata.Schema;

namespace Extend0.Metadata
{
    internal sealed partial class MetaDBManager
    {
        /// <summary>
        /// Lazily materialized wrapper for a <see cref="IMetadataTable"/> registered in <see cref="MetaDBManager"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Instances are created with a <see cref="Guid"/> identifier, an optional <see cref="TableSpec"/>,
        /// and a factory that knows how to turn that spec into a <see cref="IMetadataTable"/>.
        /// </para>
        /// <para>
        /// The underlying table is created on first access to <see cref="Table"/> and then published
        /// in a thread-safe way using <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/>.
        /// Subsequent readers see the same instance.
        /// </para>
        /// </remarks>
        private sealed class ManagedTable(Guid id, TableSpec? spec, Func<TableSpec?, IMetadataTable> factory)
        {
            /// <summary>
            /// Stable identifier assigned to this managed table at registration time.
            /// </summary>
            public Guid Id { get; } = id;

            /// <summary>
            /// Optional logical name of the table, taken from the original <see cref="TableSpec"/>.
            /// </summary>
            public string? Name => spec?.Name;

            /// <summary>
            /// Factory used to create the underlying <see cref="IMetadataTable"/> from the stored spec.
            /// </summary>
            /// <exception cref="ArgumentNullException">
            /// Thrown if <paramref name="factory"/> is <see langword="null"/> when the <see cref="ManagedTable"/> is constructed.
            /// </exception>
            private readonly Func<TableSpec?, IMetadataTable> _factory = factory ?? throw new ArgumentNullException(nameof(factory));

            /// <summary>
            /// Backing field for <see cref="Table"/>. Published with <see cref="Volatile.Read{T}(ref T)"/>
            /// and <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/>; <see langword="null"/> means “not created yet”.
            /// </summary>
            private IMetadataTable? _table; // published with Interlocked; null => not created

            /// <summary>
            /// Indicates whether the underlying <see cref="IMetadataTable"/> has already been created and published.
            /// </summary>
            public bool IsCreated => _table is not null;

            /// <summary>
            /// Lazily created and cached <see cref="IMetadataTable"/> instance.
            /// </summary>
            /// <remarks>
            /// <para>
            /// The first caller that observes a <see langword="null"/> backing field creates a new <see cref="IMetadataTable"/>
            /// using the factory and attempts to publish it with <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/>.
            /// If the publish succeeds, the original <c>spec</c> reference is cleared to allow earlier GC.
            /// </para>
            /// <para>
            /// If another thread wins the race and publishes its instance first, this property discards the
            /// just-created instance and returns the one that was published. All callers eventually see the same table.
            /// </para>
            /// </remarks>
            public IMetadataTable Table
            {
                get
                {
                    // fast path
                    var t = Volatile.Read(ref _table);
                    if (t is not null) return t;

                    // create
                    t = _factory(spec);
                    var published = Interlocked.CompareExchange(ref _table, t, null);
                    if (published is null)
                    {
                        // we “won”; drop spec to free memory sooner
                        spec = null;
                        return t;
                    }

                    // someone else published first: discard ours and use the published
                    return published;
                }
            }
        }

    }
}
