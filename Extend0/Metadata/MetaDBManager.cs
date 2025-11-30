using Extend0.Metadata.Diagnostics;
using Extend0.Metadata.Refs;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Extend0.Metadata
{
    /// <summary>
    /// High-level manager that orchestrates <see cref="MetadataTable"/> creation, registration,
    /// and operations with structured logging, validation, and lazy initialization.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    ///   <item>
    ///     <description><b>Internal registries:</b> keeps a map by <c>Id</c> and by unique <c>Name</c>.</description>
    ///   </item>
    ///   <item>
    ///     <description><b>Lazy creation:</b> tables can be registered without creating the underlying mapping until first use.</description>
    ///   </item>
    ///   <item>
    ///     <description><b>Fill / Copy with auto-grow:</b> supports capacity growth via the per-manager <c>GrowColumnTo</c> hook.</description>
    ///   </item>
    ///   <item>
    ///     <description><b>Parent→child links:</b> caches child tables per (parentId, row) to avoid duplicates.</description>
    ///   </item>
    ///   <item>
    ///     <description><b>Operation scopes:</b> emits structured, timed logs for visual tracing.</description>
    ///   </item>
    /// </list>
    /// </remarks>
    /// <para>
    /// Uses <see cref="ConcurrentDictionary{TKey, TValue}"/> internally to allow concurrent registration and lookups.
    /// Thread-safety of individual <see cref="MetadataTable"/> operations depends on the table’s own guarantees.
    /// </para>
    /// <seealso cref="MetadataTable"/>
    /// <seealso cref="TableSpec"/>
    [SuppressMessage("Reliability", "CA2014", Justification = "SO is controlled and the stack allocations in loops are limited.")]
    public sealed partial class MetaDBManager
    {
        private const int DEFAULT_BATCH_SIZE = 512;

        /// <summary>
        /// Shared <see cref="ActivitySource"/> used to emit OpenTelemetry-compatible activities
        /// for high-level MetaDB operations (e.g. fills, copies, ref-linking, pipelines).
        /// </summary>
        /// <remarks>
        /// Attach this source to your tracing pipeline to correlate MetaDB spans with the rest of your
        /// distributed system. All helper scopes and <c>BeginOp</c> operations derive their activities
        /// from this source by default.
        /// </remarks>
        public static readonly ActivitySource ActivitySrc = new(typeof(MetaDBManager).AssemblyQualifiedName ?? "sevensevenseven.MetaDB");

        /// <summary>
        /// Application logger used for structured traces, scopes, and diagnostics.
        /// </summary>
        private readonly ILogger? _log;
        private readonly bool _isLogActivated;
        private readonly CapacityPolicy _capacityPolicy = CapacityPolicy.Throw;

        /// <summary>
        /// Factory that creates <see cref="MetadataTable"/> instances from a <see cref="TableSpec"/>.
        /// Injectable for testing; invoked lazily on first access.
        /// </summary>
        private readonly Func<TableSpec?, MetadataTable> _factory;

        /// <summary>
        /// Registry of managed tables by identifier.
        /// </summary>
        private readonly ConcurrentDictionary<Guid, ManagedTable> _byId = new();

        /// <summary>
        /// Registry of table identifiers by unique name (case-sensitive, <see cref="StringComparer.Ordinal"/>).
        /// </summary>
        private readonly ConcurrentDictionary<string, Guid> _byName = new(StringComparer.Ordinal);

        /// <summary>
        /// Cache mapping a parent table id to a per-row map of child table ids, so children can be reused per row.
        /// </summary>
        private readonly ConcurrentDictionary<Guid, ConcurrentDictionary<uint, Guid>> _childIndex = new();

        /// <summary>
        /// Attempts to retrieve the materialized <see cref="MetadataTable"/> associated
        /// with the specified identifier without throwing on failure.
        /// </summary>
        /// <param name="id">
        /// The table identifier to look up. If no managed table is registered
        /// for this id, the method returns <see langword="false"/>.
        /// </param>
        /// <param name="table">
        /// When this method returns <see langword="true"/>, contains the
        /// resolved <see cref="MetadataTable"/> instance. When it returns
        /// <see langword="false"/>, this value is <see langword="null"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a managed table was found for the given
        /// <paramref name="id"/> and its underlying <see cref="MetadataTable"/>
        /// could be obtained; otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This is a non-throwing probe around the internal id registry:
        /// it returns <see langword="false"/> when no entry exists, when the
        /// managed wrapper is <see langword="null"/>, or in any other failure
        /// scenario, and never forces creation beyond accessing
        /// <see cref="ManagedTable.Table"/>.
        /// </para>
        /// <para>
        /// The <see cref="NotNullWhenAttribute"/> on <paramref name="table"/>
        /// allows callers to rely on flow analysis when the method returns
        /// <see langword="true"/>.
        /// </para>
        /// </remarks>
        public bool TryGetManaged(Guid id, [NotNullWhen(true)] out MetadataTable? table)
        {
            table = null;
            var found = _byId.TryGetValue(id, out ManagedTable? managed);
            if (!found) return false;
            if (managed is null) return false;
            table = managed.Table;
            return true;
        }

        /// <summary>
        /// Per-instance hook that attempts to grow a column to at least the requested
        /// row capacity for a given <see cref="MetadataTable"/> and column index.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When configured (typically via the <see cref="MetaDBManager"/> constructor),
        /// this delegate is invoked by helpers such as <see cref="EnsureRowCapacity"/>
        /// whenever <see cref="CapacityPolicy.AutoGrowZeroInit"/> is in effect.
        /// </para>
        /// <para>
        /// The delegate receives the target table, the zero-based column index and the
        /// minimum required row count. It must return <see langword="true"/> when the
        /// column was successfully grown (or already large enough), or
        /// <see langword="false"/> if growth could not be achieved.
        /// </para>
        /// </remarks>
        private Func<MetadataTable, uint, uint, bool>? GrowColumnTo { get; set; }

        /// <summary>
        /// Factory used by <see cref="_childIndex"/> to lazily create the per-parent
        /// row-to-child-table map on first access.
        /// </summary>
        /// <remarks>
        /// The factory ignores the parent <see cref="Guid"/> and always returns a new
        /// <see cref="ConcurrentDictionary{TKey, TValue}"/> mapping parent row indices
        /// to child table identifiers. It is passed to
        /// <see cref="ConcurrentDictionary{TKey, TValue}.GetOrAdd(TKey, Func{TKey, TValue})"/>
        /// to avoid allocating intermediate lambdas on each call.
        /// </remarks>
        private static readonly Func<Guid, ConcurrentDictionary<uint, Guid>> s_ChildMapFactory =
            static _ => new ConcurrentDictionary<uint, Guid>();

        /// <summary>
        /// Default <see cref="MetadataTable"/> factory used when no custom factory
        /// is supplied to the <see cref="MetaDBManager"/> constructor.
        /// </summary>
        /// <remarks>
        /// Throws <see cref="ArgumentNullException"/> if the provided <see cref="TableSpec"/> is null.
        /// </remarks>
        private static readonly Func<TableSpec?, MetadataTable> s_FactoryCreator = static (spec) =>
        {
            if (!spec.HasValue)
                throw new ArgumentNullException(nameof(spec));

            return new MetadataTable(spec.Value);
        };

        /// <summary>
        /// Initializes a new <see cref="MetaDBManager"/> that orchestrates <see cref="MetadataTable"/> creation,
        /// registration and high-level operations with structured logging and optional growth hooks.
        /// </summary>
        /// <param name="logger">
        /// Application logger used for structured traces, scopes, and diagnostics. Must not be <c>null</c>.
        /// </param>
        /// <param name="growHook">
        /// Optional column-growth hook for the underlying MetaDB (e.g., <c>(t, c, r) => t.GrowTo(c, r)</c>).
        /// If provided, it sets the per-instance <see cref="GrowColumnTo"/> delegate.
        /// </param>
        /// <param name="factory">
        /// Optional factory that creates a <see cref="MetadataTable"/> from a <see cref="TableSpec"/>.
        /// If <c>null</c>, defaults to <see cref="CreateTable(TableSpec)"/>. The factory is invoked lazily on first use.
        /// </param>
        /// <param name="capacityPolicy">
        /// Default capacity policy to apply when a per-call policy is not provided
        /// (i.e. when a method receives <see cref="CapacityPolicy.None"/>).
        /// If <see cref="CapacityPolicy.None"/> is passed here, it is normalized
        /// internally to <see cref="CapacityPolicy.Throw"/>.
        /// </param>
        public MetaDBManager(
            ILogger? logger,
            Func<MetadataTable, uint, uint, bool>? growHook = null,
            Func<TableSpec?, MetadataTable>? factory = null,
            CapacityPolicy capacityPolicy = CapacityPolicy.Throw)
        {
            _log = logger;
            _isLogActivated = _log != null && _log.IsEnabled(LogLevel.Debug);

            GrowColumnTo = growHook;

            _factory = factory ?? s_FactoryCreator;
            _capacityPolicy = capacityPolicy == CapacityPolicy.None ? CapacityPolicy.Throw : capacityPolicy;
        }

        /// <summary>
        /// Registers a table by specification. By default this is lazy: the underlying file/mapping
        /// is not created until the first actual use, unless <paramref name="createNow"/> is true.
        /// </summary>
        /// <param name="spec">The table specification (name, map path and columns). Must not be null.</param>
        /// <param name="createNow">
        /// If true, forces immediate creation of the underlying <see cref="MetadataTable"/> (eager).
        /// If false (default), the table is created on first access (lazy).
        /// </param>
        /// <returns>The generated <see cref="Guid"/> identifier of the registered table.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown if <see cref="TableSpec.Name"/> or <see cref="TableSpec.MapPath"/> is null/whitespace,
        /// or if <see cref="TableSpec.Columns"/> is null or empty.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown if a table with the same name is already registered, or if the internal id registry
        /// cannot accept the new entry due to an id conflict.
        /// </exception>
        public Guid RegisterTable(TableSpec spec, bool createNow = false)
        {
            if (string.IsNullOrWhiteSpace(spec.Name))
                throw new ArgumentException("TableSpec.Name cannot be null or whitespace.", nameof(spec));
            if (string.IsNullOrWhiteSpace(spec.MapPath))
                throw new ArgumentException("TableSpec.MapPath cannot be null or whitespace.", nameof(spec));
            if (spec.Columns is null || spec.Columns.Length == 0)
                throw new ArgumentException("TableSpec.Columns must contain at least one column.", nameof(spec));

            if (_isLogActivated) Log.TableRegistering(_log!, spec.Name);

            var id = Guid.CreateVersion7();
            var m = new ManagedTable(id, spec, _factory);

            RegisterManagedTable(m);

            if (createNow)
            {
                // Force creation
                _ = m.Table;
                if (_isLogActivated) Log.TableCreatedNow(_log!, spec.Name, id, spec.MapPath);
            }
            else if (_isLogActivated) Log.TableRegisteredLazy(_log!, spec.Name, id, spec.MapPath);

            return id;
        }

        /// <summary>
        /// Convenience overload to register a table from individual arguments.
        /// </summary>
        /// <param name="name">Unique table name (case-sensitive).</param>
        /// <param name="mapPath">Target file path for the table mapping.</param>
        /// <param name="columns">Column configurations to create the table with.</param>
        /// <returns>The generated <see cref="Guid"/> identifier.</returns>
        /// <exception cref="ArgumentException">Thrown for null/whitespace <paramref name="name"/> or <paramref name="mapPath"/> or empty <paramref name="columns"/>.</exception>
        public Guid RegisterTable(string name, string mapPath, params ColumnConfiguration[] columns) => RegisterTable(new TableSpec(name, mapPath, columns));

        /// <summary>
        /// Returns the table instance, forcing creation if it does not exist yet.
        /// </summary>
        /// <param name="tableId">Table identifier.</param>
        /// <returns>The materialized <see cref="MetadataTable"/>.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableId"/> is <see cref="Guid.Empty"/>.</exception>
        /// <exception cref="KeyNotFoundException">Thrown when the id is not registered.</exception>
        public MetadataTable GetOrCreate(Guid tableId)
        {
            if (tableId == Guid.Empty)
                throw new ArgumentException("Table id must not be empty (Guid.Empty).", nameof(tableId));

            if (!_byId.TryGetValue(tableId, out var m))
                throw new KeyNotFoundException($"Unknown table id: {tableId}");

            return m.Table;
        }

        /// <summary>
        /// Opens an existing <see cref="MetadataTable"/> from a given map path by loading its adjacent
        /// <c>.tablespec.json</c> file and registering the managed table in this manager.
        /// </summary>
        /// <param name="mapPath">
        /// The map file path for the table (e.g., <c>C:\data\users.meta</c>). The method will look for
        /// a <c>.tablespec.json</c> file next to it (i.e., <c>users.meta.tablespec.json</c>).
        /// </param>
        /// <returns>The opened, managed <see cref="MetadataTable"/>.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="mapPath"/> is null or whitespace.</exception>
        /// <exception cref="FileNotFoundException">
        /// Thrown when the <c>.tablespec.json</c> file does not exist for the provided <paramref name="mapPath"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when registration fails due to id conflict or duplicate table name.
        /// </exception>
        public MetadataTable Open(string mapPath)
        {
            if (string.IsNullOrWhiteSpace(mapPath)) throw new ArgumentException("Map path cannot be null or whitespace.", nameof(mapPath));

            // Resolve spec path (accept either '...meta' or direct '...meta.tablespec.json')
            var specPath = mapPath.EndsWith(".tablespec.json", StringComparison.OrdinalIgnoreCase)
                ? mapPath
                : mapPath + ".tablespec.json";

            if (!File.Exists(specPath)) throw new FileNotFoundException("TableSpec file not found for the given map path.", specPath);

            var loaded = TableSpec.LoadFromFile(specPath);
            var id = Guid.CreateVersion7();
            var managedTable = new ManagedTable(id, loaded, _factory);

            RegisterManagedTable(managedTable);

            try
            {
                // Force creation to validate mapping immediately (fail fast)
                var table = managedTable.Table;
                if (_isLogActivated) Log.TableOpened(_log!, loaded.Name, mapPath, id);
                return table;
            }
            catch
            {
                // Roll back both registries on failure
                _byId.TryRemove(id, out _);
                _byName.TryRemove(loaded.Name, out _);
                throw;
            }
        }

        /// <summary>
        /// Registers a <see cref="ManagedTable"/> instance in the internal registries,
        /// enforcing unique identifiers and table names.
        /// </summary>
        /// <param name="m">
        /// The <see cref="ManagedTable"/> to register. Its <see cref="ManagedTable.Id"/>
        /// must be unique within this manager and its <see cref="ManagedTable.Name"/>
        /// must be a non-null, unique key.
        /// </param>
        /// <remarks>
        /// <para>
        /// The method updates three internal structures:
        /// </para>
        /// <list type="bullet">
        ///   <item>
        ///     <description><c>_byId</c>: maps table <see cref="Guid"/> to <see cref="ManagedTable"/>.</description>
        ///   </item>
        ///   <item>
        ///     <description><c>_byName</c>: maps table name to its <see cref="Guid"/>.</description>
        ///   </item>
        ///   <item>
        ///     <description><c>_childIndex</c>: initializes the per-parent child-table cache.</description>
        ///   </item>
        /// <para>
        /// If name registration fails, the previously added id entry is rolled back and a warning
        /// is logged when debug logging is enabled.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the managed table has no name, when a table with the same id is already
        /// registered, or when a table with the same name is already registered.
        /// </exception>
        private void RegisterManagedTable(ManagedTable m)
        {
            var id = m.Id;
            var name = m.Name ?? throw new InvalidOperationException("ManagedTable must have a Name.");

            if (!_byId.TryAdd(id, m))
                throw new InvalidOperationException("Could not register table: Id conflict.");

            if (!_byName.TryAdd(name, id))
            {
                _byId.TryRemove(id, out _);
                if (_isLogActivated) Log.TableNameDuplicate(_log!, name);
                throw new InvalidOperationException($"A table named '{name}' is already registered.");
            }

            _childIndex.TryAdd(id, new ConcurrentDictionary<uint, Guid>());

            return;
        }

        /// <summary>
        /// Attempts to get a table identifier by its unique name (case-sensitive, <see cref="StringComparer.Ordinal"/>).
        /// </summary>
        /// <param name="name">Registered table name. Matching is case-sensitive.</param>
        /// <param name="id">When this method returns <see langword="true"/>, contains the table <see cref="Guid"/>.</param>
        /// <returns>
        /// <see langword="true"/> if a table with the given <paramref name="name"/> is registered; otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This method never throws for null/empty names; it simply returns <see langword="false"/> and sets <paramref name="id"/> to <see cref="Guid.Empty"/>.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        public bool TryGetIdByName(string name, out Guid id)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                id = Guid.Empty;
                return false;
            }

            return _byName.TryGetValue(name, out id);
        }

        /// <summary>
        /// Attempts to get the already-created <see cref="MetadataTable"/> by name without forcing creation.
        /// </summary>
        /// <param name="name">Registered table name (case-sensitive).</param>
        /// <param name="table">
        /// When this method returns <see langword="true"/>, contains the existing <see cref="MetadataTable"/> instance.
        /// When it returns <see langword="false"/>, <paramref name="table"/> is <see langword="null"/> and no creation is triggered.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if a table with the given <paramref name="name"/> is registered AND has already been created;
        /// otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This is a non-allocating, non-throwing probe:
        /// it returns <see langword="false"/> for unknown names, missing ids, or tables not yet created,
        /// and it does not force lazy creation.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        public bool TryGetTableIfCreated(string name, [NotNullWhen(true)] out MetadataTable? table)
        {
            table = null;

            if (string.IsNullOrWhiteSpace(name))
                return false;

            if (!_byName.TryGetValue(name, out var id))
                return false;

            if (!_byId.TryGetValue(id, out var m))
                return false;

            if (!m.IsCreated)
                return false;

            table = m.Table; // safe: already created
            return true;
        }

        /// <summary>
        /// Returns the <see cref="ManagedTable"/> for the given <paramref name="id"/> or throws if it is not registered.
        /// </summary>
        /// <param name="id">The table identifier.</param>
        /// <returns>The corresponding <see cref="ManagedTable"/>.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="id"/> is <see cref="Guid.Empty"/>.</exception>
        /// <exception cref="KeyNotFoundException">Thrown when the table id is not registered in the manager.</exception>
        /// <remarks>
        /// This is a hot-path guard used by high-level APIs; it is aggressively inlined and marked
        /// <see cref="DebuggerStepThroughAttribute"/> to reduce debug noise.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        private ManagedTable Require(Guid id)
        {
            if (id == default)
                throw new ArgumentException("Table id must not be empty (Guid.Empty).", nameof(id));

            if (_byId.TryGetValue(id, out var m))
                return m;

            // Miss: clear, conventional exception for dictionary lookups.
            throw new KeyNotFoundException($"Unknown table id: {id}");
        }

        // ─────────────────────────────────────────────────────────────────────
        // Operaciones de COLUMNA: Fill / Copy
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Fills a blittable column using a per-row factory. Applies capacity growth if requested.
        /// </summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="tableId">Target table id.</param>
        /// <param name="column">Target column index.</param>
        /// <param name="rows">Number of rows to fill (starting at 0).</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="policy">
        /// Capacity policy. When <see cref="CapacityPolicy.AutoGrowZeroInit"/> the manager will attempt to grow.
        /// </param>
        public void FillColumn<T>(Guid tableId, uint column, uint rows, Func<uint, T> factory, CapacityPolicy policy = CapacityPolicy.None) where T : unmanaged
        {
            var m = Require(tableId);
            Stopwatch? sw = _isLogActivated ? Stopwatch.StartNew() : null;
            var effectivePolicy = policy == CapacityPolicy.None ? _capacityPolicy : policy;
            uint valueSize = GetColumnValueSize(m.Table, column);

            if (_isLogActivated) Log.FillColumnStart(_log!, m.Name ?? string.Empty, column, rows, typeof(T).Name, effectivePolicy);

            // Size validation and actual write are delegated to the static helper
            FillColumn(m.Table, column, rows, factory, effectivePolicy, ComputeBatchFromValueSize(valueSize));

            if (_isLogActivated) Log.FillColumnEnd(_log!, m.Name ?? string.Empty, typeof(T).Name, sw!.Elapsed.TotalMilliseconds);
        }

        /// <summary>
        /// Fills a column using a raw writer callback that receives (row, valuePtr, valueSize).
        /// Applies capacity growth if requested.
        /// </summary>
        /// <param name="tableId">Target table id.</param>
        /// <param name="column">Target column index.</param>
        /// <param name="rows">Number of rows to fill (starting at 0).</param>
        /// <param name="writer">
        /// Callback invoked per row with a pointer to the VALUE buffer (not the KEY) and the buffer size in bytes.
        /// </param>
        /// <param name="policy">
        /// Capacity policy. When <see cref="CapacityPolicy.AutoGrowZeroInit"/> the manager will attempt to grow.
        /// </param>
        public void FillColumn(Guid tableId, uint column, uint rows, Action<uint, IntPtr, uint> writer, CapacityPolicy policy = CapacityPolicy.None)
        {
            var m = Require(tableId);
            Stopwatch? sw = _isLogActivated ? Stopwatch.StartNew() : null;
            var effectivePolicy = policy == CapacityPolicy.None ? _capacityPolicy : policy;
            uint valueSize = GetColumnValueSize(m.Table, column);

            if (_isLogActivated) Log.FillRawStart(_log!, m.Name ?? string.Empty, column, rows, effectivePolicy);

            FillColumn(m.Table, column, rows, writer, effectivePolicy, ComputeBatchFromValueSize(valueSize));

            if (_isLogActivated) Log.FillRawEnd(_log!, m.Name ?? string.Empty, sw!.Elapsed.TotalMilliseconds);
        }

        /// <summary>
        /// Copies VALUE buffers row-by-row from a source column to a destination column
        /// (possibly across different tables). Optionally ensures destination capacity.
        /// </summary>
        /// <param name="srcTableId">Source table id.</param>
        /// <param name="srcCol">Source column index.</param>
        /// <param name="dstTableId">Destination table id.</param>
        /// <param name="dstCol">Destination column index.</param>
        /// <param name="rows">Number of rows to copy (starting at 0).</param>
        /// <param name="dstPolicy">
        /// Capacity policy for the destination. When not <see cref="CapacityPolicy.Throw"/>,
        /// the manager will ensure destination capacity before copying.
        /// </param>
        public void CopyColumn(Guid srcTableId, uint srcCol, Guid dstTableId, uint dstCol, uint rows, CapacityPolicy dstPolicy = CapacityPolicy.None)
        {
            var s = Require(srcTableId);
            var d = Require(dstTableId);
            Stopwatch? sw = _isLogActivated ? Stopwatch.StartNew() : null;
            var effectivePolicy = dstPolicy == CapacityPolicy.None ? _capacityPolicy : dstPolicy;
            uint valueSize = GetColumnValueSize(d.Table, dstCol);

            if (_isLogActivated) Log.CopyStart(_log!, s.Name ?? string.Empty, srcCol, d.Name ?? string.Empty, dstCol, rows, effectivePolicy);

            // Ensure capacity at destination if requested
            if (effectivePolicy != CapacityPolicy.Throw) EnsureRowCapacity(d.Table, dstCol, rows, effectivePolicy);

            // Will throw if value sizes differ per row; that exception is the signal.
            CopyColumn(s.Table, srcCol, d.Table, dstCol, rows, effectivePolicy, ComputeBatchFromValueSize(valueSize));

            if (_isLogActivated) Log.CopyEnd(_log!, s.Name ?? string.Empty, d.Name ?? string.Empty, sw!.Elapsed.TotalMilliseconds);
        }

        // ─────────────────────────────────────────────────────────────────────
        // Refs (padres → hijos)
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Ensures that the parent's reference vector (refs column) is created and initialized
        /// for the specified <paramref name="parentRow"/>. This method is idempotent.
        /// </summary>
        /// <param name="parentTableId">
        /// Registered parent table identifier. If unknown, a <see cref="KeyNotFoundException"/> is thrown.
        /// </param>
        /// <param name="refsCol">Zero-based index of the refs column in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index whose refs vector must exist.</param>
        /// <param name="policy">
        /// Capacity policy. With <see cref="CapacityPolicy.AutoGrowZeroInit"/> capacity is ensured
        /// (using <see cref="GrowColumnTo"/>); with <see cref="CapacityPolicy.Throw"/> an
        /// <see cref="InvalidOperationException"/> is thrown if the row capacity is insufficient.
        /// </param>
        /// <remarks>
        /// <para>
        /// This implementation uses a fast path: it ensures row capacity, probes the cell once,
        /// and initializes the vector only when the current count is zero. It logs
        /// <c>EnsureRefVecInit</c> exactly when initialization happens.
        /// </para>
        /// <para>
        /// If the cell's VALUE size cannot hold at least one reference entry
        /// (e.g., header + one ref), an <see cref="InvalidOperationException"/> is thrown.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentException">If <paramref name="parentTableId"/> is <see cref="Guid.Empty"/>.</exception>
        /// <exception cref="KeyNotFoundException">If the table id is not registered.</exception>
        /// <exception cref="InvalidOperationException">
        /// If <paramref name="policy"/> is <see cref="CapacityPolicy.Throw"/> and capacity is insufficient,
        /// or if <see cref="CapacityPolicy.AutoGrowZeroInit"/> is used and <see cref="GrowColumnTo"/> is not configured or fails,
        /// or if the cell VALUE size is too small to host the refs vector metadata.
        /// </exception>
        public void EnsureRefVec(Guid parentTableId, uint refsCol, uint parentRow, CapacityPolicy policy = CapacityPolicy.None)
        {
            var p = Require(parentTableId);
            var effective = policy == CapacityPolicy.None ? _capacityPolicy : policy;
            var didInit = EnsureRefVec(p.Table, refsCol, parentRow, effective);
            if (_isLogActivated && didInit) Log.EnsureRefVecInit(_log!, p.Name ?? string.Empty, refsCol, parentRow);
        }

        /// <summary>
        /// Adds an explicit link (existing child) into the parent's ref vector at
        /// (<paramref name="refsCol"/>, <paramref name="parentRow"/>).
        /// </summary>
        /// <param name="parentTableId">Registered parent table identifier.</param>
        /// <param name="refsCol">Zero-based index of the refs column in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index in the parent table.</param>
        /// <param name="childTableId">Existing child table identifier to reference.</param>
        /// <param name="childCol">Child column index the reference should point to (default 0).</param>
        /// <param name="childRow">Child row index the reference should point to (default 0).</param>
        /// <remarks>
        /// <para>
        /// This method ensures the refs vector exists (idempotent) and then appends the reference.
        /// If the vector is full, an <see cref="InvalidOperationException"/> is thrown. Consider
        /// increasing the parent cell VALUE size or implementing an overflow strategy.
        /// </para>
        /// <para>
        /// For high-throughput scenarios where the refs vector is known to be initialized
        /// (e.g., primed during setup), avoiding per-call ensures can reduce per-row overhead.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentException">If any id parameter is <see cref="Guid.Empty"/>.</exception>
        /// <exception cref="KeyNotFoundException">
        /// If <paramref name="parentTableId"/> or <paramref name="childTableId"/> is not registered.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// If capacity is insufficient and growth is disabled/misconfigured, or if the refs vector is full.
        /// </exception>
        public void LinkRef(Guid parentTableId, uint refsCol, uint parentRow, Guid childTableId, uint childCol = 0, uint childRow = 0, CapacityPolicy policy = CapacityPolicy.None)
        {
            var p = Require(parentTableId);
            _ = Require(childTableId); // validate child exists (throws if unknown)

            var effective = policy == CapacityPolicy.None ? _capacityPolicy : policy;
            EnsureRefVec(p.Table, refsCol, parentRow, effective);

            // Append the reference
            var tref = new MetadataTableRef
            {
                TableId  = childTableId,
                Column   = childCol,
                Row      = childRow,
                Reserved = 0
            };
            LinkRef(p.Table, refsCol, parentRow, in tref);

            if (_isLogActivated)
                Log.LinkRefAdded(_log!, p.Name ?? string.Empty, parentRow, childTableId, childCol, childRow);

            // Index for reuse (GetOrCreateAndLinkChild fast path)
            var byRow = _childIndex.GetOrAdd(parentTableId, s_ChildMapFactory);
            byRow[parentRow] = childTableId;
        }

        /// <summary>
        /// Gets (or creates) a child table for the given (<paramref name="parentTableId"/>, <paramref name="parentRow"/>)
        /// and links it into the parent's refs vector. If a child was already registered for that row, it is reused,
        /// and the link is (idempotently) ensured.
        /// </summary>
        /// <param name="parentTableId">Registered parent table identifier.</param>
        /// <param name="refsCol">Zero-based index of the refs column in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index.</param>
        /// <param name="childSpecFactory">
        /// Factory that receives <paramref name="parentRow"/> and returns the child's <see cref="TableSpec"/>.
        /// Use this to vary map paths/names per row (e.g., <c>user_{row}.meta</c>).
        /// </param>
        /// <param name="childCol">Child column the reference should point to (default 0).</param>
        /// <param name="childRow">Child row the reference should point to (default 0).</param>
        /// <returns>The <see cref="Guid"/> of the reused or newly created child.</returns>
        /// <remarks>
        /// This method relies on <see cref="LinkRef(Guid, uint, uint, Guid, uint, uint)"/> to ensure the parent's ref
        /// vector exists (no separate <c>EnsureRefVec</c> here). If you want to enforce a different capacity policy
        /// (e.g., <see cref="CapacityPolicy.Throw"/>), call the policy overload of <see cref="LinkRef"/>.
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="childSpecFactory"/> is null.</exception>
        public Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, Func<uint, TableSpec> childSpecFactory, uint childCol = 0, uint childRow = 0)
        {
            var p = Require(parentTableId);

            // reuse if already assigned for this row
            var byRow = _childIndex.GetOrAdd(parentTableId, s_ChildMapFactory);
            if (byRow.TryGetValue(parentRow, out var existingChildId))
            {
                // idempotently ensure link (also ensures ref vec)
                LinkRef(parentTableId, refsCol, parentRow, existingChildId, childCol, childRow);
                if (_isLogActivated) Log.ChildReused(_log!, p.Name ?? string.Empty, parentRow, existingChildId);
                return existingChildId;
            }

            // create child (register + eager)
            var spec = childSpecFactory(parentRow);
            var childId = RegisterTable(spec, createNow: true);

            // link and cache (LinkRef ensures ref vec)
            LinkRef(parentTableId, refsCol, parentRow, childId, childCol, childRow);
            if (_isLogActivated) Log.ChildCreatedLinked(_log!, p.Name ?? string.Empty, parentRow, spec.Name, childId);
            byRow[parentRow] = childId;

            return childId;
        }

        /// <summary>
        /// Executes a named, traceable operation against this manager (useful for higher-level pipelines).
        /// Emits structured start/end/fail log events and measures elapsed time.
        /// </summary>
        /// <param name="operationName">Human-readable operation name. Used in logs.</param>
        /// <param name="action">Operation to execute.</param>
        /// <param name="state">
        /// Optional structured state captured in a logging scope (e.g., new { tenant = "acme", batch = 42 }).
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="operationName"/> or <paramref name="action"/> is <c>null</c>.
        /// </exception>
        public void Run(string operationName, Action<MetaDBManager> action, object? state = null) =>
            RunCore(operationName, (mgr) =>
            {
                action(mgr);
                return Task.CompletedTask;
            }, state).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous version of <see cref="Run(string, Action{MetaDBManager}, object?)"/>.
        /// Executes a named, traceable async operation with structured start/end/fail logs and timing.
        /// </summary>
        /// <param name="operationName">Human-readable operation name. Used in logs.</param>
        /// <param name="action">Async operation to execute.</param>
        /// <param name="state">
        /// Optional structured state captured in a logging scope (e.g., new { tenant = "acme", batch = 42 }).
        /// </param>
        /// <returns>A task that completes when the operation finishes.</returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="operationName"/> or <paramref name="action"/> is <c>null</c>.
        /// </exception>
        public async Task RunAsync(string operationName, Func<MetaDBManager, Task> action, object? state = null) =>
            await RunCore(operationName, action, state).ConfigureAwait(false);

        private async Task RunCore(string operationName, Func<MetaDBManager, Task> action, object? state = null)
        {
            ArgumentNullException.ThrowIfNull(operationName);
            ArgumentNullException.ThrowIfNull(action);

            if (!_isLogActivated || _log is null)
            {
                await action(this).ConfigureAwait(false);
                return;
            }

            using var op = new OpScope(
                _log,
                operationName,
                state,
                LogLevel.Information,
                ActivitySrc,
                ActivityKind.Internal);

            try
            {
                await action(this).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                op.Fail(ex);
                throw;
            }
        }

        /// <summary>
        /// Ensures that column <paramref name="column"/> can hold at least
        /// <paramref name="neededRows"/> rows, applying the specified capacity policy.
        /// </summary>
        /// <param name="table">Target metadata table.</param>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="neededRows">Minimum required row count (logical length).</param>
        /// <param name="policy">
        /// Capacity policy to apply:
        /// <list type="bullet">
        ///   <item><description>
        ///   <see cref="CapacityPolicy.Throw"/> – throws if the current capacity is insufficient.
        ///   </description></item>
        ///   <item><description>
        ///   <see cref="CapacityPolicy.AutoGrowZeroInit"/> – attempts to grow using
        ///   <see cref="GrowColumnTo"/> and verifies the new capacity.
        ///   </description></item>
        /// </list>
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when growth is required but <paramref name="policy"/> is
        /// <see cref="CapacityPolicy.Throw"/>, or when <see cref="CapacityPolicy.AutoGrowZeroInit"/>
        /// is used but <see cref="GrowColumnTo"/> is not configured or fails.
        /// </exception>
        private void EnsureRowCapacity(MetadataTable table, uint column, uint neededRows, CapacityPolicy policy)
        {
            if (neededRows == 0) return;

            // Intento rápido: tocar la última fila requerida. Si falla o lanza: grow/throw.
            bool ok = true;
            try
            {
                table.GetOrCreateCell(column, neededRows - 1);
            }
            catch (Exception ex)
            {
                ok = false;
                if (_isLogActivated) Log.EnsureRowCapacityProbeFailed(_log!, table.Spec.Name, column, neededRows, ex);
            }

            if (ok) return;

            if (policy == CapacityPolicy.Throw)
                throw new InvalidOperationException($"Column {column}: insufficient row capacity for {neededRows} rows.");

            // AutoGrow
            if (GrowColumnTo is null)
                throw new InvalidOperationException($"CapacityPolicy.AutoGrowZeroInit requires configuring GrowColumnTo on this MetaDBManager before usage (table={table.Spec.Name}, col={column}, neededRows={neededRows}).");


            if (!GrowColumnTo(table, column, neededRows))
                throw new InvalidOperationException($"GrowColumnTo could not grow column {column}, {neededRows} rows.");

            // Verificación post-crecimiento
            table.GetOrCreateCell(column, neededRows - 1);
        }

        /// <summary>
        /// Computes a heuristic batch size for column operations based on the VALUE size.
        /// </summary>
        /// <param name="valueSize">
        /// Per-row VALUE size in bytes. When zero (unknown or variable-size),
        /// a conservative default is returned.
        /// </param>
        /// <returns>
        /// A batch size in rows, clamped to a safe range and aligned to a multiple of four
        /// to play nicely with unrolled loops.
        /// </returns>
        /// <remarks>
        /// The heuristic targets roughly half of an L2 cache per core (~256 KiB)
        /// assuming a read+write pattern per row.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static int ComputeBatchFromValueSize(uint valueSize)
        {
            // Heurística segura (bandwidth-bound). target ≈ mitad de L2 por núcleo (~256 KiB).
            const int targetBytes = 256 * 1024;
            const int minBatch = 64, maxBatch = 4096;

            if (valueSize == 0) return DEFAULT_BATCH_SIZE;   // columna variable o desconocida → conservador
            int perRow = (int)(valueSize * 2u);              // read + write
            int b = Math.Clamp(targetBytes / Math.Max(1, perRow), minBatch, maxBatch);
            // align to 4 for unrolled loops
            return (b + 3) & ~3;
        }

        /// <summary>
        /// Attempts to determine the VALUE size (in bytes) for a given column.
        /// </summary>
        /// <param name="table">Target table.</param>
        /// <param name="col">Zero-based column index.</param>
        /// <returns>
        /// The VALUE size in bytes when discoverable (e.g. via <see cref="ColumnBlock"/> or
        /// an existing cell), or <c>0</c> if the column is variable-sized or empty.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static uint GetColumnValueSize(MetadataTable table, uint col)
        {
            if (table.TryGetColumnBlock(col, out var blk)) return (uint)blk.ValueSize;
            if (table.TryGetCell(col, 0, out var cell)) return (uint)cell.ValueSize;
            return 0; // desconocido/variable
        }

        /// <summary>
        /// Core implementation for filling a blittable column using a per-row factory,
        /// with support for column-block fast paths, batching and per-cell fallback.
        /// </summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="table">Target table.</param>
        /// <param name="column">Zero-based column index to fill.</param>
        /// <param name="rows">Number of rows to fill (starting at 0).</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="policy">
        /// Capacity policy controlling how to react when the current row capacity is insufficient.
        /// </param>
        /// <param name="batchSize">
        /// Optional batch size override. When not provided, a heuristic based on VALUE size is used.
        /// </param>
        /// <remarks>
        /// The method tries, in order:
        /// <list type="number">
        ///   <item><description>
        ///   A contiguous <see cref="ColumnBlock"/> fast path where VALUEs are back-to-back
        ///   and <typeparamref name="T"/> matches the VALUE size.
        ///   </description></item>
        ///   <item><description>
        ///   A strided write fast path using pointer arithmetic.
        ///   </description></item>
        ///   <item><description>
        ///   A per-cell fallback using <see cref="MetadataTable.GetOrCreateCell(uint, uint)"/>.
        ///   </description></item>
        /// </list>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private unsafe void FillColumn<T>(MetadataTable table, uint column, uint rows, Func<uint, T> factory, CapacityPolicy policy, int batchSize = DEFAULT_BATCH_SIZE) where T : unmanaged
        {
            if (rows == 0) return;

            EnsureRowCapacity(table, column, rows, policy);

            int tSize = Unsafe.SizeOf<T>();

            // Hard cap for stackalloc usage to avoid SO (tune: 8–32 KB)
            const int MaxStackBytes = 16 * 1024;

            // ── FAST PATH: via column block (MMF / flat buffer) ────────────────────
            if (table.TryGetColumnBlock(column, out var blk))
            {
                // Variable-size values cannot use the block fast path; fall back.
                if (blk.ValueSize == 0)
                {
                    PerCellFill(table, column, rows, factory, batchSize, tSize, MaxStackBytes);
                    return;
                }

                if (tSize > blk.ValueSize)
                    throw new InvalidOperationException(
                        $"[{table}] col={column}: VALUE {blk.ValueSize} < sizeof({typeof(T).Name})={tSize}");

                if (TryContiguousFill(rows, factory, batchSize, tSize, MaxStackBytes, blk)) return;

                StridedFill(rows, factory, batchSize, tSize, MaxStackBytes, blk);
                return;
            }

            // ── SLOW PATH: per-cell (compat) — still batch the factory ─────────────
            PerCellFill(table, column, rows, factory, batchSize, tSize, MaxStackBytes);
        }

        /// <summary>Fills a column cell-by-cell using a per-row factory, batching value creation and using stack allocation or an array pool depending on the total byte size.</summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="table">Target <see cref="MetadataTable"/> whose column will be filled.</param>
        /// <param name="column">Zero-based column index to fill.</param>
        /// <param name="rows">Number of rows to fill, starting at 0.</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="batchSize">Maximum number of rows processed per batch when generating and writing values.</param>
        /// <param name="tSize">Size in bytes of <typeparamref name="T"/>; used to compute batch byte size.</param>
        /// <param name="MaxStackBytes">Maximum total bytes allowed on the stack before falling back to the shared array pool.</param>
        /// <exception cref="InvalidOperationException">Thrown when a cell's VALUE buffer is smaller than <paramref name="tSize"/>.</exception>
        private static unsafe void PerCellFill<T>(MetadataTable table, uint column, uint rows, Func<uint, T> factory, int batchSize, int tSize, int MaxStackBytes) where T : unmanaged
        {
            for (uint start = 0; start < rows; start += (uint)batchSize)
            {
                int count = (int)Math.Min((uint)batchSize, rows - start);

                int bytes = count * tSize;
                T[]? rented = null;
                Span<T> batch = bytes <= MaxStackBytes
                    ? stackalloc T[count]
                    : (rented = ArrayPool<T>.Shared.Rent(count)).AsSpan(0, count);

                for (int i = 0; i < count; i++)
                    batch[i] = factory(start + (uint)i);

                for (int i = 0; i < count; i++)
                {
                    uint row = start + (uint)i;
                    var cell = table.GetOrCreateCell(column, row);
                    if (cell.ValueSize < tSize)
                        throw new InvalidOperationException(
                            $"[{table}] col={column} row={row}: VALUE {cell.ValueSize} < sizeof({typeof(T).Name})={tSize}");

                    Unsafe.WriteUnaligned(cell.GetValuePointer(), batch[i]);
                }

                if (rented is not null)
                    ArrayPool<T>.Shared.Return(rented, clearArray: false);
            }
        }

        /// <summary>Fills a fixed-size VALUE column using strided writes over a <see cref="ColumnBlock"/>, batching allocations and using stack or pooled buffers based on byte size.</summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="rows">Number of rows to fill, starting at 0.</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="batchSize">Maximum number of rows processed per batch when generating and writing values.</param>
        /// <param name="tSize">Size in bytes of <typeparamref name="T"/>; used to compute batch byte size.</param>
        /// <param name="MaxStackBytes">Maximum total bytes allowed on the stack before falling back to the shared array pool.</param>
        /// <param name="blk">Column block describing the underlying fixed-size VALUE layout (base pointer, stride and offsets).</param>
        private static unsafe void StridedFill<T>(uint rows, Func<uint, T> factory, int batchSize, int tSize, int MaxStackBytes, ColumnBlock blk) where T : unmanaged
        {
            byte* colBase = blk.Base + blk.ValueOffset;
            nuint pitch = (nuint)blk.Stride;

            for (uint start = 0; start < rows; start += (uint)batchSize)
            {
                int count = (int)Math.Min((uint)batchSize, rows - start);

                // Decide stack vs pool by bytes, not items
                int bytes = count * tSize;
                T[]? rented = null;
                Span<T> batch = bytes <= MaxStackBytes
                    ? stackalloc T[count]
                    : (rented = ArrayPool<T>.Shared.Rent(count)).AsSpan(0, count);

                // Produce values
                for (int i = 0; i < count; i++)
                    batch[i] = factory(start + (uint)i);

                // Store values strided with native-sized pointer math
                byte* p = colBase + pitch * (nuint)start;
                for (int i = 0; i < count; i++)
                {
                    Unsafe.WriteUnaligned(p, batch[i]);
                    p += pitch;
                }

                if (rented is not null)
                    ArrayPool<T>.Shared.Return(rented, clearArray: false);
            }
        }

        /// <summary>Attempts a contiguous fast-path column fill over a <see cref="ColumnBlock"/> when VALUEs are tightly packed and match <typeparamref name="T"/> in size.</summary>
        /// <typeparam name="T">Unmanaged value type written into each row.</typeparam>
        /// <param name="rows">Number of rows to fill, starting at 0.</param>
        /// <param name="factory">Factory that produces a value for a given row index.</param>
        /// <param name="batchSize">Maximum number of rows processed per batch when generating values.</param>
        /// <param name="tSize">Size in bytes of <typeparamref name="T"/>; must match <see cref="ColumnBlock.ValueSize"/> for the fast path to be used.</param>
        /// <param name="MaxStackBytes">Maximum total bytes allowed on the stack before falling back to the shared array pool.</param>
        /// <param name="blk">Column block describing the contiguous VALUE layout (base pointer, stride and offsets).</param>
        /// <returns><see langword="true"/> if the contiguous fast path was taken and all rows were written; otherwise <see langword="false"/> so that callers can fall back to strided or per-cell paths.</returns>
        private static unsafe bool TryContiguousFill<T>(uint rows, Func<uint, T> factory, int batchSize, int tSize, int MaxStackBytes, ColumnBlock blk) where T : unmanaged
        {
            // Only when truly contiguous AND sizes match exactly.
            if (blk.Stride == blk.ValueSize && tSize == blk.ValueSize)
            {
                byte* dst = blk.GetValuePtr(0);
                for (uint start = 0; start < rows; start += (uint)batchSize)
                {
                    int count = (int)Math.Min((uint)batchSize, rows - start);
                    int bytes = count * tSize;
                    T[]? rented = null;

                    Span<T> batch = bytes <= MaxStackBytes
                        ? stackalloc T[count]
                        : (rented = ArrayPool<T>.Shared.Rent(count)).AsSpan(0, count);

                    for (int i = 0; i < count; i++)
                        batch[i] = factory(start + (uint)i);

                    // Bulk copy the bytes
                    var srcBytes = MemoryMarshal.AsBytes(batch);
                    fixed (byte* srcPtr = &MemoryMarshal.GetReference(srcBytes))
                    {
                        nuint offset = (nuint)start * (nuint)blk.ValueSize;
                        byte* dest = dst + offset;
                        Buffer.MemoryCopy(srcPtr, dest, srcBytes.Length, srcBytes.Length);
                    }

                    if (rented is not null) ArrayPool<T>.Shared.Return(rented, false);
                }
                return true;
            }
            return false;
        }

        /// <summary>
        /// Core implementation for filling a column using a raw writer callback that
        /// receives a pointer to the VALUE buffer per row.
        /// </summary>
        /// <param name="table">Target table.</param>
        /// <param name="column">Zero-based column index to fill.</param>
        /// <param name="rows">Number of rows to fill (starting at 0).</param>
        /// <param name="writer">
        /// Callback invoked per row with a pointer to the row VALUE buffer and its size in bytes.
        /// </param>
        /// <param name="policy">
        /// Capacity policy controlling how to react when the current row capacity is insufficient.
        /// </param>
        /// <param name="batchSize">
        /// Optional batch size for the column-block fast path. In per-cell mode it is used
        /// only to chunk iterations.
        /// </param>
        /// <remarks>
        /// When a fixed-size <see cref="ColumnBlock"/> is available, the method uses a strided
        /// pointer-based loop; otherwise it falls back to per-cell access.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private unsafe void FillColumn(MetadataTable table, uint column, uint rows, Action<uint, IntPtr, uint> writer, CapacityPolicy policy, int batchSize = DEFAULT_BATCH_SIZE)
        {
            if (rows == 0) return;

            EnsureRowCapacity(table, column, rows, policy);

            // ── FAST PATH: via column block (MMF / flat buffer) ────────────────────
            if (TryContiguousFill(table, column, rows, writer, batchSize)) return;

            // ── SLOW PATH: per-cell (compat) ───────────────────────────────────────
            PerCellFill(table, column, rows, writer, batchSize);

            static unsafe void PerCellFill(
                MetadataTable table, uint column, uint rows,
                Action<uint, nint, uint> writer, int batchSize)
            {
                for (uint start = 0; start < rows; start += (uint)batchSize)
                {
                    int count = (int)Math.Min((uint)batchSize, rows - start);

                    for (int i = 0; i < count; i++)
                    {
                        uint row = start + (uint)i;
                        var cell = table.GetOrCreateCell(column, row);
                        writer(row, (IntPtr)cell.GetValuePointer(), (uint)cell.ValueSize);
                    }
                }
            }

            static unsafe bool TryContiguousFill(
                MetadataTable table, uint column, uint rows,
                Action<uint, nint, uint> writer, int batchSize)
            {
                if (!table.TryGetColumnBlock(column, out var blk))
                    return false;

                // Variable-size values cannot use the block fast path; fall back.
                if (blk.ValueSize == 0)
                    return false;

                byte* valueBase = blk.Base + blk.ValueOffset;
                uint vsize = (uint)blk.ValueSize;
                nuint pitch = (nuint)blk.Stride;

                // Batching keeps the hot loop short and branch-free
                for (uint start = 0; start < rows; start += (uint)batchSize)
                {
                    int count = (int)Math.Min((uint)batchSize, rows - start);

                    byte* p = valueBase + pitch * (nuint)start;
                    for (int i = 0; i < count; i++)
                    {
                        writer(start + (uint)i, (IntPtr)p, vsize);
                        p += pitch;
                    }
                }

                return true;
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // COPIAS
        // ─────────────────────────────────────────────────────────────────────
        /// <summary>
        /// Core implementation that copies VALUE payloads row-by-row from a source column
        /// to a destination column, possibly across different tables.
        /// </summary>
        /// <param name="src">Source table.</param>
        /// <param name="srcCol">Source column index.</param>
        /// <param name="dst">Destination table.</param>
        /// <param name="dstCol">Destination column index.</param>
        /// <param name="rows">Number of rows to copy (starting at 0).</param>
        /// <param name="dstPolicy">
        /// Capacity policy for the destination column.
        /// </param>
        /// <param name="batchSize">
        /// Batch size for the block-based fast paths. Used to chunk strided copies.
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when VALUE sizes differ between source and destination, when source rows
        /// are missing, or when destination capacity cannot be ensured under the selected policy.
        /// </exception>
        /// <remarks>
        /// <para>
        /// Fast paths use <see cref="ColumnBlock"/>s for both source and destination:
        /// </para>
        /// <list type="bullet">
        ///   <item><description>
        ///   Contiguous→contiguous copy (single bulk <see cref="Buffer.MemoryCopy"/>).
        ///   </description></item>
        ///   <item><description>
        ///   Strided copy specialized for VALUE sizes of 64, 128 or 256 bytes.
        ///   </description></item>
        ///   <item><description>
        ///   A generic strided copy for other VALUE sizes.
        ///   </description></item>
        /// </list>
        /// If no column blocks are available, the method falls back to per-cell copying.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private unsafe void CopyColumn(MetadataTable src, uint srcCol, MetadataTable dst, uint dstCol, uint rows, CapacityPolicy dstPolicy, int batchSize = DEFAULT_BATCH_SIZE)
        {
            EnsureRowCapacity(dst, dstCol, rows, dstPolicy);

            bool done = PerBlockStridedCopy(src, srcCol, dst, dstCol, rows, batchSize);
            if (done) return;

            // ===== Fallback: API por celda =====
            PerCellCopy(src, srcCol, dst, dstCol, rows);
        }

        /// <summary>
        /// Copies an entire column from a source <see cref="MetadataTable"/> to a destination table
        /// using block-based, strided memory copies when possible.
        /// </summary>
        /// <param name="src">The source <see cref="MetadataTable"/> containing the column to copy.</param>
        /// <param name="srcCol">The zero-based index of the source column.</param>
        /// <param name="dst">The destination <see cref="MetadataTable"/> that will receive the data.</param>
        /// <param name="dstCol">The zero-based index of the destination column.</param>
        /// <param name="rows">The number of rows to copy.</param>
        /// <param name="batchSize">
        /// Maximum number of rows to copy per batch when performing strided copies.
        /// The actual batch size is rounded up to the next multiple of 4.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if both columns exist and the copy operation completed successfully;
        /// <see langword="false"/> if either the source or destination column block could not be obtained.
        /// </returns>
        /// <remarks>
        /// <para>
        /// If both source and destination column blocks are contiguous (value size equals stride),
        /// the method uses a single call to <see cref="Buffer.MemoryCopy"/> for a fast, bulk copy.
        /// </para>
        /// <para>
        /// For non-contiguous (strided) layouts, the method uses specialized strided copy routines
        /// for common value sizes (<c>64</c>, <c>128</c>, and <c>256</c> bytes) and falls back to a
        /// generic strided copy implementation for other sizes.
        /// </para>
        /// <para>
        /// If <paramref name="rows"/> is zero or the column value size is zero, the method returns
        /// <see langword="true"/> without performing any copy.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the source and destination column blocks have different <c>ValueSize</c>.
        /// </exception>
        private static unsafe bool PerBlockStridedCopy(MetadataTable src, uint srcCol, MetadataTable dst, uint dstCol, uint rows, int batchSize)
        {
            // Fast exit when blocks are not available
            if (!src.TryGetColumnBlock(srcCol, out ColumnBlock srcBlk) ||
                !dst.TryGetColumnBlock(dstCol, out ColumnBlock dstBlk))
                return false;

            if (srcBlk.ValueSize != dstBlk.ValueSize)
                throw new InvalidOperationException($"CopyColumn: VALUE sizes differ (src {srcBlk.ValueSize} != dst {dstBlk.ValueSize})");

            var valueSize = (uint)srcBlk.ValueSize;
            if (valueSize == 0 || rows == 0) return true;

            bool srcContig = srcBlk.Stride == srcBlk.ValueSize;
            bool dstContig = dstBlk.Stride == dstBlk.ValueSize;

            // ===== FAST-PATH: contiguous (values back-to-back) =====
            if (srcContig && dstContig)
            {
                byte* s0 = srcBlk.GetValuePtr(0);
                byte* d0 = dstBlk.GetValuePtr(0);

                long total = checked(rows * srcBlk.ValueSize);
                Buffer.MemoryCopy(s0, d0, total, total);
                return true;
            }

            // ===== STRIDED =====
            var sPitch = (nuint)srcBlk.Stride;
            var dPitch = (nuint)dstBlk.Stride;
            var sStart = srcBlk.GetValuePtr(0);
            var dStart = dstBlk.GetValuePtr(0);

            uint maxBatch = (uint)Math.Max(1, batchSize);
            maxBatch = (maxBatch + 3u) & ~3u; // round up to multiple of 4

            switch (valueSize)
            {
                case 64:
                    CopyStridedBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, &StridedCopy64);
                    break;
                case 128:
                    CopyStridedBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, &StridedCopy128);
                    break;
                case 256:
                    CopyStridedBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, &StridedCopy256);
                    break;
                default:
                    CopyStridedGenericBatched(rows, maxBatch, dStart, sStart, dPitch, sPitch, valueSize);
                    break;
            }

            return true;
        }

        /// <summary>
        /// Executes a strided copy loop in batches, delegating the actual per-row copy
        /// to a specialized worker for a fixed VALUE size.
        /// </summary>
        /// <param name="rows">Total number of rows to copy.</param>
        /// <param name="maxBatch">
        /// Maximum number of rows to process per batch. Typically already aligned
        /// to a multiple of 4 for unrolled inner loops.
        /// </param>
        /// <param name="dStart">Pointer to the first destination VALUE in the column.</param>
        /// <param name="sStart">Pointer to the first source VALUE in the column.</param>
        /// <param name="dPitch">Destination stride in bytes between consecutive rows.</param>
        /// <param name="sPitch">Source stride in bytes between consecutive rows.</param>
        /// <param name="worker">
        /// Function pointer that performs the actual strided copy for a given batch
        /// and VALUE size (e.g., <see cref="StridedCopy64"/>, <see cref="StridedCopy128"/>).
        /// </param>
        private static unsafe void CopyStridedBatched(uint rows, uint maxBatch, byte* dStart, byte* sStart, nuint dPitch, nuint sPitch, delegate*<byte*, byte*, nuint, nuint, uint, void> worker)
        {
            uint done = 0;
            while (done < rows)
            {
                var take = Math.Min(rows - done, maxBatch);
                worker(dStart + dPitch * done, sStart + sPitch * done, dPitch, sPitch, take);
                done += take;
            }
        }

        /// <summary>
        /// Executes a strided copy loop in batches for arbitrary VALUE sizes,
        /// using <see cref="StridedCopyGeneric"/> as the inner worker.
        /// </summary>
        /// <param name="rows">Total number of rows to copy.</param>
        /// <param name="maxBatch">
        /// Maximum number of rows to process per batch. Typically already aligned
        /// to a multiple of 4 for unrolled inner loops.
        /// </param>
        /// <param name="dStart">Pointer to the first destination VALUE in the column.</param>
        /// <param name="sStart">Pointer to the first source VALUE in the column.</param>
        /// <param name="dPitch">Destination stride in bytes between consecutive rows.</param>
        /// <param name="sPitch">Source stride in bytes between consecutive rows.</param>
        /// <param name="valueSize">Size in bytes of each VALUE payload to copy.</param>
        private static unsafe void CopyStridedGenericBatched(uint rows, uint maxBatch, byte* dStart, byte* sStart, nuint dPitch, nuint sPitch, uint valueSize)
        {
            uint done = 0;
            while (done < rows)
            {
                var take = Math.Min(rows - done, maxBatch);
                var aPitch = dStart + dPitch * done;
                var bPitch = sStart + sPitch * done;
                StridedCopyGeneric(aPitch, bPitch, dPitch, sPitch, valueSize, take);
                done += take;
            }
        }


        /// <summary>
        /// Copies values row-by-row from a source column to a destination column,
        /// using per-cell accessors as a fallback when block-based copy is not applicable.
        /// </summary>
        /// <param name="src">The source <see cref="MetadataTable"/> containing the column to copy.</param>
        /// <param name="srcCol">The zero-based index of the source column.</param>
        /// <param name="dst">The destination <see cref="MetadataTable"/> that will receive the data.</param>
        /// <param name="dstCol">The zero-based index of the destination column.</param>
        /// <param name="rows">The number of rows to copy.</param>
        /// <remarks>
        /// <para>
        /// This method retrieves each cell via <c>TryGetCell</c>/<c>GetOrCreateCell</c> and copies the
        /// value bytes with <see cref="Buffer.MemoryCopy"/>. It is intended as a correctness-oriented
        /// fallback when contiguous or strided block copies cannot be used.
        /// </para>
        /// <para>
        /// The method enforces that source and destination cells for each row have the same value size.
        /// Any mismatch results in an exception.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when a source cell for a given row is missing, or when the value sizes
        /// of the source and destination cells differ for any row.
        /// </exception>
        private static unsafe void PerCellCopy(MetadataTable src, uint srcCol, MetadataTable dst, uint dstCol, uint rows)
        {
            for (uint row = 0; row < rows; row++)
            {
                if (!src.TryGetCell(srcCol, row, out var sCell))
                    throw new InvalidOperationException($"CopyColumn: src missing row {row}.");

                var dCell = dst.GetOrCreateCell(dstCol, row);
                if (dCell.ValueSize != sCell.ValueSize)
                    throw new InvalidOperationException($"CopyColumn: row {row} VALUE sizes differ (src {sCell.ValueSize} != dst {dCell.ValueSize})");

                Buffer.MemoryCopy(
                    sCell.GetValuePointer(),
                    dCell.GetValuePointer(),
                    dCell.ValueSize,
                    sCell.ValueSize);
            }
        }

        // ===== helpers strided =====

        /// <summary>
        /// Specialized strided copy for 64-byte VALUEs, unrolled in groups of four rows
        /// to reduce loop overhead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopy64(byte* d, byte* s, nuint dPitch, nuint sPitch, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, 64);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, 64);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, 64);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, 64);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, 64);
                d += dPitch; s += sPitch;
            }
        }
        /// <summary>
        /// Specialized strided copy for 128-byte VALUEs, unrolled in groups of four rows.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopy128(byte* d, byte* s, nuint dPitch, nuint sPitch, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, 128);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, 128);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, 128);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, 128);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, 128);
                d += dPitch; s += sPitch;
            }
        }
        /// <summary>
        /// Specialized strided copy for 256-byte VALUEs, unrolled in groups of four rows.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopy256(byte* d, byte* s, nuint dPitch, nuint sPitch, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, 256);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, 256);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, 256);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, 256);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, 256);
                d += dPitch; s += sPitch;
            }
        }
        /// <summary>
        /// Generic strided copy for arbitrary VALUE sizes, with a four-row unrolled inner loop.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void StridedCopyGeneric(byte* d, byte* s, nuint dPitch, nuint sPitch, uint valueSize, uint rows)
        {
            uint r = 0;
            for (; r + 3 < rows; r += 4)
            {
                Unsafe.CopyBlockUnaligned(d, s, valueSize);
                Unsafe.CopyBlockUnaligned(d + dPitch, s + sPitch, valueSize);
                Unsafe.CopyBlockUnaligned(d + dPitch * 2, s + sPitch * 2, valueSize);
                Unsafe.CopyBlockUnaligned(d + dPitch * 3, s + sPitch * 3, valueSize);
                d += dPitch * 4; s += sPitch * 4;
            }
            for (; r < rows; r++)
            {
                Unsafe.CopyBlockUnaligned(d, s, valueSize);
                d += dPitch; s += sPitch;
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // REFS (parent → child links)
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Ensures the ref vector for (<paramref name="refsCol"/>, <paramref name="parentRow"/>) exists and is ready.
        /// Returns <c>true</c> if it performed first-time initialization on this call.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if VALUE is too small to hold at least one entry (needs at least
        /// <c>HeaderSize + EntrySize</c> bytes), or if growth is required but disallowed/misconfigured.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool EnsureRefVec(MetadataTable table, uint refsCol, uint parentRow, CapacityPolicy policy)
        {
            // 1) Ensure row capacity (may grow)
            EnsureRowCapacity(table, refsCol, parentRow + 1, policy);

            // 2) Get VALUE buffer
            var cell = table.GetOrCreateCell(refsCol, parentRow);
            int valueSize = cell.ValueSize;
            var buf = new Span<byte>(cell.GetValuePointer(), valueSize);

            // 3) Validate minimum size for 1 entry
            int minBytes = MetadataTableRefVec.HeaderSize + MetadataTableRefVec.EntrySize;
            if (valueSize < minBytes)
                throw new InvalidOperationException(
                    $"REFS: cell VALUE={valueSize} is insufficient; needs ≥ {minBytes} " +
                    $"({MetadataTableRefVec.HeaderSize}-byte header + {MetadataTableRefVec.EntrySize}-byte entry).");

            // 4) Initialize once (distinguish 'fresh' from 'empty but inited')
            if (!MetadataTableRefVec.IsInitialized(buf))
            {
                MetadataTableRefVec.Init(buf);
                return true; // did init just now
            }

            return false; // already initialized (maybe empty)
        }

        /// <summary>
        /// Inserts a reference into the parent's refs vector at
        /// (<paramref name="refsCol"/>, <paramref name="parentRow"/>).
        /// </summary>
        /// <param name="parent">Parent table.</param>
        /// <param name="refsCol">Zero-based index of the refs column.</param>
        /// <param name="parentRow">Zero-based row index in the parent.</param>
        /// <param name="tref">Reference (child table/column/row) to insert.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the refs vector is full for the current VALUE size
        /// (increase VALUE bytes or implement an overflow strategy).
        /// </exception>
        /// <remarks>
        /// This method assumes the cell is already initialized; call
        /// <see cref="EnsureRefVec(MetadataTable, uint, uint, CapacityPolicy)"/> first if unsure.
        /// </remarks>
        private static unsafe void LinkRef(MetadataTable parent, uint refsCol, uint parentRow, in MetadataTableRef tref)
        {
            var cell = parent.GetOrCreateCell(refsCol, parentRow);
            var buf = new Span<byte>(cell.GetValuePointer(), cell.ValueSize);

            if (!MetadataTableRefVec.TryAdd(buf, in tref, buf.Length))
                throw new InvalidOperationException("Refs vector is full; increase VALUE size or add an overflow strategy.");
        }
    }
}
