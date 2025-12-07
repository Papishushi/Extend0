using Extend0.Metadata.Diagnostics;
using Extend0.Metadata.Refs;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

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
    public sealed partial class MetaDBManager
    {
        /// <summary>
        /// Default batch size(in rows) used by column operations when no explicit batch size is provided.
        /// Tuned as a safe, conservative value for most workloads.
        /// </summary>
        internal const int DEFAULT_BATCH_SIZE = 512;

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
        public MetaDBManager(ILogger? logger, Func<MetadataTable, uint, uint, bool>? growHook = null, Func<TableSpec?, MetadataTable>? factory = null, CapacityPolicy capacityPolicy = CapacityPolicy.Throw)
        {
            _log = logger;
            _isLogActivated = _log != null && _log.IsEnabled(LogLevel.Debug);

            GrowColumnTo = growHook;

            _factory = factory ?? s_FactoryCreator;
            _capacityPolicy = capacityPolicy == CapacityPolicy.None ? CapacityPolicy.Throw : capacityPolicy;
        }

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
        /// Registers a table according to the provided <see cref="TableSpec"/>. 
        /// Registration is lazy by default: the physical <see cref="MetadataTable"/> is not
        /// created until it is first accessed, unless <paramref name="createNow"/> is set to <c>true</c>.
        /// </summary>
        /// <param name="spec">
        /// The table specification describing the table name, map path, and column definitions.
        /// Must not be <c>null</c> and must contain a non-empty name, map path, and at least one column.
        /// </param>
        /// <param name="createNow">
        /// When <c>true</c>, forces eager creation of the underlying mapped table immediately upon
        /// registration. When <c>false</c> (default), table creation is deferred until first use.
        /// </param>
        /// <returns>
        /// The <see cref="Guid"/> identifier assigned to the registered table.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="spec"/> is <c>null</c>.
        /// </exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <see cref="TableSpec.Name"/> or <see cref="TableSpec.MapPath"/> is null or whitespace,
        /// or when <see cref="TableSpec.Columns"/> is null or contains no entries.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when a table with the same name is already registered, or when an internal id conflict
        /// prevents inserting the new table into the registry.
        /// </exception>
        public Guid RegisterTable(TableSpec spec, bool createNow = false)
        {
            MetaDBManagerHelpers.ValidateTableSpec(spec);

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
            var (m, sw, effectivePolicy, batchSize) = PrepareFill(tableId, column, rows, policy);

            var tableName = m.Name ?? string.Empty;
            var typeName = typeof(T).Name;

            if (_isLogActivated)
                Log.FillColumnStart(_log!, tableName, column, rows, typeName, effectivePolicy);

            // Size validation and actual write are delegated to the static helper
            FillColumn(m.Table, column, rows, factory, effectivePolicy, batchSize);

            if (_isLogActivated)
                Log.FillColumnEnd(_log!, tableName, typeName, sw!.Elapsed.TotalMilliseconds);
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
            var (m, sw, effectivePolicy, batchSize) = PrepareFill(tableId, column, rows, policy);

            var tableName = m.Name ?? string.Empty;

            if (_isLogActivated)
                Log.FillRawStart(_log!, tableName, column, rows, effectivePolicy);

            FillColumn(m.Table, column, rows, writer, effectivePolicy, batchSize);

            if (_isLogActivated)
                Log.FillRawEnd(_log!, tableName, sw!.Elapsed.TotalMilliseconds);
        }

        /// <summary>
        /// Prepares common state for column fill operations: resolves the managed table,
        /// computes the effective capacity policy, derives a batch size heuristic and
        /// optionally starts a stopwatch for logging.
        /// </summary>
        /// <param name="tableId">Target table identifier.</param>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="rows">Number of rows to fill.</param>
        /// <param name="policyOverride">
        /// Per-call capacity policy. When <see cref="CapacityPolicy.None"/>, the manager-wide
        /// default policy is used.
        /// </param>
        /// <param name="tableName">
        /// Outputs the resolved table name (never <see langword="null"/>, empty if missing).
        /// </param>
        /// <returns>
        /// A tuple containing the resolved <see cref="ManagedTable"/>, the optional
        /// <see cref="Stopwatch"/> (only when logging is enabled), the effective capacity
        /// policy and the computed batch size in rows.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PrepareFillReturnValue PrepareFill(Guid tableId, uint column, uint rows, CapacityPolicy policyOverride)
        {
            var m = Require(tableId);

            var effectivePolicy = policyOverride == CapacityPolicy.None ? _capacityPolicy : policyOverride;

            uint valueSize = MetaDBManagerHelpers.GetColumnValueSize(m.Table, column);
            int batchSize = MetaDBManagerHelpers.ComputeBatchFromValueSize(valueSize);

            var sw = _isLogActivated ? Stopwatch.StartNew() : null;

            return (m, sw, effectivePolicy, batchSize);
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
            var sName = s.Name ?? string.Empty;
            var dName = d.Name ?? string.Empty;

            Stopwatch? sw = null;
            var effectivePolicy = dstPolicy == CapacityPolicy.None ? _capacityPolicy : dstPolicy;
            uint valueSize = MetaDBManagerHelpers.GetColumnValueSize(d.Table, dstCol);

            if (_isLogActivated)
            {
                sw = Stopwatch.StartNew();
                Log.CopyStart(_log!, sName, srcCol, dName, dstCol, rows, effectivePolicy);
            }

            // Ensure capacity at destination if requested
            EnsureRowCapacity(d.Table, dstCol, rows, effectivePolicy);

            // Will throw if value sizes differ per row; that exception is the signal.
            CopyColumn(s.Table, srcCol, d.Table, dstCol, rows, effectivePolicy, MetaDBManagerHelpers.ComputeBatchFromValueSize(valueSize));

            if (_isLogActivated) Log.CopyEnd(_log!, sName, dName, sw!.Elapsed.TotalMilliseconds);
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
            (
                tableId: childTableId,
                column: childCol,
                row: childRow,
                reserved: 0
            );
            MetaDBManagerHelpers.LinkRef(p.Table, refsCol, parentRow, in tref);

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

            var tableName = p.Name ?? string.Empty;

            // reuse if already assigned for this row
            var byRow = _childIndex.GetOrAdd(parentTableId, s_ChildMapFactory);
            if (byRow.TryGetValue(parentRow, out var existingChildId))
            {
                // idempotently ensure link (also ensures ref vec)
                LinkRef(parentTableId, refsCol, parentRow, existingChildId, childCol, childRow);
                if (_isLogActivated) Log.ChildReused(_log!, tableName, parentRow, existingChildId);
                return existingChildId;
            }

            // create child (register + eager)
            var spec = childSpecFactory(parentRow);
            var childId = RegisterTable(spec, createNow: true);

            // link and cache (LinkRef ensures ref vec)
            LinkRef(parentTableId, refsCol, parentRow, childId, childCol, childRow);
            if (_isLogActivated) Log.ChildCreatedLinked(_log!, tableName, parentRow, spec.Name, childId);
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

        /// <summary>
        /// Executes a named operation and, upon successful completion, rebuilds
        /// per-column (and optionally global) indexes for all materialized tables.
        /// </summary>
        /// <param name="operationName">Human-readable operation name. Used in logs.</param>
        /// <param name="action">Operation to execute.</param>
        /// <param name="includeGlobal">
        /// <see langword="true"/> to rebuild global indexes in addition to per-column indexes;
        /// <see langword="false"/> to rebuild only per-column indexes.
        /// </param>
        public void RunWithReindexAll(string operationName, Action<MetaDBManager> action, bool includeGlobal = true) =>
            Run(operationName, mgr =>
            {
                action(mgr);
                mgr.RebuildAllIndexes(includeGlobal);
            });

        /// <summary>
        /// Asynchronous version of <see cref="RunWithReindexAll"/> that rebuilds
        /// indexes for all materialized tables when the operation completes successfully.
        /// </summary>
        /// <param name="operationName">Human-readable operation name. Used in logs.</param>
        /// <param name="action">Async operation to execute.</param>
        /// <param name="includeGlobal">
        /// <see langword="true"/> to rebuild global indexes in addition to per-column indexes;
        /// <see langword="false"/> to rebuild only per-column indexes.
        /// </param>
        /// <returns>A task that completes when the operation and reindexing have finished.</returns>
        public async Task RunWithReindexAllAsync(string operationName, Func<MetaDBManager, Task> action, bool includeGlobal = true) =>
            await RunCore(operationName, async mgr =>
            {
                await action(mgr).ConfigureAwait(false);
                mgr.RebuildAllIndexes(includeGlobal);
            }).ConfigureAwait(false);

        /// <summary>
        /// Executes a named operation and, upon successful completion, rebuilds
        /// the key indexes for a single target table.
        /// </summary>
        /// <param name="operationName">Human-readable operation name. Used in logs.</param>
        /// <param name="tableId">Identifier of the table whose indexes should be rebuilt.</param>
        /// <param name="action">Operation to execute.</param>
        /// <param name="includeGlobal">
        /// <see langword="true"/> to rebuild the global index for the table as well as per-column indexes;
        /// <see langword="false"/> to rebuild only per-column indexes.
        /// </param>
        public void RunWithReindexTable(string operationName, Guid tableId, Action<MetaDBManager> action, bool includeGlobal = true) =>
            Run(operationName, mgr =>
            {
                action(mgr);
                mgr.RebuildIndexes(tableId, includeGlobal);
            });

        /// <summary>
        /// Asynchronous version of <see cref="RunWithReindexTable"/> that rebuilds
        /// the key indexes for a single table when the operation completes successfully.
        /// </summary>
        /// <param name="operationName">Human-readable operation name. Used in logs.</param>
        /// <param name="tableId">Identifier of the table whose indexes should be rebuilt.</param>
        /// <param name="action">Async operation to execute.</param>
        /// <param name="includeGlobal">
        /// <see langword="true"/> to rebuild the global index for the table as well as per-column indexes;
        /// <see langword="false"/> to rebuild only per-column indexes.
        /// </param>
        /// <returns>A task that completes when the operation and reindexing have finished.</returns>
        public async Task RunWithReindexTableAsync(string operationName, Guid tableId, Func<MetaDBManager, Task> action, bool includeGlobal = true) =>
            await RunCore(operationName, async mgr =>
            {
                await action(mgr).ConfigureAwait(false);
                mgr.RebuildIndexes(tableId, includeGlobal);
            }).ConfigureAwait(false);

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
        private void FillColumn<T>(MetadataTable table, uint column, uint rows, Func<uint, T> factory, CapacityPolicy policy, int batchSize = DEFAULT_BATCH_SIZE) where T : unmanaged
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
                    MetaDBManagerHelpers.PerCellFill(table, column, rows, factory, batchSize, tSize, MaxStackBytes);
                    return;
                }

                if (tSize > blk.ValueSize)
                    throw new InvalidOperationException(
                        $"[{table}] col={column}: VALUE {blk.ValueSize} < sizeof({typeof(T).Name})={tSize}");

                if (MetaDBManagerHelpers.TryContiguousFill(rows, factory, batchSize, tSize, MaxStackBytes, blk)) return;
                MetaDBManagerHelpers.StridedFill(rows, factory, batchSize, tSize, MaxStackBytes, blk);
                return;
            }

            // ── SLOW PATH: per-cell (compat) — still batch the factory ─────────────
            MetaDBManagerHelpers.PerCellFill(table, column, rows, factory, batchSize, tSize, MaxStackBytes);
        }

        /// <summary>
        /// Core implementation for filling a column using a raw writer callback that
        /// receives a pointer to the VALUE buffer per row.
        /// </summary>
        /// <param name="table">Target table whose column will be written.</param>
        /// <param name="column">Zero-based column index to fill.</param>
        /// <param name="rows">Number of logical rows to fill (starting at 0).</param>
        /// <param name="writer">
        /// Callback invoked once per row with:
        /// <list type="bullet">
        ///   <item><description>The zero-based row index.</description></item>
        ///   <item><description>A pointer to the row VALUE buffer.</description></item>
        ///   <item><description>The VALUE buffer size in bytes.</description></item>
        /// </list>
        /// The callback is responsible for writing exactly the intended payload
        /// into the provided buffer.
        /// </param>
        /// <param name="policy">
        /// Capacity policy controlling how to react when the current row capacity
        /// is insufficient (e.g. <see cref="CapacityPolicy.Throw"/> or
        /// <see cref="CapacityPolicy.AutoGrowZeroInit"/>).
        /// </param>
        /// <param name="batchSize">
        /// Optional batch size for the column-block fast path. When a
        /// <see cref="ColumnBlock"/> is not available, it is only used to chunk
        /// the per-cell loop.
        /// </param>
        /// <remarks>
        /// <para>
        /// When a fixed-size <see cref="ColumnBlock"/> is available for the target
        /// column, the method uses a strided pointer-based loop over the underlying
        /// memory-mapped buffer to minimize per-row overhead.
        /// </para>
        /// <para>
        /// If no suitable column block exists (or the VALUE size is variable),
        /// the method falls back to per-cell access using
        /// <see cref="MetadataTable.GetOrCreateCell(uint, uint)"/>.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private void FillColumn(MetadataTable table, uint column, uint rows, Action<uint, IntPtr, uint> writer, CapacityPolicy policy, int batchSize = DEFAULT_BATCH_SIZE)
        {
            if (rows == 0) return;

            EnsureRowCapacity(table, column, rows, policy);

            // ── FAST PATH: via column block (MMF / flat buffer) ────────────────────
            if (MetaDBManagerHelpers.TryContiguousFillRaw(table, column, rows, writer, batchSize)) return;
            MetaDBManagerHelpers.PerCellFillRaw(table, column, rows, writer, batchSize);
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
        private void CopyColumn(MetadataTable src, uint srcCol, MetadataTable dst, uint dstCol, uint rows, CapacityPolicy dstPolicy, int batchSize = DEFAULT_BATCH_SIZE)
        {
            EnsureRowCapacity(dst, dstCol, rows, dstPolicy);

            bool done = MetaDBManagerHelpers.PerBlockStridedCopy(src, srcCol, dst, dstCol, rows, batchSize);
            if (done) return;

            // ===== Fallback: API por celda =====
            MetaDBManagerHelpers.PerCellCopy(src, srcCol, dst, dstCol, rows);
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
        /// Rebuilds the key indexes of the metadata table identified by the specified <paramref name="tableId"/>.
        /// </summary>
        /// <param name="tableId">
        /// The unique identifier of the table whose indexes should be rebuilt.
        /// </param>
        /// <param name="includeGlobal">
        /// <see langword="true"/> to rebuild both per-column and global key indexes;
        /// <see langword="false"/> to rebuild only per-column indexes.
        /// </param>
        /// <remarks>
        /// <para>
        /// This method retrieves (or materializes on demand) the <see cref="MetadataTable"/>
        /// associated with <paramref name="tableId"/> and invokes
        /// <see cref="MetadataTable.RebuildIndexes(bool)"/> on it.
        /// </para>
        /// <para>
        /// Only the target table is affected. To rebuild indexes for all materialized tables,
        /// use <see cref="RebuildAllIndexes(bool)"/>.
        /// </para>
        /// </remarks>
        public void RebuildIndexes(Guid tableId, bool includeGlobal = true)
        {
            var table = GetOrCreate(tableId);
            table.RebuildIndexes(includeGlobal);
        }

        /// <summary>
        /// Rebuilds per-column and global key indexes for all materialized tables
        /// managed by this <see cref="MetaDBManager"/>.
        /// </summary>
        /// <param name="includeGlobal">
        /// <see langword="true"/> to rebuild global key indexes as well as per-column indexes;
        /// <see langword="false"/> to rebuild only per-column indexes.
        /// </param>
        /// <remarks>
        /// Only tables whose underlying <see cref="MetadataTable"/> has already been created
        /// (<c>ManagedTable.IsCreated == true</c>) are processed. Tables that are registered
        /// but still lazy are skipped.
        /// </remarks>
        private void RebuildAllIndexes(bool includeGlobal = true)
        {
            // _byId is a ConcurrentDictionary → snapshot enumeration is safe.
            foreach (var managed in _byId.Values)
            {
                if (managed is null || !managed.IsCreated)
                    continue;

                // For now we rebuild unique per-column + global indexes.
                // Later, when we add multi-index support, this can call the extended overload.
                managed.Table.RebuildIndexes(includeGlobal);
            }
        }

        /// <summary>
        /// Lightweight value container returned by the <c>PrepareFill</c> helper,
        /// bundling the resolved managed table, an optional stopwatch, the effective
        /// capacity policy and the computed batch size for column operations.
        /// </summary>
        /// <param name="ManagedTable">
        /// The resolved <see cref="ManagedTable"/> instance targeted by the fill operation.
        /// </param>
        /// <param name="StopWatch">
        /// Optional <see cref="Stopwatch"/> used to measure elapsed time when logging is enabled;
        /// <see langword="null"/> when timing is not active.
        /// </param>
        /// <param name="Policy">
        /// Effective <see cref="CapacityPolicy"/> to apply for the current operation, after
        /// normalizing any per-call override against the manager-wide default.
        /// </param>
        /// <param name="BatchSize">
        /// Batch size in rows computed from the column VALUE size and used to drive
        /// cache-friendly inner loops in the fill helpers.
        /// </param>
        private record struct PrepareFillReturnValue(ManagedTable ManagedTable, Stopwatch? StopWatch, CapacityPolicy Policy, int BatchSize) : IEquatable<PrepareFillReturnValue>
        {
            /// <summary>
            /// Implicitly converts a <see cref="PrepareFillReturnValue"/> into a tuple
            /// compatible with deconstruction patterns used by the caller.
            /// </summary>
            public static implicit operator (ManagedTable m, Stopwatch? sw, CapacityPolicy policy, int batchSize)(PrepareFillReturnValue value)
            {
                return (value.ManagedTable, value.StopWatch, value.Policy, value.BatchSize);
            }

            /// <summary>
            /// Implicitly converts a tuple produced by <c>PrepareFill</c> into a
            /// <see cref="PrepareFillReturnValue"/> instance, allowing symmetric usage
            /// between tuple and record forms.
            /// </summary>
            public static implicit operator PrepareFillReturnValue((ManagedTable m, Stopwatch? sw, CapacityPolicy policy, int batchSize) value)
            {
                return new PrepareFillReturnValue(value.m, value.sw, value.policy, value.batchSize);
            }
        }

    }
}
