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
    /// High-level orchestrator for Extend0 MetaDB operations.
    /// Manages <see cref="MetadataTable"/> lifecycles (register/open/close), provides
    /// column pipelines (fill/copy) with capacity policies, and supports parent→child
    /// reference linking with idempotent semantics and structured observability.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>What this manager does</b> (in human terms): it keeps a registry of tables by
    /// <see cref="Guid"/> and by unique name, materializes tables lazily when requested,
    /// and exposes higher-level operations that can be traced and logged consistently.
    /// </para>
    ///
    /// <para>
    /// <b>Registries &amp; identity</b>:
    /// tables are tracked in two concurrent maps:
    /// <list type="bullet">
    ///   <item>
    ///     <description><c>_byId</c> maps a table <see cref="Guid"/> to a <c>ManagedTable</c> wrapper.</description>
    ///   </item>
    ///   <item>
    ///     <description><c>_byName</c> maps a unique table name (<see cref="StringComparer.Ordinal"/>) to its id.</description>
    ///   </item>
    /// </list>
    /// Registration can be <i>lazy</i> (default) or <i>eager</i> (<c>createNow</c>).
    /// </para>
    ///
    /// <para>
    /// <b>Open / relocation</b>:
    /// existing tables can be opened from a map path by loading an adjacent
    /// <c>.tablespec.json</c>. When <c>forceRelocation</c> is enabled, the loaded
    /// <see cref="TableSpec.MapPath"/> is overridden with the caller-provided map path,
    /// allowing “relocated” deployments.
    /// </para>
    ///
    /// <para>
    /// <b>Column pipelines (Fill/Copy)</b>:
    /// the manager offers high-throughput column operations with a capacity policy:
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       <see cref="CapacityPolicy.Throw"/>: fail fast when capacity is insufficient.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <see cref="CapacityPolicy.AutoGrowZeroInit"/>: attempts to grow using the table
    ///       <c><see cref="MetadataTable.TryGrowColumnTo(uint, uint, bool)"/></c> hook, then validates capacity.
    ///     </description>
    ///   </item>
    /// </list>
    /// Fill and copy methods use batching heuristics based on VALUE size and prefer
    /// fast paths through <see cref="ColumnBlock"/> where possible, falling back to per-cell access.
    /// </para>
    ///
    /// <para>
    /// <b>Parent→child references</b>:
    /// supports storing a small ref-vector inside a parent cell (refs column) and linking child tables
    /// using <see cref="MetadataTableRef"/>. The “get-or-create-and-link” APIs can enforce uniqueness per
    /// parent row using a stable <c>childKey</c> (stored in <see cref="MetadataTableRef.Reserved"/>),
    /// making linking idempotent and avoiding duplicate children.
    /// </para>
    ///
    /// <para>
    /// <b>Ephemeral scopes</b>:
    /// provides helper methods that open/register a table, run a user callback, and always close/unregister
    /// the table in a <c>finally</c>. Optional deletion (<c>deleteNow</c>) can be requested; when deletion is
    /// blocked by transient OS locks, paths may be queued for eventual deletion.
    /// </para>
    ///
    /// <para>
    /// <b>Delete queue &amp; background worker</b>:
    /// when not running in the browser (<see cref="OperatingSystem.IsBrowser"/>), the manager can persist a
    /// delete queue to disk and run a background worker that retries deletions with backoff. The queue is
    /// append-only during normal operation and may be compacted periodically.
    /// </para>
    ///
    /// <para>
    /// <b>Observability</b>:
    /// when a logger is provided and debug logging is enabled, operations emit structured start/end/failure
    /// events and may create OpenTelemetry-compatible spans using <see cref="ActivitySrc"/> and
    /// <see cref="OpScope"/>.
    /// </para>
    ///
    /// <para>
    /// <b>Threading</b>:
    /// internal registries are <see cref="ConcurrentDictionary{TKey, TValue}"/> to support concurrent
    /// registration and lookup. Thread-safety of <see cref="MetadataTable"/> operations themselves depends on
    /// the guarantees of the table implementation and the caller’s usage patterns.
    /// </para>
    /// </remarks>
    /// <seealso cref="MetadataTable"/>
    /// <seealso cref="TableSpec"/>
    /// <seealso cref="ColumnBlock"/>
    /// <seealso cref="MetadataTableRef"/>
    public sealed partial class MetaDBManager : IMetaDBManager
    {
        /// <summary>
        /// Default batch size(in rows) used by column operations when no explicit batch size is provided.
        /// Tuned as a safe, conservative value for most workloads.
        /// </summary>
        internal const int DEFAULT_BATCH_SIZE = 512;

        /// <summary>
        /// The default name for the delete-queue persistence file.
        /// </summary>
        private const string DELETES_FILE_DEFAULT_NAME = "metadb.deletes.log";

        /// <summary>
        /// Shared <see cref="ActivitySource"/> used to emit OpenTelemetry-compatible activities
        /// for high-level MetaDB operations (e.g. fills, copies, ref-linking, pipelines).
        /// </summary>
        /// <remarks>
        /// Attach this source to your tracing pipeline to correlate MetaDB spans with the rest of your
        /// distributed system. When logging is enabled, helper scopes and operation wrappers
        /// (<see cref="OpScope"/>, <c>Run*</c> methods) create activities from this source.
        /// </remarks>
        public static readonly ActivitySource ActivitySrc = new(OpScope.LIBRARY_NAME, OpScope.LibraryVersion);

        /// <summary>
        /// Application logger used for structured traces, scopes, and diagnostics.
        /// </summary>
        private readonly ILogger? _log;

        /// <summary>
        /// Indicates whether structured logging and timing are enabled for this manager instance.
        /// </summary>
        /// <remarks>
        /// When <see langword="true"/>, operations may emit start/end/fail log events and measure elapsed time
        /// (e.g. via <see cref="Stopwatch"/>). When <see langword="false"/>, logging-related work is skipped
        /// to minimize overhead.
        /// </remarks>
        private readonly bool _isLogActivated;

        /// <summary>
        /// Default capacity growth policy applied to operations that may require expanding table storage.
        /// </summary>
        /// <remarks>
        /// Used when a call does not provide an explicit policy override (e.g. <see cref="CapacityPolicy.None"/>).
        /// Typical behaviors include throwing on overflow, auto-growing capacity, or performing zero-initialized growth,
        /// depending on the selected <see cref="CapacityPolicy"/>.
        /// </remarks>
        private readonly CapacityPolicy _capacityPolicy = CapacityPolicy.Throw;

        /// <summary>
        /// Disposal state flag for this manager.
        /// </summary>
        /// <remarks>
        /// Stored as an <see cref="int"/> to support atomic transitions (e.g. via <see cref="System.Threading.Interlocked"/>),
        /// making disposal idempotent and thread-safe. Conventionally: 0 = not disposed, 1 = disposed.
        /// </remarks>
        private int _disposed;

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
        /// Pending deletions for this manager instance.
        /// </summary>
        /// <remarks>
        /// Acts as an idempotent, thread-safe set of deletion keys. A key present in this dictionary
        /// is considered queued for deletion; repeated enqueue attempts are ignored.
        /// Uses case-insensitive matching (<see cref="StringComparer.OrdinalIgnoreCase"/>).
        /// </remarks>
        private readonly ConcurrentDictionary<string, byte> _pendingDeletes = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Absolute path to the persistence file that stores the delete queue for this manager instance.
        /// </summary>
        /// <remarks>
        /// Used to survive process restarts by persisting queued deletions. Access to this file is synchronized
        /// via <see cref="_deleteFileLock"/>.
        /// </remarks>
        private string _deleteQueuePath;

        /// <summary>
        /// Cancellation token source that controls the lifetime of the background delete worker.
        /// </summary>
        /// <remarks>
        /// Cancelling this source signals <see cref="_deleteWorkerTask"/> to stop. Must be cancelled/disposed
        /// during manager shutdown/dispose to avoid leaking resources.
        /// </remarks>
        private CancellationTokenSource? _deleteCts = new();

        /// <summary>
        /// Background worker task responsible for processing queued deletions.
        /// </summary>
        /// <remarks>
        /// Drains <see cref="_pendingDeletes"/> and applies deletions, persisting queue updates to
        /// <see cref="_deleteQueuePath"/> as needed.
        /// </remarks>
        private Task? _deleteWorkerTask;

        /// <summary>
        /// Synchronization primitive used to serialize access to the delete queue persistence file.
        /// </summary>
        /// <remarks>
        /// Ensures atomic read/write operations on <see cref="_deleteQueuePath"/> across concurrent threads.
        /// </remarks>
        private readonly Lock _deleteFileLock = new();

        /// <summary>
        /// Synchronization gate used to serialize delete worker restarts.
        /// </summary>
        private readonly SemaphoreSlim _deleteWorkerGate = new(1, 1);

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
        /// <param name="deleteQueuePath">
        /// Optional path for the persisted delete queue file. When <see langword="null"/>,
        /// defaults to <c>{AppContext.BaseDirectory}\metadb.deletes.log</c>.
        /// Ignored when running in browser (<see cref="OperatingSystem.IsBrowser"/>).
        /// </param>
        public MetaDBManager(ILogger? logger, Func<TableSpec?, MetadataTable>? factory = null, CapacityPolicy capacityPolicy = CapacityPolicy.Throw, string? deleteQueuePath = null)
        {
            _log = logger;
            _isLogActivated = _log != null && _log.IsEnabled(LogLevel.Debug);

            _factory = factory ?? s_FactoryCreator;
            _capacityPolicy = capacityPolicy == CapacityPolicy.None ? CapacityPolicy.Throw : capacityPolicy;

            if (!OperatingSystem.IsBrowser())
            {
                _deleteQueuePath = deleteQueuePath ?? Path.Combine(AppContext.BaseDirectory, DELETES_FILE_DEFAULT_NAME);
                LoadDeleteQueueFromDisk();
                _deleteWorkerTask = Task.Run(() => DeleteWorkerLoopAsync(_deleteCts.Token));
            }
            else
            {
                _deleteQueuePath = string.Empty;
                _deleteCts?.Dispose();
                _deleteCts = null;
                _deleteWorkerTask = Task.CompletedTask;
            }
        }

        /// <summary>
        /// Restarts the background delete worker, optionally overriding the persisted delete-queue file path.
        /// </summary>
        /// <param name="deleteQueuePath">
        /// Optional path to the delete-queue log file.
        /// If <see langword="null"/>, the previously configured path is reused; if none exists, a default
        /// file named <c>metadb.deletes.log</c> under <see cref="AppContext.BaseDirectory"/> is used.
        /// </param>
        /// <remarks>
        /// <para>
        /// This method is serialized via an async gate (<c>_deleteWorkerGate</c>) to prevent concurrent restarts
        /// (or shutdown) from racing each other.
        /// </para>
        /// <para>
        /// It always requests cancellation of the current worker (best-effort) and awaits its completion.
        /// Any <see cref="OperationCanceledException"/> is swallowed; other worker failures are logged at debug level.
        /// </para>
        /// <para>
        /// On browser runtimes (<see cref="OperatingSystem.IsBrowser"/>), background workers and file I/O are not supported.
        /// The method disables deletion processing by clearing the queue path and setting the worker task to
        /// <see cref="Task.CompletedTask"/>.
        /// </para>
        /// <para>
        /// On non-browser runtimes, after the current worker has stopped, the method updates <c>_deleteQueuePath</c>,
        /// disposes the previous <see cref="CancellationTokenSource"/>, creates a new one, reloads any persisted queue
        /// entries from disk, and starts a fresh worker loop with the new token.
        /// </para>
        /// <para>
        /// Reloading the persisted queue is best-effort: failures to read or parse the queue file are treated as non-fatal
        /// and logged as warnings (the in-memory queue remains the source of truth for the current process).
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">
        /// Thrown if this manager has been disposed, or if the gate/CTS is disposed concurrently while restarting.
        /// </exception>
        public async Task RestartDeleteWorker(string? deleteQueuePath = null)
        {
            ThrowIfDisposed();
            await _deleteWorkerGate.WaitAsync().ConfigureAwait(false);
            try
            {
                await CancelAndWaitForWorkerEnd().ConfigureAwait(false);

                if (OperatingSystem.IsBrowser())
                {
                    _deleteCts?.Dispose();
                    _deleteCts = null;
                    _deleteQueuePath = string.Empty;
                    _deleteWorkerTask = Task.CompletedTask;
                    return;
                }

                var usedPath = deleteQueuePath ?? _deleteQueuePath;
                if (string.IsNullOrEmpty(usedPath))
                    usedPath = Path.Combine(AppContext.BaseDirectory, DELETES_FILE_DEFAULT_NAME);

                _deleteQueuePath = usedPath;

                _deleteCts?.Dispose();
                _deleteCts = new CancellationTokenSource();

                LoadDeleteQueueFromDisk();
                _deleteWorkerTask = Task.Run(() => DeleteWorkerLoopAsync(_deleteCts.Token));
            }
            finally
            {
                _deleteWorkerGate.Release();
            }
        }

        /// <summary>
        /// Requests cancellation of the current delete worker (best-effort) and waits for it to finish.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This helper snapshots <see cref="_deleteCts"/> and <see cref="_deleteWorkerTask"/> into locals to reduce
        /// race windows with concurrent restarts/disposal.
        /// </para>
        /// <para>
        /// Cancellation is requested in a best-effort manner: exceptions thrown by
        /// <see cref="CancellationTokenSource.Cancel()"/> are swallowed.
        /// </para>
        /// <para>
        /// If a worker task exists, it is awaited. <see cref="OperationCanceledException"/> is swallowed.
        /// Any other exception coming from the worker is swallowed and logged at debug level.
        /// </para>
        /// <para>
        /// This method never throws: its job is to drain/stop the worker without masking a primary failure
        /// in the caller.
        /// </para>
        /// </remarks>
        private async Task CancelAndWaitForWorkerEnd()
        {
            var cts = _deleteCts;
            if (cts is not null)
            {
                try { cts.Cancel(); } catch { /* best-effort */ }
            }

            var worker = _deleteWorkerTask;
            if (worker is not null)
            {
                try { await worker.ConfigureAwait(false); }
                catch (OperationCanceledException) { }
                catch (Exception ex) { _log?.LogDebug(ex, "Delete worker ended with error."); }
            }
        }

        /// <summary>
        /// Background deletion worker loop that retries pending file deletions until they succeed
        /// or the worker is cancelled.
        /// </summary>
        /// <param name="ct">Cancellation token used to stop the worker.</param>
        /// <remarks>
        /// <para>
        /// This loop is designed to tolerate transient OS-level locks (MMF views, antivirus, delayed handle release).
        /// It iterates over the in-memory pending-delete set and attempts to delete each path.
        /// </para>
        /// <para>
        /// The loop uses a dynamic heuristic based on:
        /// <list type="bullet">
        ///   <item><description>Backlog size and trend (queue growing vs draining)</description></item>
        ///   <item><description>Estimated arrival rate (items enqueued while processing)</description></item>
        ///   <item><description>Deletion success rate (signal for lock contention)</description></item>
        /// </list>
        /// </para>
        /// <para>
        /// Compaction of the persisted queue file is deliberately throttled:
        /// during “storm” conditions (high inflow or non-decreasing backlog), compaction is rare;
        /// during “drain” conditions (backlog shrinking with progress), compaction becomes more frequent.
        /// </para>
        /// </remarks>
        private async Task DeleteWorkerLoopAsync(CancellationToken ct)
        {
            const int idleDelayMs = 500;
            const int busyDelayMs = 50;

            // Baselines (dynamically scaled with backlog)
            const int baseMaxDeletes = 64;
            const int baseMaxAttempts = 256;

            // Dynamic compaction throttle
            long lastCompactMs = Environment.TickCount64;
            int compactCooldownMs = 1_000;
            const int minCooldownMs = 250;
            const int maxCooldownMs = 60_000;

            // Small “storm persistence” score to reduce jitter
            int stormScore = 0;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    int n0 = _pendingDeletes.Count;
                    if (n0 == 0)
                    {
                        stormScore = 0;
                        await MetaDBManagerHelpers.DelaySafe(idleDelayMs, ct).ConfigureAwait(false);
                        continue;
                    }

                    // Determine per-cycle budget (deletes/attempts) based on backlog size.
                    MetaDBManagerHelpers.ComputeCycleBudget(n0, baseMaxDeletes, baseMaxAttempts, out int maxDeletesPerCycle, out int maxAttemptsPerCycle);

                    // Process one cycle of deletion attempts using snapshot enumeration semantics.
                    var stats = await ProcessDeleteCycleAsync(maxDeletesPerCycle, maxAttemptsPerCycle, ct).ConfigureAwait(false);

                    int n1 = _pendingDeletes.Count;

                    // Estimate how many items were enqueued while we were processing.
                    int arrivals = MetaDBManagerHelpers.EstimateArrivals(n0, n1, stats.Deleted);

                    // Determine whether we are under “storm” conditions (avoid compaction) or draining (allow compaction).
                    bool storm = MetaDBManagerHelpers.IsStorm(n0, n1, stats.Deleted, arrivals);

                    // Update dynamic compaction cooldown based on the current mode.
                    MetaDBManagerHelpers.UpdateCompactionCooldown(storm, ref stormScore, ref compactCooldownMs, minCooldownMs, maxCooldownMs);

                    // Compact opportunistically only when it is likely to be useful and when cooldown allows it.
                    MaybeCompactDeleteQueueFile(
                        storm: storm,
                        backlogAfter: n1,
                        deletedThisCycle: stats.Deleted,
                        ref lastCompactMs,
                        compactCooldownMs);

                    // Decide next delay based on progress and contention.
                    int delay = MetaDBManagerHelpers.ComputeNextDelayMs(stats, busyDelayMs, idleDelayMs);

                    await MetaDBManagerHelpers.DelaySafe(delay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    _log?.LogWarning(ex, "Delete worker loop crashed; continuing.");
                    await MetaDBManagerHelpers.DelaySafe(1000, ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Executes a single deletion cycle by enumerating pending keys and attempting deletions
        /// until budgets are exhausted or cancellation is requested.
        /// </summary>
        /// <param name="maxDeletesPerCycle">Stop after deleting this many items.</param>
        /// <param name="maxAttemptsPerCycle">Stop after attempting this many deletions.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Per-cycle statistics (deleted/attempted).</returns>
        /// <remarks>
        /// This method intentionally enumerates <see cref="_pendingDeletes"/> keys directly.
        /// ConcurrentDictionary enumeration has snapshot semantics and does not require copying keys.
        /// </remarks>
        private async Task<DeleteCycleStats> ProcessDeleteCycleAsync(int maxDeletesPerCycle, int maxAttemptsPerCycle, CancellationToken ct)
        {
            int deleted = 0;
            int attempts = 0;

            foreach (var path in _pendingDeletes.Keys)
            {
                ct.ThrowIfCancellationRequested();

                if (++attempts > maxAttemptsPerCycle)
                    break;

                if (await MetaDBManagerHelpers.TryDeleteWithRetries(path).ConfigureAwait(false))
                {
                    _pendingDeletes.TryRemove(path, out _);
                    deleted++;

                    if (deleted >= maxDeletesPerCycle)
                        break;
                }
            }

            return new DeleteCycleStats(deleted, attempts);
        }

        /// <summary>
        /// Performs opportunistic compaction of the persisted delete queue file based on cooldown and mode.
        /// </summary>
        /// <param name="storm">If <see langword="true"/>, compaction is discouraged.</param>
        /// <param name="backlogAfter">Backlog size after the cycle.</param>
        /// <param name="deletedThisCycle">How many deletes succeeded in this cycle.</param>
        /// <param name="lastCompactMs">Last compaction tick timestamp (updated in place).</param>
        /// <param name="compactCooldownMs">Current cooldown threshold in milliseconds.</param>
        /// <remarks>
        /// Compaction rewrites the queue file, which can be expensive under high churn.
        /// This method ensures:
        /// <list type="bullet">
        ///   <item><description>No compaction if no progress was made.</description></item>
        ///   <item><description>Respect a dynamic cooldown to reduce disk churn.</description></item>
        ///   <item><description>Prefer compaction when draining or when backlog is small.</description></item>
        /// </list>
        /// </remarks>
        private void MaybeCompactDeleteQueueFile(bool storm, int backlogAfter, int deletedThisCycle, ref long lastCompactMs, int compactCooldownMs)
        {
            if (deletedThisCycle <= 0)
                return;

            long nowMs = Environment.TickCount64;
            if ((nowMs - lastCompactMs) < compactCooldownMs)
                return;

            // If storm: only compact when backlog is small (safe window).
            if (storm && backlogAfter >= 256)
                return;

            TryRewriteDeleteQueueFile();
            lastCompactMs = nowMs;
        }

        /// <summary>
        /// Loads persisted pending deletions from the on-disk delete queue file into the
        /// in-memory pending-delete set (<c>_pendingDeletes</c>).
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is intended to be called once during manager initialization (before starting
        /// the delete worker) so that deletions requested in previous runs can be retried.
        /// </para>
        /// <para>
        /// Each non-empty line in <c>_deleteQueuePath</c> is interpreted as a file path and added
        /// to <c>_pendingDeletes</c> in an idempotent manner.
        /// </para>
        /// <para>
        /// Failures to read or parse the queue file are treated as non-fatal and are logged as warnings.
        /// </para>
        /// </remarks>
        private void LoadDeleteQueueFromDisk()
        {
            lock (_deleteFileLock)
            {
                try
                {
                    if (!File.Exists(_deleteQueuePath))
                        return;

                    foreach (var line in File.ReadLines(_deleteQueuePath))
                    {
                        var path = line?.Trim();
                        if (string.IsNullOrWhiteSpace(path)) continue;
                        _pendingDeletes.TryAdd(path, 0);
                    }
                }
                catch (Exception ex)
                {
                    _log?.LogWarning(ex, "Failed to load delete queue file (Path={QueuePath})", _deleteQueuePath);
                }
            }
        }

        /// <summary>
        /// Best-effort compaction of the on-disk delete queue file to reflect the current
        /// contents of the in-memory pending-delete set.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The queue is append-only during normal operation (see <see cref="EnqueueDelete(string)"/>),
        /// so this method periodically rewrites the file to contain only paths that are still pending.
        /// This prevents unbounded growth over long-running workloads.
        /// </para>
        /// <para>
        /// The rewrite uses a temporary file followed by an overwrite move to reduce the chance of
        /// leaving a partially-written file behind. This is "atomic-ish" and sufficient for this
        /// best-effort durability mechanism.
        /// </para>
        /// <para>
        /// Any exceptions are swallowed and logged at debug level; failure to compact does not affect
        /// correctness because the in-memory set remains the source of truth for the current process.
        /// </para>
        /// </remarks>
        private void TryRewriteDeleteQueueFile()
        {
            try
            {
                // Snapshot to avoid holding up deletions.
                var snapshot = _pendingDeletes.Keys.ToArray();

                MetaDBManagerHelpers.EnsureDirForFile(_deleteQueuePath);

                // Atomic-ish rewrite.
                var tmp = _deleteQueuePath + ".tmp";
                lock (_deleteFileLock)
                {
                    File.WriteAllLines(tmp, snapshot);
                    File.Move(tmp, _deleteQueuePath, overwrite: true);
                }
            }
            catch (Exception ex)
            {
                _log?.LogDebug(ex, "Failed to compact delete queue file (Path={QueuePath})", _deleteQueuePath);
            }
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
        public MetadataTable Open(string mapPath) => Open(mapPath, false).Table;

        /// <summary>
        /// Opens an existing table from <paramref name="mapPath"/> by loading its adjacent
        /// <c>.tablespec.json</c> file, registering it in this manager, and returning both
        /// the generated table id and the materialized <see cref="MetadataTable"/>.
        /// </summary>
        /// <param name="mapPath">
        /// The map file path for the table (e.g., <c>C:\data\users.meta</c>).
        /// If a <c>.tablespec.json</c> path is provided, it is used directly.
        /// Otherwise, <c>.tablespec.json</c> is appended (e.g., <c>users.meta.tablespec.json</c>).
        /// </param>
        /// <param name="forceRelocation">
        /// When <see langword="true"/>, overrides the loaded <see cref="TableSpec.MapPath"/> with the
        /// input map path used for opening (i.e., relocates the spec to the caller-provided map path).
        /// When <see langword="false"/>, the loaded spec is used as-is; a mismatch is only optionally logged.
        /// </param>
        /// <returns>
        /// A tuple containing the generated table id and the opened, managed <see cref="MetadataTable"/>.
        /// </returns>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="mapPath"/> is null or whitespace.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the resolved <c>.tablespec.json</c> file does not exist.</exception>
        /// <exception cref="InvalidOperationException">Thrown when registration fails due to id conflict or duplicate table name.</exception>
        public (Guid Id, MetadataTable Table) Open(string mapPath, bool forceRelocation = false)
        {
            ThrowIfDisposed();
            if (string.IsNullOrWhiteSpace(mapPath))
                throw new ArgumentException("Map path cannot be null or whitespace.", nameof(mapPath));

            // Resolve spec path (accept either '...meta' or direct '...meta.tablespec.json')
            var specPath = mapPath.EndsWith(".tablespec.json", StringComparison.OrdinalIgnoreCase)
                ? mapPath
                : mapPath + ".tablespec.json";

            if (!File.Exists(specPath))
                throw new FileNotFoundException("TableSpec file not found for the given map path.", specPath);

            // If caller passed the spec file path, derive the map file path.
            var mapFilePath = mapPath.EndsWith(".tablespec.json", StringComparison.OrdinalIgnoreCase)
                ? mapPath[..^".tablespec.json".Length]
                : mapPath;

            var loaded = TableSpec.LoadFromFile(specPath);

            // Observability: detect mismatch (we only force relocation if requested)
            if (_isLogActivated && !string.IsNullOrWhiteSpace(loaded.MapPath) &&
                !string.Equals(loaded.MapPath, mapFilePath, StringComparison.OrdinalIgnoreCase))
            {
                _log?.LogDebug(
                    "OpenManaged: TableSpec MapPath differs from input map path (SpecMapPath={SpecMapPath}, InputMapPath={InputMapPath}, ForceRelocation={ForceRelocation})",
                    loaded.MapPath,
                    mapFilePath,
                    forceRelocation);
            }

            // Relocation (explicit, opt-in)
            var spec = forceRelocation ? loaded with { MapPath = mapFilePath } : loaded;

            var id = Guid.CreateVersion7();
            var managedTable = new ManagedTable(id, spec, _factory); // <-- USE spec (relocated or not)

            RegisterManagedTable(managedTable);

            try
            {
                // Force creation to validate mapping immediately
                var table = managedTable.Table;

                if (_isLogActivated)
                    Log.TableOpened(_log!, spec.Name, spec.MapPath, id);

                return (id, table);
            }
            catch
            {
                // Roll back both registries on failure
                _byId.TryRemove(id, out _);
                _byName.TryRemove(spec.Name, out _);
                throw;
            }
        }

        /// <summary>
        /// Closes and unregisters the table associated with the given identifier.
        /// </summary>
        /// <param name="tableId">The table identifier to close and unregister.</param>
        /// <returns>
        /// <see langword="true"/> if a managed table existed for <paramref name="tableId"/>
        /// and was successfully removed (its underlying <see cref="MetadataTable"/> disposed
        /// if it had been created); otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method performs the following steps:
        /// </para>
        /// <list type="number">
        ///   <item><description>
        ///   Removes the <see cref="ManagedTable"/> from the internal id registry (<c>_byId</c>).
        ///   </description></item>
        ///   <item><description>
        ///   Removes the associated name entry from <c>_byName</c>, when present.
        ///   </description></item>
        ///   <item><description>
        ///   Removes any cached child-table mappings from <c>_childIndex</c>.
        ///   </description></item>
        ///   <item><description>
        ///   If the table was already materialized (<c>ManagedTable.IsCreated == true</c>),
        ///   disposes its underlying <see cref="MetadataTable"/>.
        ///   </description></item>
        /// </list>
        /// <para>
        /// The backing files on disk (e.g. <c>.meta</c> and <c>.tablespec.json</c>) are
        /// <b>not</b> deleted by this method; it only releases in-process resources and
        /// unregisters the table from this manager.
        /// </para>
        /// </remarks>
        public bool Close(Guid tableId)
        {
            if (tableId == Guid.Empty)
                return false;

            // Remove from id registry first; if it wasn't there, nothing to do.
            if (!_byId.TryRemove(tableId, out var managed) || managed is null)
                return false;

            var name = managed.Name;

            // Best-effort cleanup of secondary registries.
            if (name is not null)
                _byName.TryRemove(name, out _);

            // Dispose only if the underlying table was actually created.
            if (managed.IsCreated)
            {
                try
                {
                    managed.Table.Dispose();
                }
                catch (Exception ex)
                {
                    if (_isLogActivated && _log is not null)
                        _log.LogError(ex, "Error disposing metadata table {Name} ({Id})", name ?? string.Empty, tableId);

                    // Re-throw to let callers decide how to handle disposal failures.
                    throw;
                }
            }

            if (_isLogActivated && _log is not null)
                _log.LogDebug("Closed metadata table {Name} ({Id})", name ?? string.Empty, tableId);

            return true;
        }

        /// <summary>
        /// Closes and unregisters a table by its registered name.
        /// </summary>
        /// <param name="name">The registered table name (case-sensitive).</param>
        /// <returns>
        /// <see langword="true"/> if a table with the given <paramref name="name"/>
        /// was found and closed; otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This is a convenience overload over <see cref="Close(Guid)"/> that resolves
        /// the identifier from the name registry.
        /// </remarks>
        public bool Close(string name)
        {
            if (!TryGetIdByName(name, out var id) || id == Guid.Empty)
                return false;

            return Close(id);
        }

        /// <summary>
        /// Closes and unregisters all currently managed tables.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method iterates over the current snapshot of table identifiers and
        /// calls <see cref="Close(Guid)"/> for each one. It is safe to call multiple
        /// times; tables that have already been closed are simply ignored.
        /// </para>
        /// <para>
        /// Like <see cref="Close(Guid)"/>, this only releases in-process resources
        /// and does not delete any underlying files on disk.
        /// </para>
        /// <para>
        /// This is a fail-fast operation intended for strict shutdown scenarios
        /// (e.g. tests or tooling).
        /// </para>
        /// </remarks>
        /// <exception cref="Exception">
        /// Propagates the first exception thrown by <see cref="Close(Guid)"/>.
        /// The operation stops immediately on failure.
        /// </exception>
        public void CloseAllStrict()
        {
            // ConcurrentDictionary supports snapshot enumeration; modifications
            // during iteration do not invalidate the enumerator.
            foreach (var id in _byId.Keys)
            {
                // Ignore result; per-table errors (dispose failures) will surface as exceptions.
                Close(id);
            }
        }

        /// <summary>
        /// Closes and unregisters all currently managed tables.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method iterates over the current snapshot of table identifiers and
        /// calls <see cref="Close(Guid)"/> for each one. It is safe to call multiple
        /// times; tables that have already been closed are simply ignored.
        /// </para>
        /// <para>
        /// Like <see cref="Close(Guid)"/>, this only releases in-process resources
        /// and does not delete any underlying files on disk.
        /// </para>
        /// <para>
        /// This is a best-effort operation: all tables are attempted even if
        /// some fail to close.
        /// </para>
        /// </remarks>
        /// <exception cref="AggregateException">
        /// Thrown when one or more tables failed to close. All tables are still
        /// attempted before the exception is raised.
        /// </exception>
        public void CloseAll()
        {
            List<Exception> exceptions = [];
            // ConcurrentDictionary supports snapshot enumeration; modifications
            // during iteration do not invalidate the enumerator.
            foreach (var id in _byId.Keys)
            {
                try
                {
                    // Ignore result; per-table errors (dispose failures) will surface as exceptions.
                    Close(id);
                }
                catch (Exception ex) when (ex is not ObjectDisposedException)
                {
                    exceptions.Add(ex);
                }
            }
            if (exceptions.Count > 0)
                throw new AggregateException("One or more errors occurred while closing metadata tables.", exceptions);
        }

        /// <summary>
        /// Resolves the <see cref="MetadataTable"/> identified by <paramref name="tableId"/>.
        /// </summary>
        /// <param name="tableId">Identifier of the table to resolve.</param>
        /// <returns>The resolved <see cref="MetadataTable"/> instance.</returns>
        /// <remarks>
        /// This method is a small internal helper that:
        /// <list type="bullet">
        ///   <item><description>Validates that the manager has not been disposed.</description></item>
        ///   <item><description>Resolves the table through <see cref="GetOrCreate(Guid)"/>, materializing it if it was registered lazily.</description></item>
        /// </list>
        /// It does not close, unregister, or otherwise alter the lifetime of the returned table.
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private MetadataTable ResolveTable(Guid tableId)
        {
            ThrowIfDisposed();
            return GetOrCreate(tableId);
        }

        /// <summary>
        /// Executes <paramref name="action"/> with the materialized table for <paramref name="tableId"/>.
        /// </summary>
        /// <param name="tableId">Identifier of the table to resolve.</param>
        /// <param name="action">Callback executed with the resolved <see cref="MetadataTable"/>.</param>
        /// <remarks>
        /// <para>
        /// This helper is a convenience scope: it resolves the table via <see cref="ResolveTable(Guid)"/> and invokes
        /// <paramref name="action"/>. It does <b>not</b> close or unregister the table afterwards.
        /// </para>
        /// <para>
        /// If the table was registered with lazy creation, this call will trigger materialization.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="action"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        public void WithTable(Guid tableId, Action<MetadataTable> action)
        {
            ArgumentNullException.ThrowIfNull(action);
            action(ResolveTable(tableId));
        }

        /// <summary>
        /// Executes <paramref name="func"/> with the materialized table for <paramref name="tableId"/> and returns its result.
        /// </summary>
        /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
        /// <param name="tableId">Identifier of the table to resolve.</param>
        /// <param name="func">Callback executed with the resolved <see cref="MetadataTable"/>.</param>
        /// <returns>The value produced by <paramref name="func"/>.</returns>
        /// <remarks>
        /// <para>
        /// This helper resolves the table via <see cref="ResolveTable(Guid)"/> and invokes <paramref name="func"/>.
        /// It does <b>not</b> close or unregister the table afterwards.
        /// </para>
        /// <para>
        /// If the table was registered with lazy creation, this call will trigger materialization.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="func"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        public TResult WithTable<TResult>(Guid tableId, Func<MetadataTable, TResult> func)
        {
            ArgumentNullException.ThrowIfNull(func);
            return func(ResolveTable(tableId));
        }

        /// <summary>
        /// Asynchronously executes <paramref name="func"/> with the materialized table for <paramref name="tableId"/>.
        /// </summary>
        /// <param name="tableId">Identifier of the table to resolve.</param>
        /// <param name="func">Async callback executed with the resolved <see cref="MetadataTable"/>.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// <para>
        /// This helper resolves the table via <see cref="ResolveTable(Guid)"/> and invokes <paramref name="func"/>.
        /// It does <b>not</b> close or unregister the table afterwards.
        /// </para>
        /// <para>
        /// If the table was registered with lazy creation, this call will trigger materialization.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="func"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        public async Task WithTableAsync(Guid tableId, Func<MetadataTable, Task> func)
        {
            ArgumentNullException.ThrowIfNull(func);
            await func(ResolveTable(tableId)).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously executes <paramref name="func"/> with the materialized table for <paramref name="tableId"/> and returns its result.
        /// </summary>
        /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
        /// <param name="tableId">Identifier of the table to resolve.</param>
        /// <param name="func">Async callback executed with the resolved <see cref="MetadataTable"/>.</param>
        /// <returns>A task producing the value returned by <paramref name="func"/>.</returns>
        /// <remarks>
        /// <para>
        /// This helper resolves the table via <see cref="ResolveTable(Guid)"/> and invokes <paramref name="func"/>.
        /// It does <b>not</b> close or unregister the table afterwards.
        /// </para>
        /// <para>
        /// If the table was registered with lazy creation, this call will trigger materialization.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="func"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        public async Task<TResult> WithTableAsync<TResult>(Guid tableId, Func<MetadataTable, Task<TResult>> func)
        {
            ArgumentNullException.ThrowIfNull(func);
            return await func(ResolveTable(tableId)).ConfigureAwait(false);
        }

        /// <summary>
        /// Attempts to close an ephemeral table identified by <paramref name="id"/> using best-effort semantics.
        /// </summary>
        /// <param name="id">The identifier of the table to close.</param>
        /// <remarks>
        /// <para>
        /// This helper is intentionally forgiving: it skips <see cref="Guid.Empty"/> and suppresses any exception thrown by
        /// <see cref="Close(Guid)"/>, logging the failure when a logger is available.
        /// </para>
        /// <para>
        /// It is meant to be used from <c>finally</c> blocks so cleanup does not mask the original failure.
        /// </para>
        /// </remarks>
        private void CloseEphemeralBestEffort(Guid id)
        {
            if (id == Guid.Empty) return;
            try { Close(id); }
            catch (Exception ex) { _log?.LogError(ex, "WithTableEphemeral failed closing table (Id={Id})", id); }
        }

        /// <summary>
        /// Opens a table from <paramref name="mapPath"/>, executes <paramref name="action"/>, and always closes the table afterwards.
        /// </summary>
        /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
        /// <param name="action">Callback executed with the opened <see cref="MetadataTable"/>.</param>
        /// <param name="forceRelocation">
        /// When <see langword="true"/>, forces relocation behavior during <see cref="Open(string, bool)"/> (implementation-defined).
        /// </param>
        /// <remarks>
        /// <para>
        /// This method provides an ephemeral usage scope: it calls <see cref="Open(string, bool)"/>, invokes <paramref name="action"/>,
        /// and then closes the opened table in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// Closing is best-effort: failures during <see cref="Close(Guid)"/> are swallowed and logged so they do not hide exceptions
        /// thrown by <paramref name="action"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="mapPath"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="action"/> is <see langword="null"/>.</exception>
        public void WithTableEphemeral(string mapPath, Action<MetadataTable> action, bool forceRelocation = false)
        {
            ThrowIfDisposed();
            ArgumentException.ThrowIfNullOrWhiteSpace(mapPath);
            ArgumentNullException.ThrowIfNull(action);

            Guid id = Guid.Empty;
            try
            {
                (id, var table) = Open(mapPath, forceRelocation);
                action(table);
            }
            finally
            {
                CloseEphemeralBestEffort(id);
            }
        }

        /// <summary>
        /// Opens a table from <paramref name="mapPath"/>, executes <paramref name="func"/>, and always closes the table afterwards,
        /// returning the callback result.
        /// </summary>
        /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
        /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
        /// <param name="func">Callback executed with the opened <see cref="MetadataTable"/>.</param>
        /// <param name="forceRelocation">
        /// When <see langword="true"/>, forces relocation behavior during <see cref="Open(string, bool)"/> (implementation-defined).
        /// </param>
        /// <returns>The value produced by <paramref name="func"/>.</returns>
        /// <remarks>
        /// <para>
        /// This method provides an ephemeral usage scope: it calls <see cref="Open(string, bool)"/>, invokes <paramref name="func"/>,
        /// and then closes the opened table in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// Closing is best-effort: failures during <see cref="Close(Guid)"/> are swallowed and logged so they do not hide exceptions
        /// thrown by <paramref name="func"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="mapPath"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="func"/> is <see langword="null"/>.</exception>
        public TResult WithTableEphemeral<TResult>(string mapPath, Func<MetadataTable, TResult> func, bool forceRelocation = false)
        {
            ThrowIfDisposed();
            ArgumentException.ThrowIfNullOrWhiteSpace(mapPath);
            ArgumentNullException.ThrowIfNull(func);

            Guid id = Guid.Empty;
            try
            {
                (id, var table) = Open(mapPath, forceRelocation);
                return func(table);
            }
            finally
            {
                CloseEphemeralBestEffort(id);
            }
        }

        /// <summary>
        /// Asynchronously opens a table from <paramref name="mapPath"/>, awaits <paramref name="func"/>, and always closes the table afterwards.
        /// </summary>
        /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
        /// <param name="func">Async callback executed with the opened <see cref="MetadataTable"/>.</param>
        /// <param name="forceRelocation">
        /// When <see langword="true"/>, forces relocation behavior during <see cref="Open(string, bool)"/> (implementation-defined).
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// <para>
        /// This method provides an ephemeral usage scope: it calls <see cref="Open(string, bool)"/>, invokes <paramref name="func"/>,
        /// and then closes the opened table in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// Closing is best-effort: failures during <see cref="Close(Guid)"/> are swallowed and logged so they do not hide exceptions
        /// thrown by <paramref name="func"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="mapPath"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="func"/> is <see langword="null"/>.</exception>
        public async Task WithTableEphemeralAsync(string mapPath, Func<MetadataTable, Task> func, bool forceRelocation = false)
        {
            ThrowIfDisposed();
            ArgumentException.ThrowIfNullOrWhiteSpace(mapPath);
            ArgumentNullException.ThrowIfNull(func);

            Guid id = Guid.Empty;
            try
            {
                (id, var table) = Open(mapPath, forceRelocation);
                await func(table).ConfigureAwait(false);
            }
            finally
            {
                CloseEphemeralBestEffort(id);
            }
        }

        /// <summary>
        /// Asynchronously opens a table from <paramref name="mapPath"/>, awaits <paramref name="func"/>, and always closes the table afterwards,
        /// returning the callback result.
        /// </summary>
        /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
        /// <param name="mapPath">Path to the memory-mapped file backing the table.</param>
        /// <param name="func">Async callback executed with the opened <see cref="MetadataTable"/>.</param>
        /// <param name="forceRelocation">
        /// When <see langword="true"/>, forces relocation behavior during <see cref="Open(string, bool)"/> (implementation-defined).
        /// </param>
        /// <returns>A task producing the value returned by <paramref name="func"/>.</returns>
        /// <remarks>
        /// <para>
        /// This method provides an ephemeral usage scope: it calls <see cref="Open(string, bool)"/>, invokes <paramref name="func"/>,
        /// and then closes the opened table in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// Closing is best-effort: failures during <see cref="Close(Guid)"/> are swallowed and logged so they do not hide exceptions
        /// thrown by <paramref name="func"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="mapPath"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="func"/> is <see langword="null"/>.</exception>
        public async Task<TResult> WithTableEphemeralAsync<TResult>(string mapPath, Func<MetadataTable, Task<TResult>> func, bool forceRelocation = false)
        {
            ThrowIfDisposed();
            ArgumentException.ThrowIfNullOrWhiteSpace(mapPath);
            ArgumentNullException.ThrowIfNull(func);

            Guid id = Guid.Empty;
            try
            {
                (id, var table) = Open(mapPath, forceRelocation);
                return await func(table).ConfigureAwait(false);
            }
            finally
            {
                CloseEphemeralBestEffort(id);
            }
        }

        /// <summary>
        /// Registers and opens an ephemeral table from the provided <paramref name="spec"/>.
        /// </summary>
        /// <param name="spec">The table specification used to register and (optionally) create the table.</param>
        /// <param name="createNow">
        /// When <see langword="true"/>, forces immediate creation/materialization during registration (implementation-defined).
        /// </param>
        /// <returns>
        /// A tuple containing:
        /// <list type="bullet">
        ///   <item><description><c>id</c>: The registration identifier.</description></item>
        ///   <item><description><c>table</c>: The resolved <see cref="MetadataTable"/> instance.</description></item>
        ///   <item><description><c>mapPath</c>: The backing map path for the table (if available).</description></item>
        ///   <item><description><c>specPath</c>: The path of the spec sidecar file (<c>{mapPath}.tablespec.json</c>) (if available).</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// <para>
        /// This helper composes the common ephemeral-open workflow:
        /// it registers the table via <see cref="RegisterTable(TableSpec, bool)"/> and then resolves it via <see cref="GetOrCreate(Guid)"/>.
        /// </para>
        /// <para>
        /// The returned paths are derived from <see cref="MetadataTable.Spec"/> and are intended to support follow-up cleanup
        /// (e.g., deleting the map file and the spec sidecar).
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed (via the underlying operations).</exception>
        private (Guid id, MetadataTable table, string? mapPath, string? specPath) OpenEphemeralFromSpec(TableSpec spec, bool createNow)
        {
            var id = RegisterTable(spec, createNow: createNow);
            var table = GetOrCreate(id);

            var mapPath = table.Spec.MapPath;
            var specPath = mapPath + ".tablespec.json";

            return (id, table, mapPath, specPath);
        }

        /// <summary>
        /// Finalizes an ephemeral table cleanup using either strict or best-effort semantics.
        /// </summary>
        /// <param name="deleteNow">Whether deletion of the backing files should be attempted immediately.</param>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, cleanup is strict and any deletion failure is surfaced to the caller.
        /// When <see langword="false"/>, cleanup is best-effort and failures are logged (if a logger is available).
        /// </param>
        /// <param name="id">The identifier of the ephemeral table registration.</param>
        /// <param name="mapPath">The table backing map path, if known.</param>
        /// <param name="specPath">The spec sidecar path, if known.</param>
        /// <remarks>
        /// <para>
        /// This method delegates the actual cleanup to <c>CleanupEphemeral</c> and then decides how to observe its completion:
        /// </para>
        /// <list type="bullet">
        ///   <item><description><b>Strict:</b> synchronously blocks on completion via <see cref="System.Runtime.CompilerServices.TaskAwaiter.GetResult"/>.</description></item>
        ///   <item><description><b>Best-effort:</b> if not already completed successfully, runs completion in the background and logs failures.</description></item>
        /// </list>
        /// <para>
        /// Designed for use in <c>finally</c> blocks where you may want cleanup not to mask the original exception unless explicitly requested.
        /// </para>
        /// </remarks>
        /// <exception cref="Exception">
        /// Propagates cleanup failures when <paramref name="throwIfDeleteFails"/> is <see langword="true"/>.
        /// </exception>
        private void FinalizeEphemeral(bool deleteNow, bool throwIfDeleteFails, Guid id, string? mapPath, string? specPath)
        {
            var vt = CleanupEphemeral(deleteNow, throwIfDeleteFails, id, mapPath, specPath);

            if (vt.IsCompletedSuccessfully)
            {
                if (throwIfDeleteFails)
                    vt.GetAwaiter().GetResult();
                return;
            }

            var t = vt.AsTask();

            if (throwIfDeleteFails) t.GetAwaiter().GetResult(); // strict
            else t.Forget(_log); // best-effort
        }

        /// <summary>
        /// Asynchronously finalizes an ephemeral table cleanup using either strict or best-effort semantics.
        /// </summary>
        /// <param name="deleteNow">Whether deletion of the backing files should be attempted immediately.</param>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, cleanup is strict and any deletion failure is surfaced to the caller.
        /// When <see langword="false"/>, cleanup is best-effort and failures are logged (if a logger is available).
        /// </param>
        /// <param name="id">The identifier of the ephemeral table registration.</param>
        /// <param name="mapPath">The table backing map path, if known.</param>
        /// <param name="specPath">The spec sidecar path, if known.</param>
        /// <returns>A task representing the asynchronous finalize operation.</returns>
        /// <remarks>
        /// <para>
        /// This is the async counterpart to <see cref="FinalizeEphemeral(bool, bool, Guid, string?, string?)"/>:
        /// it delegates cleanup to <c>CleanupEphemeral</c> and then either awaits strictly or schedules best-effort completion.
        /// </para>
        /// </remarks>
        /// <exception cref="Exception">
        /// Propagates cleanup failures when <paramref name="throwIfDeleteFails"/> is <see langword="true"/>.
        /// </exception>
        private async Task FinalizeEphemeralAsync(bool deleteNow, bool throwIfDeleteFails, Guid id, string? mapPath, string? specPath)
        {
            var vt = CleanupEphemeral(deleteNow, throwIfDeleteFails, id, mapPath, specPath);

            if (vt.IsCompletedSuccessfully)
            {
                if (throwIfDeleteFails)
                    await vt.ConfigureAwait(false);
                return;
            }

            var t = vt.AsTask();
            if (throwIfDeleteFails) await t.ConfigureAwait(false);
            else t.Forget(_log);
        }

        /// <summary>
        /// Registers and opens an ephemeral table from <paramref name="spec"/>, executes <paramref name="action"/>,
        /// and then finalizes the ephemeral resources.
        /// </summary>
        /// <param name="spec">The table specification used to register and open the ephemeral table.</param>
        /// <param name="action">Callback executed with the ephemeral table id and the opened <see cref="MetadataTable"/>.</param>
        /// <param name="createNow">
        /// When <see langword="true"/>, forces immediate creation/materialization during registration (implementation-defined).
        /// </param>
        /// <param name="deleteNow">
        /// When <see langword="true"/>, attempts to delete the backing files as part of finalization (implementation-defined).
        /// </param>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, finalization is strict and deletion failures are propagated to the caller.
        /// When <see langword="false"/>, finalization is best-effort and deletion failures are logged when possible.
        /// </param>
        /// <remarks>
        /// <para>
        /// This method provides an ephemeral usage scope based on a <see cref="TableSpec"/>:
        /// it opens the table via <see cref="OpenEphemeralFromSpec(TableSpec, bool)"/>, invokes <paramref name="action"/>,
        /// and then finalizes via <see cref="FinalizeEphemeral(bool, bool, Guid, string?, string?)"/> in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// If <paramref name="throwIfDeleteFails"/> is enabled, finalization errors may mask an exception thrown by
        /// <paramref name="action"/> (by design).
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
        public void WithTableEphemeral(TableSpec spec, Action<Guid, MetadataTable> action, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(action);

            Guid id = Guid.Empty;
            string? mapPath = null, specPath = null;

            try
            {
                (id, var table, mapPath, specPath) = OpenEphemeralFromSpec(spec, createNow);
                action(id, table);
            }
            finally
            {
                FinalizeEphemeral(deleteNow, throwIfDeleteFails, id, mapPath, specPath);
            }
        }

        /// <summary>
        /// Registers and opens an ephemeral table from <paramref name="spec"/>, executes <paramref name="func"/>,
        /// and then finalizes the ephemeral resources, returning the callback result.
        /// </summary>
        /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
        /// <param name="spec">The table specification used to register and open the ephemeral table.</param>
        /// <param name="func">Callback executed with the ephemeral table id and the opened <see cref="MetadataTable"/>.</param>
        /// <param name="createNow">
        /// When <see langword="true"/>, forces immediate creation/materialization during registration (implementation-defined).
        /// </param>
        /// <param name="deleteNow">
        /// When <see langword="true"/>, attempts to delete the backing files as part of finalization (implementation-defined).
        /// </param>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, finalization is strict and deletion failures are propagated to the caller.
        /// When <see langword="false"/>, finalization is best-effort and deletion failures are logged when possible.
        /// </param>
        /// <returns>The value produced by <paramref name="func"/>.</returns>
        /// <remarks>
        /// <para>
        /// This method provides an ephemeral usage scope based on a <see cref="TableSpec"/>:
        /// it opens the table via <see cref="OpenEphemeralFromSpec(TableSpec, bool)"/>, invokes <paramref name="func"/>,
        /// and then finalizes via <see cref="FinalizeEphemeral(bool, bool, Guid, string?, string?)"/> in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// If <paramref name="throwIfDeleteFails"/> is enabled, finalization errors may mask an exception thrown by
        /// <paramref name="func"/> (by design).
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec"/> or <paramref name="func"/> is <see langword="null"/>.</exception>
        public TResult WithTableEphemeral<TResult>(TableSpec spec, Func<Guid, MetadataTable, TResult> func, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(func);

            Guid id = Guid.Empty;
            string? mapPath = null, specPath = null;

            try
            {
                (id, var table, mapPath, specPath) = OpenEphemeralFromSpec(spec, createNow);
                return func(id, table);
            }
            finally
            {
                FinalizeEphemeral(deleteNow, throwIfDeleteFails, id, mapPath, specPath);
            }
        }

        /// <summary>
        /// Asynchronously registers and opens an ephemeral table from <paramref name="spec"/>, awaits <paramref name="func"/>,
        /// and then finalizes the ephemeral resources.
        /// </summary>
        /// <param name="spec">The table specification used to register and open the ephemeral table.</param>
        /// <param name="func">Async callback executed with the ephemeral table id and the opened <see cref="MetadataTable"/>.</param>
        /// <param name="createNow">
        /// When <see langword="true"/>, forces immediate creation/materialization during registration (implementation-defined).
        /// </param>
        /// <param name="deleteNow">
        /// When <see langword="true"/>, attempts to delete the backing files as part of finalization (implementation-defined).
        /// </param>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, finalization is strict and deletion failures are propagated to the caller.
        /// When <see langword="false"/>, finalization is best-effort and deletion failures are logged when possible.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// <para>
        /// This is the async counterpart to <see cref="WithTableEphemeral(TableSpec, Action{Guid, MetadataTable}, bool, bool, bool)"/>.
        /// It opens the table via <see cref="OpenEphemeralFromSpec(TableSpec, bool)"/>, awaits <paramref name="func"/>,
        /// and then finalizes via <see cref="FinalizeEphemeralAsync(bool, bool, Guid, string?, string?)"/> in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// If <paramref name="throwIfDeleteFails"/> is enabled, finalization errors may mask an exception thrown by
        /// <paramref name="func"/> (by design).
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec"/> or <paramref name="func"/> is <see langword="null"/>.</exception>
        public async Task WithTableEphemeralAsync(TableSpec spec, Func<Guid, MetadataTable, Task> func, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(func);

            Guid id = Guid.Empty;
            string? mapPath = null, specPath = null;

            try
            {
                (id, var table, mapPath, specPath) = OpenEphemeralFromSpec(spec, createNow);
                await func(id, table).ConfigureAwait(false);
            }
            finally
            {
                await FinalizeEphemeralAsync(deleteNow, throwIfDeleteFails, id, mapPath, specPath).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Asynchronously registers and opens an ephemeral table from <paramref name="spec"/>, awaits <paramref name="func"/>,
        /// and then finalizes the ephemeral resources, returning the callback result.
        /// </summary>
        /// <typeparam name="TResult">The result type returned by <paramref name="func"/>.</typeparam>
        /// <param name="spec">The table specification used to register and open the ephemeral table.</param>
        /// <param name="func">Async callback executed with the ephemeral table id and the opened <see cref="MetadataTable"/>.</param>
        /// <param name="createNow">
        /// When <see langword="true"/>, forces immediate creation/materialization during registration (implementation-defined).
        /// </param>
        /// <param name="deleteNow">
        /// When <see langword="true"/>, attempts to delete the backing files as part of finalization (implementation-defined).
        /// </param>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, finalization is strict and deletion failures are propagated to the caller.
        /// When <see langword="false"/>, finalization is best-effort and deletion failures are logged when possible.
        /// </param>
        /// <returns>A task producing the value returned by <paramref name="func"/>.</returns>
        /// <remarks>
        /// <para>
        /// This is the async counterpart to <see cref="WithTableEphemeral{TResult}(TableSpec, Func{Guid, MetadataTable, TResult}, bool, bool, bool)"/>.
        /// It opens the table via <see cref="OpenEphemeralFromSpec(TableSpec, bool)"/>, awaits <paramref name="func"/>,
        /// and then finalizes via <see cref="FinalizeEphemeralAsync(bool, bool, Guid, string?, string?)"/> in a <c>finally</c> block.
        /// </para>
        /// <para>
        /// If <paramref name="throwIfDeleteFails"/> is enabled, finalization errors may mask an exception thrown by
        /// <paramref name="func"/> (by design).
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">Thrown if this manager has been disposed.</exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec"/> or <paramref name="func"/> is <see langword="null"/>.</exception>
        public async Task<TResult> WithTableEphemeralAsync<TResult>(TableSpec spec, Func<Guid, MetadataTable, Task<TResult>> func, bool createNow = true, bool deleteNow = false, bool throwIfDeleteFails = false)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(func);

            Guid id = Guid.Empty;
            string? mapPath = null, specPath = null;

            try
            {
                (id, var table, mapPath, specPath) = OpenEphemeralFromSpec(spec, createNow);
                return await func(id, table).ConfigureAwait(false);
            }
            finally
            {
                await FinalizeEphemeralAsync(deleteNow, throwIfDeleteFails, id, mapPath, specPath).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Performs best-effort cleanup for an ephemeral table scope: closes/unregisters the table and,
        /// optionally, deletes its backing files.
        /// </summary>
        /// <param name="deleteNow">
        /// When <see langword="true"/>, attempts to delete the table backing files after closing
        /// (typically the <c>.meta</c> map file and its adjacent <c>.tablespec.json</c>).
        /// </param>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, throws an <see cref="IOException"/> if deletion was requested
        /// but could not be completed. When <see langword="false"/>, deletion failures are logged
        /// and execution continues.
        /// </param>
        /// <param name="id">
        /// The managed table identifier to close. If <see cref="Guid.Empty"/>, the close step is skipped.
        /// </param>
        /// <param name="mapPath">
        /// The map file path for the table. Required when <paramref name="deleteNow"/> is <see langword="true"/>.
        /// </param>
        /// <param name="specPath">
        /// The spec file path (typically <c>{mapPath}.tablespec.json</c>). Required when
        /// <paramref name="deleteNow"/> is <see langword="true"/>.
        /// </param>
        /// <returns>
        /// A <see cref="ValueTask"/> that completes synchronously when no deletion is requested,
        /// or completes asynchronously when deletion is requested.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method is designed for use in <c>finally</c> blocks: it always attempts to close/unregister
        /// the table (best-effort) and only performs asynchronous work when <paramref name="deleteNow"/> is enabled.
        /// </para>
        /// <para>
        /// The close step is best-effort: exceptions thrown by <see cref="Close(Guid)"/> are caught and logged
        /// to avoid masking any primary exception from the calling operation.
        /// </para>
        /// <para>
        /// When deletion is requested, this method delegates to <see cref="CleanupEphemeralDeleteAsync"/>.
        /// A failure indicates the files could not be deleted immediately (e.g., OS locks, antivirus/indexers,
        /// permission issues). In that case, behavior depends on <paramref name="throwIfDeleteFails"/>.
        /// </para>
        /// <para>
        /// If <paramref name="throwIfDeleteFails"/> is <see langword="true"/>, this method may throw from a
        /// <c>finally</c> block by design (useful for tests/tooling strictness).
        /// </para>
        /// </remarks>
        /// <exception cref="IOException">
        /// Thrown when <paramref name="deleteNow"/> is <see langword="true"/>, deletion fails, and
        /// <paramref name="throwIfDeleteFails"/> is <see langword="true"/>.
        /// </exception>
        private ValueTask CleanupEphemeral(bool deleteNow, bool throwIfDeleteFails, Guid id, string? mapPath, string? specPath)
        {
            if (id != Guid.Empty)
            {
                try { Close(id); }
                catch (Exception ex) { _log?.LogError(ex, "WithTableEphemeral cleanup failed closing table (Id={Id})", id); }
            }

            // fast path: nothing to delete (no async)
            if (!deleteNow || string.IsNullOrWhiteSpace(mapPath) || string.IsNullOrWhiteSpace(specPath))
                return ValueTask.CompletedTask;

            // slow path: real async delete
            return CleanupEphemeralDeleteAsync(throwIfDeleteFails, id, mapPath!, specPath!);
        }

        /// <summary>
        /// Performs the asynchronous deletion portion of ephemeral cleanup by attempting to delete the table
        /// backing files immediately (tolerating transient OS locks).
        /// </summary>
        /// <param name="throwIfDeleteFails">
        /// When <see langword="true"/>, throws an <see cref="IOException"/> if the backing files could not be deleted.
        /// When <see langword="false"/>, deletion failures are logged and the method completes successfully.
        /// </param>
        /// <param name="id">Table id used for diagnostics/logging.</param>
        /// <param name="mapPath">Full path to the table map file (e.g., <c>users.meta</c>).</param>
        /// <param name="specPath">Full path to the adjacent spec file (e.g., <c>users.meta.tablespec.json</c>).</param>
        /// <returns>A <see cref="ValueTask"/> that completes when the delete attempt has finished.</returns>
        /// <remarks>
        /// <para>
        /// This method calls <see cref="TryDeleteNow(string, string, Guid)"/> and interprets a <see langword="false"/>
        /// result as "could not delete now". Depending on implementation, the underlying delete routine may enqueue
        /// the paths for eventual deletion by a background worker.
        /// </para>
        /// <para>
        /// This method does not attempt to close/unregister the table; that is handled by
        /// <see cref="CleanupEphemeral(bool, bool, Guid, string?, string?)"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="IOException">
        /// Thrown when deletion fails and <paramref name="throwIfDeleteFails"/> is <see langword="true"/>.
        /// </exception>
        private async ValueTask CleanupEphemeralDeleteAsync(bool throwIfDeleteFails, Guid id, string mapPath, string specPath)
        {
            if (!await TryDeleteNow(mapPath, specPath, id).ConfigureAwait(false))
            {
                if (throwIfDeleteFails)
                    throw new IOException($"deleteNow requested but could not delete table files (Id={id}).");

                _log?.LogError("deleteNow requested but could not delete table files (Id={id}).", id);
            }
        }

        /// <summary>
        /// Attempts to delete the table backing files (<paramref name="mapPath"/> and <paramref name="specPath"/>)
        /// immediately, tolerating transient OS locks.
        /// </summary>
        /// <param name="mapPath">Full path to the table map file (e.g., <c>users.meta</c>).</param>
        /// <param name="specPath">Full path to the adjacent spec file (e.g., <c>users.meta.tablespec.json</c>).</param>
        /// <param name="id">Table id used only for diagnostics/logging.</param>
        /// <returns>
        /// <see langword="true"/> if both files were deleted (or did not exist);
        /// otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Strategy:
        /// </para>
        /// <list type="number">
        ///   <item><description>
        ///   Try direct deletion with small backoff retries for each file.
        ///   </description></item>
        ///   <item><description>
        ///   If deletion fails (typically due to OS-level sharing locks), attempt a move-aside
        ///   rename to a unique <c>.deleting.&lt;guid&gt;</c> name and retry deleting the moved files.
        ///   </description></item>
        ///   <item><description>
        ///   If move-aside or second delete still fails, enqueue the original paths for later cleanup
        ///   (best-effort / eventual consistency).
        ///   </description></item>
        /// </list>
        /// <para>
        /// The move-aside step is useful because it decouples the logical "file should be gone" intent
        /// from the physical deletion timing: once renamed, future code won't re-open the file by its
        /// original name, and a later cleanup pass can delete the renamed file when the OS lock is released.
        /// </para>
        /// <para>
        /// Note: move-aside itself can fail for the same reason as delete (sharing violations) or due to
        /// permissions/ACLs. This method logs and falls back to enqueueing in those cases.
        /// </para>
        /// </remarks>
        private async Task<bool> TryDeleteNow(string mapPath, string specPath, Guid id)
        {
            // Try delete. If locked, move-aside and retry delete. If still locked, enqueue.
            bool ok = await MetaDBManagerHelpers.TryDeleteWithRetries(mapPath) & await MetaDBManagerHelpers.TryDeleteWithRetries(specPath); // bitwise AND to ensure both are attempted
            if (ok) return true;

            string toEnqueue1 = mapPath;
            string toEnqueue2 = specPath;
            try
            {
                var movedMap = MetaDBManagerHelpers.TryMoveAside(mapPath);
                var movedSpec = MetaDBManagerHelpers.TryMoveAside(specPath);

                bool ok2 = true;
                if (movedMap is not null) ok2 &= await MetaDBManagerHelpers.TryDeleteWithRetries(movedMap);
                if (movedSpec is not null) ok2 &= await MetaDBManagerHelpers.TryDeleteWithRetries(movedSpec);

                if (ok2) return true;

                toEnqueue1 = movedMap ?? mapPath;
                toEnqueue2 = movedSpec ?? specPath;
            }
            catch (Exception ex)
            {
                _log?.LogWarning(ex, "Move-aside delete failed (Id={Id})", id);
            }

            // Last resort: enqueue for later cleanup
            _log?.LogWarning("Could not delete table files now; enqueueing cleanup (Id={Id}, Map={MapPath})", id, mapPath);
            EnqueueDelete(toEnqueue1);
            EnqueueDelete(toEnqueue2);
            return false;
        }

        /// <summary>
        /// Enqueues a file path for later deletion attempts.
        /// </summary>
        /// <param name="path">Absolute or relative file path to delete later.</param>
        /// <remarks>
        /// <para>
        /// This is a last-resort mechanism for cases where the OS has not released file locks yet
        /// (e.g., memory-mapped files, antivirus scanners, delayed handle finalization).
        /// The path is tracked in an in-memory, per-instance set and processed by a background
        /// delete worker owned by this <see cref="MetaDBManager"/> instance.
        /// </para>
        /// <para>
        /// The operation is <b>idempotent</b>: the same path may be enqueued multiple times by callers,
        /// but only the first enqueue is recorded. Deletion treats "file not found" as success.
        /// </para>
        /// <para>
        /// The enqueue is also <b>durable</b>: the path is best-effort appended to an on-disk queue file
        /// (see <c>_deleteQueuePath</c>) so pending deletions can be retried after process restarts.
        /// If persistence fails, the item still remains queued in-memory for this process lifetime.
        /// </para>
        /// <para>
        /// This method never throws for persistence failures; it logs a warning and returns.
        /// The background worker is responsible for retries and eventual compaction of the queue file.
        /// </para>
        /// </remarks>
        private void EnqueueDelete(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return;

            // browser / disabled persistence
            if (string.IsNullOrWhiteSpace(_deleteQueuePath))
            {
                _pendingDeletes.TryAdd(path, 0);
                return;
            }

            // Idempotent: only enqueue once per path.
            if (!_pendingDeletes.TryAdd(path, 0))
                return;

            // Persist immediately (append-only, best-effort).
            try
            {
                MetaDBManagerHelpers.EnsureDirForFile(_deleteQueuePath);
                lock (_deleteFileLock)
                    File.AppendAllText(_deleteQueuePath, path + Environment.NewLine);
            }
            catch (Exception ex)
            {
                _log?.LogWarning(ex, "Failed to persist delete queue entry (Path={Path})", path);
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
            ThrowIfDisposed();

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
            if (rows != 0) MetaDBManagerHelpers.FillColumn(m.Table, column, rows, factory, effectivePolicy, batchSize);

            if (_isLogActivated)
                Log.FillColumnEnd(_log!, tableName, typeName, sw?.Elapsed.TotalMilliseconds ?? 0);
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

            if (rows != 0) MetaDBManagerHelpers.FillColumn(m.Table, column, rows, writer, effectivePolicy, batchSize);

            if (_isLogActivated)
                Log.FillRawEnd(_log!, tableName, sw?.Elapsed.TotalMilliseconds ?? 0);
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

            if (rows == 0) return (m, null, effectivePolicy, 0);

            uint valueSize = MetaDBManagerHelpers.GetColumnValueSize(m.Table, column);
            int batchSize = MetaDBManagerHelpers.ComputeBatchFromValueSize(valueSize);

            if ((uint)batchSize > rows) batchSize = (int)rows;

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

            // Will throw if value sizes differ per row; that exception is the signal.
            MetaDBManagerHelpers.CopyColumn(s.Table, srcCol, d.Table, dstCol, rows, effectivePolicy, MetaDBManagerHelpers.ComputeBatchFromValueSize(valueSize));

            if (_isLogActivated) Log.CopyEnd(_log!, sName, dName, sw!.Elapsed.TotalMilliseconds);
        }

        // ─────────────────────────────────────────────────────────────────────
        // Refs (padres → hijos)
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Ensures that the parent row at (<paramref name="refsCol"/>, <paramref name="parentRow"/>)
        /// has a reference-vector header allocated and initialized. This method is idempotent.
        /// </summary>
        /// <param name="parentTableId">
        /// Registered parent table identifier. If unknown, a <see cref="KeyNotFoundException"/> is thrown.
        /// </param>
        /// <param name="refsCol">Zero-based index of the refs column in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index whose refs vector must exist.</param>
        /// <param name="policy">
        /// Capacity policy. When <see cref="CapacityPolicy.Throw"/>, the operation fails fast if the
        /// destination row is out of range. When <see cref="CapacityPolicy.AutoGrowZeroInit"/>,
        /// the underlying implementation may attempt to grow capacity before initializing.
        /// </param>
        /// <remarks>
        /// <para>
        /// This method delegates to <see cref="MetaDBManagerHelpers.EnsureRefVec"/> and emits the
        /// <c>EnsureRefVecInit</c> log event only when the vector header is created for the first time
        /// (i.e., when the current ref count is zero and initialization occurs).
        /// </para>
        /// <para>
        /// If the cell VALUE buffer is too small to hold the reference-vector metadata
        /// (for example: header + at least one reference slot), an <see cref="InvalidOperationException"/> is thrown.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentException">Thrown if <paramref name="parentTableId"/> is <see cref="Guid.Empty"/>.</exception>
        /// <exception cref="KeyNotFoundException">Thrown if the table id is not registered.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when capacity is insufficient for the selected <paramref name="policy"/>, or when the cell VALUE buffer
        /// cannot host the refs-vector metadata.
        /// </exception>
        public void EnsureRefVec(Guid parentTableId, uint refsCol, uint parentRow, CapacityPolicy policy = CapacityPolicy.None)
        {
            var p = Require(parentTableId);
            var effective = policy == CapacityPolicy.None ? _capacityPolicy : policy;
            var didInit = MetaDBManagerHelpers.EnsureRefVec(p.Table, refsCol, parentRow, effective);
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
            MetaDBManagerHelpers.EnsureRefVec(p.Table, refsCol, parentRow, effective);

            // Append the reference
            var tref = new MetadataTableRef
            (
                tableId: childTableId,
                column: childCol,
                row: childRow,
                reserved: 0
            );

            if (MetaDBManagerHelpers.TryHasRef(p.Table, refsCol, parentRow, in tref))
                return;

            MetaDBManagerHelpers.LinkRef(p.Table, refsCol, parentRow, in tref);

            if (_isLogActivated)
                Log.LinkRefAdded(_log!, p.Name ?? string.Empty, parentRow, childTableId, childCol, childRow);
        }

        /// <summary>
        /// Gets (or creates) a child table for the given (<paramref name="parentTableId"/>, <paramref name="parentRow"/>, <paramref name="childKey"/>)
        /// and links it into the parent's refs vector. If a child with the same key already exists for that row, it is reused,
        /// and the link is ensured without duplicates.
        /// </summary>
        /// <param name="parentTableId">Registered parent table identifier.</param>
        /// <param name="refsCol">Zero-based index of the refs column in the parent table.</param>
        /// <param name="parentRow">Zero-based parent row index.</param>
        /// <param name="childKey">Stable key used to distinguish multiple children within the same parent row (stored in <see cref="MetadataTableRef.Reserved"/>).</param>
        /// <param name="childSpecFactory">Factory that receives <paramref name="parentRow"/> and returns the child's <see cref="TableSpec"/>.</param>
        /// <param name="childCol">Child column the reference should point to (default 0).</param>
        /// <param name="childRow">Child row the reference should point to (default 0).</param>
        /// <returns>The <see cref="Guid"/> of the reused or newly created child.</returns>
        /// <remarks>
        /// This method uses <see cref="MetadataRefLinker"/> to ensure the refs vector exists and to enforce idempotent linking.
        /// Uniqueness is enforced by <paramref name="childKey"/> (not by table/col/row triple).
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="childSpecFactory"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown if the refs vector buffer is too small or full.</exception>
        public unsafe Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, uint childKey, Func<uint, TableSpec> childSpecFactory, uint childCol = 0, uint childRow = 0)
        {
            ArgumentNullException.ThrowIfNull(childSpecFactory);

            var p = Require(parentTableId);

            if (MetadataRefLinker.TryFindChildByKey(p.Table, refsCol, parentRow, childKey, out var existing))
            {
                MetadataRefLinker.EnsureLinkRefNoDupByKey(p.Table, refsCol, parentRow, existing, childCol, childRow, childKey, _capacityPolicy);

                if (_isLogActivated)
                    Log.ChildReused(_log!, p.Name ?? string.Empty, parentRow, existing);

                return existing;
            }

            var spec = childSpecFactory(parentRow);
            var childId = RegisterTable(spec, createNow: true);

            MetadataRefLinker.EnsureLinkRefNoDupByKey(p.Table, refsCol, parentRow, childId, childCol, childRow, childKey, _capacityPolicy);

            if (_isLogActivated)
                Log.ChildCreatedLinked(_log!, p.Name ?? string.Empty, parentRow, spec.Name, childId);

            return childId;
        }

        /// <summary>
        /// Backward-compatible overload that uses <c>childKey = 0</c> (single child semantics per row unless you pass keys explicitly).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Guid GetOrCreateAndLinkChild(Guid parentTableId, uint refsCol, uint parentRow, Func<uint, TableSpec> childSpecFactory, uint childCol = 0, uint childRow = 0) => GetOrCreateAndLinkChild(parentTableId, refsCol, parentRow, 0u, childSpecFactory, childCol, childRow);

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
        public void Run(string operationName, Action<IMetaDBManager> action, object? state = null) =>
            RunCore(operationName, (mgr) =>
            {
                action(mgr);
                return Task.CompletedTask;
            }, state).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous version of <see cref="Run(string, Action{IMetaDBManager}, object?)"/>.
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
        public async Task RunAsync(string operationName, Func<IMetaDBManager, Task> action, object? state = null) =>
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
        public void RunWithReindexAll(string operationName, Action<IMetaDBManager> action, object? state = null, bool includeGlobal = true) =>
            Run(operationName, mgr =>
            {
                action(mgr);
                mgr.RebuildAllIndexes(includeGlobal);
            }, state);

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
        public async Task RunWithReindexAllAsync(string operationName, Func<IMetaDBManager, Task> action, object? state = null, bool includeGlobal = true) =>
            await RunCore(operationName, async mgr =>
            {
                await action(mgr).ConfigureAwait(false);
                mgr.RebuildAllIndexes(includeGlobal);
            }, state).ConfigureAwait(false);

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
        public void RunWithReindexTable(string operationName, Guid tableId, Action<IMetaDBManager> action, object? state, bool includeGlobal = true) =>
            Run(operationName, mgr =>
            {
                action(mgr);
                mgr.RebuildIndexes(tableId, includeGlobal);
            }, state);

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
        public async Task RunWithReindexTableAsync(string operationName, Guid tableId, Func<IMetaDBManager, Task> action, object? state, bool includeGlobal = true) =>
            await RunCore(operationName, async mgr =>
            {
                await action(mgr).ConfigureAwait(false);
                mgr.RebuildIndexes(tableId, includeGlobal);
            }, state).ConfigureAwait(false);

        private async Task RunCore(string operationName, Func<IMetaDBManager, Task> action, object? state = null)
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
        public void RebuildAllIndexes(bool includeGlobal = true)
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
        /// Throws an <see cref="ObjectDisposedException"/> when this <see cref="MetaDBManager"/> instance
        /// has already been disposed.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is intended for hot-path guards (e.g., <c>Require</c>) where operating on a disposed
        /// manager would otherwise produce confusing downstream failures.
        /// </para>
        /// <para>
        /// The disposed state is read using <see cref="Volatile.Read(ref int)"/> to ensure correct visibility
        /// across threads. The check is aggressively inlined to minimize overhead.
        /// </para>
        /// </remarks>
        /// <exception cref="ObjectDisposedException">
        /// Thrown when this instance has already been disposed.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, nameof(MetaDBManager));

        /// <remarks>
        /// This method performs a synchronous shutdown by blocking on <see cref="DisposeAsync"/>.
        /// When this method returns, all resources have been released.
        /// </remarks>
        public void Dispose()
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Asynchronously releases all resources used by this <see cref="MetaDBManager"/>,
        /// stops the background delete worker (if active) and closes any materialized tables.
        /// </summary>
        /// <returns>A <see cref="ValueTask"/> that completes when disposal has finished.</returns>
        /// <remarks>
        /// <para>
        /// This method is idempotent: subsequent calls after the first successful disposal
        /// return immediately.
        /// </para>
        /// <para>
        /// The shutdown sequence is:
        /// </para>
        /// <list type="number">
        ///   <item><description>Close all registered tables (best-effort).</description></item>
        ///   <item><description>Clear registries and release hooks.</description></item>
        ///   <item><description>Cancel the delete worker and await its completion.</description></item>
        /// </list>
        /// <para>
        /// Any worker failures are logged at debug level and swallowed to avoid throwing from disposal.
        /// </para>
        /// </remarks>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

            await _deleteWorkerGate.WaitAsync().ConfigureAwait(false); // Prevent races with the DeleteWorker
            try
            {
                var temp = _byId.Keys.ToArray();
                foreach (var id in temp)
                {
                    try { Close(id); }
                    catch (Exception ex) { _log?.LogDebug(ex, "Close failed during DisposeAsync (Id={Id})", id); }
                }

                // Stop worker nicely (best-effort)
                await CancelAndWaitForWorkerEnd().ConfigureAwait(false);
            }
            finally
            {
                _pendingDeletes.Clear();
                _byName.Clear();
                _byId.Clear();

                try { _deleteCts?.Dispose(); } catch { /* best-effort */ }
                _deleteCts = null;
                _deleteWorkerTask = Task.CompletedTask;

                if (_isLogActivated) _log?.LogDebug("MetaDBManager disposed.");

                _deleteWorkerGate.Release();
                GC.SuppressFinalize(this);
            }
        }
    }
}