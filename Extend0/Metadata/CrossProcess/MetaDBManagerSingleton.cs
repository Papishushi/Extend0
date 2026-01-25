using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.Contract;
using Extend0.Metadata.CrossProcess.Internal;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Extend0.Metadata.CrossProcess
{
    /// <summary>
    /// Cross-process singleton host/client wrapper for <see cref="IMetaDBManagerRPCCompatible"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This type provides a single, stable access point to MetaDB across process boundaries by
    /// leveraging <see cref="CrossProcessSingleton{T}"/>. The first process that instantiates the
    /// singleton becomes the <em>owner</em> (server) and hosts the RPC endpoint; subsequent processes
    /// connect as clients and obtain a proxy to <see cref="IMetaDBManagerRPCCompatible"/>.
    /// </para>
    ///
    /// <para>
    /// The cross-process endpoint name is intentionally aligned with the service pipe name:
    /// <c>"Extend0.MetaDB"</c>. This ensures that all processes rendezvous on the same IPC channel.
    /// </para>
    ///
    /// <para>
    /// <b>Upgrade behavior</b>:
    /// when the owner process restarts (or the underlying transport reports an upgrade condition),
    /// <see cref="RpcDispatchProxy{T}.UpgradeHandler"/> is used to best-effort recreate the singleton
    /// so clients can transparently reconnect.
    /// </para>
    ///
    /// <para>
    /// <b>Factory note</b>:
    /// the optional <paramref name="factory"/> delegate is captured only within the current process
    /// (delegates cannot be transported cross-process). The upgrade handler reuses the last factory
    /// provided in the same process, when available.
    /// </para>
    /// </remarks>
    public sealed class MetaDBManagerSingleton : CrossProcessSingleton<IMetaDBManagerRPCCompatible>
    {
        /// <summary>
        /// The cross-process name used to identify the MetaDB singleton endpoint.
        /// </summary>
        /// <remarks>
        /// This matches the pipe name used by <see cref="MetaDBManagerRPCCompatible"/> so all processes
        /// connect to the same IPC rendezvous point.
        /// </remarks>
        private const string NAME = "Extend0.MetaDB";

        /// <summary>
        /// Last factory delegate used in the current process, stored so the upgrade handler can reuse it.
        /// </summary>
        private static Func<TableSpec?, IMetadataTable>? s_lastFactory;

        /// <summary>
        /// Last capacity policy used in the current process.
        /// </summary>
        private static CapacityPolicy s_lastCapacityPolicy = CapacityPolicy.Throw;

        /// <summary>
        /// Last delete queue path used in the current process.
        /// </summary>
        private static string? s_lastDeleteQueuePath;

        /// <summary>
        /// Last server name used for cross-process hosting/connection.
        /// </summary>
        private static string s_lastServer = ".";

        /// <summary>
        /// Last connect timeout (milliseconds) used for cross-process connection attempts.
        /// </summary>
        private static int s_lastTimeoutMs = 5000;

        /// <summary>
        /// Last overwrite behavior used when initializing the cross-process singleton.
        /// </summary>
        private static bool s_lastOverwrite = true;

        /// <summary>
        /// Optional logger factory provider for this process.
        /// </summary>
        /// <remarks>
        /// This is process-local. If set by the host application, the upgrade handler will use it to emit
        /// diagnostics when recreating the singleton after a remote/transport upgrade.
        /// When not set, a <see cref="NullLogger"/> is used.
        /// </remarks>
        private static ILoggerFactory? s_lastLogging = null;

        /// <summary>
        /// Initializes a new cross-process singleton instance for MetaDB.
        /// </summary>
        /// <param name="logger">
        /// Logger used by the cross-process host/client infrastructure and passed to the hosted
        /// <see cref="MetaDBManagerRPCCompatible"/> instance when this process becomes owner.
        /// </param>
        /// <param name="factory">
        /// Optional table factory used by the hosted manager. This is process-local and cannot be
        /// transferred across processes; it is stored only so the upgrade handler can reuse it within
        /// the same process.
        /// </param>
        /// <param name="capacityPolicy">
        /// Default capacity policy to use when the hosted manager must decide how to handle capacity
        /// shortfalls (e.g., throw vs. best-effort growth).
        /// </param>
        /// <param name="deleteQueuePath">
        /// Optional persisted delete queue path used by the hosted manager’s delete worker.
        /// </param>
        /// <param name="crossProcessServer">
        /// The server name used by the cross-process infrastructure (typically <c>"."</c> for local).
        /// </param>
        /// <param name="connectTimeoutMs">
        /// Maximum time (milliseconds) to wait while connecting to an existing owner.
        /// </param>
        /// <param name="overwrite">
        /// When <see langword="true"/>, allows taking ownership even if a previous owner endpoint exists
        /// but is stale. Use with care in environments where multiple owners must never exist.
        /// </param>
        public MetaDBManagerSingleton(
            ILoggerFactory? loggerFactory = null,
            Func<TableSpec?, IMetadataTable>? factory = null,
            CapacityPolicy capacityPolicy = CapacityPolicy.Throw,
            string? deleteQueuePath = null,
            string crossProcessServer = ".",
            int connectTimeoutMs = 5000,
            bool overwrite = false)
            : base(
                factory: () => new MetaDBManagerRPCCompatible(
                    loggerFactory?.CreateLogger<MetaDBManagerRPCCompatible>(),
                    factory,
                    capacityPolicy,
                    deleteQueuePath),
                options: new()
                {
                    Mode = SingletonMode.CrossProcess,
                    CrossProcessName = NAME,
                    CrossProcessServer = crossProcessServer,
                    CrossProcessConnectTimeoutMs = connectTimeoutMs,
                    Overwrite = overwrite,
                    Logger = loggerFactory?.CreateLogger<CrossProcessSingletonOptions>()
                },
                loggerFactory: loggerFactory)
        {
            // Persist args for upgrade recreation (best-effort; delegates are process-local).
            Volatile.Write(ref s_lastLogging, loggerFactory);
            Volatile.Write(ref s_lastFactory, factory);
            s_lastCapacityPolicy = capacityPolicy;
            s_lastDeleteQueuePath = deleteQueuePath;
            s_lastServer = crossProcessServer;
            s_lastTimeoutMs = connectTimeoutMs;
            s_lastOverwrite = overwrite;
        }

        /// <summary>
        /// Wires the upgrade handler used by the cross-process dispatch proxy.
        /// The handler attempts to recreate the singleton in the current process so that if the owner
        /// process restarts, callers can transparently reconnect on the next invocation.
        /// </summary>
        static MetaDBManagerSingleton() => RpcDispatchProxy<IMetaDBManagerRPCCompatible>.UpgradeHandler = UpgradeHandler;

        private static int s_upgradeInProgress;

        /// <summary>
        /// Attempts to recover from a remote/transport upgrade condition by recreating the MetaDB cross-process singleton
        /// in the current process.
        /// </summary>
        /// <param name="ex">
        /// The remote invocation exception that triggered the upgrade path (typically indicating the owner process restarted
        /// or the transport endpoint became stale).
        /// </param>
        /// <returns>
        /// <see langword="true"/> when the handler successfully initiated a best-effort recreation of the singleton (or when
        /// a recreation is already in progress); otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This handler is invoked by <see cref="RpcDispatchProxy{T}.UpgradeHandler"/> when an invocation fails with an
        /// upgradeable condition (for example, the owner process restarted).
        /// </para>
        /// <para>
        /// Concurrency: only one recreation attempt is allowed at a time. A process-wide gate is implemented via
        /// <see cref="Interlocked.Exchange(ref int, int)"/> on <c>s_upgradeInProgress</c>. Concurrent calls return
        /// <see langword="true"/> immediately, assuming the in-flight recreation will make subsequent calls succeed.
        /// </para>
        /// <para>
        /// The recreation uses the last process-local configuration values captured when this type was constructed:
        /// <c>s_lastLogging</c>, <c>s_lastFactory</c>, <c>s_lastCapacityPolicy</c>, <c>s_lastDeleteQueuePath</c>,
        /// <c>s_lastServer</c>, <c>s_lastTimeoutMs</c>, and <c>s_lastOverwrite</c>. Delegates are process-local and are
        /// therefore only reusable within the same process.
        /// </para>
        /// <para>
        /// Logging is best-effort. If no logger factory was provided, no diagnostics are emitted.
        /// </para>
        /// <para>
        /// Note: the <paramref name="ex"/> parameter is currently used only to decide that an upgrade path was requested; the
        /// handler does not attempt to classify or rethrow it.
        /// </para>
        /// </remarks>
        private static async ValueTask<bool> UpgradeHandler(RemoteInvocationException ex)
        {
            if (Interlocked.Exchange(ref s_upgradeInProgress, 1) == 1)
                return true;

            // capture once (process-local)
            var lf = Volatile.Read(ref s_lastLogging);
            var log = lf?.CreateLogger<MetaDBManagerSingleton>();
            try
            {
                var factory = Volatile.Read(ref s_lastFactory);

                _ = new MetaDBManagerSingleton(
                    lf, factory, s_lastCapacityPolicy, s_lastDeleteQueuePath,
                    s_lastServer, s_lastTimeoutMs, s_lastOverwrite);

                log?.LogInformation("[Upgrade] Recreated MetaDBManagerSingleton. IsOwner = {IsOwner}", IsOwner);
                await Task.Yield();
                return true;
            }
            catch (Exception e)
            {
                log?.LogError(e, "[Upgrade] Failed to recreate MetaDBManagerSingleton.");
                return false;
            }
            finally
            {
                Volatile.Write(ref s_upgradeInProgress, 0);
            }
        }
    }
}
