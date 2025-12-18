using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.Schema;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Extend0.Metadata.CrossProcess
{
    /// <summary>
    /// Cross-process singleton host/client wrapper for <see cref="IMetaDBManagerRCPCompatible"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This type provides a single, stable access point to MetaDB across process boundaries by
    /// leveraging <see cref="CrossProcessSingleton{T}"/>. The first process that instantiates the
    /// singleton becomes the <em>owner</em> (server) and hosts the RPC endpoint; subsequent processes
    /// connect as clients and obtain a proxy to <see cref="IMetaDBManagerRCPCompatible"/>.
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
    public sealed class MetaDBManagerSingleton : CrossProcessSingleton<IMetaDBManagerRCPCompatible>
    {
        /// <summary>
        /// The cross-process name used to identify the MetaDB singleton endpoint.
        /// </summary>
        /// <remarks>
        /// This matches the pipe name used by <see cref="MetaDBManagerRCPCompatible"/> so all processes
        /// connect to the same IPC rendezvous point.
        /// </remarks>
        private const string NAME = "Extend0.MetaDB";

        /// <summary>
        /// Last factory delegate used in the current process, stored so the upgrade handler can reuse it.
        /// </summary>
        private static Func<TableSpec?, MetadataTable>? s_lastFactory;

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
        public static ILoggerFactory? Logging { get; set; }

        /// <summary>
        /// Initializes a new cross-process singleton instance for MetaDB.
        /// </summary>
        /// <param name="logger">
        /// Logger used by the cross-process host/client infrastructure and passed to the hosted
        /// <see cref="MetaDBManagerRCPCompatible"/> instance when this process becomes owner.
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
            ILogger? logger,
            Func<TableSpec?, MetadataTable>? factory = null,
            CapacityPolicy capacityPolicy = CapacityPolicy.Throw,
            string? deleteQueuePath = null,
            string crossProcessServer = ".",
            int connectTimeoutMs = 5000,
            bool overwrite = true)
            : base(
                () => new MetaDBManagerRCPCompatible(
                    logger,
                    factory,
                    capacityPolicy,
                    deleteQueuePath),
                new()
                {
                    Mode = SingletonMode.CrossProcess,
                    CrossProcessName = NAME,
                    CrossProcessServer = crossProcessServer,
                    CrossProcessConnectTimeoutMs = connectTimeoutMs,
                    Overwrite = overwrite,
                    Logger = logger
                })
        {
            // Persist args for upgrade recreation (best-effort; delegates are process-local).
            Volatile.Write(ref s_lastFactory, factory);
            s_lastCapacityPolicy = capacityPolicy;
            s_lastDeleteQueuePath = deleteQueuePath;
            s_lastServer = crossProcessServer;
            s_lastTimeoutMs = connectTimeoutMs;
            s_lastOverwrite = overwrite;
        }

        /// <summary>
        /// Static initializer that wires the upgrade handler used by the cross-process dispatch proxy.
        /// </summary>
        /// <remarks>
        /// The handler attempts to recreate the singleton in the current process so that if the owner
        /// process restarts, callers can transparently reconnect on the next invocation.
        /// </remarks>
        static MetaDBManagerSingleton()
        {
            RpcDispatchProxy<IMetaDBManagerRCPCompatible>.UpgradeHandler = static async ex =>
            {
                var lf = Logging; // capture once (process-local)
                var logger = lf?.CreateLogger<MetaDBManagerSingleton>() ?? NullLogger<MetaDBManagerSingleton>.Instance;

                try
                {
                    var factory = Volatile.Read(ref s_lastFactory);

                    // Re-create singleton when the owner restarts.
                    _ = new MetaDBManagerSingleton(
                        logger,
                        factory,
                        s_lastCapacityPolicy,
                        s_lastDeleteQueuePath,
                        s_lastServer,
                        s_lastTimeoutMs,
                        s_lastOverwrite);

                    Console.WriteLine("[Upgrade] Recreated MetaDBManagerSingleton. IsOwner = {0}", IsOwner);

                    await Task.Yield();
                    return true;
                }
                catch (Exception e)
                {
                    Console.WriteLine("[Upgrade] Failed to recreate MetaDBManagerSingleton: {0}", e);
                    return false;
                }
            };
        }
    }
}
