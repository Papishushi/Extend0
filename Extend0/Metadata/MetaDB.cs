using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Microsoft.Extensions.Logging;

namespace Extend0.Metadata
{
    /// <summary>
    /// Public entry surface for the Extend0 MetaDB system.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This type exposes the two primary access patterns for MetaDB:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><see cref="CreateManager"/> for same-process/local usage.</description></item>
    ///   <item><description><see cref="CreateSingleton"/> for shared owner/client usage across processes.</description></item>
    /// </list>
    /// <para>
    /// The underlying concrete manager implementation remains internal; callers interact through
    /// stable public contracts and access surfaces.
    /// </para>
    /// </remarks>
    public static class MetaDB
    {
        /// <summary>
        /// Creates a same-process MetaDB manager that exposes the full local <see cref="IMetaDBManager"/> contract.
        /// </summary>
        /// <param name="logger">Optional logger used for diagnostics and structured operation traces.</param>
        /// <param name="factory">Optional metadata table factory used to create concrete table instances.</param>
        /// <param name="capacityPolicy">Default capacity policy used when operations require growth.</param>
        /// <param name="deleteQueuePath">Optional persisted delete-queue path used by the background delete worker.</param>
        /// <returns>A new local <see cref="IMetaDBManager"/> instance.</returns>
        public static IMetaDBManager CreateManager(
            ILogger? logger = null,
            Func<TableSpec?, IMetadataTable>? factory = null,
            CapacityPolicy capacityPolicy = CapacityPolicy.Throw,
            string? deleteQueuePath = null) =>
            new MetaDBManager(logger, factory, capacityPolicy, deleteQueuePath);

        /// <summary>
        /// Creates a singleton-backed MetaDB access surface that resolves to the local owner or a cross-process client proxy.
        /// </summary>
        /// <param name="loggerFactory">Optional logger factory used by the host/client infrastructure and hosted manager.</param>
        /// <param name="factory">Optional metadata table factory used by the hosted manager.</param>
        /// <param name="capacityPolicy">Default capacity policy used by the hosted manager.</param>
        /// <param name="deleteQueuePath">Optional persisted delete-queue path used by the hosted manager.</param>
        /// <param name="crossProcessServer">Server or machine name used to reach the active owner.</param>
        /// <param name="connectTimeoutMs">Connection timeout in milliseconds for client attachment.</param>
        /// <param name="overwrite">Whether the singleton may replace a stale prior owner.</param>
        /// <returns>A new <see cref="MetaDBManagerSingleton"/> instance.</returns>
        public static MetaDBManagerSingleton CreateSingleton(
            ILoggerFactory? loggerFactory = null,
            Func<TableSpec?, IMetadataTable>? factory = null,
            CapacityPolicy capacityPolicy = CapacityPolicy.Throw,
            string? deleteQueuePath = null,
            string crossProcessServer = ".",
            int connectTimeoutMs = 5000,
            bool overwrite = false) =>
            new(loggerFactory, factory, capacityPolicy, deleteQueuePath, crossProcessServer, connectTimeoutMs, overwrite);
    }
}
