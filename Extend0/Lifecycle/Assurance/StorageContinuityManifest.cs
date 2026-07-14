using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Provider or operator-owned manifest used as portable storage continuity evidence.
/// </summary>
public sealed record StorageContinuityManifest
{
    /// <summary>
    /// Current manifest schema version.
    /// </summary>
    public const int CurrentVersion = 1;

    /// <summary>
    /// Gets the manifest schema version.
    /// </summary>
    public int Version { get; init; } = CurrentVersion;

    /// <summary>
    /// Gets the storage-continuity provider identifier.
    /// </summary>
    public string ProviderId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the optional provider implementation version.
    /// </summary>
    public string? ProviderVersion { get; init; }

    /// <summary>
    /// Gets the provider-scoped continuity identifier.
    /// </summary>
    public string ContinuityId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the storage-continuity level declared by this manifest.
    /// </summary>
    public StorageContinuityLevel ContinuityLevel { get; init; } = StorageContinuityLevel.LocalOnly;

    /// <summary>
    /// Root directory of the shared or replicated storage scope. When omitted, the manifest directory is used.
    /// </summary>
    public string? RootPath { get; init; }

    /// <summary>
    /// Gets the topology identifier associated with shared or replicated storage.
    /// </summary>
    public string? TopologyId { get; init; }

    /// <summary>
    /// Gets the UTC timestamp at which the manifest was created.
    /// </summary>
    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets an optional human-readable description of the continuity scope.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Gets the source from which this evidence was loaded or produced.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvidenceSource { get; init; }

    /// <summary>
    /// Creates a storage-continuity manifest with the current schema version.
    /// </summary>
    public static StorageContinuityManifest Create(
        string providerId,
        string continuityId,
        StorageContinuityLevel continuityLevel,
        string? rootPath = null,
        string? providerVersion = null,
        string? topologyId = null,
        string? description = null) =>
        new()
        {
            ProviderId = providerId,
            ContinuityId = continuityId,
            ContinuityLevel = continuityLevel,
            RootPath = rootPath,
            ProviderVersion = providerVersion,
            TopologyId = topologyId,
            Description = description,
            EvidenceSource = "manifest"
        };
}
