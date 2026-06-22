using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Provider or operator-owned manifest used as portable storage continuity evidence.
/// </summary>
public sealed record StorageContinuityManifest
{
    public const int CurrentVersion = 1;

    public int Version { get; init; } = CurrentVersion;

    public string ProviderId { get; init; } = string.Empty;

    public string? ProviderVersion { get; init; }

    public string ContinuityId { get; init; } = string.Empty;

    public StorageContinuityLevel ContinuityLevel { get; init; } = StorageContinuityLevel.LocalOnly;

    /// <summary>
    /// Root directory of the shared or replicated storage scope. When omitted, the manifest directory is used.
    /// </summary>
    public string? RootPath { get; init; }

    public string? TopologyId { get; init; }

    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;

    public string? Description { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvidenceSource { get; init; }

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
