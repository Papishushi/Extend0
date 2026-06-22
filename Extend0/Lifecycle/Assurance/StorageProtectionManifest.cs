using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Provider or operator-owned manifest used by the portable verifier as storage protection evidence.
/// </summary>
public sealed record StorageProtectionManifest
{
    public const int CurrentVersion = 1;

    public int Version { get; init; } = CurrentVersion;

    public string ProviderId { get; init; } = string.Empty;

    public string? ProviderVersion { get; init; }

    public string ProtectionId { get; init; } = string.Empty;

    public StorageProtectionLevel ProtectionLevel { get; init; } = StorageProtectionLevel.DeclaredEncrypted;

    /// <summary>
    /// Root directory of the protected mount. When omitted, the manifest directory is used.
    /// </summary>
    public string? RootPath { get; init; }

    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;

    public string? Description { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvidenceSource { get; init; }

    public static StorageProtectionManifest Create(
        string providerId,
        string protectionId,
        StorageProtectionLevel protectionLevel,
        string? rootPath = null,
        string? providerVersion = null,
        string? description = null) =>
        new()
        {
            ProviderId = providerId,
            ProtectionId = protectionId,
            ProtectionLevel = protectionLevel,
            RootPath = rootPath,
            ProviderVersion = providerVersion,
            Description = description,
            EvidenceSource = "manifest"
        };
}
