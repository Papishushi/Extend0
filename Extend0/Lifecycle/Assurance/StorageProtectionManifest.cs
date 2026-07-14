using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Provider or operator-owned manifest used by the portable verifier as storage protection evidence.
/// </summary>
public sealed record StorageProtectionManifest
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
    /// Gets the storage-protection provider identifier.
    /// </summary>
    public string ProviderId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the optional provider implementation version.
    /// </summary>
    public string? ProviderVersion { get; init; }

    /// <summary>
    /// Gets the provider-scoped protected storage identifier.
    /// </summary>
    public string ProtectionId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the protection level declared by this manifest.
    /// </summary>
    public StorageProtectionLevel ProtectionLevel { get; init; } = StorageProtectionLevel.DeclaredEncrypted;

    /// <summary>
    /// Root directory of the protected mount. When omitted, the manifest directory is used.
    /// </summary>
    public string? RootPath { get; init; }

    /// <summary>
    /// Gets the UTC timestamp at which the manifest was created.
    /// </summary>
    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets an optional human-readable description of the protected storage scope.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Gets the source from which this evidence was loaded or produced.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvidenceSource { get; init; }

    /// <summary>
    /// Creates a storage-protection manifest with the current schema version.
    /// </summary>
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
