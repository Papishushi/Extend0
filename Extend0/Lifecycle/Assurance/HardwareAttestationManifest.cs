using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Provider or operator-owned manifest used as portable hardware-attestation evidence.
/// </summary>
public sealed record HardwareAttestationManifest
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
    /// Gets the attestation provider identifier.
    /// </summary>
    public string ProviderId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the optional provider implementation version.
    /// </summary>
    public string? ProviderVersion { get; init; }

    /// <summary>
    /// Gets the provider-scoped attestation identifier.
    /// </summary>
    public string AttestationId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the technology that produced or qualifies the attestation evidence.
    /// </summary>
    public HardwareAttestationTechnology Technology { get; init; } = HardwareAttestationTechnology.None;

    /// <summary>
    /// Gets the attestation assurance level declared by this manifest.
    /// </summary>
    public HardwareAttestationLevel AttestationLevel { get; init; } = HardwareAttestationLevel.Declared;

    /// <summary>
    /// Storage root controlled by the attested execution environment. When omitted, the manifest directory is used.
    /// </summary>
    public string? RootPath { get; init; }

    /// <summary>
    /// Gets the expected or observed measurement associated with the attested execution environment.
    /// </summary>
    public string? Measurement { get; init; }

    /// <summary>
    /// Gets the policy identifier used to validate the attestation report.
    /// </summary>
    public string? PolicyId { get; init; }

    /// <summary>
    /// Gets the provider-specific report format.
    /// </summary>
    public string? ReportFormat { get; init; }

    /// <summary>
    /// Gets a digest of the attestation report when the full report is stored externally.
    /// </summary>
    public string? ReportDigest { get; init; }

    /// <summary>
    /// Gets the UTC timestamp at which the manifest was created.
    /// </summary>
    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets an optional human-readable description of the attestation scope.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Gets the source from which this evidence was loaded or produced.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvidenceSource { get; init; }

    /// <summary>
    /// Creates a hardware-attestation manifest with the current schema version.
    /// </summary>
    public static HardwareAttestationManifest Create(
        string providerId,
        string attestationId,
        HardwareAttestationTechnology technology,
        HardwareAttestationLevel attestationLevel,
        string? rootPath = null,
        string? providerVersion = null,
        string? measurement = null,
        string? policyId = null,
        string? reportFormat = null,
        string? reportDigest = null,
        string? description = null) =>
        new()
        {
            ProviderId = providerId,
            AttestationId = attestationId,
            Technology = technology,
            AttestationLevel = attestationLevel,
            RootPath = rootPath,
            ProviderVersion = providerVersion,
            Measurement = measurement,
            PolicyId = policyId,
            ReportFormat = reportFormat,
            ReportDigest = reportDigest,
            Description = description,
            EvidenceSource = "manifest"
        };
}
