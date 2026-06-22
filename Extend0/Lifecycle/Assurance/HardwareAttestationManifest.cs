using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Provider or operator-owned manifest used as portable hardware-attestation evidence.
/// </summary>
public sealed record HardwareAttestationManifest
{
    public const int CurrentVersion = 1;

    public int Version { get; init; } = CurrentVersion;

    public string ProviderId { get; init; } = string.Empty;

    public string? ProviderVersion { get; init; }

    public string AttestationId { get; init; } = string.Empty;

    public HardwareAttestationTechnology Technology { get; init; } = HardwareAttestationTechnology.None;

    public HardwareAttestationLevel AttestationLevel { get; init; } = HardwareAttestationLevel.Declared;

    /// <summary>
    /// Storage root controlled by the attested execution environment. When omitted, the manifest directory is used.
    /// </summary>
    public string? RootPath { get; init; }

    public string? Measurement { get; init; }

    public string? PolicyId { get; init; }

    public string? ReportFormat { get; init; }

    public string? ReportDigest { get; init; }

    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;

    public string? Description { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvidenceSource { get; init; }

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
