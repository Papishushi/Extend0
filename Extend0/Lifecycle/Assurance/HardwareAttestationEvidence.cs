namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Evidence and decision produced by hardware-attestation verification.
/// </summary>
/// <param name="InputPath">Path originally provided by the caller.</param>
/// <param name="FullPath">Resolved absolute path that was verified.</param>
/// <param name="Policy">Policy used to evaluate the path.</param>
/// <param name="ObservedLevel">Attestation level observed from provider or manifest evidence.</param>
/// <param name="ObservedTechnology">Attestation technology observed from provider or manifest evidence.</param>
/// <param name="Decision">Final verification decision.</param>
/// <param name="ProviderId">Provider identifier reported by the evidence, when available.</param>
/// <param name="ProviderVersion">Provider implementation version, when available.</param>
/// <param name="AttestationId">Provider-scoped attestation identifier, when available.</param>
/// <param name="Measurement">Observed or expected execution measurement, when available.</param>
/// <param name="PolicyId">Provider-defined policy identifier, when available.</param>
/// <param name="ReportFormat">Provider-specific report format, when available.</param>
/// <param name="ReportDigest">Digest of the attestation report, when available.</param>
/// <param name="RootPath">Root path controlled by the attested environment, when available.</param>
/// <param name="PathInsideRoot">Whether <paramref name="FullPath"/> is inside <paramref name="RootPath"/>.</param>
/// <param name="EvidenceSource">Human-readable source of the evidence.</param>
/// <param name="VerifiedAtUtc">UTC timestamp at which verification ran.</param>
/// <param name="Findings">Detailed verification findings.</param>
public sealed record HardwareAttestationEvidence(
    string InputPath,
    string FullPath,
    HardwareAttestationPolicy Policy,
    HardwareAttestationLevel ObservedLevel,
    HardwareAttestationTechnology ObservedTechnology,
    HardwareAttestationDecision Decision,
    string? ProviderId,
    string? ProviderVersion,
    string? AttestationId,
    string? Measurement,
    string? PolicyId,
    string? ReportFormat,
    string? ReportDigest,
    string? RootPath,
    bool PathInsideRoot,
    string EvidenceSource,
    DateTimeOffset VerifiedAtUtc,
    IReadOnlyList<HardwareAttestationFinding> Findings)
{
    /// <summary>
    /// Gets the number of informational findings.
    /// </summary>
    public int InfoCount => Findings.Count(static f => f.Severity == HardwareAttestationFindingSeverity.Info);

    /// <summary>
    /// Gets the number of warning findings.
    /// </summary>
    public int WarningCount => Findings.Count(static f => f.Severity == HardwareAttestationFindingSeverity.Warning);

    /// <summary>
    /// Gets the number of error findings.
    /// </summary>
    public int ErrorCount => Findings.Count(static f => f.Severity == HardwareAttestationFindingSeverity.Error);
}
