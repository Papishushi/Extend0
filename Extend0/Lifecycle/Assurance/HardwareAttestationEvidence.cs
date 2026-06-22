namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Evidence and decision produced by hardware-attestation verification.
/// </summary>
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
    public int InfoCount => Findings.Count(static f => f.Severity == HardwareAttestationFindingSeverity.Info);

    public int WarningCount => Findings.Count(static f => f.Severity == HardwareAttestationFindingSeverity.Warning);

    public int ErrorCount => Findings.Count(static f => f.Severity == HardwareAttestationFindingSeverity.Error);
}
