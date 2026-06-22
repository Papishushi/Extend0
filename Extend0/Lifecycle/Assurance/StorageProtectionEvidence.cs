namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Evidence and decision produced by storage protection verification.
/// </summary>
public sealed record StorageProtectionEvidence(
    string InputPath,
    string FullPath,
    StorageProtectionPolicy Policy,
    StorageProtectionLevel ObservedLevel,
    StorageProtectionDecision Decision,
    string? ProviderId,
    string? ProviderVersion,
    string? ProtectionId,
    string? MountRoot,
    bool PathInsideMount,
    string EvidenceSource,
    DateTimeOffset VerifiedAtUtc,
    IReadOnlyList<StorageProtectionFinding> Findings)
{
    public int InfoCount => Findings.Count(static f => f.Severity == StorageProtectionFindingSeverity.Info);

    public int WarningCount => Findings.Count(static f => f.Severity == StorageProtectionFindingSeverity.Warning);

    public int ErrorCount => Findings.Count(static f => f.Severity == StorageProtectionFindingSeverity.Error);
}
