namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Evidence and decision produced by storage continuity verification.
/// </summary>
public sealed record StorageContinuityEvidence(
    string InputPath,
    string FullPath,
    StorageContinuityPolicy Policy,
    StorageContinuityLevel ObservedLevel,
    StorageContinuityDecision Decision,
    string? ProviderId,
    string? ProviderVersion,
    string? ContinuityId,
    string? TopologyId,
    string? RootPath,
    bool PathInsideRoot,
    string EvidenceSource,
    DateTimeOffset VerifiedAtUtc,
    IReadOnlyList<StorageContinuityFinding> Findings)
{
    public int InfoCount => Findings.Count(static f => f.Severity == StorageContinuityFindingSeverity.Info);

    public int WarningCount => Findings.Count(static f => f.Severity == StorageContinuityFindingSeverity.Warning);

    public int ErrorCount => Findings.Count(static f => f.Severity == StorageContinuityFindingSeverity.Error);
}
