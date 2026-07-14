namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Evidence and decision produced by storage continuity verification.
/// </summary>
/// <param name="InputPath">Path originally provided by the caller.</param>
/// <param name="FullPath">Resolved absolute path that was verified.</param>
/// <param name="Policy">Policy used to evaluate the path.</param>
/// <param name="ObservedLevel">Continuity level observed from provider or manifest evidence.</param>
/// <param name="Decision">Final verification decision.</param>
/// <param name="ProviderId">Provider identifier reported by the evidence, when available.</param>
/// <param name="ProviderVersion">Provider implementation version, when available.</param>
/// <param name="ContinuityId">Provider-scoped shared-store or replication-group identifier, when available.</param>
/// <param name="TopologyId">Topology identifier associated with the continuity scope, when available.</param>
/// <param name="RootPath">Root path of the shared or replicated storage scope, when available.</param>
/// <param name="PathInsideRoot">Whether <paramref name="FullPath"/> is inside <paramref name="RootPath"/>.</param>
/// <param name="EvidenceSource">Human-readable source of the evidence.</param>
/// <param name="VerifiedAtUtc">UTC timestamp at which verification ran.</param>
/// <param name="Findings">Detailed verification findings.</param>
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
    /// <summary>
    /// Gets the number of informational findings.
    /// </summary>
    public int InfoCount => Findings.Count(static f => f.Severity == StorageContinuityFindingSeverity.Info);

    /// <summary>
    /// Gets the number of warning findings.
    /// </summary>
    public int WarningCount => Findings.Count(static f => f.Severity == StorageContinuityFindingSeverity.Warning);

    /// <summary>
    /// Gets the number of error findings.
    /// </summary>
    public int ErrorCount => Findings.Count(static f => f.Severity == StorageContinuityFindingSeverity.Error);
}
