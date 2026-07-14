namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Evidence and decision produced by storage protection verification.
/// </summary>
/// <param name="InputPath">Path originally provided by the caller.</param>
/// <param name="FullPath">Resolved absolute path that was verified.</param>
/// <param name="Policy">Policy used to evaluate the path.</param>
/// <param name="ObservedLevel">Protection level observed from provider or manifest evidence.</param>
/// <param name="Decision">Final verification decision.</param>
/// <param name="ProviderId">Provider identifier reported by the evidence, when available.</param>
/// <param name="ProviderVersion">Provider implementation version, when available.</param>
/// <param name="ProtectionId">Provider-scoped protected storage identifier, when available.</param>
/// <param name="MountRoot">Root path of the protected mount or storage scope, when available.</param>
/// <param name="PathInsideMount">Whether <paramref name="FullPath"/> is inside <paramref name="MountRoot"/>.</param>
/// <param name="EvidenceSource">Human-readable source of the evidence.</param>
/// <param name="VerifiedAtUtc">UTC timestamp at which verification ran.</param>
/// <param name="Findings">Detailed verification findings.</param>
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
    /// <summary>
    /// Gets the number of informational findings.
    /// </summary>
    public int InfoCount => Findings.Count(static f => f.Severity == StorageProtectionFindingSeverity.Info);

    /// <summary>
    /// Gets the number of warning findings.
    /// </summary>
    public int WarningCount => Findings.Count(static f => f.Severity == StorageProtectionFindingSeverity.Warning);

    /// <summary>
    /// Gets the number of error findings.
    /// </summary>
    public int ErrorCount => Findings.Count(static f => f.Severity == StorageProtectionFindingSeverity.Error);
}
