namespace Extend0.Cli;

public sealed record DoctorReport(
    string RepositoryRoot,
    DateTimeOffset GeneratedAtUtc,
    IReadOnlyList<DoctorCheck> Checks,
    int PassCount,
    int WarningCount,
    int ErrorCount)
{
    public static DoctorReport Create(string repositoryRoot, DateTimeOffset generatedAtUtc, IReadOnlyList<DoctorCheck> checks)
    {
        ArgumentNullException.ThrowIfNull(checks);

        return new DoctorReport(
            repositoryRoot,
            generatedAtUtc,
            checks,
            checks.Count(static c => c.Status == DoctorStatus.Pass),
            checks.Count(static c => c.Status == DoctorStatus.Warning),
            checks.Count(static c => c.Status == DoctorStatus.Error));
    }
}
