using System.Text.Json.Serialization;
using System.Runtime.InteropServices;

namespace Extend0.Cli;

public sealed record DoctorReport(
    [property: JsonPropertyName("version")] string Version,
    [property: JsonPropertyName("runtime_identifier")] string RuntimeIdentifier,
    [property: JsonPropertyName("architecture")] string Architecture,
    [property: JsonPropertyName("metadb_ready")] bool MetaDbReady,
    string RepositoryRoot,
    DateTimeOffset GeneratedAtUtc,
    IReadOnlyList<DoctorCheck> Checks,
    int PassCount,
    int WarningCount,
    int ErrorCount)
{
    public DoctorReport(
        string repositoryRoot,
        DateTimeOffset generatedAtUtc,
        IReadOnlyList<DoctorCheck> checks,
        int passCount,
        int warningCount,
        int errorCount)
        : this(
            "unknown",
            RuntimeInformation.RuntimeIdentifier,
            RuntimeInformation.OSArchitecture.ToString(),
            false,
            repositoryRoot,
            generatedAtUtc,
            checks,
            passCount,
            warningCount,
            errorCount)
    {
    }

    public static DoctorReport Create(string repositoryRoot, DateTimeOffset generatedAtUtc, IReadOnlyList<DoctorCheck> checks) =>
        Create(
            "unknown",
            RuntimeInformation.RuntimeIdentifier,
            RuntimeInformation.OSArchitecture.ToString(),
            false,
            repositoryRoot,
            generatedAtUtc,
            checks);

    public static DoctorReport Create(
        string version,
        string runtimeIdentifier,
        string architecture,
        bool metaDbReady,
        string repositoryRoot,
        DateTimeOffset generatedAtUtc,
        IReadOnlyList<DoctorCheck> checks)
    {
        ArgumentNullException.ThrowIfNull(checks);

        return new DoctorReport(
            version,
            runtimeIdentifier,
            architecture,
            metaDbReady,
            repositoryRoot,
            generatedAtUtc,
            checks,
            checks.Count(static c => c.Status == DoctorStatus.Pass),
            checks.Count(static c => c.Status == DoctorStatus.Warning),
            checks.Count(static c => c.Status == DoctorStatus.Error));
    }

    public void Deconstruct(
        out string repositoryRoot,
        out DateTimeOffset generatedAtUtc,
        out IReadOnlyList<DoctorCheck> checks,
        out int passCount,
        out int warningCount,
        out int errorCount)
    {
        repositoryRoot = RepositoryRoot;
        generatedAtUtc = GeneratedAtUtc;
        checks = Checks;
        passCount = PassCount;
        warningCount = WarningCount;
        errorCount = ErrorCount;
    }
}
