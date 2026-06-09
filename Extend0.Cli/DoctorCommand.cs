using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Linq;

namespace Extend0.Cli;

public static class DoctorCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static DoctorCommand()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public static Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentException.ThrowIfNullOrWhiteSpace(workingDirectory);

        var parse = DoctorOptions.TryParse(args, workingDirectory, out var options, out var parseError);
        if (!parse)
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteHelp(error);
            return Task.FromResult(2);
        }

        if (options.ShowHelp)
        {
            WriteHelp(output);
            return Task.FromResult(0);
        }

        cancellationToken.ThrowIfCancellationRequested();

        var report = BuildReport(options.RepositoryRoot);
        if (options.Json)
        {
            output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
        }
        else
        {
            WriteHumanReport(output, report);
        }

        return Task.FromResult(report.ErrorCount > 0 ? 1 : 0);
    }

    private static DoctorReport BuildReport(string repositoryRoot)
    {
        var root = Path.GetFullPath(repositoryRoot);
        var checks = new List<DoctorCheck>();

        CheckFile(checks, root, "solution", "Extend0.sln", required: true);
        CheckFile(checks, root, "core-project", Path.Combine("Extend0", "Extend0.csproj"), required: true);
        CheckFile(checks, root, "readme", "README.md", required: true);
        CheckFile(checks, root, "adr-index", Path.Combine("docs", "ADR.md"), required: true);
        CheckAnyFile(checks, root, "adr-major-1-baseline", Path.Combine("docs", "ADR"), "1-000-*-ADR-*.md", required: true);
        CheckFile(checks, root, "ontology-tbox", Path.Combine("ontology", "tbox", "extend0.owl"), required: true);
        CheckFile(checks, root, "ontology-abox-schema", Path.Combine("ontology", "abox", "abox-schema.ttl"), required: false);
        CheckFile(checks, root, "ontology-example-abox", Path.Combine("ontology", "abox", "example-abox.ttl"), required: false);
        CheckFile(checks, root, "ontology-query-tool", Path.Combine("ontology", "skills", "ontology-query", "query.py"), required: false);
        CheckFile(checks, root, "test-project", Path.Combine("Extend0.Tests", "Extend0.Tests.csproj"), required: false);
        CheckFile(checks, root, "testing-harness-project", Path.Combine("Extend0.Testing", "Extend0.Testing.csproj"), required: false);

        CheckTargetFramework(checks, root);
        CheckReadmeTargetFramework(checks, root);
        CheckOntologyAccessSurfaceRange(checks, root);

        return DoctorReport.Create(root, DateTimeOffset.UtcNow, checks);
    }

    private static void CheckFile(List<DoctorCheck> checks, string root, string id, string relativePath, bool required)
    {
        var path = Path.Combine(root, relativePath);
        if (File.Exists(path))
        {
            checks.Add(DoctorCheck.Pass(id, $"Found {relativePath}."));
            return;
        }

        var status = required ? DoctorStatus.Error : DoctorStatus.Warning;
        checks.Add(new DoctorCheck(id, status, $"Missing {relativePath}."));
    }

    private static void CheckAnyFile(List<DoctorCheck> checks, string root, string id, string relativeDirectory, string pattern, bool required)
    {
        var directory = Path.Combine(root, relativeDirectory);
        if (Directory.Exists(directory) && Directory.EnumerateFiles(directory, pattern).Any())
        {
            checks.Add(DoctorCheck.Pass(id, $"Found {relativeDirectory}/{pattern}."));
            return;
        }

        var status = required ? DoctorStatus.Error : DoctorStatus.Warning;
        checks.Add(new DoctorCheck(id, status, $"Missing {relativeDirectory}/{pattern}."));
    }

    private static void CheckTargetFramework(List<DoctorCheck> checks, string root)
    {
        var projectPath = Path.Combine(root, "Extend0", "Extend0.csproj");
        if (!File.Exists(projectPath))
            return;

        try
        {
            var doc = XDocument.Load(projectPath);
            var targetFramework = doc.Descendants("TargetFramework").FirstOrDefault()?.Value.Trim();
            if (string.Equals(targetFramework, "net10.0", StringComparison.OrdinalIgnoreCase))
            {
                checks.Add(DoctorCheck.Pass("core-target-framework", "Extend0 targets net10.0."));
                return;
            }

            checks.Add(new DoctorCheck("core-target-framework", DoctorStatus.Warning, $"Extend0 targets '{targetFramework ?? "<missing>"}', expected net10.0 for current major 1 docs."));
        }
        catch (Exception ex)
        {
            checks.Add(new DoctorCheck("core-target-framework", DoctorStatus.Error, $"Could not read Extend0.csproj: {ex.Message}"));
        }
    }

    private static void CheckReadmeTargetFramework(List<DoctorCheck> checks, string root)
    {
        var readmePath = Path.Combine(root, "README.md");
        if (!File.Exists(readmePath))
            return;

        var readme = File.ReadAllText(readmePath);
        if (readme.Contains("net10.0", StringComparison.OrdinalIgnoreCase))
        {
            checks.Add(DoctorCheck.Pass("readme-target-framework", "README mentions net10.0."));
            return;
        }

        checks.Add(new DoctorCheck("readme-target-framework", DoctorStatus.Warning, "README does not mention net10.0."));
    }

    private static void CheckOntologyAccessSurfaceRange(List<DoctorCheck> checks, string root)
    {
        var tboxPath = Path.Combine(root, "ontology", "tbox", "extend0.owl");
        if (!File.Exists(tboxPath))
            return;

        var tbox = File.ReadAllText(tboxPath);
        var propertyIndex = tbox.IndexOf("rdf:about=\"#governsAccessTo\"", StringComparison.OrdinalIgnoreCase);
        if (propertyIndex < 0)
        {
            checks.Add(new DoctorCheck("ontology-governs-access-range", DoctorStatus.Warning, "TBox does not define governsAccessTo."));
            return;
        }

        var propertyBody = tbox[propertyIndex..Math.Min(tbox.Length, propertyIndex + 500)];
        if (propertyBody.Contains("rdf:resource=\"#AccessSurface\"", StringComparison.OrdinalIgnoreCase))
        {
            checks.Add(DoctorCheck.Pass("ontology-governs-access-range", "governsAccessTo ranges over AccessSurface."));
            return;
        }

        checks.Add(new DoctorCheck("ontology-governs-access-range", DoctorStatus.Error, "governsAccessTo should range over AccessSurface."));
    }

    private static void WriteHumanReport(TextWriter output, DoctorReport report)
    {
        output.WriteLine("Extend0 doctor");
        output.WriteLine($"Root: {report.RepositoryRoot}");
        output.WriteLine();

        foreach (var check in report.Checks)
            output.WriteLine($"[{FormatStatus(check.Status)}] {check.Id}: {check.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.PassCount} ok, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static string FormatStatus(DoctorStatus status) =>
        status switch
        {
            DoctorStatus.Pass => "ok",
            DoctorStatus.Warning => "warn",
            DoctorStatus.Error => "error",
            _ => status.ToString().ToLowerInvariant()
        };

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 doctor [--repo <path>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --repo <path>    Repository root to inspect. Defaults to the current working directory.");
        writer.WriteLine("  --json           Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help       Show command help.");
    }
}
