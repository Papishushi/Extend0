using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Metadata.Schema;

namespace Extend0.Cli;

public static class MetaDbSchemaCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static MetaDbSchemaCommand()
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

        var parse = MetaDbSchemaOptions.TryParse(args, workingDirectory, out var options, out var parseError);
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

        if (!MetaDbInspectCommand.TryResolveSpecPath(options.SourcePath!, out var sourceSpecPath, out var sourceError))
        {
            error.WriteLine(sourceError);
            return Task.FromResult(1);
        }

        if (!MetaDbInspectCommand.TryResolveSpecPath(options.TargetPath!, out var targetSpecPath, out var targetError))
        {
            error.WriteLine(targetError);
            return Task.FromResult(1);
        }

        try
        {
            var source = TableSpec.Helpers.LoadFromFile(sourceSpecPath);
            var target = TableSpec.Helpers.LoadFromFile(targetSpecPath);
            var plan = TableSpecMigration.CreatePlan(source, target);
            var report = MetaDbSchemaReport.FromPlan(sourceSpecPath, targetSpecPath, plan);

            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanReport(output, report);

            return Task.FromResult(report.Level == TableSpecCompatibilityLevel.Incompatible ? 1 : 0);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not compare MetaDB schemas: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    private static void WriteHumanReport(TextWriter output, MetaDbSchemaReport report)
    {
        output.WriteLine("Extend0 MetaDB schema");
        output.WriteLine($"Source: {report.SourceSpecPath}");
        output.WriteLine($"Target: {report.TargetSpecPath}");
        output.WriteLine($"Source version: {report.SourceSchemaVersion}");
        output.WriteLine($"Target version: {report.TargetSchemaVersion}");
        output.WriteLine($"Compatibility: {report.Level}");
        output.WriteLine($"Can apply automatically: {report.CanApplyAutomatically}");
        output.WriteLine($"Requires manual data transform: {report.RequiresManualDataTransform}");
        output.WriteLine();

        output.WriteLine("Findings:");
        foreach (var finding in report.Findings)
            output.WriteLine($"[{finding.Severity.ToString().ToLowerInvariant()}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine("Plan:");
        foreach (var step in report.Steps)
            output.WriteLine($"- {step.Kind} ({step.Impact}): {step.Description}");
    }

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 metadb schema <source> <target> [--json]");
        writer.WriteLine();
        writer.WriteLine("Arguments:");
        writer.WriteLine("  <source>  Source TableSpec, map path resolved via TableSpec save conventions, or chunked table directory.");
        writer.WriteLine("  <target>  Target TableSpec, map path resolved via TableSpec save conventions, or chunked table directory.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --json    Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help");
    }
}

public sealed record MetaDbSchemaReport(
    string SourceSpecPath,
    string TargetSpecPath,
    int SourceSchemaVersion,
    int TargetSchemaVersion,
    TableSpecCompatibilityLevel Level,
    bool CanApplyAutomatically,
    bool RequiresManualDataTransform,
    IReadOnlyList<TableSpecCompatibilityFinding> Findings,
    IReadOnlyList<TableSpecMigrationStep> Steps)
{
    public static MetaDbSchemaReport FromPlan(string sourceSpecPath, string targetSpecPath, TableSpecMigrationPlan plan) =>
        new(
            Path.GetFullPath(sourceSpecPath),
            Path.GetFullPath(targetSpecPath),
            plan.Source.EffectiveSchemaVersion,
            plan.Target.EffectiveSchemaVersion,
            plan.Compatibility.Level,
            plan.CanApplyAutomatically,
            plan.RequiresManualDataTransform,
            plan.Compatibility.Findings,
            plan.Steps);
}
