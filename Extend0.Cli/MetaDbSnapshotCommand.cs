using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Metadata.Schema;

namespace Extend0.Cli;

public static class MetaDbSnapshotCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static MetaDbSnapshotCommand()
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

        var parse = MetaDbSnapshotOptions.TryParse(args, workingDirectory, out var options, out var parseError);
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

        if (!MetaDbInspectCommand.TryResolveSpecPath(options.InputPath!, out var specPath, out var resolutionError))
        {
            error.WriteLine(resolutionError);
            return Task.FromResult(1);
        }

        try
        {
            var spec = TableSpec.Helpers.LoadFromFile(specPath);
            var manifest = MetaDbSnapshot.Create(spec, options.OutputDirectory!, options.Label, options.Overwrite);
            var report = MetaDbSnapshotCliReport.FromManifest(options.InputPath!, specPath, options.OutputDirectory!, manifest);

            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanReport(output, report);

            return Task.FromResult(0);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not create MetaDB snapshot: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    private static void WriteHumanReport(TextWriter output, MetaDbSnapshotCliReport report)
    {
        output.WriteLine("Extend0 MetaDB snapshot");
        output.WriteLine($"Input: {report.InputPath}");
        output.WriteLine($"Spec: {report.SpecPath}");
        output.WriteLine($"Snapshot: {report.SnapshotDirectory}");
        output.WriteLine($"Name: {report.Name}");
        output.WriteLine($"Schema version: {report.SchemaVersion}");
        output.WriteLine($"Storage: {report.Storage.Layout}");
        output.WriteLine($"Files: {report.FileCount}");
        output.WriteLine($"Runtime storage captured: {report.ContainsRuntimeStorage}");
        if (!string.IsNullOrWhiteSpace(report.Label))
            output.WriteLine($"Label: {report.Label}");
    }

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 metadb snapshot <path> --out <snapshot-dir> [--label <text>] [--overwrite] [--json]");
        writer.WriteLine();
        writer.WriteLine("Arguments:");
        writer.WriteLine("  <path>    TableSpec file, map path resolved via TableSpec save conventions, or chunked table directory.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --out <snapshot-dir>  Directory where the snapshot will be written.");
        writer.WriteLine("  --label <text>        Optional snapshot label.");
        writer.WriteLine("  --overwrite           Replace known snapshot files in an existing snapshot directory.");
        writer.WriteLine("  --json                Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help");
    }
}

public sealed record MetaDbSnapshotCliReport(
    string InputPath,
    string SpecPath,
    string SnapshotDirectory,
    string Name,
    string MapPath,
    int SchemaVersion,
    TableStorageOptions Storage,
    string? Label,
    DateTimeOffset CreatedAtUtc,
    int FileCount,
    bool ContainsRuntimeStorage,
    IReadOnlyList<MetaDbSnapshotFile> Files)
{
    public static MetaDbSnapshotCliReport FromManifest(
        string inputPath,
        string specPath,
        string snapshotDirectory,
        MetaDbSnapshotManifest manifest) =>
        new(
            Path.GetFullPath(inputPath),
            Path.GetFullPath(specPath),
            Path.GetFullPath(snapshotDirectory),
            manifest.OriginalSpec.Name,
            manifest.OriginalSpec.MapPath,
            manifest.OriginalSpec.EffectiveSchemaVersion,
            manifest.Storage,
            manifest.Label,
            manifest.CreatedAtUtc,
            manifest.Files.Length,
            manifest.ContainsRuntimeStorage,
            manifest.Files);
}
