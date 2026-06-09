using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Metadata.Schema;

namespace Extend0.Cli;

public static class MetaDbRestoreCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static MetaDbRestoreCommand()
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

        var parse = MetaDbRestoreOptions.TryParse(args, workingDirectory, out var options, out var parseError);
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

        try
        {
            var restored = MetaDbSnapshot.Restore(options.SnapshotDirectory!, options.RestoreMapPath!, options.Overwrite);
            var report = MetaDbRestoreReport.FromSpec(options.SnapshotDirectory!, restored);

            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanReport(output, report);

            return Task.FromResult(0);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not restore MetaDB snapshot: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    private static void WriteHumanReport(TextWriter output, MetaDbRestoreReport report)
    {
        output.WriteLine("Extend0 MetaDB restore");
        output.WriteLine($"Snapshot: {report.SnapshotDirectory}");
        output.WriteLine($"Name: {report.Name}");
        output.WriteLine($"MapPath: {report.MapPath}");
        output.WriteLine($"Spec: {report.RestoredSpecPath}");
        output.WriteLine($"Schema version: {report.SchemaVersion}");
        output.WriteLine($"Storage: {report.Storage.Layout}");
    }

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 metadb restore <snapshot-dir> --map-path <path> [--overwrite] [--json]");
        writer.WriteLine();
        writer.WriteLine("Arguments:");
        writer.WriteLine("  <snapshot-dir>  Directory containing snapshot.json.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --map-path <path>  Restore target map file for single-file snapshots or table directory for chunked snapshots.");
        writer.WriteLine("  --overwrite        Overwrite restored files when they already exist.");
        writer.WriteLine("  --json             Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help");
    }
}

public sealed record MetaDbRestoreReport(
    string SnapshotDirectory,
    string Name,
    string MapPath,
    string RestoredSpecPath,
    int SchemaVersion,
    TableStorageOptions Storage)
{
    public static MetaDbRestoreReport FromSpec(string snapshotDirectory, TableSpec spec)
    {
        var storage = spec.Storage.Normalize();
        var specPath = storage.Layout == TableStorageLayout.Chunked
            ? Path.Combine(spec.MapPath, "tablespec.json")
            : spec.MapPath + ".tablespec.json";

        return new MetaDbRestoreReport(
            Path.GetFullPath(snapshotDirectory),
            spec.Name,
            spec.MapPath,
            Path.GetFullPath(specPath),
            spec.EffectiveSchemaVersion,
            storage);
    }
}
