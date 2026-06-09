using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Schema;

namespace Extend0.Cli;

public static class MetaDbInspectCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static MetaDbInspectCommand()
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

        var parse = MetaDbInspectOptions.TryParse(args, workingDirectory, out var options, out var parseError);
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

        if (!TryResolveSpecPath(options.InputPath!, out var specPath, out var resolutionError))
        {
            error.WriteLine(resolutionError);
            return Task.FromResult(1);
        }

        try
        {
            var spec = TableSpec.Helpers.LoadFromFile(specPath);
            var report = MetaDbInspectReport.FromSpec(options.InputPath!, specPath, spec);

            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanReport(output, report);

            return Task.FromResult(0);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not inspect MetaDB spec: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    internal static bool TryResolveSpecPath(string inputPath, out string specPath, out string error)
    {
        var fullInput = Path.GetFullPath(inputPath);

        if (Directory.Exists(fullInput))
        {
            var chunkedSpec = Path.Combine(fullInput, "tablespec.json");
            if (File.Exists(chunkedSpec))
            {
                specPath = chunkedSpec;
                error = string.Empty;
                return true;
            }

            var specs = Directory.EnumerateFiles(fullInput, "*.tablespec.json").ToArray();
            if (specs.Length == 1)
            {
                specPath = specs[0];
                error = string.Empty;
                return true;
            }

            specPath = string.Empty;
            error = specs.Length == 0
                ? $"No TableSpec found in directory '{fullInput}'."
                : $"Multiple TableSpec files found in directory '{fullInput}'. Pass one explicitly.";
            return false;
        }

        if (File.Exists(fullInput))
        {
            if (IsSpecFile(fullInput))
            {
                specPath = fullInput;
                error = string.Empty;
                return true;
            }

            var sidecar = fullInput + ".tablespec.json";
            if (File.Exists(sidecar))
            {
                specPath = sidecar;
                error = string.Empty;
                return true;
            }

            var directory = Path.GetDirectoryName(fullInput);
            if (!string.IsNullOrWhiteSpace(directory) && Directory.Exists(directory))
            {
                var siblingSpecs = Directory.EnumerateFiles(directory, "*.tablespec.json").ToArray();
                if (siblingSpecs.Length == 1)
                {
                    specPath = siblingSpecs[0];
                    error = string.Empty;
                    return true;
                }

                if (siblingSpecs.Length > 1)
                {
                    specPath = string.Empty;
                    error = $"Input file '{fullInput}' has no direct sidecar and multiple TableSpec files exist in '{directory}'. Pass one explicitly.";
                    return false;
                }
            }

            specPath = string.Empty;
            error = $"Input file '{fullInput}' is not a TableSpec and no sidecar '{sidecar}' exists.";
            return false;
        }

        var missingSidecar = fullInput + ".tablespec.json";
        if (File.Exists(missingSidecar))
        {
            specPath = missingSidecar;
            error = string.Empty;
            return true;
        }

        var missingChunkedSpec = Path.Combine(fullInput, "tablespec.json");
        if (File.Exists(missingChunkedSpec))
        {
            specPath = missingChunkedSpec;
            error = string.Empty;
            return true;
        }

        specPath = string.Empty;
        error = $"No TableSpec found for '{fullInput}'.";
        return false;
    }

    private static bool IsSpecFile(string path)
    {
        var fileName = Path.GetFileName(path);
        return string.Equals(fileName, "tablespec.json", StringComparison.OrdinalIgnoreCase)
            || fileName.EndsWith(".tablespec.json", StringComparison.OrdinalIgnoreCase);
    }

    private static void WriteHumanReport(TextWriter output, MetaDbInspectReport report)
    {
        output.WriteLine("Extend0 MetaDB inspect");
        output.WriteLine($"Input: {report.InputPath}");
        output.WriteLine($"Spec: {report.SpecPath}");
        output.WriteLine($"Name: {report.Name}");
        output.WriteLine($"MapPath: {report.MapPath}");
        output.WriteLine($"Storage: {report.Storage.Layout}");
        output.WriteLine($"ChunkSize: {report.Storage.ChunkSize}");
        output.WriteLine($"Columns: {report.ColumnCount}");
        output.WriteLine();

        foreach (var column in report.Columns)
        {
            output.WriteLine(
                $"[{column.Index}] {column.Name}: key={column.KeyBytes}, value={column.ValueBytes}, entry={column.EntryBytes}, capacity={column.InitialCapacity}, readOnly={column.ReadOnly}");
        }
    }

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 metadb inspect <path> [--json]");
        writer.WriteLine();
        writer.WriteLine("Arguments:");
        writer.WriteLine("  <path>    TableSpec file, map file with .tablespec.json sidecar, or chunked table directory.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --json    Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help");
    }
}

public sealed record MetaDbInspectReport(
    string InputPath,
    string SpecPath,
    string Name,
    string MapPath,
    TableStorageOptions Storage,
    int ColumnCount,
    IReadOnlyList<MetaDbColumnReport> Columns)
{
    public static MetaDbInspectReport FromSpec(string inputPath, string specPath, TableSpec spec)
    {
        var storage = spec.Storage.Normalize();
        var columns = spec.Columns
            .Select((column, index) =>
            {
                var keyBytes = column.Size.GetKeySize();
                var valueBytes = column.Size.GetValueSize();
                return new MetaDbColumnReport(
                    index,
                    column.Name,
                    keyBytes,
                    valueBytes,
                    keyBytes + valueBytes,
                    column.InitialCapacity,
                    column.ReadOnly);
            })
            .ToArray();

        return new MetaDbInspectReport(
            Path.GetFullPath(inputPath),
            Path.GetFullPath(specPath),
            spec.Name,
            spec.MapPath,
            storage,
            columns.Length,
            columns);
    }
}

public sealed record MetaDbColumnReport(
    int Index,
    string Name,
    int KeyBytes,
    int ValueBytes,
    int EntryBytes,
    uint InitialCapacity,
    bool ReadOnly);
