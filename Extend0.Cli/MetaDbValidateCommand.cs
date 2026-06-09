using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Metadata.Schema;

namespace Extend0.Cli;

public static class MetaDbValidateCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static MetaDbValidateCommand()
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

        var parse = MetaDbValidateOptions.TryParse(args, workingDirectory, out var options, out var parseError);
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
            var report = BuildReport(options.InputPath!, specPath, spec);

            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanReport(output, report);

            return Task.FromResult(report.ErrorCount > 0 ? 1 : 0);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not validate MetaDB spec: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    private static MetaDbValidateReport BuildReport(string inputPath, string specPath, TableSpec spec)
    {
        var inspect = MetaDbInspectReport.FromSpec(inputPath, specPath, spec);
        var findings = new List<ValidationFinding>
        {
            ValidationFinding.Info("spec-loaded", $"Loaded TableSpec '{inspect.Name}'.")
        };

        ValidateStorage(inspect, findings);
        ValidateColumns(inspect, findings);
        ValidateSidecarConventions(inspect, findings);

        return MetaDbValidateReport.Create(
            inspect.InputPath,
            inspect.SpecPath,
            inspect.Name,
            inspect.MapPath,
            inspect.Storage,
            inspect.ColumnCount,
            inspect.Columns,
            EstimateLogicalBytes(inspect),
            EstimateStorageBytes(inspect),
            findings);
    }

    private static void ValidateStorage(MetaDbInspectReport inspect, List<ValidationFinding> findings)
    {
        switch (inspect.Storage.Layout)
        {
            case TableStorageLayout.SingleFile:
                findings.Add(ValidationFinding.Info("storage-layout", "Single-file storage layout."));
                if (inspect.Storage.ChunkSize > 0)
                    findings.Add(ValidationFinding.Info("single-file-chunk-alignment", $"Single-file growth is chunk-aligned to {inspect.Storage.ChunkSize} bytes."));
                break;

            case TableStorageLayout.Chunked:
                findings.Add(ValidationFinding.Info("storage-layout", "Chunked storage layout."));
                if (inspect.Storage.ChunkSize <= 0)
                    findings.Add(ValidationFinding.Error("chunk-size", "Chunked storage requires a positive chunk size."));
                break;

            default:
                findings.Add(ValidationFinding.Error("storage-layout", $"Unknown storage layout '{inspect.Storage.Layout}'."));
                break;
        }
    }

    private static void ValidateColumns(MetaDbInspectReport inspect, List<ValidationFinding> findings)
    {
        var duplicateNames = inspect.Columns
            .GroupBy(static column => column.Name, StringComparer.Ordinal)
            .Where(static group => group.Count() > 1)
            .Select(static group => group.Key)
            .ToArray();

        foreach (var duplicate in duplicateNames)
            findings.Add(ValidationFinding.Error("duplicate-column-name", $"Column name '{duplicate}' appears more than once."));

        foreach (var column in inspect.Columns)
        {
            if (string.IsNullOrWhiteSpace(column.Name))
                findings.Add(ValidationFinding.Error("column-name", $"Column {column.Index} has an empty name."));

            if (column.KeyBytes < 0)
                findings.Add(ValidationFinding.Error("column-key-size", $"Column '{column.Name}' has a negative key size."));
            else if (column.KeyBytes == 0)
                findings.Add(ValidationFinding.Info("value-only-column", $"Column '{column.Name}' is value-only with no key bytes."));

            if (column.ValueBytes <= 0)
                findings.Add(ValidationFinding.Error("column-value-size", $"Column '{column.Name}' must reserve at least one value byte."));

            if (column.EntryBytes <= 0)
                findings.Add(ValidationFinding.Error("column-entry-size", $"Column '{column.Name}' has a non-positive entry size."));

            if (column.InitialCapacity == 0)
                findings.Add(ValidationFinding.Warning("column-capacity", $"Column '{column.Name}' has zero initial capacity."));

            if (inspect.Storage.Layout == TableStorageLayout.Chunked && inspect.Storage.ChunkSize > 0)
            {
                if (column.EntryBytes > inspect.Storage.ChunkSize)
                {
                    findings.Add(ValidationFinding.Error(
                        "chunk-entry-size",
                        $"Column '{column.Name}' entry size {column.EntryBytes} bytes is larger than chunk size {inspect.Storage.ChunkSize} bytes."));
                }
                else if (inspect.Storage.ChunkSize % column.EntryBytes != 0)
                {
                    findings.Add(ValidationFinding.Warning(
                        "chunk-entry-fit",
                        $"Column '{column.Name}' entry size {column.EntryBytes} bytes does not divide chunk size {inspect.Storage.ChunkSize}; trailing bytes will be unused."));
                }
            }

            if (inspect.Storage.Layout == TableStorageLayout.SingleFile
                && inspect.Storage.ChunkSize > 0
                && column.EntryBytes > inspect.Storage.ChunkSize)
            {
                findings.Add(ValidationFinding.Warning(
                    "single-file-entry-chunk-fit",
                    $"Column '{column.Name}' entry size {column.EntryBytes} bytes is larger than the single-file chunk alignment {inspect.Storage.ChunkSize} bytes."));
            }
        }
    }

    private static void ValidateSidecarConventions(MetaDbInspectReport inspect, List<ValidationFinding> findings)
    {
        if (inspect.Storage.Layout == TableStorageLayout.Chunked)
        {
            var expected = Path.Combine(Path.GetFullPath(inspect.MapPath), "tablespec.json");
            if (string.Equals(Path.GetFullPath(inspect.SpecPath), expected, StringComparison.OrdinalIgnoreCase))
                findings.Add(ValidationFinding.Info("sidecar-convention", "Chunked TableSpec uses tablespec.json inside the table directory."));
            else
                findings.Add(ValidationFinding.Warning("sidecar-convention", $"Chunked TableSpec is usually stored at '{expected}'."));

            return;
        }

        var expectedSidecar = Path.GetFullPath(inspect.MapPath) + ".tablespec.json";
        if (string.Equals(Path.GetFullPath(inspect.SpecPath), expectedSidecar, StringComparison.OrdinalIgnoreCase))
            findings.Add(ValidationFinding.Info("sidecar-convention", "Single-file TableSpec uses the map-path .tablespec.json sidecar."));
        else
            findings.Add(ValidationFinding.Warning("sidecar-convention", $"Single-file TableSpec is usually stored at '{expectedSidecar}'."));
    }

    private static long EstimateLogicalBytes(MetaDbInspectReport inspect) =>
        inspect.Columns.Sum(static column => checked((long)column.EntryBytes * column.InitialCapacity));

    private static long EstimateStorageBytes(MetaDbInspectReport inspect)
    {
        var logicalBytes = EstimateLogicalBytes(inspect);
        if (inspect.Storage.Layout == TableStorageLayout.SingleFile)
        {
            return inspect.Storage.ChunkSize > 0
                ? RoundUp(logicalBytes, inspect.Storage.ChunkSize)
                : logicalBytes;
        }

        if (inspect.Storage.ChunkSize <= 0)
            return logicalBytes;

        return inspect.Columns.Sum(column =>
        {
            var columnBytes = checked((long)column.EntryBytes * column.InitialCapacity);
            return RoundUp(columnBytes, inspect.Storage.ChunkSize);
        });
    }

    private static long RoundUp(long value, int multiple)
    {
        if (value == 0 || multiple <= 0)
            return value;

        var remainder = value % multiple;
        return remainder == 0 ? value : checked(value + multiple - remainder);
    }

    private static void WriteHumanReport(TextWriter output, MetaDbValidateReport report)
    {
        output.WriteLine("Extend0 MetaDB validate");
        output.WriteLine($"Input: {report.InputPath}");
        output.WriteLine($"Spec: {report.SpecPath}");
        output.WriteLine($"Name: {report.Name}");
        output.WriteLine($"MapPath: {report.MapPath}");
        output.WriteLine($"Storage: {report.Storage.Layout}");
        output.WriteLine($"ChunkSize: {report.Storage.ChunkSize}");
        output.WriteLine($"Columns: {report.ColumnCount}");
        output.WriteLine($"Estimated logical bytes: {report.EstimatedLogicalBytes}");
        output.WriteLine($"Estimated storage bytes: {report.EstimatedStorageBytes}");
        output.WriteLine();

        foreach (var finding in report.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.InfoCount} info, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static string FormatSeverity(ValidationSeverity severity) =>
        severity switch
        {
            ValidationSeverity.Info => "info",
            ValidationSeverity.Warning => "warn",
            ValidationSeverity.Error => "error",
            _ => severity.ToString().ToLowerInvariant()
        };

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 metadb validate <path> [--json]");
        writer.WriteLine();
        writer.WriteLine("Arguments:");
        writer.WriteLine("  <path>    TableSpec file, map file with .tablespec.json sidecar, or chunked table directory.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --json    Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help");
    }
}

public sealed record MetaDbValidateReport(
    string InputPath,
    string SpecPath,
    string Name,
    string MapPath,
    TableStorageOptions Storage,
    int ColumnCount,
    IReadOnlyList<MetaDbColumnReport> Columns,
    long EstimatedLogicalBytes,
    long EstimatedStorageBytes,
    IReadOnlyList<ValidationFinding> Findings,
    int InfoCount,
    int WarningCount,
    int ErrorCount)
{
    public static MetaDbValidateReport Create(
        string inputPath,
        string specPath,
        string name,
        string mapPath,
        TableStorageOptions storage,
        int columnCount,
        IReadOnlyList<MetaDbColumnReport> columns,
        long estimatedLogicalBytes,
        long estimatedStorageBytes,
        IReadOnlyList<ValidationFinding> findings) =>
        new(
            inputPath,
            specPath,
            name,
            mapPath,
            storage,
            columnCount,
            columns,
            estimatedLogicalBytes,
            estimatedStorageBytes,
            findings,
            findings.Count(static f => f.Severity == ValidationSeverity.Info),
            findings.Count(static f => f.Severity == ValidationSeverity.Warning),
            findings.Count(static f => f.Severity == ValidationSeverity.Error));
}
