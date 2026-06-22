using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Lifecycle.Assurance;

namespace Extend0.Cli;

public static class StorageDiagnoseCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static StorageDiagnoseCommand()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public static Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken = default) =>
        RunAsync(
            args,
            output,
            error,
            workingDirectory,
            "Extend0 storage diagnose",
            "extend0 storage diagnose",
            cancellationToken);

    internal static Task<int> RunLifecycleAssuranceAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken = default) =>
        RunAsync(
            args,
            output,
            error,
            workingDirectory,
            "Extend0 lifecycle assurance storage diagnose",
            "extend0 lifecycle assurance storage diagnose",
            cancellationToken);

    private static Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        string reportTitle,
        string usageCommand,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentException.ThrowIfNullOrWhiteSpace(workingDirectory);
        ArgumentException.ThrowIfNullOrWhiteSpace(reportTitle);
        ArgumentException.ThrowIfNullOrWhiteSpace(usageCommand);

        var parse = StorageDiagnoseOptions.TryParse(args, workingDirectory, out var options, out var parseError);
        if (!parse)
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteHelp(error, usageCommand);
            return Task.FromResult(2);
        }

        if (options.ShowHelp)
        {
            WriteHelp(output, usageCommand);
            return Task.FromResult(0);
        }

        cancellationToken.ThrowIfCancellationRequested();

        var evidence = StorageProtectionVerifier.DiagnosePath(
            options.InputPath!,
            options.ToPolicy(),
            options.ManifestPath);

        if (options.Json)
            output.WriteLine(JsonSerializer.Serialize(evidence, JsonOptions));
        else
            WriteHumanReport(output, evidence, reportTitle);

        return Task.FromResult(evidence.Decision == StorageProtectionDecision.FailClosed ? 1 : 0);
    }

    internal static void WriteHumanReport(TextWriter output, StorageProtectionEvidence evidence, string title = "Extend0 storage diagnose")
    {
        output.WriteLine(title);
        output.WriteLine($"Path: {evidence.FullPath}");
        output.WriteLine($"Required protection: {evidence.Policy.RequiredLevel}");
        output.WriteLine($"Observed protection: {evidence.ObservedLevel}");
        output.WriteLine($"Decision: {evidence.Decision}");
        output.WriteLine($"Evidence source: {evidence.EvidenceSource}");
        if (!string.IsNullOrWhiteSpace(evidence.ProviderId))
            output.WriteLine($"Provider: {evidence.ProviderId}");
        if (!string.IsNullOrWhiteSpace(evidence.ProviderVersion))
            output.WriteLine($"Provider version: {evidence.ProviderVersion}");
        if (!string.IsNullOrWhiteSpace(evidence.ProtectionId))
            output.WriteLine($"Protection id: {evidence.ProtectionId}");
        if (!string.IsNullOrWhiteSpace(evidence.MountRoot))
            output.WriteLine($"Mount root: {evidence.MountRoot}");
        output.WriteLine($"Path inside mount: {evidence.PathInsideMount}");
        output.WriteLine($"Verified UTC: {evidence.VerifiedAtUtc:O}");
        output.WriteLine();

        foreach (var finding in evidence.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {evidence.InfoCount} info, {evidence.WarningCount} warnings, {evidence.ErrorCount} errors");
    }

    private static string FormatSeverity(StorageProtectionFindingSeverity severity) =>
        severity switch
        {
            StorageProtectionFindingSeverity.Info => "info",
            StorageProtectionFindingSeverity.Warning => "warn",
            StorageProtectionFindingSeverity.Error => "error",
            _ => severity.ToString().ToLowerInvariant()
        };

    private static void WriteHelp(TextWriter writer, string usageCommand = "extend0 storage diagnose")
    {
        writer.WriteLine("Usage:");
        writer.WriteLine($"  {usageCommand} <path> [--require <level>] [--provider <id>] [--protection-id <id>] [--manifest <path>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Arguments:");
        writer.WriteLine("  <path>    File or directory path whose storage protection posture should be evaluated.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --require <level>       Required level: none, declared, provider-attested, platform-verified, managed.");
        writer.WriteLine("  --provider <id>         Required provider id. Requires --require.");
        writer.WriteLine("  --protection-id <id>    Required protected volume/mount id. Requires --require.");
        writer.WriteLine("  --manifest <path>       Explicit storage protection manifest. Defaults to nearest .extend0-protection.json.");
        writer.WriteLine("  --json                  Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help");
    }
}
