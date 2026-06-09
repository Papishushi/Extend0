using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Cli;

public static class LifecycleProbeCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static LifecycleProbeCommand()
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

        var parse = LifecycleProbeOptions.TryParse(args, workingDirectory, out var options, out var parseError);
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

        var report = BuildReport(options);
        if (options.Json)
            output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
        else
            WriteHumanReport(output, report);

        return Task.FromResult(report.ErrorCount > 0 || (report.ConnectAttempted && report.Connected != true) ? 1 : 0);
    }

    private static LifecycleProbeReport BuildReport(LifecycleProbeOptions options)
    {
        var findings = new List<ValidationFinding>
        {
            ValidationFinding.Info("ownership-non-mutating", "Probe does not acquire ownership or start an owner host.")
        };

        CrossProcessProtocolDescriptor? protocol = null;
        string? endpointName = null;
        var builtInClientAvailable = options.TransportKind == TransportKind.NamedPipe;
        bool? connected = null;
        string? connectError = null;

        try
        {
            var explicitProtocol = options.ProtocolId is null
                ? null
                : new CrossProcessProtocolDescriptor(options.TransportKind, options.ProtocolId, options.ProtocolVersion!.Value);
            protocol = CrossProcessTransportFactory.ResolveProtocolDescriptor(options.TransportKind, explicitProtocol, options.AllowCustom);
            findings.Add(ValidationFinding.Info("protocol-resolved", $"Resolved protocol '{protocol.ProtocolId}' version {protocol.ProtocolVersion}."));
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Error("protocol-resolution", ex.Message));
        }

        try
        {
            endpointName = CrossProcessTransportFactory.ResolveEndpointName(
                options.Name,
                options.TransportKind,
                options.EndpointName,
                allowLogicalFallback: options.AllowCustom);
            findings.Add(ValidationFinding.Info("endpoint-resolved", $"Resolved endpoint '{endpointName}'."));
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Error("endpoint-resolution", ex.Message));
        }

        if (!builtInClientAvailable)
            findings.Add(ValidationFinding.Warning("client-transport-not-built-in", $"Transport kind '{options.TransportKind}' does not have a built-in client transport."));

        if (options.Connect)
        {
            if (protocol is null || endpointName is null)
            {
                connected = false;
                connectError = "Connectivity was not attempted because protocol or endpoint resolution failed.";
                findings.Add(ValidationFinding.Error("connectivity-skipped", connectError));
            }
            else
            {
                try
                {
                    using var transport = CrossProcessTransportFactory.CreateClientTransport(
                        new ClientTransportFactoryContext(options.TransportKind, protocol, endpointName, options.ServerName, options.TimeoutMs));
                    connected = true;
                    findings.Add(ValidationFinding.Info("connectivity", $"Connected through {transport.Kind}."));
                }
                catch (Exception ex)
                {
                    connected = false;
                    connectError = ex.Message;
                    findings.Add(ValidationFinding.Error("connectivity", $"Connection or handshake failed: {ex.Message}"));
                }
            }
        }

        return LifecycleProbeReport.Create(
            options.Name,
            options.TransportKind,
            endpointName,
            options.ServerName,
            options.TimeoutMs,
            protocol?.ProtocolId,
            protocol?.ProtocolVersion,
            builtInClientAvailable,
            options.Connect,
            connected,
            connectError,
            findings);
    }

    private static void WriteHumanReport(TextWriter output, LifecycleProbeReport report)
    {
        output.WriteLine("Extend0 lifecycle probe");
        output.WriteLine($"Name: {report.Name}");
        output.WriteLine($"Transport: {report.TransportKind}");
        output.WriteLine($"Endpoint: {report.EndpointName ?? "<unresolved>"}");
        output.WriteLine($"Server: {report.ServerName}");
        output.WriteLine($"Timeout: {report.TimeoutMs} ms");
        output.WriteLine($"Protocol: {FormatProtocol(report)}");
        output.WriteLine($"Built-in client transport: {(report.BuiltInClientAvailable ? "yes" : "no")}");
        output.WriteLine($"Ownership: not acquired (probe is non-mutating)");
        output.WriteLine($"Connect attempted: {(report.ConnectAttempted ? "yes" : "no")}");

        if (report.ConnectAttempted)
        {
            output.WriteLine($"Connected: {report.Connected}");
            if (!string.IsNullOrWhiteSpace(report.ConnectError))
                output.WriteLine($"Connect error: {report.ConnectError}");
        }

        output.WriteLine();
        foreach (var finding in report.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.InfoCount} info, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static string FormatProtocol(LifecycleProbeReport report) =>
        report.ProtocolId is null
            ? "<unresolved>"
            : $"{report.ProtocolId} v{report.ProtocolVersion}";

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
        writer.WriteLine("  extend0 lifecycle probe [--name <identity>] [--transport <kind>] [--endpoint <name>] [--server <name>] [--timeout <ms>] [--connect] [--json]");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --name <identity>          Logical lifecycle service identity. Defaults to Extend0.Lifecycle.Probe.");
        writer.WriteLine("  --transport <kind>         TransportKind value. Defaults to NamedPipe.");
        writer.WriteLine("  --endpoint <name>          Explicit endpoint override.");
        writer.WriteLine("  --server <name>            Server or machine name for client probes. Defaults to '.'.");
        writer.WriteLine("  --timeout <ms>             Connection timeout in milliseconds. Defaults to 3000.");
        writer.WriteLine("  --protocol-id <id>         Explicit protocol id for custom transports.");
        writer.WriteLine("  --protocol-version <n>     Explicit protocol version for custom transports.");
        writer.WriteLine("  --allow-custom             Allow logical endpoint fallback for non-built-in transports.");
        writer.WriteLine("  --connect                  Attempt a real client connection and handshake.");
        writer.WriteLine("  --json                     Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help                 Show command help.");
    }
}

public sealed record LifecycleProbeReport(
    string Name,
    TransportKind TransportKind,
    string? EndpointName,
    string ServerName,
    int TimeoutMs,
    string? ProtocolId,
    int? ProtocolVersion,
    bool BuiltInClientAvailable,
    bool ConnectAttempted,
    bool? Connected,
    string? ConnectError,
    IReadOnlyList<ValidationFinding> Findings,
    int InfoCount,
    int WarningCount,
    int ErrorCount)
{
    public static LifecycleProbeReport Create(
        string name,
        TransportKind transportKind,
        string? endpointName,
        string serverName,
        int timeoutMs,
        string? protocolId,
        int? protocolVersion,
        bool builtInClientAvailable,
        bool connectAttempted,
        bool? connected,
        string? connectError,
        IReadOnlyList<ValidationFinding> findings) =>
        new(
            name,
            transportKind,
            endpointName,
            serverName,
            timeoutMs,
            protocolId,
            protocolVersion,
            builtInClientAvailable,
            connectAttempted,
            connected,
            connectError,
            findings,
            findings.Count(static f => f.Severity == ValidationSeverity.Info),
            findings.Count(static f => f.Severity == ValidationSeverity.Warning),
            findings.Count(static f => f.Severity == ValidationSeverity.Error));
}
