using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Cli;

public static class LifecycleDiagnoseCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static LifecycleDiagnoseCommand()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public static async Task<int> RunAsync(
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

        var parse = LifecycleProbeOptions.TryParse(args, workingDirectory, out var options, out var parseError, commandName: "diagnose");
        if (!parse)
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteHelp(error);
            return 2;
        }

        if (options.ShowHelp)
        {
            WriteHelp(output);
            return 0;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var report = await BuildReportAsync(options, cancellationToken).ConfigureAwait(false);
        if (options.Json)
            output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
        else
            WriteHumanReport(output, report);

        return report.ErrorCount > 0 ? 1 : 0;
    }

    private static async Task<LifecycleDiagnoseReport> BuildReportAsync(LifecycleProbeOptions options, CancellationToken cancellationToken)
    {
        var findings = new List<ValidationFinding>
        {
            ValidationFinding.Info("diagnostics-connects", "Diagnose attempts a real client connection, handshake, service-info call, and heartbeat call.")
        };

        CrossProcessProtocolDescriptor? protocol = null;
        string? endpointName = null;
        var builtInClientAvailable = options.TransportKind is
            TransportKind.NamedPipe or
            TransportKind.UnixDomainSocket or
            TransportKind.TcpSocket or
            TransportKind.TlsTcpSocket;
        var ownerStatus = LifecycleOwnerStatus.Unknown;
        var handshakeStatus = LifecycleHandshakeStatus.NotAttempted;
        var heartbeatStatus = LifecycleHeartbeatStatus.Unknown;
        var leaseStatus = LifecycleLeaseStatus.NotExposed;
        string? connectError = null;
        string? handshakeError = null;
        ServiceInfo? owner = null;
        Heartbeat? heartbeat = null;
        Lease? lease = null;
        long? heartbeatAgeMilliseconds = null;
        bool? ownerReportedCanConnect = null;

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
            endpointName = LifecycleEndpointResolver.ResolveEndpointName(options);
            findings.Add(ValidationFinding.Info("endpoint-resolved", $"Resolved endpoint '{endpointName}'."));
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Error("endpoint-resolution", ex.Message));
        }

        if (!builtInClientAvailable)
        {
            findings.Add(ValidationFinding.Error(
                "client-transport-not-built-in",
                $"Transport kind '{options.TransportKind}' cannot be diagnosed by the CLI without an injected client transport factory."));
        }

        if (protocol is null || endpointName is null || !builtInClientAvailable)
        {
            findings.Add(ValidationFinding.Error("diagnostics-skipped", "Diagnostics were not attempted because protocol, endpoint, or client transport resolution failed."));
            return LifecycleDiagnoseReport.Create(
                options,
                endpointName,
                protocol,
                builtInClientAvailable,
                ownerStatus,
                handshakeStatus,
                heartbeatStatus,
                leaseStatus,
                false,
                connectError,
                handshakeError,
                owner,
                heartbeat,
                lease,
                heartbeatAgeMilliseconds,
                ownerReportedCanConnect,
                findings);
        }

        try
        {
            using var rpcTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            rpcTimeout.CancelAfter(options.TimeoutMs);
            using var transport = CrossProcessTransportFactory.CreateClientTransport(
                new ClientTransportFactoryContext(options.TransportKind, protocol, endpointName, options.ServerName, options.TimeoutMs, options.ToAuthenticationOptions(), options.ToTlsOptions()));

            handshakeStatus = LifecycleHandshakeStatus.Passed;
            ownerStatus = LifecycleOwnerStatus.OwnerObserved;
            leaseStatus = LifecycleLeaseStatus.ImpliedByOwnerObservation;
            findings.Add(ValidationFinding.Info("handshake", $"Handshake passed for {options.TransportKind} using protocol '{protocol.ProtocolId}' v{protocol.ProtocolVersion}."));
            findings.Add(ValidationFinding.Info("owner-observed", "A Lifecycle owner responded on the resolved endpoint."));

            var proxy = RpcDispatchProxy<ICrossProcessService>.Create(transport, rpcTimeout.Token);

            owner = await DiagnoseServiceInfoAsync(proxy, options, endpointName, findings).ConfigureAwait(false);
            (leaseStatus, lease) = await DiagnoseLeaseAsync(proxy, owner, findings).ConfigureAwait(false);
            (heartbeatStatus, heartbeat, heartbeatAgeMilliseconds) = await DiagnoseHeartbeatAsync(proxy, owner, findings).ConfigureAwait(false);
            ownerReportedCanConnect = await DiagnoseOwnerConnectivityAsync(proxy, findings).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsHandshakeFailure(ex))
        {
            handshakeStatus = LifecycleHandshakeStatus.Failed;
            ownerStatus = LifecycleOwnerStatus.Unknown;
            connectError = ex.Message;
            handshakeError = ex.Message;
            findings.Add(ValidationFinding.Error("handshake", $"Connection reached an endpoint, but the handshake failed: {ex.Message}"));
        }
        catch (Exception ex)
        {
            handshakeStatus = LifecycleHandshakeStatus.NotAttempted;
            ownerStatus = LifecycleOwnerStatus.NoReachableOwner;
            connectError = ex.Message;
            findings.Add(ValidationFinding.Error("owner-not-reachable", $"No compatible Lifecycle owner could be reached: {ex.Message}"));
            LifecycleNamedPipeDiscovery.AddContractScopedCandidateFindings(options, endpointName, findings);
        }

        return LifecycleDiagnoseReport.Create(
            options,
            endpointName,
            protocol,
            builtInClientAvailable,
            ownerStatus,
            handshakeStatus,
            heartbeatStatus,
            leaseStatus,
            ownerStatus == LifecycleOwnerStatus.OwnerObserved,
            connectError,
            handshakeError,
            owner,
            heartbeat,
            lease,
            heartbeatAgeMilliseconds,
            ownerReportedCanConnect,
            findings);
    }

    private static async Task<ServiceInfo?> DiagnoseServiceInfoAsync(
        ICrossProcessService proxy,
        LifecycleProbeOptions options,
        string endpointName,
        List<ValidationFinding> findings)
    {
        try
        {
            var owner = await proxy.GetServiceInfoAsync().ConfigureAwait(false);
            findings.Add(ValidationFinding.Info(
                "owner-info",
                $"Owner is {owner.MachineName}/{owner.ProcessName} pid {owner.ProcessId}, implementation '{owner.ImplementationName}'."));

            if (owner.TransportKind != options.TransportKind)
            {
                findings.Add(ValidationFinding.Warning(
                    "owner-transport-mismatch",
                    $"Owner reports transport '{owner.TransportKind}', but diagnostics expected '{options.TransportKind}'."));
            }

            if (!string.IsNullOrWhiteSpace(owner.EndpointName)
                && !string.Equals(owner.EndpointName, endpointName, StringComparison.Ordinal))
            {
                findings.Add(ValidationFinding.Warning(
                    "owner-endpoint-mismatch",
                    $"Owner reports endpoint '{owner.EndpointName}', but diagnostics connected to '{endpointName}'."));
            }

            return owner;
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Error("owner-info", $"Owner was reachable but service info failed: {ex.Message}"));
            return null;
        }
    }

    private static async Task<(LifecycleLeaseStatus Status, Lease? Lease)> DiagnoseLeaseAsync(
        ICrossProcessService proxy,
        ServiceInfo? owner,
        List<ValidationFinding> findings)
    {
        try
        {
            var lease = await proxy.GetLeaseAsync().ConfigureAwait(false);
            findings.Add(ValidationFinding.Info(
                "lease",
                $"Lease is {(lease.IsActive ? "active" : "inactive")} for ownership '{lease.OwnershipName}' using {lease.CoordinationKind}."));

            if (owner is not null && !string.Equals(owner.Fingerprint, lease.Fingerprint, StringComparison.Ordinal))
            {
                findings.Add(ValidationFinding.Warning(
                    "lease-fingerprint-mismatch",
                    $"ServiceInfo fingerprint '{owner.Fingerprint}' differs from lease fingerprint '{lease.Fingerprint}'."));
            }

            return (lease.IsActive ? LifecycleLeaseStatus.Active : LifecycleLeaseStatus.Inactive, lease);
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Warning(
                "lease-not-exposed",
                $"Owner was reachable but did not expose a lease snapshot: {ex.Message}. Falling back to owner-observation semantics."));
            return (LifecycleLeaseStatus.ImpliedByOwnerObservation, null);
        }
    }

    private static async Task<(LifecycleHeartbeatStatus Status, Heartbeat? Heartbeat, long? AgeMilliseconds)> DiagnoseHeartbeatAsync(
        ICrossProcessService proxy,
        ServiceInfo? owner,
        List<ValidationFinding> findings)
    {
        try
        {
            var heartbeat = await proxy.PingAsync().ConfigureAwait(false);
            var ageMilliseconds = Math.Abs((long)(DateTimeOffset.UtcNow - heartbeat.UtcTime).TotalMilliseconds);
            findings.Add(ValidationFinding.Info(
                "heartbeat",
                $"Heartbeat is alive; owner uptime is {heartbeat.UptimeSeconds}s and observed age is {ageMilliseconds}ms."));

            if (owner is not null && !string.Equals(owner.Fingerprint, heartbeat.Fingerprint, StringComparison.Ordinal))
            {
                findings.Add(ValidationFinding.Warning(
                    "fingerprint-mismatch",
                    $"ServiceInfo fingerprint '{owner.Fingerprint}' differs from heartbeat fingerprint '{heartbeat.Fingerprint}'."));
            }

            return (LifecycleHeartbeatStatus.Alive, heartbeat, ageMilliseconds);
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Error("heartbeat", $"Owner was reachable but heartbeat failed: {ex.Message}"));
            return (LifecycleHeartbeatStatus.Failed, null, null);
        }
    }

    private static async Task<bool?> DiagnoseOwnerConnectivityAsync(ICrossProcessService proxy, List<ValidationFinding> findings)
    {
        try
        {
            var canConnect = await proxy.CanConnectAsync().ConfigureAwait(false);
            if (canConnect)
            {
                findings.Add(ValidationFinding.Info("owner-connectivity", "Owner-reported endpoint connectivity probe returned true."));
            }
            else
            {
                findings.Add(ValidationFinding.Warning(
                    "owner-connectivity",
                    "Owner-reported endpoint connectivity probe returned false. This can indicate a stale endpoint, transport-specific probe gap, or owner-side self-probe failure."));
            }

            return canConnect;
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Warning("owner-connectivity", $"Owner-reported connectivity probe failed: {ex.Message}"));
            return null;
        }
    }

    private static bool IsHandshakeFailure(Exception ex) =>
        ex is IOException
        && (ex.Message.Contains("handshake", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("authentication", StringComparison.OrdinalIgnoreCase));

    private static void WriteHumanReport(TextWriter output, LifecycleDiagnoseReport report)
    {
        output.WriteLine("Extend0 lifecycle diagnose");
        output.WriteLine($"Name: {report.Name}");
        output.WriteLine($"Contract: {report.ContractKind}");
        output.WriteLine($"Transport: {report.TransportKind}");
        output.WriteLine($"Endpoint: {report.EndpointName ?? "<unresolved>"}");
        output.WriteLine($"Server: {report.ServerName}");
        output.WriteLine($"Timeout: {report.TimeoutMs} ms");
        output.WriteLine($"Protocol: {FormatProtocol(report)}");
        output.WriteLine($"Authentication: {report.AuthenticationMode}");
        output.WriteLine($"Built-in client transport: {(report.BuiltInClientAvailable ? "yes" : "no")}");
        output.WriteLine($"Owner status: {report.OwnerStatus}");
        output.WriteLine($"Owner reachable: {report.OwnerReachable}");
        output.WriteLine($"Handshake: {report.HandshakeStatus}");
        output.WriteLine($"Lease: {report.LeaseStatus}");
        output.WriteLine($"Heartbeat: {report.HeartbeatStatus}");

        if (report.Owner is not null)
        {
            output.WriteLine();
            output.WriteLine("Owner:");
            output.WriteLine($"  Contract: {report.Owner.ContractName}");
            output.WriteLine($"  Implementation: {report.Owner.ImplementationName}");
            output.WriteLine($"  Machine/process: {report.Owner.MachineName}/{report.Owner.ProcessName} pid {report.Owner.ProcessId}");
            output.WriteLine($"  Started UTC: {report.Owner.StartTimeUtc:O}");
            output.WriteLine($"  Fingerprint: {report.Owner.Fingerprint}");
            output.WriteLine($"  Endpoint: {report.Owner.EndpointServerName ?? "<none>"}/{report.Owner.EndpointName ?? "<none>"}");
        }

        if (report.Lease is not null)
        {
            output.WriteLine();
            output.WriteLine("Lease:");
            output.WriteLine($"  Id: {report.Lease.LeaseId}");
            output.WriteLine($"  Ownership: {report.Lease.OwnershipName}");
            output.WriteLine($"  Coordination: {report.Lease.CoordinationKind} {report.Lease.CoordinationScope ?? "<none>"}");
            output.WriteLine($"  Active: {report.Lease.IsActive}");
            output.WriteLine($"  Exclusive: {report.Lease.IsExclusive}");
            output.WriteLine($"  Acquired UTC: {report.Lease.AcquiredUtc:O}");
            output.WriteLine($"  Observed UTC: {report.Lease.ObservedUtc:O}");
            output.WriteLine($"  Expires UTC: {(report.Lease.ExpiresUtc is null ? "<none>" : report.Lease.ExpiresUtc.Value.ToString("O"))}");
        }

        if (report.Heartbeat is not null)
        {
            output.WriteLine();
            output.WriteLine("Heartbeat:");
            output.WriteLine($"  UTC: {report.Heartbeat.UtcTime:O}");
            output.WriteLine($"  Uptime seconds: {report.Heartbeat.UptimeSeconds}");
            output.WriteLine($"  Observed age ms: {report.HeartbeatAgeMilliseconds}");
        }

        if (!string.IsNullOrWhiteSpace(report.ConnectError))
            output.WriteLine($"Connect error: {report.ConnectError}");
        if (!string.IsNullOrWhiteSpace(report.HandshakeError))
            output.WriteLine($"Handshake error: {report.HandshakeError}");

        output.WriteLine();
        foreach (var finding in report.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.InfoCount} info, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static string FormatProtocol(LifecycleDiagnoseReport report) =>
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
        writer.WriteLine("  extend0 lifecycle diagnose [--contract <kind>] [--name <identity>] [--transport <kind>] [--endpoint <name>] [--server <name>] [--timeout <ms>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --contract <kind>          Contract scope. Built-ins: Probe, MetaDB. Defaults to Probe.");
        writer.WriteLine("  --name <identity>          Logical lifecycle service identity. Defaults to Extend0.Lifecycle.Probe, or Extend0.MetaDB for --contract MetaDB.");
        writer.WriteLine("  --transport <kind>         TransportKind value. Built-ins: NamedPipe, UnixDomainSocket, TcpSocket, TlsTcpSocket. Defaults to NamedPipe.");
        writer.WriteLine("  --endpoint <name>          Explicit endpoint override. TcpSocket/TlsTcpSocket require host:port; UnixDomainSocket accepts a socket path.");
        writer.WriteLine("  --tls-target-host <name>   Target host for TLS certificate validation when using TlsTcpSocket.");
        writer.WriteLine("  --server <name>            Server or machine name for client diagnostics. Defaults to '.'.");
        writer.WriteLine("  --timeout <ms>             Connection and RPC timeout in milliseconds. Defaults to 3000.");
        writer.WriteLine("  --protocol-id <id>         Explicit protocol id for custom transports.");
        writer.WriteLine("  --protocol-version <n>     Explicit protocol version for custom transports.");
        writer.WriteLine("  --auth <mode>              Authentication mode. Supported: none, shared-secret-hmac.");
        writer.WriteLine("  --secret <value>           Shared secret for --auth shared-secret-hmac. The value is never printed.");
        writer.WriteLine("  --allow-custom             Allow logical endpoint fallback for non-built-in transports.");
        writer.WriteLine("  --json                     Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help                 Show command help.");
    }
}

public enum LifecycleOwnerStatus
{
    Unknown,
    NoReachableOwner,
    OwnerObserved
}

public enum LifecycleHandshakeStatus
{
    NotAttempted,
    Passed,
    Failed
}

public enum LifecycleHeartbeatStatus
{
    Unknown,
    Alive,
    Failed
}

public enum LifecycleLeaseStatus
{
    NotExposed,
    ImpliedByOwnerObservation,
    Active,
    Inactive,
    Failed
}

public sealed record LifecycleDiagnoseReport(
    string Name,
    LifecycleContractKind ContractKind,
    TransportKind TransportKind,
    string? EndpointName,
    string ServerName,
    int TimeoutMs,
    string? ProtocolId,
    int? ProtocolVersion,
    AuthenticationMode AuthenticationMode,
    bool BuiltInClientAvailable,
    LifecycleOwnerStatus OwnerStatus,
    LifecycleHandshakeStatus HandshakeStatus,
    LifecycleHeartbeatStatus HeartbeatStatus,
    LifecycleLeaseStatus LeaseStatus,
    bool OwnerReachable,
    string? ConnectError,
    string? HandshakeError,
    ServiceInfo? Owner,
    Heartbeat? Heartbeat,
    Lease? Lease,
    long? HeartbeatAgeMilliseconds,
    bool? OwnerReportedCanConnect,
    IReadOnlyList<ValidationFinding> Findings,
    int InfoCount,
    int WarningCount,
    int ErrorCount)
{
    internal static LifecycleDiagnoseReport Create(
        LifecycleProbeOptions options,
        string? endpointName,
        CrossProcessProtocolDescriptor? protocol,
        bool builtInClientAvailable,
        LifecycleOwnerStatus ownerStatus,
        LifecycleHandshakeStatus handshakeStatus,
        LifecycleHeartbeatStatus heartbeatStatus,
        LifecycleLeaseStatus leaseStatus,
        bool ownerReachable,
        string? connectError,
        string? handshakeError,
        ServiceInfo? owner,
        Heartbeat? heartbeat,
        Lease? lease,
        long? heartbeatAgeMilliseconds,
        bool? ownerReportedCanConnect,
        IReadOnlyList<ValidationFinding> findings) =>
        new(
            options.Name,
            options.ContractKind,
            options.TransportKind,
            endpointName,
            options.ServerName,
            options.TimeoutMs,
            protocol?.ProtocolId,
            protocol?.ProtocolVersion,
            options.AuthenticationMode,
            builtInClientAvailable,
            ownerStatus,
            handshakeStatus,
            heartbeatStatus,
            leaseStatus,
            ownerReachable,
            connectError,
            handshakeError,
            owner,
            heartbeat,
            lease,
            heartbeatAgeMilliseconds,
            ownerReportedCanConnect,
            findings,
            findings.Count(static f => f.Severity == ValidationSeverity.Info),
            findings.Count(static f => f.Severity == ValidationSeverity.Warning),
            findings.Count(static f => f.Severity == ValidationSeverity.Error));
}
