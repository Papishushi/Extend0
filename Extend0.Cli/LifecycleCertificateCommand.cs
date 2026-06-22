using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Lifecycle.Certificates;

namespace Extend0.Cli;

public static class LifecycleCertificateCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static LifecycleCertificateCommand()
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

        if (args.Length == 0 || IsHelp(args[0]))
        {
            WriteHelp(output);
            return Task.FromResult(0);
        }

        var command = args[0];
        if (string.Equals(command, "dns-01", StringComparison.OrdinalIgnoreCase)
            || string.Equals(command, "dns01", StringComparison.OrdinalIgnoreCase))
        {
            return RunDns01Async(args[1..], output, error, workingDirectory, cancellationToken);
        }

        error.WriteLine($"Unknown lifecycle certificate command '{command}'.");
        error.WriteLine();
        WriteHelp(error);
        return Task.FromResult(2);
    }

    private static async Task<int> RunDns01Async(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        if (args.Length > 0 && LifecycleCertificateAcmeDns01Command.IsCommand(args[0]))
            return await LifecycleCertificateAcmeDns01Command.RunAsync(args, output, error, workingDirectory, cancellationToken).ConfigureAwait(false);

        if (!Dns01Options.TryParse(args, out var options, out var parseError))
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteDns01Help(error);
            return 2;
        }

        if (options.ShowHelp)
        {
            WriteDns01Help(output);
            return 0;
        }

        try
        {
            var challenge = options.KeyAuthorization is not null
                ? Dns01Challenge.Create(options.Domain!, options.Token!, options.KeyAuthorization)
                : Dns01Challenge.CreateFromAccountThumbprint(options.Domain!, options.Token!, options.AccountThumbprint!);

            var provisioningOptions = Dns01ProvisioningOptions.Create(options.TtlSeconds);
            var provisioner = new ManualDns01RecordProvisioner();
            var result = await provisioner.ProvisionAsync(challenge, provisioningOptions, cancellationToken).ConfigureAwait(false);
            var findings = new List<ValidationFinding>
            {
                ValidationFinding.Info("dns01-proof-created", $"Prepared DNS-01 TXT proof for '{result.Domain}'."),
                ValidationFinding.Info("manual-dns-provider", "No DNS provider API was called; publish the TXT record through your authoritative DNS provider."),
                ValidationFinding.Info("dns-propagation-required", "ACME validation should run only after the TXT record has propagated.")
            };

            var report = LifecycleCertificateDns01Report.Create(result, findings);
            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanDns01Report(output, report);

            return 0;
        }
        catch (ArgumentOutOfRangeException ex)
        {
            error.WriteLine(ex.Message);
            error.WriteLine();
            WriteDns01Help(error);
            return 2;
        }
        catch (ArgumentException ex)
        {
            error.WriteLine(ex.Message);
            error.WriteLine();
            WriteDns01Help(error);
            return 2;
        }
    }

    private static void WriteHumanDns01Report(TextWriter output, LifecycleCertificateDns01Report report)
    {
        output.WriteLine("Extend0 lifecycle certificate dns-01");
        output.WriteLine($"Provider: {report.ProviderName}");
        output.WriteLine($"Domain: {report.Domain}");
        output.WriteLine($"Authorization domain: {report.AuthorizationDomain}");
        output.WriteLine();
        output.WriteLine("TXT record:");
        output.WriteLine($"  Name: {report.RecordName}");
        output.WriteLine($"  Type: {report.RecordType}");
        output.WriteLine($"  Value: {report.RecordValue}");
        output.WriteLine($"  TTL: {report.TtlSeconds} seconds");
        output.WriteLine();
        output.WriteLine("Instructions:");
        foreach (var instruction in report.Instructions)
            output.WriteLine($"  - {instruction}");

        output.WriteLine();
        foreach (var finding in report.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.InfoCount} info, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static bool IsHelp(string arg) =>
        string.Equals(arg, "-h", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "--help", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "help", StringComparison.OrdinalIgnoreCase);

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
        writer.WriteLine("  extend0 lifecycle certificate dns-01 --domain <domain> --token <token> (--key-authorization <value> | --account-thumbprint <value>) [--ttl <seconds>] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 order --domain <domain> --email <email> --accept-terms --state <path> [--protect-state passphrase] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 validate --state <path> [--protect-state passphrase] [--wait-seconds <n>] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 finalize --state <path> --out <directory> [--protect-state passphrase] [--pfx-password <value>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Commands:");
        writer.WriteLine("  dns-01   Prepare DNS-01 TXT records or run the ACME DNS-01 order lifecycle.");
        writer.WriteLine();
        writer.WriteLine("Notes:");
        writer.WriteLine("  The direct --token form prepares a proof from an existing ACME challenge.");
        writer.WriteLine("  The order/validate/finalize subcommands create a real ACME DNS-01 certificate order.");
        writer.WriteLine("  Provider-specific DNS APIs are intentionally separate future layers.");
    }

    private static void WriteDns01Help(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 --domain <domain> --token <token> --key-authorization <value> [--ttl <seconds>] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 --domain <domain> --token <token> --account-thumbprint <value> [--ttl <seconds>] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 order --domain <domain> --email <email> --accept-terms --state <path> [--staging|--production] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 validate --state <path> [--wait-seconds <n>] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 finalize --state <path> --out <directory> [--pfx-password <value>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --domain <domain>              DNS name being authorized. Wildcards like *.example.com are normalized.");
        writer.WriteLine("  --token <token>                ACME challenge token.");
        writer.WriteLine("  --key-authorization <value>    Full ACME key authorization '<token>.<account-thumbprint>'.");
        writer.WriteLine("  --account-thumbprint <value>   Account JWK thumbprint; used to derive key authorization with --token.");
        writer.WriteLine("  --ttl <seconds>                Suggested TXT TTL. Defaults to 300.");
        writer.WriteLine("  --json                         Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help                     Show command help.");
    }

    private sealed record Dns01Options(
        string? Domain,
        string? Token,
        string? KeyAuthorization,
        string? AccountThumbprint,
        int TtlSeconds,
        bool Json,
        bool ShowHelp)
    {
        public static bool TryParse(string[] args, out Dns01Options options, out string error)
        {
            string? domain = null;
            string? token = null;
            string? keyAuthorization = null;
            string? accountThumbprint = null;
            var ttlSeconds = Dns01ProvisioningOptions.DefaultTtlSeconds;
            var json = false;
            var showHelp = false;

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                switch (arg)
                {
                    case "-h":
                    case "--help":
                        showHelp = true;
                        break;

                    case "--json":
                        json = true;
                        break;

                    case "--domain":
                        if (!TryReadValue(args, ref i, "--domain", out domain, out error))
                        {
                            options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
                            return false;
                        }

                        break;

                    case "--token":
                        if (!TryReadValue(args, ref i, "--token", out token, out error))
                        {
                            options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
                            return false;
                        }

                        break;

                    case "--key-authorization":
                    case "--key-auth":
                        if (!TryReadValue(args, ref i, arg, out keyAuthorization, out error))
                        {
                            options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
                            return false;
                        }

                        break;

                    case "--account-thumbprint":
                    case "--thumbprint":
                        if (!TryReadValue(args, ref i, arg, out accountThumbprint, out error))
                        {
                            options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
                            return false;
                        }

                        break;

                    case "--ttl":
                        if (!TryReadValue(args, ref i, "--ttl", out var ttlToken, out error))
                        {
                            options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
                            return false;
                        }

                        if (!int.TryParse(ttlToken, out ttlSeconds) || ttlSeconds <= 0)
                        {
                            options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
                            error = "--ttl requires a positive integer value in seconds.";
                            return false;
                        }

                        break;

                    default:
                        if (!arg.StartsWith("-", StringComparison.Ordinal) && domain is null)
                        {
                            domain = arg;
                            break;
                        }

                        options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
                        error = arg.StartsWith("-", StringComparison.Ordinal)
                            ? $"Unknown lifecycle certificate dns-01 option '{arg}'."
                            : $"Unexpected lifecycle certificate dns-01 argument '{arg}'.";
                        return false;
                }
            }

            options = Create(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);
            if (showHelp)
            {
                error = string.Empty;
                return true;
            }

            if (string.IsNullOrWhiteSpace(domain))
            {
                error = "--domain is required.";
                return false;
            }

            if (string.IsNullOrWhiteSpace(token))
            {
                error = "--token is required.";
                return false;
            }

            if (!string.IsNullOrWhiteSpace(keyAuthorization) && !string.IsNullOrWhiteSpace(accountThumbprint))
            {
                error = "Supply either --key-authorization or --account-thumbprint, not both.";
                return false;
            }

            if (string.IsNullOrWhiteSpace(keyAuthorization) && string.IsNullOrWhiteSpace(accountThumbprint))
            {
                error = "--key-authorization or --account-thumbprint is required.";
                return false;
            }

            error = string.Empty;
            return true;
        }

        private static Dns01Options Create(
            string? domain,
            string? token,
            string? keyAuthorization,
            string? accountThumbprint,
            int ttlSeconds,
            bool json,
            bool showHelp) =>
            new(domain, token, keyAuthorization, accountThumbprint, ttlSeconds, json, showHelp);

        private static bool TryReadValue(string[] args, ref int index, string option, out string value, out string error)
        {
            if (index + 1 >= args.Length)
            {
                value = string.Empty;
                error = $"{option} requires a value.";
                return false;
            }

            value = args[++index];
            if (string.IsNullOrWhiteSpace(value))
            {
                error = $"{option} cannot be empty.";
                return false;
            }

            error = string.Empty;
            return true;
        }
    }
}

public sealed record LifecycleCertificateDns01Report(
    string ProviderName,
    string Domain,
    string AuthorizationDomain,
    string RecordName,
    string RecordType,
    string RecordValue,
    int TtlSeconds,
    bool RequiresManualAction,
    DateTimeOffset PreparedAtUtc,
    IReadOnlyList<string> Instructions,
    IReadOnlyList<ValidationFinding> Findings,
    int InfoCount,
    int WarningCount,
    int ErrorCount)
{
    public static LifecycleCertificateDns01Report Create(
        Dns01ProvisioningResult result,
        IReadOnlyList<ValidationFinding> findings) =>
        new(
            result.ProviderName,
            result.Domain,
            result.AuthorizationDomain,
            result.RecordName,
            result.RecordType,
            result.RecordValue,
            result.TtlSeconds,
            result.RequiresManualAction,
            result.PreparedAtUtc,
            result.Instructions,
            findings,
            findings.Count(static finding => finding.Severity == ValidationSeverity.Info),
            findings.Count(static finding => finding.Severity == ValidationSeverity.Warning),
            findings.Count(static finding => finding.Severity == ValidationSeverity.Error));
}
