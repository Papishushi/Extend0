using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Lifecycle.Certificates;

namespace Extend0.Cli;

internal static class LifecycleCertificateAcmeDns01Command
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static LifecycleCertificateAcmeDns01Command()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public static bool IsCommand(string arg) =>
        string.Equals(arg, "order", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "validate", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "status", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "finalize", StringComparison.OrdinalIgnoreCase);

    public static async Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken = default)
    {
        if (args.Length == 0 || IsHelp(args[0]))
        {
            WriteHelp(output);
            return 0;
        }

        var command = args[0];
        if (string.Equals(command, "order", StringComparison.OrdinalIgnoreCase))
            return await RunOrderAsync(args[1..], output, error, workingDirectory, cancellationToken).ConfigureAwait(false);
        if (string.Equals(command, "validate", StringComparison.OrdinalIgnoreCase))
            return await RunValidateAsync(args[1..], output, error, workingDirectory, cancellationToken).ConfigureAwait(false);
        if (string.Equals(command, "status", StringComparison.OrdinalIgnoreCase))
            return await RunStatusAsync(args[1..], output, error, workingDirectory, cancellationToken).ConfigureAwait(false);
        if (string.Equals(command, "finalize", StringComparison.OrdinalIgnoreCase))
            return await RunFinalizeAsync(args[1..], output, error, workingDirectory, cancellationToken).ConfigureAwait(false);

        error.WriteLine($"Unknown lifecycle certificate dns-01 command '{command}'.");
        error.WriteLine();
        WriteHelp(error);
        return 2;
    }

    private static async Task<int> RunOrderAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        if (!OrderOptions.TryParse(args, out var options, out var parseError))
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteOrderHelp(error);
            return 2;
        }

        if (options.ShowHelp)
        {
            WriteOrderHelp(output);
            return 0;
        }

        try
        {
            var directoryUrl = AcmeCertificateAuthority.ResolveDirectoryUrl(options.DirectoryUrl, options.Staging, options.Production);
            var request = AcmeDns01OrderRequest.Create(
                directoryUrl,
                options.Domains,
                options.Email!,
                options.AcceptTerms,
                options.AccountKeyBits,
                options.CertificateKeyBits);
            var client = new AcmeDns01Client();
            var state = await client.CreateOrderAsync(request, cancellationToken).ConfigureAwait(false);
            var statePath = ResolvePath(workingDirectory, options.StatePath!);
            var stateProtection = options.ToStateProtectionOptions();
            AcmeDns01StateFile.Save(statePath, state, stateProtection);

            var report = AcmeDns01CliReport.Create(
                "order",
                statePath,
                stateProtection.Kind,
                state,
                files: null,
                [
                    ValidationFinding.Info("acme-order-created", $"Created ACME DNS-01 order '{state.OrderUrl}'."),
                    ValidationFinding.Info("state-saved", $"Saved ACME DNS-01 state to '{statePath}' with {stateProtection.Kind} protection."),
                    ValidationFinding.Info("publish-dns", "Publish all TXT records before running dns-01 validate.")
                ]);

            WriteReport(output, report, options.Json, "Extend0 lifecycle certificate dns-01 order");
            return 0;
        }
        catch (Exception ex) when (IsRuntimeFailure(ex))
        {
            error.WriteLine(ex.Message);
            return 1;
        }
    }

    private static async Task<int> RunValidateAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        if (!StateCommandOptions.TryParse(args, requireOut: false, out var options, out var parseError))
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteValidateHelp(error);
            return 2;
        }

        if (options.ShowHelp)
        {
            WriteValidateHelp(output);
            return 0;
        }

        try
        {
            var statePath = ResolvePath(workingDirectory, options.StatePath!);
            var stateProtection = options.ToStateProtectionOptions();
            var state = AcmeDns01StateFile.Load(statePath, stateProtection, out var detectedProtection);
            var saveProtection = ResolveSaveProtection(stateProtection, detectedProtection);
            var client = new AcmeDns01Client();
            var updated = await client.RequestValidationAsync(
                state,
                TimeSpan.FromSeconds(options.WaitSeconds),
                TimeSpan.FromSeconds(options.PollSeconds),
                cancellationToken).ConfigureAwait(false);
            AcmeDns01StateFile.Save(statePath, updated, saveProtection);

            var report = AcmeDns01CliReport.Create(
                "validate",
                statePath,
                saveProtection.Kind,
                updated,
                files: null,
                [
                    ValidationFinding.Info("acme-validation-requested", "Requested ACME DNS-01 validation for pending authorizations."),
                    ValidationFinding.Info("state-saved", $"Saved refreshed ACME DNS-01 state to '{statePath}' with {saveProtection.Kind} protection.")
                ]);

            WriteReport(output, report, options.Json, "Extend0 lifecycle certificate dns-01 validate");
            return IsOrderFailed(updated) ? 1 : 0;
        }
        catch (Exception ex) when (IsRuntimeFailure(ex))
        {
            error.WriteLine(ex.Message);
            return 1;
        }
    }

    private static async Task<int> RunStatusAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        if (!StateCommandOptions.TryParse(args, requireOut: false, out var options, out var parseError))
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteStatusHelp(error);
            return 2;
        }

        if (options.ShowHelp)
        {
            WriteStatusHelp(output);
            return 0;
        }

        try
        {
            var statePath = ResolvePath(workingDirectory, options.StatePath!);
            var stateProtection = options.ToStateProtectionOptions();
            var state = AcmeDns01StateFile.Load(statePath, stateProtection, out var detectedProtection);
            var saveProtection = ResolveSaveProtection(stateProtection, detectedProtection);
            var client = new AcmeDns01Client();
            var updated = await client.RefreshOrderAsync(state, cancellationToken).ConfigureAwait(false);
            AcmeDns01StateFile.Save(statePath, updated, saveProtection);

            var report = AcmeDns01CliReport.Create(
                "status",
                statePath,
                saveProtection.Kind,
                updated,
                files: null,
                [ValidationFinding.Info("acme-status-refreshed", "Refreshed ACME order and authorization status.")]);

            WriteReport(output, report, options.Json, "Extend0 lifecycle certificate dns-01 status");
            return IsOrderFailed(updated) ? 1 : 0;
        }
        catch (Exception ex) when (IsRuntimeFailure(ex))
        {
            error.WriteLine(ex.Message);
            return 1;
        }
    }

    private static async Task<int> RunFinalizeAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        if (!StateCommandOptions.TryParse(args, requireOut: true, out var options, out var parseError))
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteFinalizeHelp(error);
            return 2;
        }

        if (options.ShowHelp)
        {
            WriteFinalizeHelp(output);
            return 0;
        }

        try
        {
            var statePath = ResolvePath(workingDirectory, options.StatePath!);
            var outputDirectory = ResolvePath(workingDirectory, options.OutputDirectory!);
            var stateProtection = options.ToStateProtectionOptions();
            var state = AcmeDns01StateFile.Load(statePath, stateProtection, out var detectedProtection);
            var saveProtection = ResolveSaveProtection(stateProtection, detectedProtection);
            var client = new AcmeDns01Client();
            var result = await client.FinalizeAsync(
                state,
                TimeSpan.FromSeconds(options.WaitSeconds),
                TimeSpan.FromSeconds(options.PollSeconds),
                cancellationToken).ConfigureAwait(false);
            AcmeDns01StateFile.Save(statePath, result.State, saveProtection);
            var files = AcmeDns01Client.WriteCertificateFiles(result, outputDirectory, options.PfxPassword);

            var report = AcmeDns01CliReport.Create(
                "finalize",
                statePath,
                saveProtection.Kind,
                result.State,
                files,
                [
                    ValidationFinding.Info("acme-order-finalized", "Finalized ACME order and downloaded certificate chain."),
                    ValidationFinding.Info("certificate-files-written", $"Wrote certificate files under '{outputDirectory}'.")
                ]);

            WriteReport(output, report, options.Json, "Extend0 lifecycle certificate dns-01 finalize");
            return 0;
        }
        catch (Exception ex) when (IsRuntimeFailure(ex))
        {
            error.WriteLine(ex.Message);
            return 1;
        }
    }

    private static void WriteReport(TextWriter output, AcmeDns01CliReport report, bool json, string title)
    {
        if (json)
        {
            output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            return;
        }

        output.WriteLine(title);
        output.WriteLine($"Command: {report.Command}");
        output.WriteLine($"Directory: {report.DirectoryUrl}");
        output.WriteLine($"State: {report.StatePath}");
        output.WriteLine($"State protection: {report.StateProtectionKind}");
        output.WriteLine($"Order: {report.OrderUrl}");
        output.WriteLine($"Order status: {report.OrderStatus}");
        output.WriteLine($"Certificate URL: {report.CertificateUrl ?? "<not issued>"}");
        output.WriteLine();
        output.WriteLine("TXT records:");
        foreach (var record in report.TxtRecords)
        {
            output.WriteLine($"  Domain: {record.Domain}");
            output.WriteLine($"  Status: {record.AuthorizationStatus}");
            output.WriteLine($"  Name: {record.RecordName}");
            output.WriteLine($"  Type: TXT");
            output.WriteLine($"  Value: {record.RecordValue}");
        }

        if (report.Files is not null)
        {
            output.WriteLine();
            output.WriteLine("Files:");
            output.WriteLine($"  Certificate chain: {report.Files.CertificateChainPath}");
            output.WriteLine($"  Private key: {report.Files.PrivateKeyPath}");
            output.WriteLine($"  PFX: {report.Files.PfxPath ?? "<not requested>"}");
        }

        output.WriteLine();
        output.WriteLine("Next steps:");
        foreach (var instruction in report.Instructions)
            output.WriteLine($"  - {instruction}");

        output.WriteLine();
        foreach (var finding in report.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.InfoCount} info, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static bool IsRuntimeFailure(Exception ex) =>
        ex is ArgumentException
            or ArgumentOutOfRangeException
            or InvalidOperationException
            or InvalidDataException
            or HttpRequestException
            or JsonException;

    private static bool IsOrderFailed(AcmeDns01OrderState state) =>
        string.Equals(state.OrderStatus, "invalid", StringComparison.OrdinalIgnoreCase)
        || state.Authorizations.Any(static authorization => string.Equals(authorization.Status, "invalid", StringComparison.OrdinalIgnoreCase));

    private static AcmeDns01StateProtectionOptions ResolveSaveProtection(
        AcmeDns01StateProtectionOptions requestedProtection,
        AcmeDns01StateProtectionKind detectedProtectionKind) =>
        requestedProtection.Kind == AcmeDns01StateProtectionKind.Passphrase
            || detectedProtectionKind == AcmeDns01StateProtectionKind.Passphrase
            ? requestedProtection
            : AcmeDns01StateProtectionOptions.None;

    private static string ResolvePath(string workingDirectory, string path) =>
        Path.GetFullPath(Path.IsPathRooted(path) ? path : Path.Combine(workingDirectory, path));

    private static string FormatSeverity(ValidationSeverity severity) =>
        severity switch
        {
            ValidationSeverity.Info => "info",
            ValidationSeverity.Warning => "warn",
            ValidationSeverity.Error => "error",
            _ => severity.ToString().ToLowerInvariant()
        };

    private static bool IsHelp(string arg) =>
        string.Equals(arg, "-h", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "--help", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "help", StringComparison.OrdinalIgnoreCase);

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 order --domain <domain> --email <email> --accept-terms --state <path> [--protect-state passphrase] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 validate --state <path> [--protect-state passphrase] [--wait-seconds <n>] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 status --state <path> [--protect-state passphrase] [--json]");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 finalize --state <path> --out <directory> [--protect-state passphrase] [--pfx-password <value>] [--json]");
    }

    private static void WriteOrderHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 order --domain <domain> --email <email> --accept-terms --state <path> [--protect-state passphrase] [--staging|--production|--directory-url <url>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --domain <domain>        DNS name to include. Repeat for SANs; wildcards are supported.");
        writer.WriteLine("  --email <email>          ACME account contact email.");
        writer.WriteLine("  --accept-terms           Required acknowledgement of the CA terms of service.");
        writer.WriteLine("  --state <path>           Local JSON state file containing ACME order/account material.");
        writer.WriteLine("  --staging                Use Let's Encrypt staging. Default.");
        writer.WriteLine("  --production             Use Let's Encrypt production.");
        writer.WriteLine("  --directory-url <url>    Use a custom ACME directory URL.");
        writer.WriteLine("  --account-key-bits <n>   RSA account key size. Defaults to 2048.");
        writer.WriteLine("  --certificate-key-bits <n> RSA certificate key size. Defaults to 2048.");
        writer.WriteLine("  --protect-state <kind>    State protection. Supported: none, passphrase.");
        writer.WriteLine("  --state-passphrase <value> Passphrase for --protect-state passphrase. Prefer --state-passphrase-env for real use.");
        writer.WriteLine("  --state-passphrase-env <name> Read the state passphrase from an environment variable.");
        writer.WriteLine("  --json                   Emit a machine-readable JSON report.");
    }

    private static void WriteValidateHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 validate --state <path> [--protect-state passphrase] [--wait-seconds <n>] [--poll-seconds <n>] [--json]");
        writer.WriteLine();
        writer.WriteLine("State protection options:");
        writer.WriteLine("  --protect-state passphrase --state-passphrase-env <name>");
        writer.WriteLine("  --protect-state passphrase --state-passphrase <value>");
    }

    private static void WriteStatusHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 status --state <path> [--protect-state passphrase] [--json]");
    }

    private static void WriteFinalizeHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 lifecycle certificate dns-01 finalize --state <path> --out <directory> [--protect-state passphrase] [--pfx-password <value>] [--wait-seconds <n>] [--json]");
    }

    private sealed record OrderOptions(
        IReadOnlyList<string> Domains,
        string? Email,
        string? StatePath,
        bool AcceptTerms,
        bool Staging,
        bool Production,
        string? DirectoryUrl,
        int AccountKeyBits,
        int CertificateKeyBits,
        bool Json,
        bool ShowHelp)
    {
        public AcmeDns01StateProtectionKind StateProtectionKind { get; init; } = AcmeDns01StateProtectionKind.None;

        public string? StatePassphrase { get; init; }

        public string? StatePassphraseEnvironmentVariable { get; init; }

        public AcmeDns01StateProtectionOptions ToStateProtectionOptions() =>
            CreateStateProtectionOptions(StateProtectionKind, StatePassphrase, StatePassphraseEnvironmentVariable);

        public static bool TryParse(string[] args, out OrderOptions options, out string error)
        {
            var domains = new List<string>();
            string? email = null;
            string? statePath = null;
            var acceptTerms = false;
            var staging = false;
            var production = false;
            string? directoryUrl = null;
            var accountKeyBits = AcmeDns01OrderRequest.DefaultAccountKeyBits;
            var certificateKeyBits = AcmeDns01OrderRequest.DefaultCertificateKeyBits;
            var stateProtectionKind = AcmeDns01StateProtectionKind.None;
            string? statePassphrase = null;
            string? statePassphraseEnvironmentVariable = null;
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
                    case "--accept-terms":
                        acceptTerms = true;
                        break;
                    case "--staging":
                        staging = true;
                        break;
                    case "--production":
                        production = true;
                        break;
                    case "--domain":
                        if (!TryReadValue(args, ref i, "--domain", out var domain, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        domains.Add(domain);
                        break;
                    case "--email":
                        if (!TryReadValue(args, ref i, "--email", out email, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        break;
                    case "--state":
                        if (!TryReadValue(args, ref i, "--state", out statePath, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        break;
                    case "--directory-url":
                        if (!TryReadValue(args, ref i, "--directory-url", out directoryUrl, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        break;
                    case "--account-key-bits":
                        if (!TryReadPositiveInt(args, ref i, "--account-key-bits", out accountKeyBits, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        break;
                    case "--certificate-key-bits":
                        if (!TryReadPositiveInt(args, ref i, "--certificate-key-bits", out certificateKeyBits, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        break;
                    case "--protect-state":
                        if (!TryReadValue(args, ref i, "--protect-state", out var protectStateToken, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        if (!TryParseStateProtectionKind(protectStateToken, out stateProtectionKind))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            error = $"Unknown state protection kind '{protectStateToken}'. Supported: none, passphrase.";
                            return false;
                        }

                        break;
                    case "--state-passphrase":
                        if (!TryReadValue(args, ref i, "--state-passphrase", out statePassphrase, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        break;
                    case "--state-passphrase-env":
                        if (!TryReadValue(args, ref i, "--state-passphrase-env", out statePassphraseEnvironmentVariable, out error))
                        {
                            options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                            return false;
                        }

                        break;
                    default:
                        if (!arg.StartsWith("-", StringComparison.Ordinal))
                        {
                            domains.Add(arg);
                            break;
                        }

                        options = Create(domains, email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp);
                        error = $"Unknown lifecycle certificate dns-01 order option '{arg}'.";
                        return false;
                }
            }

            options = Create(
                domains,
                email,
                statePath,
                acceptTerms,
                staging,
                production,
                directoryUrl,
                accountKeyBits,
                certificateKeyBits,
                json,
                showHelp,
                stateProtectionKind,
                statePassphrase,
                statePassphraseEnvironmentVariable);
            if (showHelp)
            {
                error = string.Empty;
                return true;
            }

            if (domains.Count == 0)
            {
                error = "--domain is required.";
                return false;
            }

            if (string.IsNullOrWhiteSpace(email))
            {
                error = "--email is required.";
                return false;
            }

            if (string.IsNullOrWhiteSpace(statePath))
            {
                error = "--state is required.";
                return false;
            }

            if (!acceptTerms)
            {
                error = "--accept-terms is required before creating an ACME account.";
                return false;
            }

            if (!TryValidateStateProtectionOptions(stateProtectionKind, statePassphrase, statePassphraseEnvironmentVariable, out error))
                return false;

            error = string.Empty;
            return true;
        }

        private static OrderOptions Create(
            IReadOnlyList<string> domains,
            string? email,
            string? statePath,
            bool acceptTerms,
            bool staging,
            bool production,
            string? directoryUrl,
            int accountKeyBits,
            int certificateKeyBits,
            bool json,
            bool showHelp,
            AcmeDns01StateProtectionKind stateProtectionKind = AcmeDns01StateProtectionKind.None,
            string? statePassphrase = null,
            string? statePassphraseEnvironmentVariable = null) =>
            new(domains.ToArray(), email, statePath, acceptTerms, staging, production, directoryUrl, accountKeyBits, certificateKeyBits, json, showHelp)
            {
                StateProtectionKind = stateProtectionKind,
                StatePassphrase = statePassphrase,
                StatePassphraseEnvironmentVariable = statePassphraseEnvironmentVariable
            };
    }

    private sealed record StateCommandOptions(
        string? StatePath,
        string? OutputDirectory,
        string? PfxPassword,
        int WaitSeconds,
        int PollSeconds,
        bool Json,
        bool ShowHelp)
    {
        public AcmeDns01StateProtectionKind StateProtectionKind { get; init; } = AcmeDns01StateProtectionKind.None;

        public string? StatePassphrase { get; init; }

        public string? StatePassphraseEnvironmentVariable { get; init; }

        public AcmeDns01StateProtectionOptions ToStateProtectionOptions() =>
            CreateStateProtectionOptions(StateProtectionKind, StatePassphrase, StatePassphraseEnvironmentVariable);

        public static bool TryParse(string[] args, bool requireOut, out StateCommandOptions options, out string error)
        {
            string? statePath = null;
            string? outputDirectory = null;
            string? pfxPassword = null;
            var waitSeconds = 120;
            var pollSeconds = 5;
            var stateProtectionKind = AcmeDns01StateProtectionKind.None;
            string? statePassphrase = null;
            string? statePassphraseEnvironmentVariable = null;
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
                    case "--state":
                        if (!TryReadValue(args, ref i, "--state", out statePath, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        break;
                    case "--out":
                    case "--output":
                        if (!TryReadValue(args, ref i, arg, out outputDirectory, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        break;
                    case "--pfx-password":
                        if (!TryReadValue(args, ref i, "--pfx-password", out pfxPassword, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        break;
                    case "--wait-seconds":
                        if (!TryReadPositiveInt(args, ref i, "--wait-seconds", out waitSeconds, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        break;
                    case "--poll-seconds":
                        if (!TryReadPositiveInt(args, ref i, "--poll-seconds", out pollSeconds, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        break;
                    case "--protect-state":
                        if (!TryReadValue(args, ref i, "--protect-state", out var protectStateToken, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        if (!TryParseStateProtectionKind(protectStateToken, out stateProtectionKind))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            error = $"Unknown state protection kind '{protectStateToken}'. Supported: none, passphrase.";
                            return false;
                        }

                        break;
                    case "--state-passphrase":
                        if (!TryReadValue(args, ref i, "--state-passphrase", out statePassphrase, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        break;
                    case "--state-passphrase-env":
                        if (!TryReadValue(args, ref i, "--state-passphrase-env", out statePassphraseEnvironmentVariable, out error))
                        {
                            options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                            return false;
                        }

                        break;
                    default:
                        options = Create(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp);
                        error = arg.StartsWith("-", StringComparison.Ordinal)
                            ? $"Unknown lifecycle certificate dns-01 option '{arg}'."
                            : $"Unexpected lifecycle certificate dns-01 argument '{arg}'.";
                        return false;
                }
            }

            options = Create(
                statePath,
                outputDirectory,
                pfxPassword,
                waitSeconds,
                pollSeconds,
                json,
                showHelp,
                stateProtectionKind,
                statePassphrase,
                statePassphraseEnvironmentVariable);
            if (showHelp)
            {
                error = string.Empty;
                return true;
            }

            if (string.IsNullOrWhiteSpace(statePath))
            {
                error = "--state is required.";
                return false;
            }

            if (requireOut && string.IsNullOrWhiteSpace(outputDirectory))
            {
                error = "--out is required.";
                return false;
            }

            if (!TryValidateStateProtectionOptions(stateProtectionKind, statePassphrase, statePassphraseEnvironmentVariable, out error))
                return false;

            error = string.Empty;
            return true;
        }

        private static StateCommandOptions Create(
            string? statePath,
            string? outputDirectory,
            string? pfxPassword,
            int waitSeconds,
            int pollSeconds,
            bool json,
            bool showHelp,
            AcmeDns01StateProtectionKind stateProtectionKind = AcmeDns01StateProtectionKind.None,
            string? statePassphrase = null,
            string? statePassphraseEnvironmentVariable = null) =>
            new(statePath, outputDirectory, pfxPassword, waitSeconds, pollSeconds, json, showHelp)
            {
                StateProtectionKind = stateProtectionKind,
                StatePassphrase = statePassphrase,
                StatePassphraseEnvironmentVariable = statePassphraseEnvironmentVariable
            };
    }

    private static AcmeDns01StateProtectionOptions CreateStateProtectionOptions(
        AcmeDns01StateProtectionKind kind,
        string? passphrase,
        string? passphraseEnvironmentVariable)
    {
        if (kind == AcmeDns01StateProtectionKind.None)
            return AcmeDns01StateProtectionOptions.None;

        var resolvedPassphrase = ResolveStatePassphrase(passphrase, passphraseEnvironmentVariable);
        return AcmeDns01StateProtectionOptions.FromPassphrase(resolvedPassphrase);
    }

    private static bool TryValidateStateProtectionOptions(
        AcmeDns01StateProtectionKind kind,
        string? passphrase,
        string? passphraseEnvironmentVariable,
        out string error)
    {
        if (!string.IsNullOrWhiteSpace(passphrase) && !string.IsNullOrWhiteSpace(passphraseEnvironmentVariable))
        {
            error = "Supply either --state-passphrase or --state-passphrase-env, not both.";
            return false;
        }

        if (kind == AcmeDns01StateProtectionKind.None
            && (!string.IsNullOrWhiteSpace(passphrase) || !string.IsNullOrWhiteSpace(passphraseEnvironmentVariable)))
        {
            error = "--state-passphrase and --state-passphrase-env require --protect-state passphrase.";
            return false;
        }

        if (kind == AcmeDns01StateProtectionKind.Passphrase)
        {
            if (string.IsNullOrWhiteSpace(passphrase) && string.IsNullOrWhiteSpace(passphraseEnvironmentVariable))
            {
                error = "--protect-state passphrase requires --state-passphrase or --state-passphrase-env.";
                return false;
            }

            if (!string.IsNullOrWhiteSpace(passphraseEnvironmentVariable)
                && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(passphraseEnvironmentVariable)))
            {
                error = $"Environment variable '{passphraseEnvironmentVariable}' is not set or is empty.";
                return false;
            }
        }

        error = string.Empty;
        return true;
    }

    private static string ResolveStatePassphrase(string? passphrase, string? passphraseEnvironmentVariable)
    {
        if (!string.IsNullOrWhiteSpace(passphrase))
            return passphrase;

        if (!string.IsNullOrWhiteSpace(passphraseEnvironmentVariable))
            return Environment.GetEnvironmentVariable(passphraseEnvironmentVariable)
                ?? throw new InvalidOperationException($"Environment variable '{passphraseEnvironmentVariable}' is not set.");

        throw new InvalidOperationException("Passphrase state protection requires a passphrase.");
    }

    private static bool TryParseStateProtectionKind(string value, out AcmeDns01StateProtectionKind kind)
    {
        if (string.Equals(value, "none", StringComparison.OrdinalIgnoreCase))
        {
            kind = AcmeDns01StateProtectionKind.None;
            return true;
        }

        if (string.Equals(value, "passphrase", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "password", StringComparison.OrdinalIgnoreCase))
        {
            kind = AcmeDns01StateProtectionKind.Passphrase;
            return true;
        }

        kind = default;
        return false;
    }

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

    private static bool TryReadPositiveInt(string[] args, ref int index, string option, out int value, out string error)
    {
        if (!TryReadValue(args, ref index, option, out var token, out error))
        {
            value = 0;
            return false;
        }

        if (!int.TryParse(token, out value) || value <= 0)
        {
            error = $"{option} requires a positive integer value.";
            return false;
        }

        return true;
    }
}

public sealed record AcmeDns01CliReport(
    string Command,
    string DirectoryUrl,
    string StatePath,
    AcmeDns01StateProtectionKind StateProtectionKind,
    IReadOnlyList<string> Domains,
    string OrderUrl,
    string FinalizeUrl,
    string? CertificateUrl,
    string OrderStatus,
    IReadOnlyList<AcmeDns01TxtRecordReport> TxtRecords,
    AcmeCertificateFiles? Files,
    IReadOnlyList<string> Instructions,
    IReadOnlyList<ValidationFinding> Findings,
    int InfoCount,
    int WarningCount,
    int ErrorCount)
{
    public static AcmeDns01CliReport Create(
        string command,
        string statePath,
        AcmeDns01StateProtectionKind stateProtectionKind,
        AcmeDns01OrderState state,
        AcmeCertificateFiles? files,
        IReadOnlyList<ValidationFinding> findings)
    {
        var stateProtectionArgs = stateProtectionKind == AcmeDns01StateProtectionKind.Passphrase
            ? " --protect-state passphrase --state-passphrase-env <name>"
            : string.Empty;
        string[] instructions = command switch
        {
            "order" =>
            [
                "Publish every TXT record shown above in the authoritative DNS zone.",
                $"After propagation, run: extend0 lifecycle certificate dns-01 validate --state \"{statePath}\"{stateProtectionArgs}",
                $"When the order status is ready, run: extend0 lifecycle certificate dns-01 finalize --state \"{statePath}\" --out <directory>{stateProtectionArgs}"
            ],
            "validate" =>
            [
                "If order status is ready, run finalize to create the CSR and download the certificate.",
                "If status is still pending, wait for DNS propagation and run validate or status again.",
                "If status is invalid, create a new order after fixing DNS."
            ],
            "finalize" =>
            [
                "Use certificate-chain.pem and private-key.pem with TlsTcpSocket server options.",
                "Protect the private key and ACME state file; both contain sensitive key material."
            ],
            _ =>
            [
                "Use validate after publishing DNS records.",
                "Use finalize after the order becomes ready."
            ]
        };

        return new AcmeDns01CliReport(
            command,
            state.DirectoryUrl,
            statePath,
            stateProtectionKind,
            state.Domains,
            state.OrderUrl,
            state.FinalizeUrl,
            state.CertificateUrl,
            state.OrderStatus,
            state.Authorizations.Select(static authorization => new AcmeDns01TxtRecordReport(
                authorization.Identifier,
                authorization.Status,
                authorization.TxtRecordName,
                authorization.TxtRecordValue)).ToArray(),
            files,
            instructions,
            findings,
            findings.Count(static finding => finding.Severity == ValidationSeverity.Info),
            findings.Count(static finding => finding.Severity == ValidationSeverity.Warning),
            findings.Count(static finding => finding.Severity == ValidationSeverity.Error));
    }
}

public sealed record AcmeDns01TxtRecordReport(
    string Domain,
    string AuthorizationStatus,
    string RecordName,
    string RecordValue);
