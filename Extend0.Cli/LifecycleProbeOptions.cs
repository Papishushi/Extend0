using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Cli;

public enum LifecycleContractKind
{
    Probe,
    MetaDB
}

internal sealed record LifecycleProbeOptions(
    string Name,
    LifecycleContractKind ContractKind,
    TransportKind TransportKind,
    string? EndpointName,
    string ServerName,
    int TimeoutMs,
    string? ProtocolId,
    int? ProtocolVersion,
    AuthenticationMode AuthenticationMode,
    string? AuthenticationSecret,
    string? TlsTargetHost,
    bool AllowCustom,
    bool Connect,
    bool Json,
    bool ShowHelp)
{
    private const string DefaultName = "Extend0.Lifecycle.Probe";
    private const string DefaultMetaDBName = "Extend0.MetaDB";
    private const string DefaultServerName = ".";
    private const int DefaultTimeoutMs = 3_000;

    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out LifecycleProbeOptions options,
        out string error,
        string commandName = "probe")
    {
        _ = workingDirectory;

        var name = DefaultName;
        var nameSpecified = false;
        var contractKind = LifecycleContractKind.Probe;
        var transportKind = TransportKind.NamedPipe;
        string? endpointName = null;
        var serverName = DefaultServerName;
        var timeoutMs = DefaultTimeoutMs;
        string? protocolId = null;
        int? protocolVersion = null;
        var authenticationMode = AuthenticationMode.None;
        string? authenticationSecret = null;
        string? tlsTargetHost = null;
        var allowCustom = false;
        var connect = false;
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

                case "--connect":
                    connect = true;
                    break;

                case "--allow-custom":
                    allowCustom = true;
                    break;

                case "--name":
                    if (!TryReadValue(args, ref i, "--name", out name, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    nameSpecified = true;
                    break;

                case "--contract":
                    if (!TryReadValue(args, ref i, "--contract", out var contractToken, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!TryParseContract(contractToken, out contractKind))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        error = $"Unknown lifecycle contract '{contractToken}'.";
                        return false;
                    }

                    break;

                case "--transport":
                    if (!TryReadValue(args, ref i, "--transport", out var transportToken, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!Enum.TryParse(transportToken, ignoreCase: true, out transportKind)
                        || !Enum.IsDefined(transportKind))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        error = $"Unknown transport kind '{transportToken}'.";
                        return false;
                    }

                    break;

                case "--endpoint":
                    if (!TryReadValue(args, ref i, "--endpoint", out endpointName, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--server":
                    if (!TryReadValue(args, ref i, "--server", out serverName, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--timeout":
                    if (!TryReadValue(args, ref i, "--timeout", out var timeoutToken, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!int.TryParse(timeoutToken, out timeoutMs) || timeoutMs <= 0)
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        error = "--timeout requires a positive integer value in milliseconds.";
                        return false;
                    }

                    break;

                case "--protocol-id":
                    if (!TryReadValue(args, ref i, "--protocol-id", out protocolId, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--protocol-version":
                    if (!TryReadValue(args, ref i, "--protocol-version", out var versionToken, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!int.TryParse(versionToken, out var parsedVersion) || parsedVersion <= 0)
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        error = "--protocol-version requires a positive integer value.";
                        return false;
                    }

                    protocolVersion = parsedVersion;
                    break;

                case "--auth":
                    if (!TryReadValue(args, ref i, "--auth", out var authenticationToken, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!TryParseAuthenticationMode(authenticationToken, out authenticationMode))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        error = $"Unknown lifecycle authentication mode '{authenticationToken}'.";
                        return false;
                    }

                    break;

                case "--secret":
                    if (!TryReadValue(args, ref i, "--secret", out authenticationSecret, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--tls-target-host":
                    if (!TryReadValue(args, ref i, "--tls-target-host", out tlsTargetHost, out error))
                    {
                        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                default:
                    options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
                    error = arg.StartsWith("-", StringComparison.Ordinal)
                        ? $"Unknown lifecycle {commandName} option '{arg}'."
                        : $"Unexpected lifecycle {commandName} argument '{arg}'.";
                    return false;
            }
        }

        if (!nameSpecified && contractKind == LifecycleContractKind.MetaDB)
            name = DefaultMetaDBName;

        options = Create(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);
        if (!showHelp && string.IsNullOrWhiteSpace(name))
        {
            error = "--name cannot be empty.";
            return false;
        }

        if (!showHelp && (string.IsNullOrWhiteSpace(protocolId) != !protocolVersion.HasValue))
        {
            error = "--protocol-id and --protocol-version must be supplied together.";
            return false;
        }

        if (!showHelp && authenticationMode == AuthenticationMode.SharedSecretHmac && string.IsNullOrWhiteSpace(authenticationSecret))
        {
            error = "--auth shared-secret-hmac requires --secret.";
            return false;
        }

        if (!showHelp && authenticationMode == AuthenticationMode.None && !string.IsNullOrWhiteSpace(authenticationSecret))
        {
            error = "--secret requires --auth shared-secret-hmac.";
            return false;
        }

        if (!showHelp && !string.IsNullOrWhiteSpace(tlsTargetHost) && transportKind != TransportKind.TlsTcpSocket)
        {
            error = "--tls-target-host requires --transport TlsTcpSocket.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    public CrossProcessAuthenticationOptions ToAuthenticationOptions() =>
        AuthenticationMode switch
        {
            AuthenticationMode.None => CrossProcessAuthenticationOptions.None,
            AuthenticationMode.SharedSecretHmac => CrossProcessAuthenticationOptions.SharedSecretHmac(AuthenticationSecret!),
            _ => new CrossProcessAuthenticationOptions(AuthenticationMode)
        };

    public CrossProcessTlsOptions? ToTlsOptions() =>
        TransportKind == TransportKind.TlsTcpSocket
            ? CrossProcessTlsOptions.ForClient(TlsTargetHost)
            : null;

    private static LifecycleProbeOptions Create(
        string name,
        LifecycleContractKind contractKind,
        TransportKind transportKind,
        string? endpointName,
        string serverName,
        int timeoutMs,
        string? protocolId,
        int? protocolVersion,
        AuthenticationMode authenticationMode,
        string? authenticationSecret,
        string? tlsTargetHost,
        bool allowCustom,
        bool connect,
        bool json,
        bool showHelp) =>
        new(name, contractKind, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, authenticationMode, authenticationSecret, tlsTargetHost, allowCustom, connect, json, showHelp);

    private static bool TryParseContract(string value, out LifecycleContractKind contractKind)
    {
        if (string.Equals(value, "probe", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "default", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "lifecycle", StringComparison.OrdinalIgnoreCase))
        {
            contractKind = LifecycleContractKind.Probe;
            return true;
        }

        if (string.Equals(value, "metadb", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "metadata", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "IMetaDBManagerRPCCompatible", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "Extend0.Metadata.CrossProcess.Contract.IMetaDBManagerRPCCompatible", StringComparison.OrdinalIgnoreCase))
        {
            contractKind = LifecycleContractKind.MetaDB;
            return true;
        }

        contractKind = default;
        return false;
    }

    private static bool TryParseAuthenticationMode(string value, out AuthenticationMode authenticationMode)
    {
        if (string.Equals(value, "none", StringComparison.OrdinalIgnoreCase))
        {
            authenticationMode = AuthenticationMode.None;
            return true;
        }

        if (string.Equals(value, "shared-secret-hmac", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "sharedsecrethmac", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "hmac", StringComparison.OrdinalIgnoreCase))
        {
            authenticationMode = AuthenticationMode.SharedSecretHmac;
            return true;
        }

        authenticationMode = default;
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
}
