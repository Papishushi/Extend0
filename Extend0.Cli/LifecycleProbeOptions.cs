using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Cli;

internal sealed record LifecycleProbeOptions(
    string Name,
    TransportKind TransportKind,
    string? EndpointName,
    string ServerName,
    int TimeoutMs,
    string? ProtocolId,
    int? ProtocolVersion,
    bool AllowCustom,
    bool Connect,
    bool Json,
    bool ShowHelp)
{
    private const string DefaultName = "Extend0.Lifecycle.Probe";
    private const string DefaultServerName = ".";
    private const int DefaultTimeoutMs = 3_000;

    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out LifecycleProbeOptions options,
        out string error)
    {
        _ = workingDirectory;

        var name = DefaultName;
        var transportKind = TransportKind.NamedPipe;
        string? endpointName = null;
        var serverName = DefaultServerName;
        var timeoutMs = DefaultTimeoutMs;
        string? protocolId = null;
        int? protocolVersion = null;
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
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--transport":
                    if (!TryReadValue(args, ref i, "--transport", out var transportToken, out error))
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!Enum.TryParse(transportToken, ignoreCase: true, out transportKind)
                        || !Enum.IsDefined(transportKind))
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        error = $"Unknown transport kind '{transportToken}'.";
                        return false;
                    }

                    break;

                case "--endpoint":
                    if (!TryReadValue(args, ref i, "--endpoint", out endpointName, out error))
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--server":
                    if (!TryReadValue(args, ref i, "--server", out serverName, out error))
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--timeout":
                    if (!TryReadValue(args, ref i, "--timeout", out var timeoutToken, out error))
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!int.TryParse(timeoutToken, out timeoutMs) || timeoutMs <= 0)
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        error = "--timeout requires a positive integer value in milliseconds.";
                        return false;
                    }

                    break;

                case "--protocol-id":
                    if (!TryReadValue(args, ref i, "--protocol-id", out protocolId, out error))
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    break;

                case "--protocol-version":
                    if (!TryReadValue(args, ref i, "--protocol-version", out var versionToken, out error))
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        return false;
                    }

                    if (!int.TryParse(versionToken, out var parsedVersion) || parsedVersion <= 0)
                    {
                        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                        error = "--protocol-version requires a positive integer value.";
                        return false;
                    }

                    protocolVersion = parsedVersion;
                    break;

                default:
                    options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
                    error = arg.StartsWith("-", StringComparison.Ordinal)
                        ? $"Unknown lifecycle probe option '{arg}'."
                        : $"Unexpected lifecycle probe argument '{arg}'.";
                    return false;
            }
        }

        options = Create(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);
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

        error = string.Empty;
        return true;
    }

    private static LifecycleProbeOptions Create(
        string name,
        TransportKind transportKind,
        string? endpointName,
        string serverName,
        int timeoutMs,
        string? protocolId,
        int? protocolVersion,
        bool allowCustom,
        bool connect,
        bool json,
        bool showHelp) =>
        new(name, transportKind, endpointName, serverName, timeoutMs, protocolId, protocolVersion, allowCustom, connect, json, showHelp);

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
