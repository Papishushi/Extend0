using Extend0.Lifecycle.Assurance;

namespace Extend0.Cli;

internal sealed record StorageDiagnoseOptions(
    string? InputPath,
    StorageProtectionLevel? RequiredLevel,
    string? RequiredProviderId,
    string? RequiredProtectionId,
    string? ManifestPath,
    bool Json,
    bool ShowHelp)
{
    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out StorageDiagnoseOptions options,
        out string error)
    {
        string? inputPath = null;
        StorageProtectionLevel? requiredLevel = null;
        string? requiredProviderId = null;
        string? requiredProtectionId = null;
        string? manifestPath = null;
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

                case "--require":
                    if (!TryReadValue(args, ref i, "--require", out var levelToken, out error))
                    {
                        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
                        return false;
                    }

                    if (!TryParseProtectionLevel(levelToken, out var parsedLevel))
                    {
                        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
                        error = $"Unknown storage protection level '{levelToken}'.";
                        return false;
                    }

                    requiredLevel = parsedLevel;
                    break;

                case "--provider":
                    if (!TryReadValue(args, ref i, "--provider", out requiredProviderId, out error))
                    {
                        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
                        return false;
                    }

                    break;

                case "--protection-id":
                    if (!TryReadValue(args, ref i, "--protection-id", out requiredProtectionId, out error))
                    {
                        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
                        return false;
                    }

                    break;

                case "--manifest":
                    if (!TryReadValue(args, ref i, "--manifest", out var manifestToken, out error))
                    {
                        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
                        return false;
                    }

                    manifestPath = ToFullPath(workingDirectory, manifestToken);
                    break;

                default:
                    if (arg.StartsWith("-", StringComparison.Ordinal))
                    {
                        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
                        error = $"Unknown storage diagnose option '{arg}'.";
                        return false;
                    }

                    if (inputPath is not null)
                    {
                        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
                        error = "storage diagnose accepts exactly one path argument.";
                        return false;
                    }

                    inputPath = ToFullPath(workingDirectory, arg);
                    break;
            }
        }

        options = Create(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);
        if (!showHelp && string.IsNullOrWhiteSpace(inputPath))
        {
            error = "storage diagnose requires a path argument.";
            return false;
        }

        if (!showHelp && (requiredProviderId is not null || requiredProtectionId is not null) && requiredLevel is null)
        {
            error = "--provider and --protection-id require --require.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    public StorageProtectionPolicy ToPolicy() =>
        RequiredLevel is null
            ? StorageProtectionPolicy.None
            : StorageProtectionPolicy.Require(RequiredLevel.Value, RequiredProviderId, RequiredProtectionId);

    internal static bool TryParseProtectionLevel(string value, out StorageProtectionLevel level)
    {
        if (Enum.TryParse(value, ignoreCase: true, out level) && Enum.IsDefined(level))
            return true;

        var normalized = value.Replace("-", string.Empty, StringComparison.Ordinal)
            .Replace("_", string.Empty, StringComparison.Ordinal);

        if (string.Equals(normalized, "none", StringComparison.OrdinalIgnoreCase))
        {
            level = StorageProtectionLevel.None;
            return true;
        }

        if (string.Equals(normalized, "declared", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "declaredencrypted", StringComparison.OrdinalIgnoreCase))
        {
            level = StorageProtectionLevel.DeclaredEncrypted;
            return true;
        }

        if (string.Equals(normalized, "providerattested", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "providerattestedencrypted", StringComparison.OrdinalIgnoreCase))
        {
            level = StorageProtectionLevel.ProviderAttestedEncrypted;
            return true;
        }

        if (string.Equals(normalized, "platformverified", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "platformverifiedencrypted", StringComparison.OrdinalIgnoreCase))
        {
            level = StorageProtectionLevel.PlatformVerifiedEncrypted;
            return true;
        }

        if (string.Equals(normalized, "managed", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "extend0managed", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "extend0managedprotectedmount", StringComparison.OrdinalIgnoreCase))
        {
            level = StorageProtectionLevel.Extend0ManagedProtectedMount;
            return true;
        }

        level = default;
        return false;
    }

    private static StorageDiagnoseOptions Create(
        string? inputPath,
        StorageProtectionLevel? requiredLevel,
        string? requiredProviderId,
        string? requiredProtectionId,
        string? manifestPath,
        bool json,
        bool showHelp) =>
        new(inputPath, requiredLevel, requiredProviderId, requiredProtectionId, manifestPath, json, showHelp);

    private static string ToFullPath(string workingDirectory, string path) =>
        Path.IsPathRooted(path)
            ? path
            : Path.Combine(workingDirectory, path);

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
