using System.Text;
using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.CrossProcess.Contract;

namespace Extend0.Cli;

internal static class LifecycleNamedPipeDiscovery
{
    private const string NamedPipeRoot = @"\\.\pipe\";

    public static void AddContractScopedCandidateFindings(
        LifecycleProbeOptions options,
        string? expectedEndpoint,
        List<ValidationFinding> findings)
    {
        if (!OperatingSystem.IsWindows()
            || options.TransportKind != TransportKind.NamedPipe
            || !string.IsNullOrWhiteSpace(options.EndpointName))
            return;

        var contractName = ResolveContractName(options.ContractKind);
        if (contractName is null)
            return;

        var expectedFingerprint = ResolveContractFingerprint(options.ContractKind);
        foreach (var candidate in FindCandidates(contractName, options.Name))
        {
            if (string.Equals(candidate.EndpointName, expectedEndpoint, StringComparison.Ordinal))
                findings.Add(ValidationFinding.Warning(
                    "owner-candidate-unreachable",
                    $"A matching named-pipe endpoint exists but was not reachable within the timeout: '{candidate.EndpointName}'. The owner may be busy, stale, or not accepting clients."));
            else if (!string.Equals(candidate.Fingerprint, expectedFingerprint, StringComparison.Ordinal))
                findings.Add(ValidationFinding.Warning(
                    "owner-version-mismatch-candidate",
                    $"Found a named-pipe owner candidate for contract '{contractName}' and name '{options.Name}', but it belongs to build fingerprint '{candidate.Fingerprint}' while this CLI expects '{expectedFingerprint}'. Endpoint: '{candidate.EndpointName}'. Restart the owner with the same Extend0 build, run the matching CLI build, or pass --endpoint explicitly for compatibility testing."));
            else
                findings.Add(ValidationFinding.Warning(
                    "owner-alternate-endpoint-candidate",
                    $"Found a named-pipe owner candidate for contract '{contractName}' and name '{options.Name}' on a different endpoint: '{candidate.EndpointName}'."));
        }
    }

    private static IEnumerable<NamedPipeCandidate> FindCandidates(string contractName, string serviceName)
    {
        IEnumerable<string> entries;
        try
        {
            entries = Directory.EnumerateFileSystemEntries(NamedPipeRoot);
        }
        catch
        {
            yield break;
        }

        foreach (var entry in entries)
        {
            var endpointName = Path.GetFileName(entry);
            if (!TryDecodeContractScopedEndpoint(endpointName, out var decoded))
                continue;

            if (!string.Equals(decoded.ContractName, contractName, StringComparison.Ordinal)
                || !string.Equals(decoded.ServiceName, serviceName, StringComparison.Ordinal))
                continue;

            yield return decoded;
        }
    }

    private static string? ResolveContractName(LifecycleContractKind contractKind) =>
        contractKind switch
        {
            LifecycleContractKind.MetaDB => typeof(IMetaDBManagerRPCCompatible).FullName,
            _ => null
        };

    private static string? ResolveContractFingerprint(LifecycleContractKind contractKind) =>
        contractKind switch
        {
            LifecycleContractKind.MetaDB => typeof(IMetaDBManagerRPCCompatible).Assembly.ManifestModule.ModuleVersionId.ToString("N"),
            _ => null
        };

    private static bool TryDecodeContractScopedEndpoint(string endpointName, out NamedPipeCandidate candidate)
    {
        candidate = default;

        if (!endpointName.StartsWith("CPS.", StringComparison.Ordinal))
            return false;

        var encoded = endpointName["CPS.".Length..];
        if (encoded.Contains(".", StringComparison.Ordinal))
            return false; // Truncated hashed endpoints cannot be losslessly decoded.

        var base64 = encoded.Replace('-', '+').Replace('_', '/');
        switch (base64.Length % 4)
        {
            case 2:
                base64 += "==";
                break;
            case 3:
                base64 += "=";
                break;
            case 0:
                break;
            default:
                return false;
        }

        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(base64));
        }
        catch
        {
            return false;
        }

        var parts = decoded.Split(':', 4);
        if (parts.Length != 4 || !string.Equals(parts[0], "CPS", StringComparison.Ordinal))
            return false;

        candidate = new NamedPipeCandidate(endpointName, parts[1], parts[2], parts[3]);
        return true;
    }

    private readonly record struct NamedPipeCandidate(
        string EndpointName,
        string ContractName,
        string Fingerprint,
        string ServiceName);
}
