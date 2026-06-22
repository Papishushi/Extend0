using System.Text.Json;
using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Portable evidence-based verifier for hardware-attested storage access.
/// </summary>
public static class HardwareAttestationVerifier
{
    public const string ManifestFileName = ".extend0-attestation.json";

    private static readonly JsonSerializerOptions Json = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    static HardwareAttestationVerifier()
    {
        Json.Converters.Add(new JsonStringEnumConverter());
    }

    public static HardwareAttestationEvidence DiagnosePath(
        string path,
        HardwareAttestationPolicy policy = default,
        string? manifestPath = null,
        DateTimeOffset? verifiedAtUtc = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        policy.Validate();

        var now = verifiedAtUtc ?? DateTimeOffset.UtcNow;
        var fullPath = Path.GetFullPath(path);
        var findings = new List<HardwareAttestationFinding>();
        var resolvedManifestPath = ResolveManifestPath(fullPath, manifestPath);

        if (resolvedManifestPath is null)
        {
            findings.Add(policy.RequiresAttestation
                ? HardwareAttestationFinding.Error("hardware-attestation-evidence-missing", "No hardware attestation manifest was found for the path.")
                : HardwareAttestationFinding.Info("hardware-attestation-not-required", "No hardware attestation policy is required for the path."));

            return CreateEvidence(path, fullPath, policy, HardwareAttestationLevel.None, HardwareAttestationTechnology.None, null, null, null, null, null, null, null, null, false, "none", now, findings);
        }

        findings.Add(HardwareAttestationFinding.Info("hardware-attestation-manifest", $"Found hardware attestation manifest '{resolvedManifestPath}'."));

        HardwareAttestationManifest? manifest;
        try
        {
            manifest = JsonSerializer.Deserialize<HardwareAttestationManifest>(File.ReadAllText(resolvedManifestPath), Json);
        }
        catch (Exception ex)
        {
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-manifest-invalid", $"Could not read hardware attestation manifest: {ex.Message}"));
            return CreateEvidence(path, fullPath, policy, HardwareAttestationLevel.None, HardwareAttestationTechnology.None, null, null, null, null, null, null, null, null, false, "manifest", now, findings);
        }

        if (manifest is null)
        {
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-manifest-empty", "Hardware attestation manifest is empty."));
            return CreateEvidence(path, fullPath, policy, HardwareAttestationLevel.None, HardwareAttestationTechnology.None, null, null, null, null, null, null, null, null, false, "manifest", now, findings);
        }

        ValidateManifest(manifest, findings);

        var rootPath = ResolveRootPath(resolvedManifestPath, manifest.RootPath);
        var pathInsideRoot = StorageProtectionVerifier.ContainsPath(rootPath, fullPath);
        if (pathInsideRoot)
            findings.Add(HardwareAttestationFinding.Info("hardware-attestation-path-contained", "Path is inside the hardware-attested storage root."));
        else
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-path-outside-root", $"Path '{fullPath}' is outside hardware-attested storage root '{rootPath}'."));

        ValidatePolicy(policy, manifest, pathInsideRoot, findings);

        var observedLevel = pathInsideRoot && findings.All(static f => f.Id != "hardware-attestation-manifest-invalid")
            ? manifest.AttestationLevel
            : HardwareAttestationLevel.None;
        var observedTechnology = pathInsideRoot
            ? manifest.Technology
            : HardwareAttestationTechnology.None;

        return CreateEvidence(
            path,
            fullPath,
            policy,
            observedLevel,
            observedTechnology,
            manifest.ProviderId,
            manifest.ProviderVersion,
            manifest.AttestationId,
            manifest.Measurement,
            manifest.PolicyId,
            manifest.ReportFormat,
            manifest.ReportDigest,
            rootPath,
            pathInsideRoot,
            manifest.EvidenceSource ?? "manifest",
            now,
            findings);
    }

    public static void SaveManifest(string manifestPath, HardwareAttestationManifest manifest, bool overwrite = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestPath);
        ArgumentNullException.ThrowIfNull(manifest);

        var directory = Path.GetDirectoryName(Path.GetFullPath(manifestPath));
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        if (!overwrite && File.Exists(manifestPath))
            throw new IOException($"Hardware attestation manifest already exists: {manifestPath}");

        File.WriteAllText(manifestPath, JsonSerializer.Serialize(manifest, Json));
    }

    private static string? ResolveManifestPath(string fullPath, string? explicitManifestPath)
    {
        if (!string.IsNullOrWhiteSpace(explicitManifestPath))
            return Path.GetFullPath(explicitManifestPath);

        var start = Directory.Exists(fullPath)
            ? fullPath
            : Path.GetDirectoryName(fullPath);

        if (string.IsNullOrWhiteSpace(start))
            return null;

        for (var directory = new DirectoryInfo(start); directory is not null; directory = directory.Parent)
        {
            var candidate = Path.Combine(directory.FullName, ManifestFileName);
            if (File.Exists(candidate))
                return candidate;
        }

        return null;
    }

    private static string ResolveRootPath(string manifestPath, string? manifestRootPath)
    {
        if (!string.IsNullOrWhiteSpace(manifestRootPath))
            return Path.GetFullPath(manifestRootPath);

        return Path.GetDirectoryName(Path.GetFullPath(manifestPath))
            ?? Path.GetPathRoot(Path.GetFullPath(manifestPath))
            ?? Path.GetFullPath(".");
    }

    private static void ValidateManifest(HardwareAttestationManifest manifest, List<HardwareAttestationFinding> findings)
    {
        if (manifest.Version != HardwareAttestationManifest.CurrentVersion)
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-manifest-version", $"Manifest version {manifest.Version} is not supported."));
        else
            findings.Add(HardwareAttestationFinding.Info("hardware-attestation-manifest-version", $"Manifest version {manifest.Version}."));

        if (string.IsNullOrWhiteSpace(manifest.ProviderId))
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-provider", "Manifest does not declare a provider id."));
        else
            findings.Add(HardwareAttestationFinding.Info("hardware-attestation-provider", $"Provider id '{manifest.ProviderId}'."));

        if (string.IsNullOrWhiteSpace(manifest.AttestationId))
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-id", "Manifest does not declare an attestation id."));
        else
            findings.Add(HardwareAttestationFinding.Info("hardware-attestation-id", $"Attestation id '{manifest.AttestationId}'."));

        if (!Enum.IsDefined(manifest.Technology))
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-technology", $"Manifest declares unknown attestation technology '{manifest.Technology}'."));
        else
            findings.Add(HardwareAttestationFinding.Info("hardware-attestation-technology", $"Manifest declares attestation technology '{manifest.Technology}'."));

        if (!Enum.IsDefined(manifest.AttestationLevel))
            findings.Add(HardwareAttestationFinding.Error("hardware-attestation-level", $"Manifest declares unknown attestation level '{manifest.AttestationLevel}'."));
        else
            findings.Add(HardwareAttestationFinding.Info("hardware-attestation-level", $"Manifest declares attestation level '{manifest.AttestationLevel}'."));

        if (manifest.AttestationLevel >= HardwareAttestationLevel.RemoteAttested && string.IsNullOrWhiteSpace(manifest.Measurement))
        {
            findings.Add(HardwareAttestationFinding.Warning(
                "hardware-attestation-measurement-missing",
                "Remote-attested evidence does not declare a measurement; prefer matching measurements or policy ids for high-assurance deployments."));
        }
    }

    private static void ValidatePolicy(
        HardwareAttestationPolicy policy,
        HardwareAttestationManifest manifest,
        bool pathInsideRoot,
        List<HardwareAttestationFinding> findings)
    {
        if (!policy.RequiresAttestation)
        {
            findings.Add(HardwareAttestationFinding.Info("hardware-attestation-policy", "No hardware attestation policy is required."));
            return;
        }

        if (!pathInsideRoot)
            return;

        if (manifest.AttestationLevel >= policy.RequiredLevel)
        {
            findings.Add(HardwareAttestationFinding.Info(
                "hardware-attestation-level-satisfied",
                $"Observed attestation level '{manifest.AttestationLevel}' satisfies required level '{policy.RequiredLevel}'."));
        }
        else
        {
            findings.Add(HardwareAttestationFinding.Error(
                "hardware-attestation-level-not-met",
                $"Observed attestation level '{manifest.AttestationLevel}' does not satisfy required level '{policy.RequiredLevel}'."));
        }

        if (policy.RequiredTechnology != HardwareAttestationTechnology.None
            && policy.RequiredTechnology != manifest.Technology)
        {
            findings.Add(HardwareAttestationFinding.Error(
                "hardware-attestation-technology-mismatch",
                $"Attestation technology '{manifest.Technology}' does not match required technology '{policy.RequiredTechnology}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredProviderId)
            && !string.Equals(policy.RequiredProviderId, manifest.ProviderId, StringComparison.Ordinal))
        {
            findings.Add(HardwareAttestationFinding.Error(
                "hardware-attestation-provider-mismatch",
                $"Provider id '{manifest.ProviderId}' does not match required provider id '{policy.RequiredProviderId}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredAttestationId)
            && !string.Equals(policy.RequiredAttestationId, manifest.AttestationId, StringComparison.Ordinal))
        {
            findings.Add(HardwareAttestationFinding.Error(
                "hardware-attestation-id-mismatch",
                $"Attestation id '{manifest.AttestationId}' does not match required attestation id '{policy.RequiredAttestationId}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredMeasurement)
            && !string.Equals(policy.RequiredMeasurement, manifest.Measurement, StringComparison.Ordinal))
        {
            findings.Add(HardwareAttestationFinding.Error(
                "hardware-attestation-measurement-mismatch",
                $"Measurement '{manifest.Measurement ?? "<none>"}' does not match required measurement '{policy.RequiredMeasurement}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredPolicyId)
            && !string.Equals(policy.RequiredPolicyId, manifest.PolicyId, StringComparison.Ordinal))
        {
            findings.Add(HardwareAttestationFinding.Error(
                "hardware-attestation-policy-id-mismatch",
                $"Policy id '{manifest.PolicyId ?? "<none>"}' does not match required policy id '{policy.RequiredPolicyId}'."));
        }
    }

    private static HardwareAttestationEvidence CreateEvidence(
        string inputPath,
        string fullPath,
        HardwareAttestationPolicy policy,
        HardwareAttestationLevel observedLevel,
        HardwareAttestationTechnology observedTechnology,
        string? providerId,
        string? providerVersion,
        string? attestationId,
        string? measurement,
        string? policyId,
        string? reportFormat,
        string? reportDigest,
        string? rootPath,
        bool pathInsideRoot,
        string evidenceSource,
        DateTimeOffset verifiedAtUtc,
        IReadOnlyList<HardwareAttestationFinding> findings)
    {
        var hasErrors = findings.Any(static f => f.Severity == HardwareAttestationFindingSeverity.Error);
        var hasWarnings = findings.Any(static f => f.Severity == HardwareAttestationFindingSeverity.Warning);
        var decision = hasErrors || (policy.RequiresAttestation && observedLevel < policy.RequiredLevel)
            ? HardwareAttestationDecision.FailClosed
            : hasWarnings
                ? HardwareAttestationDecision.Warning
                : HardwareAttestationDecision.Pass;

        return new HardwareAttestationEvidence(
            inputPath,
            fullPath,
            policy,
            observedLevel,
            observedTechnology,
            decision,
            providerId,
            providerVersion,
            attestationId,
            measurement,
            policyId,
            reportFormat,
            reportDigest,
            rootPath,
            pathInsideRoot,
            evidenceSource,
            verifiedAtUtc,
            findings);
    }
}
