using System.Text.Json;
using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Portable evidence-based verifier for storage continuity across ownership movement.
/// </summary>
public static class StorageContinuityVerifier
{
    public const string ManifestFileName = ".extend0-continuity.json";

    private static readonly JsonSerializerOptions Json = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    static StorageContinuityVerifier()
    {
        Json.Converters.Add(new JsonStringEnumConverter());
    }

    public static StorageContinuityEvidence DiagnosePath(
        string path,
        StorageContinuityPolicy policy = default,
        string? manifestPath = null,
        DateTimeOffset? verifiedAtUtc = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        policy.Validate();

        var now = verifiedAtUtc ?? DateTimeOffset.UtcNow;
        var fullPath = Path.GetFullPath(path);
        var findings = new List<StorageContinuityFinding>();
        var resolvedManifestPath = ResolveManifestPath(fullPath, manifestPath);

        if (resolvedManifestPath is null)
        {
            findings.Add(policy.RequiresContinuity
                ? StorageContinuityFinding.Error("storage-continuity-evidence-missing", "No storage continuity manifest was found for the path.")
                : StorageContinuityFinding.Info("storage-continuity-not-required", "No storage continuity policy is required for the path."));

            return CreateEvidence(path, fullPath, policy, StorageContinuityLevel.None, null, null, null, null, null, false, "none", now, findings);
        }

        findings.Add(StorageContinuityFinding.Info("storage-continuity-manifest", $"Found storage continuity manifest '{resolvedManifestPath}'."));

        StorageContinuityManifest? manifest;
        try
        {
            manifest = JsonSerializer.Deserialize<StorageContinuityManifest>(File.ReadAllText(resolvedManifestPath), Json);
        }
        catch (Exception ex)
        {
            findings.Add(StorageContinuityFinding.Error("storage-continuity-manifest-invalid", $"Could not read storage continuity manifest: {ex.Message}"));
            return CreateEvidence(path, fullPath, policy, StorageContinuityLevel.None, null, null, null, null, null, false, "manifest", now, findings);
        }

        if (manifest is null)
        {
            findings.Add(StorageContinuityFinding.Error("storage-continuity-manifest-empty", "Storage continuity manifest is empty."));
            return CreateEvidence(path, fullPath, policy, StorageContinuityLevel.None, null, null, null, null, null, false, "manifest", now, findings);
        }

        ValidateManifest(manifest, findings);

        var rootPath = ResolveRootPath(resolvedManifestPath, manifest.RootPath);
        var pathInsideRoot = StorageProtectionVerifier.ContainsPath(rootPath, fullPath);
        if (pathInsideRoot)
            findings.Add(StorageContinuityFinding.Info("storage-continuity-path-contained", "Path is inside the declared continuity root."));
        else
            findings.Add(StorageContinuityFinding.Error("storage-continuity-path-outside-root", $"Path '{fullPath}' is outside continuity root '{rootPath}'."));

        ValidatePolicy(policy, manifest, pathInsideRoot, findings);

        var observedLevel = pathInsideRoot && findings.All(static f => f.Id != "storage-continuity-manifest-invalid")
            ? manifest.ContinuityLevel
            : StorageContinuityLevel.None;

        return CreateEvidence(
            path,
            fullPath,
            policy,
            observedLevel,
            manifest.ProviderId,
            manifest.ProviderVersion,
            manifest.ContinuityId,
            manifest.TopologyId,
            rootPath,
            pathInsideRoot,
            manifest.EvidenceSource ?? "manifest",
            now,
            findings);
    }

    public static void SaveManifest(string manifestPath, StorageContinuityManifest manifest, bool overwrite = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestPath);
        ArgumentNullException.ThrowIfNull(manifest);

        var directory = Path.GetDirectoryName(Path.GetFullPath(manifestPath));
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        if (!overwrite && File.Exists(manifestPath))
            throw new IOException($"Storage continuity manifest already exists: {manifestPath}");

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

    private static void ValidateManifest(StorageContinuityManifest manifest, List<StorageContinuityFinding> findings)
    {
        if (manifest.Version != StorageContinuityManifest.CurrentVersion)
            findings.Add(StorageContinuityFinding.Error("storage-continuity-manifest-version", $"Manifest version {manifest.Version} is not supported."));
        else
            findings.Add(StorageContinuityFinding.Info("storage-continuity-manifest-version", $"Manifest version {manifest.Version}."));

        if (string.IsNullOrWhiteSpace(manifest.ProviderId))
            findings.Add(StorageContinuityFinding.Error("storage-continuity-provider", "Manifest does not declare a provider id."));
        else
            findings.Add(StorageContinuityFinding.Info("storage-continuity-provider", $"Provider id '{manifest.ProviderId}'."));

        if (string.IsNullOrWhiteSpace(manifest.ContinuityId))
            findings.Add(StorageContinuityFinding.Error("storage-continuity-id", "Manifest does not declare a continuity id."));
        else
            findings.Add(StorageContinuityFinding.Info("storage-continuity-id", $"Continuity id '{manifest.ContinuityId}'."));

        if (!Enum.IsDefined(manifest.ContinuityLevel))
            findings.Add(StorageContinuityFinding.Error("storage-continuity-level", $"Manifest declares unknown continuity level '{manifest.ContinuityLevel}'."));
        else
            findings.Add(StorageContinuityFinding.Info("storage-continuity-level", $"Manifest declares continuity level '{manifest.ContinuityLevel}'."));
    }

    private static void ValidatePolicy(
        StorageContinuityPolicy policy,
        StorageContinuityManifest manifest,
        bool pathInsideRoot,
        List<StorageContinuityFinding> findings)
    {
        if (!policy.RequiresContinuity)
        {
            findings.Add(StorageContinuityFinding.Info("storage-continuity-policy", "No storage continuity policy is required."));
            return;
        }

        if (!pathInsideRoot)
            return;

        if (manifest.ContinuityLevel >= policy.RequiredLevel)
        {
            findings.Add(StorageContinuityFinding.Info(
                "storage-continuity-level-satisfied",
                $"Observed continuity level '{manifest.ContinuityLevel}' satisfies required level '{policy.RequiredLevel}'."));
        }
        else
        {
            findings.Add(StorageContinuityFinding.Error(
                "storage-continuity-level-not-met",
                $"Observed continuity level '{manifest.ContinuityLevel}' does not satisfy required level '{policy.RequiredLevel}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredProviderId)
            && !string.Equals(policy.RequiredProviderId, manifest.ProviderId, StringComparison.Ordinal))
        {
            findings.Add(StorageContinuityFinding.Error(
                "storage-continuity-provider-mismatch",
                $"Provider id '{manifest.ProviderId}' does not match required provider id '{policy.RequiredProviderId}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredContinuityId)
            && !string.Equals(policy.RequiredContinuityId, manifest.ContinuityId, StringComparison.Ordinal))
        {
            findings.Add(StorageContinuityFinding.Error(
                "storage-continuity-id-mismatch",
                $"Continuity id '{manifest.ContinuityId}' does not match required continuity id '{policy.RequiredContinuityId}'."));
        }
    }

    private static StorageContinuityEvidence CreateEvidence(
        string inputPath,
        string fullPath,
        StorageContinuityPolicy policy,
        StorageContinuityLevel observedLevel,
        string? providerId,
        string? providerVersion,
        string? continuityId,
        string? topologyId,
        string? rootPath,
        bool pathInsideRoot,
        string evidenceSource,
        DateTimeOffset verifiedAtUtc,
        IReadOnlyList<StorageContinuityFinding> findings)
    {
        var hasErrors = findings.Any(static f => f.Severity == StorageContinuityFindingSeverity.Error);
        var hasWarnings = findings.Any(static f => f.Severity == StorageContinuityFindingSeverity.Warning);
        var decision = hasErrors || (policy.RequiresContinuity && observedLevel < policy.RequiredLevel)
            ? StorageContinuityDecision.FailClosed
            : hasWarnings
                ? StorageContinuityDecision.Warning
                : StorageContinuityDecision.Pass;

        return new StorageContinuityEvidence(
            inputPath,
            fullPath,
            policy,
            observedLevel,
            decision,
            providerId,
            providerVersion,
            continuityId,
            topologyId,
            rootPath,
            pathInsideRoot,
            evidenceSource,
            verifiedAtUtc,
            findings);
    }
}
