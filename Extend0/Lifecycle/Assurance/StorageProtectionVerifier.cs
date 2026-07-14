using System.Text.Json;
using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Portable evidence-based verifier for protected storage paths.
/// </summary>
public static class StorageProtectionVerifier
{
    /// <summary>
    /// Default manifest file name used to declare storage protection evidence.
    /// </summary>
    public const string ManifestFileName = ".extend0-protection.json";

    private static readonly JsonSerializerOptions Json = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    static StorageProtectionVerifier()
    {
        Json.Converters.Add(new JsonStringEnumConverter());
    }

    /// <summary>
    /// Diagnoses storage protection evidence for a path using an optional manifest.
    /// </summary>
    /// <param name="path">Path to verify.</param>
    /// <param name="policy">Storage protection policy to enforce.</param>
    /// <param name="manifestPath">Optional explicit manifest path. When omitted, parent directories are searched.</param>
    /// <param name="verifiedAtUtc">Optional verification timestamp, mainly for deterministic tests.</param>
    /// <returns>Evidence describing observed protection, policy outcome, and findings.</returns>
    public static StorageProtectionEvidence DiagnosePath(
        string path,
        StorageProtectionPolicy policy = default,
        string? manifestPath = null,
        DateTimeOffset? verifiedAtUtc = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        policy.Validate();

        var now = verifiedAtUtc ?? DateTimeOffset.UtcNow;
        var fullPath = Path.GetFullPath(path);
        var findings = new List<StorageProtectionFinding>();
        var resolvedManifestPath = ResolveManifestPath(fullPath, manifestPath);

        if (resolvedManifestPath is null)
        {
            findings.Add(policy.RequiresProtection
                ? StorageProtectionFinding.Error("storage-protection-evidence-missing", "No storage protection manifest was found for the path.")
                : StorageProtectionFinding.Info("storage-protection-not-required", "No storage protection policy is required for the path."));

            return CreateEvidence(path, fullPath, policy, StorageProtectionLevel.None, null, null, null, null, false, "none", now, findings);
        }

        findings.Add(StorageProtectionFinding.Info("storage-protection-manifest", $"Found storage protection manifest '{resolvedManifestPath}'."));

        StorageProtectionManifest? manifest;
        try
        {
            manifest = JsonSerializer.Deserialize<StorageProtectionManifest>(File.ReadAllText(resolvedManifestPath), Json);
        }
        catch (Exception ex)
        {
            findings.Add(StorageProtectionFinding.Error("storage-protection-manifest-invalid", $"Could not read storage protection manifest: {ex.Message}"));
            return CreateEvidence(path, fullPath, policy, StorageProtectionLevel.None, null, null, null, null, false, "manifest", now, findings);
        }

        if (manifest is null)
        {
            findings.Add(StorageProtectionFinding.Error("storage-protection-manifest-empty", "Storage protection manifest is empty."));
            return CreateEvidence(path, fullPath, policy, StorageProtectionLevel.None, null, null, null, null, false, "manifest", now, findings);
        }

        ValidateManifest(manifest, findings);

        var mountRoot = ResolveMountRoot(resolvedManifestPath, manifest.RootPath);
        var pathInsideMount = ContainsPath(mountRoot, fullPath);
        if (pathInsideMount)
            findings.Add(StorageProtectionFinding.Info("storage-path-contained", "Path is inside the protected mount root."));
        else
            findings.Add(StorageProtectionFinding.Error("storage-path-outside-mount", $"Path '{fullPath}' is outside protected mount root '{mountRoot}'."));

        ValidatePolicy(policy, manifest, pathInsideMount, findings);

        var observedLevel = pathInsideMount && findings.All(static f => f.Id != "storage-protection-manifest-invalid")
            ? manifest.ProtectionLevel
            : StorageProtectionLevel.None;

        return CreateEvidence(
            path,
            fullPath,
            policy,
            observedLevel,
            manifest.ProviderId,
            manifest.ProviderVersion,
            manifest.ProtectionId,
            mountRoot,
            pathInsideMount,
            manifest.EvidenceSource ?? "manifest",
            now,
            findings);
    }

    /// <summary>
    /// Diagnoses storage protection evidence for a path using a provider-supplied protected storage handle.
    /// </summary>
    /// <param name="path">Path to verify.</param>
    /// <param name="handle">Protected storage handle that should cover the path.</param>
    /// <param name="policy">Storage protection policy to enforce.</param>
    /// <param name="verifiedAtUtc">Optional verification timestamp, mainly for deterministic tests.</param>
    /// <returns>Evidence describing observed protection, policy outcome, and findings.</returns>
    public static StorageProtectionEvidence DiagnosePath(
        string path,
        IProtectedStorageHandle handle,
        StorageProtectionPolicy policy = default,
        DateTimeOffset? verifiedAtUtc = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(handle);
        policy.Validate();

        var now = verifiedAtUtc ?? DateTimeOffset.UtcNow;
        var fullPath = Path.GetFullPath(path);
        var findings = new List<StorageProtectionFinding>();

        var pathInsideMount = handle.ContainsPath(fullPath);
        findings.Add(pathInsideMount
            ? StorageProtectionFinding.Info("storage-path-contained", "Path is inside the protected storage handle root.")
            : StorageProtectionFinding.Error("storage-path-outside-mount", $"Path '{fullPath}' is outside protected storage handle root '{handle.RootPath}'."));

        if (handle.IsVerified)
            findings.Add(StorageProtectionFinding.Info("storage-handle-verified", $"Storage handle '{handle.ProtectionId}' is verified by provider '{handle.ProviderId}'."));
        else
            findings.Add(StorageProtectionFinding.Error("storage-handle-unverified", $"Storage handle '{handle.ProtectionId}' is not verified."));

        ValidatePolicy(policy, handle, pathInsideMount, findings);

        var observedLevel = pathInsideMount && handle.IsVerified
            ? handle.ProtectionLevel
            : StorageProtectionLevel.None;

        return CreateEvidence(
            path,
            fullPath,
            policy,
            observedLevel,
            handle.ProviderId,
            null,
            handle.ProtectionId,
            Path.GetFullPath(handle.RootPath),
            pathInsideMount,
            "handle",
            now,
            findings);
    }

    /// <summary>
    /// Saves a storage protection manifest as JSON.
    /// </summary>
    /// <param name="manifestPath">Destination manifest path.</param>
    /// <param name="manifest">Manifest to serialize.</param>
    /// <param name="overwrite">Whether an existing file may be overwritten.</param>
    public static void SaveManifest(string manifestPath, StorageProtectionManifest manifest, bool overwrite = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestPath);
        ArgumentNullException.ThrowIfNull(manifest);

        var directory = Path.GetDirectoryName(Path.GetFullPath(manifestPath));
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        if (!overwrite && File.Exists(manifestPath))
            throw new IOException($"Storage protection manifest already exists: {manifestPath}");

        File.WriteAllText(manifestPath, JsonSerializer.Serialize(manifest, Json));
    }

    /// <summary>
    /// Determines whether a candidate path is equal to or inside a root path.
    /// </summary>
    /// <param name="rootPath">Root path that defines the protected scope.</param>
    /// <param name="candidatePath">Path to test.</param>
    /// <returns><see langword="true"/> when the candidate path is covered by the root; otherwise <see langword="false"/>.</returns>
    public static bool ContainsPath(string rootPath, string candidatePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(candidatePath);

        var root = Path.GetFullPath(rootPath);
        var candidate = Path.GetFullPath(candidatePath);
        var comparison = PathComparison;

        if (string.Equals(root, candidate, comparison))
            return true;

        var rootWithSeparator = root.EndsWith(Path.DirectorySeparatorChar)
            || root.EndsWith(Path.AltDirectorySeparatorChar)
                ? root
                : root + Path.DirectorySeparatorChar;

        return candidate.StartsWith(rootWithSeparator, comparison);
    }

    private static StringComparison PathComparison =>
        OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;

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

    private static string ResolveMountRoot(string manifestPath, string? manifestRootPath)
    {
        if (!string.IsNullOrWhiteSpace(manifestRootPath))
            return Path.GetFullPath(manifestRootPath);

        return Path.GetDirectoryName(Path.GetFullPath(manifestPath))
            ?? Path.GetPathRoot(Path.GetFullPath(manifestPath))
            ?? Path.GetFullPath(".");
    }

    private static void ValidateManifest(StorageProtectionManifest manifest, List<StorageProtectionFinding> findings)
    {
        if (manifest.Version != StorageProtectionManifest.CurrentVersion)
            findings.Add(StorageProtectionFinding.Error("storage-protection-manifest-version", $"Manifest version {manifest.Version} is not supported."));
        else
            findings.Add(StorageProtectionFinding.Info("storage-protection-manifest-version", $"Manifest version {manifest.Version}."));

        if (string.IsNullOrWhiteSpace(manifest.ProviderId))
            findings.Add(StorageProtectionFinding.Error("storage-protection-provider", "Manifest does not declare a provider id."));
        else
            findings.Add(StorageProtectionFinding.Info("storage-protection-provider", $"Provider id '{manifest.ProviderId}'."));

        if (string.IsNullOrWhiteSpace(manifest.ProtectionId))
            findings.Add(StorageProtectionFinding.Error("storage-protection-id", "Manifest does not declare a protection id."));
        else
            findings.Add(StorageProtectionFinding.Info("storage-protection-id", $"Protection id '{manifest.ProtectionId}'."));

        if (!Enum.IsDefined(manifest.ProtectionLevel))
            findings.Add(StorageProtectionFinding.Error("storage-protection-level", $"Manifest declares unknown protection level '{manifest.ProtectionLevel}'."));
        else
            findings.Add(StorageProtectionFinding.Info("storage-protection-level", $"Manifest declares protection level '{manifest.ProtectionLevel}'."));
    }

    private static void ValidatePolicy(
        StorageProtectionPolicy policy,
        StorageProtectionManifest manifest,
        bool pathInsideMount,
        List<StorageProtectionFinding> findings)
    {
        if (!policy.RequiresProtection)
        {
            findings.Add(StorageProtectionFinding.Info("storage-protection-policy", "No storage protection policy is required."));
            return;
        }

        if (!pathInsideMount)
            return;

        if (manifest.ProtectionLevel >= policy.RequiredLevel)
        {
            findings.Add(StorageProtectionFinding.Info(
                "storage-protection-level-satisfied",
                $"Observed protection level '{manifest.ProtectionLevel}' satisfies required level '{policy.RequiredLevel}'."));
        }
        else
        {
            findings.Add(StorageProtectionFinding.Error(
                "storage-protection-level-not-met",
                $"Observed protection level '{manifest.ProtectionLevel}' does not satisfy required level '{policy.RequiredLevel}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredProviderId)
            && !string.Equals(policy.RequiredProviderId, manifest.ProviderId, StringComparison.Ordinal))
        {
            findings.Add(StorageProtectionFinding.Error(
                "storage-protection-provider-mismatch",
                $"Provider id '{manifest.ProviderId}' does not match required provider id '{policy.RequiredProviderId}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredProtectionId)
            && !string.Equals(policy.RequiredProtectionId, manifest.ProtectionId, StringComparison.Ordinal))
        {
            findings.Add(StorageProtectionFinding.Error(
                "storage-protection-id-mismatch",
                $"Protection id '{manifest.ProtectionId}' does not match required protection id '{policy.RequiredProtectionId}'."));
        }
    }

    private static void ValidatePolicy(
        StorageProtectionPolicy policy,
        IProtectedStorageHandle handle,
        bool pathInsideMount,
        List<StorageProtectionFinding> findings)
    {
        if (!policy.RequiresProtection)
        {
            findings.Add(StorageProtectionFinding.Info("storage-protection-policy", "No storage protection policy is required."));
            return;
        }

        if (!pathInsideMount || !handle.IsVerified)
            return;

        if (handle.ProtectionLevel >= policy.RequiredLevel)
        {
            findings.Add(StorageProtectionFinding.Info(
                "storage-protection-level-satisfied",
                $"Observed protection level '{handle.ProtectionLevel}' satisfies required level '{policy.RequiredLevel}'."));
        }
        else
        {
            findings.Add(StorageProtectionFinding.Error(
                "storage-protection-level-not-met",
                $"Observed protection level '{handle.ProtectionLevel}' does not satisfy required level '{policy.RequiredLevel}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredProviderId)
            && !string.Equals(policy.RequiredProviderId, handle.ProviderId, StringComparison.Ordinal))
        {
            findings.Add(StorageProtectionFinding.Error(
                "storage-protection-provider-mismatch",
                $"Provider id '{handle.ProviderId}' does not match required provider id '{policy.RequiredProviderId}'."));
        }

        if (!string.IsNullOrWhiteSpace(policy.RequiredProtectionId)
            && !string.Equals(policy.RequiredProtectionId, handle.ProtectionId, StringComparison.Ordinal))
        {
            findings.Add(StorageProtectionFinding.Error(
                "storage-protection-id-mismatch",
                $"Protection id '{handle.ProtectionId}' does not match required protection id '{policy.RequiredProtectionId}'."));
        }
    }

    private static StorageProtectionEvidence CreateEvidence(
        string inputPath,
        string fullPath,
        StorageProtectionPolicy policy,
        StorageProtectionLevel observedLevel,
        string? providerId,
        string? providerVersion,
        string? protectionId,
        string? mountRoot,
        bool pathInsideMount,
        string evidenceSource,
        DateTimeOffset verifiedAtUtc,
        IReadOnlyList<StorageProtectionFinding> findings)
    {
        var hasErrors = findings.Any(static f => f.Severity == StorageProtectionFindingSeverity.Error);
        var hasWarnings = findings.Any(static f => f.Severity == StorageProtectionFindingSeverity.Warning);
        var decision = hasErrors || (policy.RequiresProtection && observedLevel < policy.RequiredLevel)
            ? StorageProtectionDecision.FailClosed
            : hasWarnings
                ? StorageProtectionDecision.Warning
                : StorageProtectionDecision.Pass;

        return new StorageProtectionEvidence(
            inputPath,
            fullPath,
            policy,
            observedLevel,
            decision,
            providerId,
            providerVersion,
            protectionId,
            mountRoot,
            pathInsideMount,
            evidenceSource,
            verifiedAtUtc,
            findings);
    }
}
