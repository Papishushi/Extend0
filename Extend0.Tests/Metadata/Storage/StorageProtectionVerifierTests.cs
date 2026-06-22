using Extend0.Lifecycle.Assurance;

namespace Extend0.Tests.Metadata.Storage;

public sealed class StorageProtectionVerifierTests
{
    [Fact]
    public void DiagnosePath_WithNoPolicyAndNoManifest_PassesAsUnprotected()
    {
        var root = CreateTempDirectory();
        try
        {
            var path = Path.Combine(root, "table.meta");

            var evidence = StorageProtectionVerifier.DiagnosePath(path);

            Assert.Equal(StorageProtectionDecision.Pass, evidence.Decision);
            Assert.Equal(StorageProtectionLevel.None, evidence.ObservedLevel);
            Assert.Equal("none", evidence.EvidenceSource);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithRequiredProtectionAndNoManifest_FailsClosed()
    {
        var root = CreateTempDirectory();
        try
        {
            var path = Path.Combine(root, "table.meta");
            var policy = StorageProtectionPolicy.Require(StorageProtectionLevel.ProviderAttestedEncrypted);

            var evidence = StorageProtectionVerifier.DiagnosePath(path, policy);

            Assert.Equal(StorageProtectionDecision.FailClosed, evidence.Decision);
            Assert.Contains(evidence.Findings, static finding => finding.Id == "storage-protection-evidence-missing");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithProviderManifestInsideMount_SatisfiesPolicy()
    {
        var root = CreateTempDirectory();
        try
        {
            var manifest = StorageProtectionManifest.Create(
                "test-provider",
                "volume-1",
                StorageProtectionLevel.ProviderAttestedEncrypted,
                root);
            StorageProtectionVerifier.SaveManifest(
                Path.Combine(root, StorageProtectionVerifier.ManifestFileName),
                manifest);

            var path = Path.Combine(root, "tables", "table.meta");
            var policy = StorageProtectionPolicy.Require(
                StorageProtectionLevel.ProviderAttestedEncrypted,
                "test-provider",
                "volume-1");

            var evidence = StorageProtectionVerifier.DiagnosePath(path, policy);

            Assert.Equal(StorageProtectionDecision.Pass, evidence.Decision);
            Assert.Equal(StorageProtectionLevel.ProviderAttestedEncrypted, evidence.ObservedLevel);
            Assert.True(evidence.PathInsideMount);
            Assert.Equal("test-provider", evidence.ProviderId);
            Assert.Equal("volume-1", evidence.ProtectionId);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithManifestButPathOutsideMount_FailsClosed()
    {
        var root = CreateTempDirectory();
        var outside = CreateTempDirectory();
        try
        {
            var manifest = StorageProtectionManifest.Create(
                "test-provider",
                "volume-1",
                StorageProtectionLevel.ProviderAttestedEncrypted,
                root);
            var manifestPath = Path.Combine(root, StorageProtectionVerifier.ManifestFileName);
            StorageProtectionVerifier.SaveManifest(manifestPath, manifest);

            var evidence = StorageProtectionVerifier.DiagnosePath(
                Path.Combine(outside, "table.meta"),
                StorageProtectionPolicy.Require(StorageProtectionLevel.DeclaredEncrypted),
                manifestPath);

            Assert.Equal(StorageProtectionDecision.FailClosed, evidence.Decision);
            Assert.False(evidence.PathInsideMount);
            Assert.Contains(evidence.Findings, static finding => finding.Id == "storage-path-outside-mount");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
            Directory.Delete(outside, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithVerifiedHandle_SatisfiesPolicy()
    {
        var root = CreateTempDirectory();
        try
        {
            var handle = new ProtectedStorageHandle(
                "handle-provider",
                "handle-volume",
                root,
                StorageProtectionLevel.Extend0ManagedProtectedMount);
            var path = Path.Combine(root, "table.meta");

            var evidence = StorageProtectionVerifier.DiagnosePath(
                path,
                handle,
                StorageProtectionPolicy.Require(StorageProtectionLevel.PlatformVerifiedEncrypted));

            Assert.Equal(StorageProtectionDecision.Pass, evidence.Decision);
            Assert.Equal("handle", evidence.EvidenceSource);
            Assert.Equal(StorageProtectionLevel.Extend0ManagedProtectedMount, evidence.ObservedLevel);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
