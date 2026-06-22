using Extend0.Lifecycle.Assurance;

namespace Extend0.Tests.Metadata.Storage;

public sealed class StorageContinuityVerifierTests
{
    [Fact]
    public void DiagnosePath_WithNoPolicyAndNoManifest_PassesAsUnspecified()
    {
        var root = CreateTempDirectory();
        try
        {
            var path = Path.Combine(root, "table.meta");

            var evidence = StorageContinuityVerifier.DiagnosePath(path);

            Assert.Equal(StorageContinuityDecision.Pass, evidence.Decision);
            Assert.Equal(StorageContinuityLevel.None, evidence.ObservedLevel);
            Assert.Equal("none", evidence.EvidenceSource);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithRequiredSharedContinuityAndNoManifest_FailsClosed()
    {
        var root = CreateTempDirectory();
        try
        {
            var path = Path.Combine(root, "table.meta");
            var policy = StorageContinuityPolicy.Require(StorageContinuityLevel.SharedBackingStore);

            var evidence = StorageContinuityVerifier.DiagnosePath(path, policy);

            Assert.Equal(StorageContinuityDecision.FailClosed, evidence.Decision);
            Assert.Contains(evidence.Findings, static finding => finding.Id == "storage-continuity-evidence-missing");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithSharedManifestInsideRoot_SatisfiesOwnershipTransferPolicy()
    {
        var root = CreateTempDirectory();
        try
        {
            var manifest = StorageContinuityManifest.Create(
                "test-continuity-provider",
                "shared-volume-1",
                StorageContinuityLevel.SharedBackingStore,
                root,
                topologyId: "cluster-a");
            StorageContinuityVerifier.SaveManifest(
                Path.Combine(root, StorageContinuityVerifier.ManifestFileName),
                manifest);

            var path = Path.Combine(root, "tables", "table.meta");
            var policy = StorageContinuityPolicy.Require(
                StorageContinuityLevel.SharedBackingStore,
                "test-continuity-provider",
                "shared-volume-1");

            var evidence = StorageContinuityVerifier.DiagnosePath(path, policy);

            Assert.Equal(StorageContinuityDecision.Pass, evidence.Decision);
            Assert.Equal(StorageContinuityLevel.SharedBackingStore, evidence.ObservedLevel);
            Assert.True(evidence.PathInsideRoot);
            Assert.Equal("test-continuity-provider", evidence.ProviderId);
            Assert.Equal("shared-volume-1", evidence.ContinuityId);
            Assert.Equal("cluster-a", evidence.TopologyId);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithLocalOnlyManifest_FailsSharedContinuityPolicy()
    {
        var root = CreateTempDirectory();
        try
        {
            var manifest = StorageContinuityManifest.Create(
                "local-provider",
                "local-volume",
                StorageContinuityLevel.LocalOnly,
                root);
            StorageContinuityVerifier.SaveManifest(
                Path.Combine(root, StorageContinuityVerifier.ManifestFileName),
                manifest);

            var evidence = StorageContinuityVerifier.DiagnosePath(
                Path.Combine(root, "table.meta"),
                StorageContinuityPolicy.Require(StorageContinuityLevel.SharedBackingStore));

            Assert.Equal(StorageContinuityDecision.FailClosed, evidence.Decision);
            Assert.Contains(evidence.Findings, static finding => finding.Id == "storage-continuity-level-not-met");
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
