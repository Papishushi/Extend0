using Extend0.Lifecycle.Assurance;

namespace Extend0.Tests.Metadata.Storage;

public sealed class HardwareAttestationVerifierTests
{
    [Fact]
    public void DiagnosePath_WithNoPolicyAndNoManifest_PassesAsUnspecified()
    {
        var root = CreateTempDirectory();
        try
        {
            var path = Path.Combine(root, "table.meta");

            var evidence = HardwareAttestationVerifier.DiagnosePath(path);

            Assert.Equal(HardwareAttestationDecision.Pass, evidence.Decision);
            Assert.Equal(HardwareAttestationLevel.None, evidence.ObservedLevel);
            Assert.Equal(HardwareAttestationTechnology.None, evidence.ObservedTechnology);
            Assert.Equal("none", evidence.EvidenceSource);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithRequiredRemoteAttestationAndNoManifest_FailsClosed()
    {
        var root = CreateTempDirectory();
        try
        {
            var path = Path.Combine(root, "table.meta");
            var policy = HardwareAttestationPolicy.Require(HardwareAttestationLevel.RemoteAttested);

            var evidence = HardwareAttestationVerifier.DiagnosePath(path, policy);

            Assert.Equal(HardwareAttestationDecision.FailClosed, evidence.Decision);
            Assert.Contains(evidence.Findings, static finding => finding.Id == "hardware-attestation-evidence-missing");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithIntelSgxManifestInsideRoot_SatisfiesPolicy()
    {
        var root = CreateTempDirectory();
        try
        {
            var manifest = HardwareAttestationManifest.Create(
                "sgx-provider",
                "quote-1",
                HardwareAttestationTechnology.IntelSgx,
                HardwareAttestationLevel.RemoteAttested,
                root,
                measurement: "mrenclave:abc",
                policyId: "policy-a");
            HardwareAttestationVerifier.SaveManifest(
                Path.Combine(root, HardwareAttestationVerifier.ManifestFileName),
                manifest);

            var path = Path.Combine(root, "tables", "table.meta");
            var policy = HardwareAttestationPolicy.Require(
                HardwareAttestationLevel.RemoteAttested,
                HardwareAttestationTechnology.IntelSgx,
                "sgx-provider",
                "quote-1",
                "mrenclave:abc",
                "policy-a");

            var evidence = HardwareAttestationVerifier.DiagnosePath(path, policy);

            Assert.Equal(HardwareAttestationDecision.Pass, evidence.Decision);
            Assert.Equal(HardwareAttestationLevel.RemoteAttested, evidence.ObservedLevel);
            Assert.Equal(HardwareAttestationTechnology.IntelSgx, evidence.ObservedTechnology);
            Assert.True(evidence.PathInsideRoot);
            Assert.Equal("sgx-provider", evidence.ProviderId);
            Assert.Equal("quote-1", evidence.AttestationId);
            Assert.Equal("mrenclave:abc", evidence.Measurement);
            Assert.Equal("policy-a", evidence.PolicyId);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void DiagnosePath_WithDifferentTechnology_FailsPolicy()
    {
        var root = CreateTempDirectory();
        try
        {
            var manifest = HardwareAttestationManifest.Create(
                "tee-provider",
                "quote-1",
                HardwareAttestationTechnology.ArmTrustZone,
                HardwareAttestationLevel.ProviderAttested,
                root);
            HardwareAttestationVerifier.SaveManifest(
                Path.Combine(root, HardwareAttestationVerifier.ManifestFileName),
                manifest);

            var evidence = HardwareAttestationVerifier.DiagnosePath(
                Path.Combine(root, "table.meta"),
                HardwareAttestationPolicy.Require(
                    HardwareAttestationLevel.ProviderAttested,
                    HardwareAttestationTechnology.IntelSgx));

            Assert.Equal(HardwareAttestationDecision.FailClosed, evidence.Decision);
            Assert.Contains(evidence.Findings, static finding => finding.Id == "hardware-attestation-technology-mismatch");
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
