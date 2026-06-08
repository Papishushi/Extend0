using Extend0.Lifecycle.CrossProcess;
using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessHandshakeTests
{
    [Fact]
    public void TryValidateHelloLine_AcceptsMatchingProtocol()
    {
        var descriptor = LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor;
        var line = LifecycleCrossProcessHarness.BuildHelloLine(descriptor);

        var ok = LifecycleCrossProcessHarness.TryValidateHelloLine(line, descriptor, out var error);

        Assert.True(ok);
        Assert.Equal(string.Empty, error);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-json")]
    [InlineData("{\"ok\":true}")]
    public void TryValidateHelloLine_RejectsMalformedPayloads(string line)
    {
        var ok = LifecycleCrossProcessHarness.TryValidateHelloLine(line, LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor, out var error);

        Assert.False(ok);
        Assert.NotEmpty(error);
    }

    [Fact]
    public void TryValidateHelloLine_RejectsProtocolKindIdentityAndVersionMismatches()
    {
        var expected = LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor;

        var wrongKind = new CrossProcessProtocolDescriptor(TransportKind.Custom, expected.ProtocolId, expected.ProtocolVersion);
        var wrongId = new CrossProcessProtocolDescriptor(expected.TransportKind, expected.ProtocolId + "-other", expected.ProtocolVersion);
        var wrongVersion = new CrossProcessProtocolDescriptor(expected.TransportKind, expected.ProtocolId, expected.ProtocolVersion + 1);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(LifecycleCrossProcessHarness.BuildHelloLine(wrongKind), expected, out var kindError));
        Assert.Contains("transport", kindError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(LifecycleCrossProcessHarness.BuildHelloLine(wrongId), expected, out var idError));
        Assert.Contains("protocol", idError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(LifecycleCrossProcessHarness.BuildHelloLine(wrongVersion), expected, out var versionError));
        Assert.Contains("version", versionError, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryValidateHelloLine_RejectsMissingFingerprintInvalidTransportAndInvalidVersionFormat()
    {
        var expected = LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor;

        var missingFingerprint = $"HELLO x=1 tk={expected.TransportKind} proto={expected.ProtocolId} ver={expected.ProtocolVersion}";
        var blankFingerprint = $"HELLO fp=\u00A0 tk={expected.TransportKind} proto={expected.ProtocolId} ver={expected.ProtocolVersion}";
        var missingTransport = $"HELLO fp=abc proto={expected.ProtocolId} ver={expected.ProtocolVersion} x=1";
        var invalidTransport = $"HELLO fp=abc tk=NotAKind proto={expected.ProtocolId} ver={expected.ProtocolVersion}";
        var missingProtocol = $"HELLO fp=abc tk={expected.TransportKind} ver={expected.ProtocolVersion} x=1";
        var missingVersion = $"HELLO fp=abc tk={expected.TransportKind} proto={expected.ProtocolId} x=1";
        var invalidVersion = $"HELLO fp=abc tk={expected.TransportKind} proto={expected.ProtocolId} ver=NaN";

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(missingFingerprint, expected, out var fpError));
        Assert.Contains("fingerprint", fpError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(blankFingerprint, expected, out var blankFpError));
        Assert.Contains("fingerprint", blankFpError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(missingTransport, expected, out var missingTransportError));
        Assert.Contains("transport", missingTransportError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(invalidTransport, expected, out var transportError));
        Assert.Contains("transport", transportError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(missingProtocol, expected, out var protocolError));
        Assert.Contains("protocol", protocolError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(missingVersion, expected, out var missingVersionError));
        Assert.Contains("version", missingVersionError, StringComparison.OrdinalIgnoreCase);

        Assert.False(LifecycleCrossProcessHarness.TryValidateHelloLine(invalidVersion, expected, out var versionError));
        Assert.Contains("version", versionError, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryValidateHelloLine_IgnoresMalformedExtraTokens_WhenRequiredFieldsAreValid()
    {
        var expected = LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor;
        var valid = LifecycleCrossProcessHarness.BuildHelloLine(expected);
        var noisy = $"{valid} junk bad= =novalue";

        var ok = LifecycleCrossProcessHarness.TryValidateHelloLine(noisy, expected, out var error);

        Assert.True(ok);
        Assert.Equal(string.Empty, error);
    }
}
