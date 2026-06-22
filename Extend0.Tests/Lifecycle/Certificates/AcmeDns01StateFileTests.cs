using Extend0.Lifecycle.Certificates;

namespace Extend0.Tests.Lifecycle.Certificates;

public sealed class AcmeDns01StateFileTests
{
    [Fact]
    public void Save_WithPassphraseProtection_EncryptsSensitiveState()
    {
        var path = Path.Combine(Path.GetTempPath(), "extend0-acme-state-" + Guid.NewGuid().ToString("N") + ".json");
        try
        {
            var state = CreateState();

            AcmeDns01StateFile.Save(
                path,
                state,
                AcmeDns01StateProtectionOptions.FromPassphrase("correct horse battery staple"));

            var text = File.ReadAllText(path);
            Assert.Contains("extend0.acme-dns01-state", text, StringComparison.Ordinal);
            Assert.DoesNotContain("SECRET_ACCOUNT_KEY", text, StringComparison.Ordinal);
            Assert.DoesNotContain("SECRET_CERTIFICATE_KEY", text, StringComparison.Ordinal);

            var loaded = AcmeDns01StateFile.Load(
                path,
                AcmeDns01StateProtectionOptions.FromPassphrase("correct horse battery staple"),
                out var detectedProtection);

            Assert.Equal(AcmeDns01StateProtectionKind.Passphrase, detectedProtection);
            Assert.Equal(state.AccountKeyPem, loaded.AccountKeyPem);
            Assert.Equal(state.CertificateKeyPem, loaded.CertificateKeyPem);
            Assert.Equal(state.OrderUrl, loaded.OrderUrl);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public void Load_WithProtectedStateAndMissingPassphrase_FailsClosed()
    {
        var path = Path.Combine(Path.GetTempPath(), "extend0-acme-state-" + Guid.NewGuid().ToString("N") + ".json");
        try
        {
            AcmeDns01StateFile.Save(
                path,
                CreateState(),
                AcmeDns01StateProtectionOptions.FromPassphrase("correct passphrase"));

            var exception = Assert.Throws<InvalidOperationException>(() =>
                AcmeDns01StateFile.Load(path, AcmeDns01StateProtectionOptions.None, out _));

            Assert.Contains("passphrase-protected", exception.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public void Load_WithProtectedStateAndWrongPassphrase_FailsClosed()
    {
        var path = Path.Combine(Path.GetTempPath(), "extend0-acme-state-" + Guid.NewGuid().ToString("N") + ".json");
        try
        {
            AcmeDns01StateFile.Save(
                path,
                CreateState(),
                AcmeDns01StateProtectionOptions.FromPassphrase("correct passphrase"));

            var exception = Assert.Throws<InvalidOperationException>(() =>
                AcmeDns01StateFile.Load(
                    path,
                    AcmeDns01StateProtectionOptions.FromPassphrase("wrong passphrase"),
                    out _));

            Assert.Contains("Could not decrypt", exception.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static AcmeDns01OrderState CreateState() =>
        new(
            "https://acme.test/directory",
            "https://acme.test/account/1",
            "SECRET_ACCOUNT_KEY",
            "thumbprint",
            "SECRET_CERTIFICATE_KEY",
            ["example.com"],
            "https://acme.test/order/1",
            "https://acme.test/finalize/1",
            null,
            "pending",
            [
                new AcmeDns01AuthorizationState(
                    "https://acme.test/auth/1",
                    "example.com",
                    false,
                    "pending",
                    "https://acme.test/challenge/1",
                    "token",
                    "_acme-challenge.example.com",
                    "txt-value")
            ],
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow);
}
