using System.Security.Cryptography;
using System.Text;
using Extend0.Lifecycle.Certificates;

namespace Extend0.Tests.Lifecycle.Certificates;

public sealed class Dns01ChallengeTests
{
    [Fact]
    public void Create_ComputesAcmeDns01TxtRecordValue()
    {
        const string keyAuthorization = "abc123.accountThumbprint";

        var challenge = Dns01Challenge.Create("Example.COM.", "abc123", keyAuthorization);

        Assert.Equal("example.com", challenge.Domain);
        Assert.Equal("example.com", challenge.AuthorizationDomain);
        Assert.Equal("_acme-challenge.example.com", challenge.TxtRecordName);
        Assert.Equal(ExpectedDns01Value(keyAuthorization), challenge.TxtRecordValue);
    }

    [Fact]
    public void Create_WithWildcardDomain_UsesBaseDomainForTxtRecord()
    {
        var challenge = Dns01Challenge.CreateFromAccountThumbprint(
            "*.api.example.com",
            "wildToken",
            "accountThumbprint");

        Assert.Equal("*.api.example.com", challenge.Domain);
        Assert.Equal("api.example.com", challenge.AuthorizationDomain);
        Assert.Equal("_acme-challenge.api.example.com", challenge.TxtRecordName);
        Assert.Equal("wildToken.accountThumbprint", challenge.KeyAuthorization);
    }

    [Fact]
    public void Create_WithMismatchedKeyAuthorization_RejectsChallenge()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            Dns01Challenge.Create("example.com", "expectedToken", "otherToken.accountThumbprint"));

        Assert.Contains("Key authorization", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Create_WithEmptyDomainLabel_RejectsDomain()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            Dns01Challenge.CreateFromAccountThumbprint("example..com", "abc123", "accountThumbprint"));

        Assert.Contains("empty DNS labels", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ManualProvisioner_ReturnsPublishableTxtInstructions()
    {
        var challenge = Dns01Challenge.CreateFromAccountThumbprint("example.com", "abc123", "accountThumbprint");
        var provisioner = new ManualDns01RecordProvisioner();

        var result = await provisioner.ProvisionAsync(
            challenge,
            Dns01ProvisioningOptions.Create(60));

        Assert.Equal("manual", result.ProviderName);
        Assert.Equal("TXT", result.RecordType);
        Assert.Equal("_acme-challenge.example.com", result.RecordName);
        Assert.Equal(challenge.TxtRecordValue, result.RecordValue);
        Assert.Equal(60, result.TtlSeconds);
        Assert.True(result.RequiresManualAction);
        Assert.NotEmpty(result.Instructions);
    }

    private static string ExpectedDns01Value(string keyAuthorization) =>
        Convert.ToBase64String(SHA256.HashData(Encoding.ASCII.GetBytes(keyAuthorization)))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
}
