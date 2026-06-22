namespace Extend0.Lifecycle.Certificates;

public sealed class ManualDns01RecordProvisioner : IDns01RecordProvisioner
{
    public const string ManualProviderName = "manual";

    public string ProviderName => ManualProviderName;

    public Task<Dns01ProvisioningResult> ProvisionAsync(
        Dns01Challenge challenge,
        Dns01ProvisioningOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(challenge);
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();

        var result = new Dns01ProvisioningResult(
            ProviderName,
            challenge.Domain,
            challenge.AuthorizationDomain,
            challenge.TxtRecordName,
            "TXT",
            challenge.TxtRecordValue,
            options.TtlSeconds,
            RequiresManualAction: true,
            DateTimeOffset.UtcNow,
            [
                "Create or update the TXT record in the authoritative DNS zone.",
                "Wait for DNS propagation before asking the ACME certificate authority to validate the challenge.",
                "Remove the TXT record after the ACME order is finalized."
            ]);

        return Task.FromResult(result);
    }
}
