namespace Extend0.Lifecycle.Certificates;

public interface IDns01RecordProvisioner
{
    string ProviderName { get; }

    Task<Dns01ProvisioningResult> ProvisionAsync(
        Dns01Challenge challenge,
        Dns01ProvisioningOptions options,
        CancellationToken cancellationToken = default);
}
