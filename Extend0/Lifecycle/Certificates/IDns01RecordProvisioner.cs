namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Publishes or prepares DNS-01 TXT records for ACME authorization.
/// </summary>
public interface IDns01RecordProvisioner
{
    /// <summary>
    /// Gets the stable provider name reported in diagnostics and provisioning results.
    /// </summary>
    string ProviderName { get; }

    /// <summary>
    /// Provisions or prepares the TXT record required by a DNS-01 challenge.
    /// </summary>
    /// <param name="challenge">DNS-01 challenge proof material.</param>
    /// <param name="options">Provisioning options such as suggested TTL.</param>
    /// <param name="cancellationToken">Token used to cancel provider work.</param>
    /// <returns>Provisioning result containing the TXT record and any follow-up instructions.</returns>
    Task<Dns01ProvisioningResult> ProvisionAsync(
        Dns01Challenge challenge,
        Dns01ProvisioningOptions options,
        CancellationToken cancellationToken = default);
}
