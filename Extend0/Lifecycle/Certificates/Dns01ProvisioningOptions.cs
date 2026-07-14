namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Options used when provisioning or presenting a DNS-01 TXT record.
/// </summary>
/// <param name="TtlSeconds">Suggested DNS TTL in seconds.</param>
public sealed record Dns01ProvisioningOptions(int TtlSeconds)
{
    /// <summary>
    /// Default DNS TXT record TTL used by manual provisioning instructions.
    /// </summary>
    public const int DefaultTtlSeconds = 300;

    /// <summary>
    /// Default DNS-01 provisioning options.
    /// </summary>
    public static Dns01ProvisioningOptions Default { get; } = new(DefaultTtlSeconds);

    /// <summary>
    /// Creates validated DNS-01 provisioning options.
    /// </summary>
    /// <param name="ttlSeconds">Suggested DNS TTL in seconds.</param>
    /// <returns>Provisioning options with a positive TTL.</returns>
    public static Dns01ProvisioningOptions Create(int ttlSeconds)
    {
        if (ttlSeconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(ttlSeconds), ttlSeconds, "TTL must be a positive number of seconds.");

        return new Dns01ProvisioningOptions(ttlSeconds);
    }
}
