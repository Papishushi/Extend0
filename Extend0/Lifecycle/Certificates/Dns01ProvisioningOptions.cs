namespace Extend0.Lifecycle.Certificates;

public sealed record Dns01ProvisioningOptions(int TtlSeconds)
{
    public const int DefaultTtlSeconds = 300;

    public static Dns01ProvisioningOptions Default { get; } = new(DefaultTtlSeconds);

    public static Dns01ProvisioningOptions Create(int ttlSeconds)
    {
        if (ttlSeconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(ttlSeconds), ttlSeconds, "TTL must be a positive number of seconds.");

        return new Dns01ProvisioningOptions(ttlSeconds);
    }
}
