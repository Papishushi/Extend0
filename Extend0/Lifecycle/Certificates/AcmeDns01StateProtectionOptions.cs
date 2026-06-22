namespace Extend0.Lifecycle.Certificates;

public sealed record AcmeDns01StateProtectionOptions(
    AcmeDns01StateProtectionKind Kind,
    string? Passphrase)
{
    public static AcmeDns01StateProtectionOptions None { get; } = new(AcmeDns01StateProtectionKind.None, null);

    public static AcmeDns01StateProtectionOptions FromPassphrase(string passphrase)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(passphrase);
        return new AcmeDns01StateProtectionOptions(AcmeDns01StateProtectionKind.Passphrase, passphrase);
    }
}
