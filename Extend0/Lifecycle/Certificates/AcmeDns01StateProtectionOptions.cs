namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Describes how ACME DNS-01 state should be protected when saved or loaded.
/// </summary>
/// <param name="Kind">Selected state protection mode.</param>
/// <param name="Passphrase">Passphrase used for passphrase-protected state, or <see langword="null"/> for unprotected state.</param>
public sealed record AcmeDns01StateProtectionOptions(
    AcmeDns01StateProtectionKind Kind,
    string? Passphrase)
{
    /// <summary>
    /// Options for unprotected JSON state.
    /// </summary>
    public static AcmeDns01StateProtectionOptions None { get; } = new(AcmeDns01StateProtectionKind.None, null);

    /// <summary>
    /// Creates passphrase-based state protection options.
    /// </summary>
    /// <param name="passphrase">Passphrase used to derive the state encryption key.</param>
    /// <returns>Passphrase protection options.</returns>
    public static AcmeDns01StateProtectionOptions FromPassphrase(string passphrase)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(passphrase);
        return new AcmeDns01StateProtectionOptions(AcmeDns01StateProtectionKind.Passphrase, passphrase);
    }
}
