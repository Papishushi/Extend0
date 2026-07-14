namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Identifies the protection mode used for an ACME DNS-01 state file.
/// </summary>
public enum AcmeDns01StateProtectionKind
{
    /// <summary>
    /// State is serialized as unprotected JSON.
    /// </summary>
    None,

    /// <summary>
    /// State is encrypted with a passphrase-derived key.
    /// </summary>
    Passphrase
}
