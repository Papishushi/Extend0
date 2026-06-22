namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Evidence level used to decide whether a storage path is protected enough for a table policy.
/// </summary>
public enum StorageProtectionLevel
{
    /// <summary>
    /// No storage protection evidence is required or observed.
    /// </summary>
    None = 0,

    /// <summary>
    /// Protection is declared by configuration or by a human/operator-controlled manifest.
    /// </summary>
    DeclaredEncrypted = 1,

    /// <summary>
    /// Protection is attested by a storage provider manifest or provider-owned handle.
    /// </summary>
    ProviderAttestedEncrypted = 2,

    /// <summary>
    /// Protection is reported as verified by platform or operating-system facilities.
    /// </summary>
    PlatformVerifiedEncrypted = 3,

    /// <summary>
    /// Protection is managed through an Extend0-approved provider lifecycle.
    /// </summary>
    Extend0ManagedProtectedMount = 4
}
