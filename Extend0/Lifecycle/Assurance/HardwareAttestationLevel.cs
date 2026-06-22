namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Describes how strongly execution has been attested before accessing protected storage.
/// </summary>
public enum HardwareAttestationLevel
{
    /// <summary>
    /// No hardware-attestation evidence is declared or required.
    /// </summary>
    None = 0,

    /// <summary>
    /// Hardware attestation is declared by configuration or manifest, but not provider-attested.
    /// </summary>
    Declared = 1,

    /// <summary>
    /// A provider attests that execution is hardware-backed or TEE-backed.
    /// </summary>
    ProviderAttested = 2,

    /// <summary>
    /// A provider reports platform or local verification of the attestation report.
    /// </summary>
    PlatformVerified = 3,

    /// <summary>
    /// Remote attestation has been verified against an expected measurement or policy.
    /// </summary>
    RemoteAttested = 4
}
