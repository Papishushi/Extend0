namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Hardware-backed or provider-defined attestation technology used to qualify execution evidence.
/// </summary>
public enum HardwareAttestationTechnology
{
    /// <summary>
    /// No hardware-attestation technology is declared.
    /// </summary>
    None = 0,

    /// <summary>
    /// Intel Software Guard Extensions.
    /// </summary>
    IntelSgx = 1,

    /// <summary>
    /// Intel Trust Domain Extensions.
    /// </summary>
    IntelTdx = 2,

    /// <summary>
    /// AMD Secure Encrypted Virtualization with Secure Nested Paging.
    /// </summary>
    AmdSevSnp = 3,

    /// <summary>
    /// Arm TrustZone based trusted execution.
    /// </summary>
    ArmTrustZone = 4,

    /// <summary>
    /// Arm Confidential Compute Architecture realm execution.
    /// </summary>
    ArmCcaRealm = 5,

    /// <summary>
    /// Trusted Platform Module sealed execution or storage evidence.
    /// </summary>
    TpmSealed = 6,

    /// <summary>
    /// Provider-specific hardware-attested environment.
    /// </summary>
    CustomHardwareAttested = 255
}
