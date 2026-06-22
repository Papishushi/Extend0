namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Hardware-backed or provider-defined attestation technology used to qualify execution evidence.
/// </summary>
public enum HardwareAttestationTechnology
{
    None = 0,
    IntelSgx = 1,
    IntelTdx = 2,
    AmdSevSnp = 3,
    ArmTrustZone = 4,
    ArmCcaRealm = 5,
    TpmSealed = 6,
    CustomHardwareAttested = 255
}
