namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Severity for hardware-attestation verification findings.
/// </summary>
public enum HardwareAttestationFindingSeverity
{
    Info = 0,
    Warning = 1,
    Error = 2
}

/// <summary>
/// A single hardware-attestation verification observation.
/// </summary>
public sealed record HardwareAttestationFinding(
    string Id,
    HardwareAttestationFindingSeverity Severity,
    string Message)
{
    public static HardwareAttestationFinding Info(string id, string message) =>
        new(id, HardwareAttestationFindingSeverity.Info, message);

    public static HardwareAttestationFinding Warning(string id, string message) =>
        new(id, HardwareAttestationFindingSeverity.Warning, message);

    public static HardwareAttestationFinding Error(string id, string message) =>
        new(id, HardwareAttestationFindingSeverity.Error, message);
}
