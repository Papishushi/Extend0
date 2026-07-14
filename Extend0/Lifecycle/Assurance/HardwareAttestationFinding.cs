namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Severity for hardware-attestation verification findings.
/// </summary>
public enum HardwareAttestationFindingSeverity
{
    /// <summary>
    /// Informational hardware-attestation observation.
    /// </summary>
    Info = 0,

    /// <summary>
    /// Hardware-attestation observation that may require operator attention.
    /// </summary>
    Warning = 1,

    /// <summary>
    /// Hardware-attestation observation that violates the requested policy.
    /// </summary>
    Error = 2
}

/// <summary>
/// A single hardware-attestation verification observation.
/// </summary>
/// <param name="Id">Stable diagnostic identifier for the finding.</param>
/// <param name="Severity">Severity assigned to the finding.</param>
/// <param name="Message">Human-readable explanation of the finding.</param>
public sealed record HardwareAttestationFinding(
    string Id,
    HardwareAttestationFindingSeverity Severity,
    string Message)
{
    /// <summary>
    /// Creates an informational hardware-attestation finding.
    /// </summary>
    public static HardwareAttestationFinding Info(string id, string message) =>
        new(id, HardwareAttestationFindingSeverity.Info, message);

    /// <summary>
    /// Creates a warning hardware-attestation finding.
    /// </summary>
    public static HardwareAttestationFinding Warning(string id, string message) =>
        new(id, HardwareAttestationFindingSeverity.Warning, message);

    /// <summary>
    /// Creates an error hardware-attestation finding.
    /// </summary>
    public static HardwareAttestationFinding Error(string id, string message) =>
        new(id, HardwareAttestationFindingSeverity.Error, message);
}
