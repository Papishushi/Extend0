namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Severity for storage protection verification findings.
/// </summary>
public enum StorageProtectionFindingSeverity
{
    /// <summary>
    /// Informational storage-protection observation.
    /// </summary>
    Info = 0,

    /// <summary>
    /// Storage-protection observation that may require operator attention.
    /// </summary>
    Warning = 1,

    /// <summary>
    /// Storage-protection observation that violates the requested policy.
    /// </summary>
    Error = 2
}

/// <summary>
/// A single storage protection verification observation.
/// </summary>
/// <param name="Id">Stable diagnostic identifier for the finding.</param>
/// <param name="Severity">Severity assigned to the finding.</param>
/// <param name="Message">Human-readable explanation of the finding.</param>
public sealed record StorageProtectionFinding(
    string Id,
    StorageProtectionFindingSeverity Severity,
    string Message)
{
    /// <summary>
    /// Creates an informational storage-protection finding.
    /// </summary>
    public static StorageProtectionFinding Info(string id, string message) =>
        new(id, StorageProtectionFindingSeverity.Info, message);

    /// <summary>
    /// Creates a warning storage-protection finding.
    /// </summary>
    public static StorageProtectionFinding Warning(string id, string message) =>
        new(id, StorageProtectionFindingSeverity.Warning, message);

    /// <summary>
    /// Creates an error storage-protection finding.
    /// </summary>
    public static StorageProtectionFinding Error(string id, string message) =>
        new(id, StorageProtectionFindingSeverity.Error, message);
}
