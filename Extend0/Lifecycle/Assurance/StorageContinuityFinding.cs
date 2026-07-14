namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Severity for storage continuity verification findings.
/// </summary>
public enum StorageContinuityFindingSeverity
{
    /// <summary>
    /// Informational storage-continuity observation.
    /// </summary>
    Info = 0,

    /// <summary>
    /// Storage-continuity observation that may require operator attention.
    /// </summary>
    Warning = 1,

    /// <summary>
    /// Storage-continuity observation that violates the requested policy.
    /// </summary>
    Error = 2
}

/// <summary>
/// A single storage continuity verification observation.
/// </summary>
/// <param name="Id">Stable diagnostic identifier for the finding.</param>
/// <param name="Severity">Severity assigned to the finding.</param>
/// <param name="Message">Human-readable explanation of the finding.</param>
public sealed record StorageContinuityFinding(
    string Id,
    StorageContinuityFindingSeverity Severity,
    string Message)
{
    /// <summary>
    /// Creates an informational storage-continuity finding.
    /// </summary>
    public static StorageContinuityFinding Info(string id, string message) =>
        new(id, StorageContinuityFindingSeverity.Info, message);

    /// <summary>
    /// Creates a warning storage-continuity finding.
    /// </summary>
    public static StorageContinuityFinding Warning(string id, string message) =>
        new(id, StorageContinuityFindingSeverity.Warning, message);

    /// <summary>
    /// Creates an error storage-continuity finding.
    /// </summary>
    public static StorageContinuityFinding Error(string id, string message) =>
        new(id, StorageContinuityFindingSeverity.Error, message);
}
