namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Severity for storage continuity verification findings.
/// </summary>
public enum StorageContinuityFindingSeverity
{
    Info = 0,
    Warning = 1,
    Error = 2
}

/// <summary>
/// A single storage continuity verification observation.
/// </summary>
public sealed record StorageContinuityFinding(
    string Id,
    StorageContinuityFindingSeverity Severity,
    string Message)
{
    public static StorageContinuityFinding Info(string id, string message) =>
        new(id, StorageContinuityFindingSeverity.Info, message);

    public static StorageContinuityFinding Warning(string id, string message) =>
        new(id, StorageContinuityFindingSeverity.Warning, message);

    public static StorageContinuityFinding Error(string id, string message) =>
        new(id, StorageContinuityFindingSeverity.Error, message);
}
