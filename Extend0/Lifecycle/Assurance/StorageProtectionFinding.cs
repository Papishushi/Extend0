namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Severity for storage protection verification findings.
/// </summary>
public enum StorageProtectionFindingSeverity
{
    Info = 0,
    Warning = 1,
    Error = 2
}

/// <summary>
/// A single storage protection verification observation.
/// </summary>
public sealed record StorageProtectionFinding(
    string Id,
    StorageProtectionFindingSeverity Severity,
    string Message)
{
    public static StorageProtectionFinding Info(string id, string message) =>
        new(id, StorageProtectionFindingSeverity.Info, message);

    public static StorageProtectionFinding Warning(string id, string message) =>
        new(id, StorageProtectionFindingSeverity.Warning, message);

    public static StorageProtectionFinding Error(string id, string message) =>
        new(id, StorageProtectionFindingSeverity.Error, message);
}
