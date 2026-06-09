namespace Extend0.Cli;

public enum ValidationSeverity
{
    Info,
    Warning,
    Error
}

public sealed record ValidationFinding(string Id, ValidationSeverity Severity, string Message)
{
    public static ValidationFinding Info(string id, string message) =>
        new(id, ValidationSeverity.Info, message);

    public static ValidationFinding Warning(string id, string message) =>
        new(id, ValidationSeverity.Warning, message);

    public static ValidationFinding Error(string id, string message) =>
        new(id, ValidationSeverity.Error, message);
}
