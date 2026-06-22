namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Final decision produced by storage protection verification.
/// </summary>
public enum StorageProtectionDecision
{
    /// <summary>
    /// The observed evidence satisfies the requested policy.
    /// </summary>
    Pass = 0,

    /// <summary>
    /// The path is usable, but the verifier found residual risk or incomplete evidence.
    /// </summary>
    Warning = 1,

    /// <summary>
    /// The path must not be used because the requested storage protection policy is not satisfied.
    /// </summary>
    FailClosed = 2
}
