namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Final decision produced by storage continuity verification.
/// </summary>
public enum StorageContinuityDecision
{
    /// <summary>
    /// The observed evidence satisfies the requested continuity policy.
    /// </summary>
    Pass = 0,

    /// <summary>
    /// The path is usable, but the verifier found residual risk or incomplete continuity evidence.
    /// </summary>
    Warning = 1,

    /// <summary>
    /// The path must not be used for the requested continuity contract.
    /// </summary>
    FailClosed = 2
}
