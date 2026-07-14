namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Declares the minimum storage continuity evidence required before a table path may be used.
/// </summary>
/// <param name="RequiredLevel">Minimum acceptable continuity evidence level.</param>
/// <param name="RequiredProviderId">Optional provider id that must match the evidence.</param>
/// <param name="RequiredContinuityId">Optional shared-store or replication-group id that must match the evidence.</param>
public readonly record struct StorageContinuityPolicy(
    StorageContinuityLevel RequiredLevel,
    string? RequiredProviderId = null,
    string? RequiredContinuityId = null)
{
    /// <summary>
    /// Gets a policy that does not require storage continuity evidence.
    /// </summary>
    public static StorageContinuityPolicy None => default;

    /// <summary>
    /// Gets whether this policy requires any storage continuity evidence.
    /// </summary>
    public bool RequiresContinuity => RequiredLevel != StorageContinuityLevel.None;

    /// <summary>
    /// Creates a policy requiring at least the specified storage continuity level.
    /// </summary>
    /// <param name="requiredLevel">Minimum acceptable continuity evidence level.</param>
    /// <param name="requiredProviderId">Optional provider id that must match the evidence.</param>
    /// <param name="requiredContinuityId">Optional shared-store or replication-group id that must match the evidence.</param>
    /// <returns>A storage continuity policy.</returns>
    public static StorageContinuityPolicy Require(
        StorageContinuityLevel requiredLevel,
        string? requiredProviderId = null,
        string? requiredContinuityId = null) =>
        new(requiredLevel, requiredProviderId, requiredContinuityId);

    /// <summary>
    /// Validates that the policy contains supported enum values and non-empty optional identifiers.
    /// </summary>
    public void Validate()
    {
        if (!Enum.IsDefined(RequiredLevel))
            throw new ArgumentOutOfRangeException(nameof(RequiredLevel), RequiredLevel, "Unknown storage continuity level.");

        if (RequiredProviderId is not null && string.IsNullOrWhiteSpace(RequiredProviderId))
            throw new ArgumentException("RequiredProviderId cannot be empty when provided.", nameof(RequiredProviderId));

        if (RequiredContinuityId is not null && string.IsNullOrWhiteSpace(RequiredContinuityId))
            throw new ArgumentException("RequiredContinuityId cannot be empty when provided.", nameof(RequiredContinuityId));
    }
}
