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
    public static StorageContinuityPolicy None => default;

    public bool RequiresContinuity => RequiredLevel != StorageContinuityLevel.None;

    public static StorageContinuityPolicy Require(
        StorageContinuityLevel requiredLevel,
        string? requiredProviderId = null,
        string? requiredContinuityId = null) =>
        new(requiredLevel, requiredProviderId, requiredContinuityId);

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
