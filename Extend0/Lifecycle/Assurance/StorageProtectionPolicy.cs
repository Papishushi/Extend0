namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Declares the minimum storage protection evidence required before a table path may be used.
/// </summary>
/// <param name="RequiredLevel">Minimum acceptable protection evidence level.</param>
/// <param name="RequiredProviderId">Optional provider id that must match the evidence.</param>
/// <param name="RequiredProtectionId">Optional protected volume/mount id that must match the evidence.</param>
public readonly record struct StorageProtectionPolicy(
    StorageProtectionLevel RequiredLevel,
    string? RequiredProviderId = null,
    string? RequiredProtectionId = null)
{
    /// <summary>
    /// Gets a policy that does not require storage protection evidence.
    /// </summary>
    public static StorageProtectionPolicy None => default;

    /// <summary>
    /// Gets whether this policy requires any storage protection evidence.
    /// </summary>
    public bool RequiresProtection => RequiredLevel != StorageProtectionLevel.None;

    /// <summary>
    /// Creates a policy requiring at least the specified storage protection level.
    /// </summary>
    /// <param name="requiredLevel">Minimum acceptable protection evidence level.</param>
    /// <param name="requiredProviderId">Optional provider id that must match the evidence.</param>
    /// <param name="requiredProtectionId">Optional protected volume or mount id that must match the evidence.</param>
    /// <returns>A storage protection policy.</returns>
    public static StorageProtectionPolicy Require(
        StorageProtectionLevel requiredLevel,
        string? requiredProviderId = null,
        string? requiredProtectionId = null) =>
        new(requiredLevel, requiredProviderId, requiredProtectionId);

    /// <summary>
    /// Validates that the policy contains supported enum values and non-empty optional identifiers.
    /// </summary>
    public void Validate()
    {
        if (!Enum.IsDefined(RequiredLevel))
            throw new ArgumentOutOfRangeException(nameof(RequiredLevel), RequiredLevel, "Unknown storage protection level.");

        if (RequiredProviderId is not null && string.IsNullOrWhiteSpace(RequiredProviderId))
            throw new ArgumentException("RequiredProviderId cannot be empty when provided.", nameof(RequiredProviderId));

        if (RequiredProtectionId is not null && string.IsNullOrWhiteSpace(RequiredProtectionId))
            throw new ArgumentException("RequiredProtectionId cannot be empty when provided.", nameof(RequiredProtectionId));
    }
}
