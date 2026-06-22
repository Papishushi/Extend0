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
    public static StorageProtectionPolicy None => default;

    public bool RequiresProtection => RequiredLevel != StorageProtectionLevel.None;

    public static StorageProtectionPolicy Require(
        StorageProtectionLevel requiredLevel,
        string? requiredProviderId = null,
        string? requiredProtectionId = null) =>
        new(requiredLevel, requiredProviderId, requiredProtectionId);

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
