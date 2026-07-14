namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Declares the minimum hardware-attestation evidence required before a table path may be used.
/// </summary>
/// <param name="RequiredLevel">Minimum acceptable attestation evidence level.</param>
/// <param name="RequiredTechnology">Optional required attestation technology.</param>
/// <param name="RequiredProviderId">Optional provider id that must match the evidence.</param>
/// <param name="RequiredAttestationId">Optional attestation identity that must match the evidence.</param>
/// <param name="RequiredMeasurement">Optional code/platform measurement that must match the evidence.</param>
/// <param name="RequiredPolicyId">Optional provider-defined policy id that must match the evidence.</param>
public readonly record struct HardwareAttestationPolicy(
    HardwareAttestationLevel RequiredLevel,
    HardwareAttestationTechnology RequiredTechnology = HardwareAttestationTechnology.None,
    string? RequiredProviderId = null,
    string? RequiredAttestationId = null,
    string? RequiredMeasurement = null,
    string? RequiredPolicyId = null)
{
    /// <summary>
    /// Gets a policy that does not require hardware-attestation evidence.
    /// </summary>
    public static HardwareAttestationPolicy None => default;

    /// <summary>
    /// Gets whether this policy requires any hardware-attestation evidence or identifier match.
    /// </summary>
    public bool RequiresAttestation => RequiredLevel != HardwareAttestationLevel.None
        || RequiredTechnology != HardwareAttestationTechnology.None
        || RequiredProviderId is not null
        || RequiredAttestationId is not null
        || RequiredMeasurement is not null
        || RequiredPolicyId is not null;

    /// <summary>
    /// Creates a policy requiring at least the specified hardware-attestation level.
    /// </summary>
    /// <param name="requiredLevel">Minimum acceptable attestation evidence level.</param>
    /// <param name="requiredTechnology">Optional required attestation technology.</param>
    /// <param name="requiredProviderId">Optional provider id that must match the evidence.</param>
    /// <param name="requiredAttestationId">Optional attestation identity that must match the evidence.</param>
    /// <param name="requiredMeasurement">Optional code or platform measurement that must match the evidence.</param>
    /// <param name="requiredPolicyId">Optional provider-defined policy id that must match the evidence.</param>
    /// <returns>A hardware-attestation policy.</returns>
    public static HardwareAttestationPolicy Require(
        HardwareAttestationLevel requiredLevel,
        HardwareAttestationTechnology requiredTechnology = HardwareAttestationTechnology.None,
        string? requiredProviderId = null,
        string? requiredAttestationId = null,
        string? requiredMeasurement = null,
        string? requiredPolicyId = null) =>
        new(requiredLevel, requiredTechnology, requiredProviderId, requiredAttestationId, requiredMeasurement, requiredPolicyId);

    /// <summary>
    /// Validates that the policy contains supported enum values and non-empty optional identifiers.
    /// </summary>
    public void Validate()
    {
        if (!Enum.IsDefined(RequiredLevel))
            throw new ArgumentOutOfRangeException(nameof(RequiredLevel), RequiredLevel, "Unknown hardware attestation level.");

        if (!Enum.IsDefined(RequiredTechnology))
            throw new ArgumentOutOfRangeException(nameof(RequiredTechnology), RequiredTechnology, "Unknown hardware attestation technology.");

        if (RequiredProviderId is not null && string.IsNullOrWhiteSpace(RequiredProviderId))
            throw new ArgumentException("RequiredProviderId cannot be empty when provided.", nameof(RequiredProviderId));

        if (RequiredAttestationId is not null && string.IsNullOrWhiteSpace(RequiredAttestationId))
            throw new ArgumentException("RequiredAttestationId cannot be empty when provided.", nameof(RequiredAttestationId));

        if (RequiredMeasurement is not null && string.IsNullOrWhiteSpace(RequiredMeasurement))
            throw new ArgumentException("RequiredMeasurement cannot be empty when provided.", nameof(RequiredMeasurement));

        if (RequiredPolicyId is not null && string.IsNullOrWhiteSpace(RequiredPolicyId))
            throw new ArgumentException("RequiredPolicyId cannot be empty when provided.", nameof(RequiredPolicyId));
    }
}
