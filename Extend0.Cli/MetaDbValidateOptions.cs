using Extend0.Lifecycle.Assurance;

namespace Extend0.Cli;

internal sealed record MetaDbValidateOptions(
    string? InputPath,
    bool Security,
    bool OwnershipTransfer,
    bool StateContinuity,
    StorageProtectionLevel? RequiredProtectionLevel,
    string? RequiredProviderId,
    string? RequiredProtectionId,
    string? ProtectionManifestPath,
    StorageContinuityLevel? RequiredContinuityLevel,
    string? RequiredContinuityProviderId,
    string? RequiredContinuityId,
    string? ContinuityManifestPath,
    bool Attestation,
    HardwareAttestationLevel? RequiredAttestationLevel,
    HardwareAttestationTechnology RequiredAttestationTechnology,
    string? RequiredAttestationProviderId,
    string? RequiredAttestationId,
    string? RequiredAttestationMeasurement,
    string? RequiredAttestationPolicyId,
    string? AttestationManifestPath,
    bool Json,
    bool ShowHelp)
{
    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out MetaDbValidateOptions options,
        out string error)
    {
        string? inputPath = null;
        var security = false;
        var ownershipTransfer = false;
        var stateContinuity = false;
        StorageProtectionLevel? requiredProtectionLevel = null;
        string? requiredProviderId = null;
        string? requiredProtectionId = null;
        string? protectionManifestPath = null;
        StorageContinuityLevel? requiredContinuityLevel = null;
        string? requiredContinuityProviderId = null;
        string? requiredContinuityId = null;
        string? continuityManifestPath = null;
        var attestation = false;
        HardwareAttestationLevel? requiredAttestationLevel = null;
        var requiredAttestationTechnology = HardwareAttestationTechnology.None;
        string? requiredAttestationProviderId = null;
        string? requiredAttestationId = null;
        string? requiredAttestationMeasurement = null;
        string? requiredAttestationPolicyId = null;
        string? attestationManifestPath = null;
        var json = false;
        var showHelp = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "-h":
                case "--help":
                    showHelp = true;
                    break;

                case "--json":
                    json = true;
                    break;

                case "--security":
                    security = true;
                    break;

                case "--ownership-transfer":
                    ownershipTransfer = true;
                    break;

                case "--state-continuity":
                case "--durable-state-transfer":
                    ownershipTransfer = true;
                    stateContinuity = true;
                    break;

                case "--require-protection":
                    if (!TryReadValue(args, ref i, "--require-protection", out var levelToken, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    if (!StorageDiagnoseOptions.TryParseProtectionLevel(levelToken, out var parsedLevel))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        error = $"Unknown storage protection level '{levelToken}'.";
                        return false;
                    }

                    requiredProtectionLevel = parsedLevel;
                    security = true;
                    break;

                case "--provider":
                    if (!TryReadValue(args, ref i, "--provider", out requiredProviderId, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    security = true;
                    break;

                case "--protection-id":
                    if (!TryReadValue(args, ref i, "--protection-id", out requiredProtectionId, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    security = true;
                    break;

                case "--protection-manifest":
                    if (!TryReadValue(args, ref i, "--protection-manifest", out var manifestToken, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    protectionManifestPath = Path.IsPathRooted(manifestToken)
                        ? manifestToken
                        : Path.Combine(workingDirectory, manifestToken);
                    security = true;
                    break;

                case "--require-continuity":
                    if (!TryReadValue(args, ref i, "--require-continuity", out var continuityToken, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    if (!TryParseContinuityLevel(continuityToken, out var parsedContinuityLevel))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        error = $"Unknown storage continuity level '{continuityToken}'.";
                        return false;
                    }

                    requiredContinuityLevel = parsedContinuityLevel;
                    ownershipTransfer = true;
                    stateContinuity = true;
                    break;

                case "--continuity-provider":
                    if (!TryReadValue(args, ref i, "--continuity-provider", out requiredContinuityProviderId, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    break;

                case "--continuity-id":
                    if (!TryReadValue(args, ref i, "--continuity-id", out requiredContinuityId, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    break;

                case "--continuity-manifest":
                    if (!TryReadValue(args, ref i, "--continuity-manifest", out var continuityManifestToken, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    continuityManifestPath = Path.IsPathRooted(continuityManifestToken)
                        ? continuityManifestToken
                        : Path.Combine(workingDirectory, continuityManifestToken);
                    ownershipTransfer = true;
                    break;

                case "--attestation":
                    attestation = true;
                    break;

                case "--require-attestation":
                    if (!TryReadValue(args, ref i, "--require-attestation", out var attestationLevelToken, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    if (!TryParseAttestationLevel(attestationLevelToken, out var parsedAttestationLevel))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        error = $"Unknown hardware attestation level '{attestationLevelToken}'.";
                        return false;
                    }

                    requiredAttestationLevel = parsedAttestationLevel;
                    attestation = true;
                    break;

                case "--attestation-technology":
                    if (!TryReadValue(args, ref i, "--attestation-technology", out var technologyToken, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    if (!TryParseAttestationTechnology(technologyToken, out requiredAttestationTechnology))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        error = $"Unknown hardware attestation technology '{technologyToken}'.";
                        return false;
                    }

                    attestation = true;
                    break;

                case "--attestation-provider":
                    if (!TryReadValue(args, ref i, "--attestation-provider", out requiredAttestationProviderId, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    attestation = true;
                    break;

                case "--attestation-id":
                    if (!TryReadValue(args, ref i, "--attestation-id", out requiredAttestationId, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    attestation = true;
                    break;

                case "--measurement":
                    if (!TryReadValue(args, ref i, "--measurement", out requiredAttestationMeasurement, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    attestation = true;
                    break;

                case "--attestation-policy-id":
                    if (!TryReadValue(args, ref i, "--attestation-policy-id", out requiredAttestationPolicyId, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    attestation = true;
                    break;

                case "--attestation-manifest":
                    if (!TryReadValue(args, ref i, "--attestation-manifest", out var attestationManifestToken, out error))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        return false;
                    }

                    attestationManifestPath = Path.IsPathRooted(attestationManifestToken)
                        ? attestationManifestToken
                        : Path.Combine(workingDirectory, attestationManifestToken);
                    attestation = true;
                    break;

                default:
                    if (arg.StartsWith("-", StringComparison.Ordinal))
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        error = $"Unknown metadb validate option '{arg}'.";
                        return false;
                    }

                    if (inputPath is not null)
                    {
                        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
                        error = "metadb validate accepts exactly one path argument.";
                        return false;
                    }

                    inputPath = Path.IsPathRooted(arg)
                        ? arg
                        : Path.Combine(workingDirectory, arg);
                    break;
            }
        }

        options = Create(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);
        if (!showHelp && string.IsNullOrWhiteSpace(inputPath))
        {
            error = "metadb validate requires a path argument.";
            return false;
        }

        if (!showHelp && (requiredProviderId is not null || requiredProtectionId is not null) && requiredProtectionLevel is null)
        {
            error = "--provider and --protection-id require --require-protection.";
            return false;
        }

        if (!showHelp && (requiredContinuityProviderId is not null || requiredContinuityId is not null) && !stateContinuity && requiredContinuityLevel is null)
        {
            error = "--continuity-provider and --continuity-id require --state-continuity or --require-continuity.";
            return false;
        }

        if (!showHelp
            && (requiredAttestationTechnology != HardwareAttestationTechnology.None
                || requiredAttestationProviderId is not null
                || requiredAttestationId is not null
                || requiredAttestationMeasurement is not null
                || requiredAttestationPolicyId is not null)
            && !attestation
            && requiredAttestationLevel is null)
        {
            error = "Hardware attestation filters require --attestation or --require-attestation.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    public StorageProtectionPolicy ToOverridePolicy() =>
        RequiredProtectionLevel is null
            ? StorageProtectionPolicy.None
            : StorageProtectionPolicy.Require(RequiredProtectionLevel.Value, RequiredProviderId, RequiredProtectionId);

    public StorageContinuityPolicy ToContinuityPolicy()
    {
        var requiredLevel = RequiredContinuityLevel
            ?? (StateContinuity ? StorageContinuityLevel.SharedBackingStore : (StorageContinuityLevel?)null);

        return requiredLevel is null
            ? StorageContinuityPolicy.None
            : StorageContinuityPolicy.Require(requiredLevel.Value, RequiredContinuityProviderId, RequiredContinuityId);
    }

    public HardwareAttestationPolicy ToAttestationPolicy()
    {
        var requiredLevel = RequiredAttestationLevel
            ?? (Attestation ? HardwareAttestationLevel.ProviderAttested : (HardwareAttestationLevel?)null);

        return requiredLevel is null && RequiredAttestationTechnology == HardwareAttestationTechnology.None
            ? HardwareAttestationPolicy.None
            : HardwareAttestationPolicy.Require(
                requiredLevel ?? HardwareAttestationLevel.ProviderAttested,
                RequiredAttestationTechnology,
                RequiredAttestationProviderId,
                RequiredAttestationId,
                RequiredAttestationMeasurement,
                RequiredAttestationPolicyId);
    }

    public static bool TryParseContinuityLevel(string token, out StorageContinuityLevel level)
    {
        switch (token.Trim().ToLowerInvariant())
        {
            case "none":
                level = StorageContinuityLevel.None;
                return true;
            case "local":
            case "local-only":
            case "localonly":
                level = StorageContinuityLevel.LocalOnly;
                return true;
            case "snapshot":
            case "restorable":
            case "restorable-snapshot":
                level = StorageContinuityLevel.RestorableSnapshot;
                return true;
            case "shared":
            case "shared-store":
            case "shared-backing":
            case "shared-backing-store":
                level = StorageContinuityLevel.SharedBackingStore;
                return true;
            case "replicated":
            case "replication":
            case "symmetric":
            case "symmetric-replication":
                level = StorageContinuityLevel.SymmetricReplication;
                return true;
            default:
                return Enum.TryParse(token, ignoreCase: true, out level) && Enum.IsDefined(level);
        }
    }

    public static bool TryParseAttestationLevel(string token, out HardwareAttestationLevel level)
    {
        switch (token.Trim().ToLowerInvariant())
        {
            case "none":
                level = HardwareAttestationLevel.None;
                return true;
            case "declared":
                level = HardwareAttestationLevel.Declared;
                return true;
            case "provider":
            case "provider-attested":
                level = HardwareAttestationLevel.ProviderAttested;
                return true;
            case "platform":
            case "platform-verified":
                level = HardwareAttestationLevel.PlatformVerified;
                return true;
            case "remote":
            case "remote-attested":
                level = HardwareAttestationLevel.RemoteAttested;
                return true;
            default:
                return Enum.TryParse(token, ignoreCase: true, out level) && Enum.IsDefined(level);
        }
    }

    public static bool TryParseAttestationTechnology(string token, out HardwareAttestationTechnology technology)
    {
        switch (token.Trim().ToLowerInvariant())
        {
            case "none":
                technology = HardwareAttestationTechnology.None;
                return true;
            case "sgx":
            case "intel-sgx":
            case "intelsgx":
                technology = HardwareAttestationTechnology.IntelSgx;
                return true;
            case "tdx":
            case "intel-tdx":
            case "inteltdx":
                technology = HardwareAttestationTechnology.IntelTdx;
                return true;
            case "sev":
            case "sev-snp":
            case "amd-sev-snp":
            case "amdsevsnp":
                technology = HardwareAttestationTechnology.AmdSevSnp;
                return true;
            case "trustzone":
            case "arm-trustzone":
            case "armtrustzone":
                technology = HardwareAttestationTechnology.ArmTrustZone;
                return true;
            case "cca":
            case "arm-cca":
            case "arm-cca-realm":
            case "armccarealm":
                technology = HardwareAttestationTechnology.ArmCcaRealm;
                return true;
            case "tpm":
            case "tpm-sealed":
                technology = HardwareAttestationTechnology.TpmSealed;
                return true;
            case "custom":
            case "custom-hardware-attested":
                technology = HardwareAttestationTechnology.CustomHardwareAttested;
                return true;
            default:
                return Enum.TryParse(token, ignoreCase: true, out technology) && Enum.IsDefined(technology);
        }
    }

    private static MetaDbValidateOptions Create(
        string? inputPath,
        bool security,
        bool ownershipTransfer,
        bool stateContinuity,
        StorageProtectionLevel? requiredProtectionLevel,
        string? requiredProviderId,
        string? requiredProtectionId,
        string? protectionManifestPath,
        StorageContinuityLevel? requiredContinuityLevel,
        string? requiredContinuityProviderId,
        string? requiredContinuityId,
        string? continuityManifestPath,
        bool attestation,
        HardwareAttestationLevel? requiredAttestationLevel,
        HardwareAttestationTechnology requiredAttestationTechnology,
        string? requiredAttestationProviderId,
        string? requiredAttestationId,
        string? requiredAttestationMeasurement,
        string? requiredAttestationPolicyId,
        string? attestationManifestPath,
        bool json,
        bool showHelp) =>
        new(inputPath, security, ownershipTransfer, stateContinuity, requiredProtectionLevel, requiredProviderId, requiredProtectionId, protectionManifestPath, requiredContinuityLevel, requiredContinuityProviderId, requiredContinuityId, continuityManifestPath, attestation, requiredAttestationLevel, requiredAttestationTechnology, requiredAttestationProviderId, requiredAttestationId, requiredAttestationMeasurement, requiredAttestationPolicyId, attestationManifestPath, json, showHelp);

    private static bool TryReadValue(string[] args, ref int index, string option, out string value, out string error)
    {
        if (index + 1 >= args.Length)
        {
            value = string.Empty;
            error = $"{option} requires a value.";
            return false;
        }

        value = args[++index];
        if (string.IsNullOrWhiteSpace(value))
        {
            error = $"{option} cannot be empty.";
            return false;
        }

        error = string.Empty;
        return true;
    }
}
