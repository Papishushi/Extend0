namespace Extend0.Lifecycle.Certificates;

public sealed record Dns01ProvisioningResult(
    string ProviderName,
    string Domain,
    string AuthorizationDomain,
    string RecordName,
    string RecordType,
    string RecordValue,
    int TtlSeconds,
    bool RequiresManualAction,
    DateTimeOffset PreparedAtUtc,
    IReadOnlyList<string> Instructions);
