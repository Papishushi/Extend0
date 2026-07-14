namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Result returned by a DNS-01 record provisioner.
/// </summary>
/// <param name="ProviderName">Provisioner name that prepared or published the record.</param>
/// <param name="Domain">DNS identifier being authorized.</param>
/// <param name="AuthorizationDomain">DNS identifier used for the authorization record, without a wildcard prefix.</param>
/// <param name="RecordName">TXT record name to publish.</param>
/// <param name="RecordType">DNS record type, normally <c>TXT</c>.</param>
/// <param name="RecordValue">TXT record value to publish.</param>
/// <param name="TtlSeconds">Suggested or applied DNS TTL in seconds.</param>
/// <param name="RequiresManualAction">Whether the caller still needs to publish or approve the record manually.</param>
/// <param name="PreparedAtUtc">UTC timestamp when the result was prepared.</param>
/// <param name="Instructions">Human-readable follow-up instructions for the caller.</param>
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
