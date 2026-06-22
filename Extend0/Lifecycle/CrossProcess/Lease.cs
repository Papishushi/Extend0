namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// Serializable snapshot of the current cross-process ownership lease.
/// </summary>
/// <remarks>
/// The current built-in owner coordination is backed by an OS mutex, so leases are
/// exclusive and non-expiring unless a future transport/coordination backend adds TTL semantics.
/// </remarks>
public sealed record Lease(
    string LeaseId,
    string ContractName,
    string OwnershipName,
    string Fingerprint,
    string MachineName,
    int ProcessId,
    string ProcessName,
    DateTimeOffset AcquiredUtc,
    DateTimeOffset ObservedUtc,
    DateTimeOffset? ExpiresUtc,
    string? EndpointName,
    string? EndpointServerName,
    TransportKind TransportKind,
    string CoordinationKind,
    string? CoordinationName,
    string? CoordinationScope,
    bool IsExclusive,
    bool IsActive
);
