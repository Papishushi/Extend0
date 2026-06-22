namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Optional extension point for providers that create, open, mount, verify, and unmount protected storage.
/// </summary>
public interface IProtectedStorageProvider
{
    string ProviderId { get; }

    Task<IProtectedStorageHandle> CreateOrOpenAsync(
        ProtectedStorageRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Provider-neutral request for a protected storage root.
/// </summary>
public sealed record ProtectedStorageRequest(
    string ProtectionId,
    string RootPath,
    StorageProtectionLevel RequiredLevel,
    bool CreateIfMissing = false);
