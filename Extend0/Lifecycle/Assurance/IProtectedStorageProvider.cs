namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Optional extension point for providers that create, open, mount, verify, and unmount protected storage.
/// </summary>
public interface IProtectedStorageProvider
{
    /// <summary>
    /// Gets the stable provider identifier.
    /// </summary>
    string ProviderId { get; }

    /// <summary>
    /// Creates or opens a protected storage root and returns a verified handle for it.
    /// </summary>
    /// <param name="request">Provider-neutral protected storage request.</param>
    /// <param name="cancellationToken">Token used to cancel provider work.</param>
    /// <returns>A protected storage handle covering the requested root path.</returns>
    Task<IProtectedStorageHandle> CreateOrOpenAsync(
        ProtectedStorageRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Provider-neutral request for a protected storage root.
/// </summary>
/// <param name="ProtectionId">Provider-scoped protected storage identifier.</param>
/// <param name="RootPath">Requested local root path for the protected storage scope.</param>
/// <param name="RequiredLevel">Minimum storage protection level expected by the caller.</param>
/// <param name="CreateIfMissing">Whether the provider may create the protected storage scope when absent.</param>
public sealed record ProtectedStorageRequest(
    string ProtectionId,
    string RootPath,
    StorageProtectionLevel RequiredLevel,
    bool CreateIfMissing = false);
