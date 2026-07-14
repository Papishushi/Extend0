namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Basic immutable implementation of <see cref="IProtectedStorageHandle"/>.
/// </summary>
/// <param name="ProviderId">Provider identifier that produced the handle.</param>
/// <param name="ProtectionId">Provider-scoped protected storage identifier.</param>
/// <param name="RootPath">Root path protected by this handle.</param>
/// <param name="ProtectionLevel">Protection level verified or declared by the provider.</param>
/// <param name="IsVerified">Whether the provider considers this handle verified for use.</param>
public sealed record ProtectedStorageHandle(
    string ProviderId,
    string ProtectionId,
    string RootPath,
    StorageProtectionLevel ProtectionLevel,
    bool IsVerified = true) : IProtectedStorageHandle
{
    /// <inheritdoc />
    public bool ContainsPath(string path) =>
        StorageProtectionVerifier.ContainsPath(RootPath, path);
}
