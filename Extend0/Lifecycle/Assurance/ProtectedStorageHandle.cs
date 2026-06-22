namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Basic immutable implementation of <see cref="IProtectedStorageHandle"/>.
/// </summary>
public sealed record ProtectedStorageHandle(
    string ProviderId,
    string ProtectionId,
    string RootPath,
    StorageProtectionLevel ProtectionLevel,
    bool IsVerified = true) : IProtectedStorageHandle
{
    public bool ContainsPath(string path) =>
        StorageProtectionVerifier.ContainsPath(RootPath, path);
}
