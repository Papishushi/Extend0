namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Verified protected storage root supplied by a provider or test harness.
/// </summary>
public interface IProtectedStorageHandle
{
    /// <summary>
    /// Gets the provider identifier that produced the protected storage handle.
    /// </summary>
    string ProviderId { get; }

    /// <summary>
    /// Gets the provider-scoped protected storage identifier.
    /// </summary>
    string ProtectionId { get; }

    /// <summary>
    /// Gets the root path that is protected by the provider.
    /// </summary>
    string RootPath { get; }

    /// <summary>
    /// Gets the protection level verified or declared by the provider.
    /// </summary>
    StorageProtectionLevel ProtectionLevel { get; }

    /// <summary>
    /// Gets whether the provider considers this handle verified for use.
    /// </summary>
    bool IsVerified { get; }

    /// <summary>
    /// Determines whether the specified path is inside the protected root.
    /// </summary>
    /// <param name="path">Path to test.</param>
    /// <returns><see langword="true"/> when the path is covered by this handle; otherwise <see langword="false"/>.</returns>
    bool ContainsPath(string path);
}
