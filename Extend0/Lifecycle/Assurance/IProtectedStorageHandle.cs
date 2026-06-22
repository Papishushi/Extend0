namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Verified protected storage root supplied by a provider or test harness.
/// </summary>
public interface IProtectedStorageHandle
{
    string ProviderId { get; }

    string ProtectionId { get; }

    string RootPath { get; }

    StorageProtectionLevel ProtectionLevel { get; }

    bool IsVerified { get; }

    bool ContainsPath(string path);
}
