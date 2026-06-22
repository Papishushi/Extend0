namespace Extend0.Lifecycle.Assurance;

/// <summary>
/// Describes how strongly a storage path can preserve contents across ownership movement.
/// </summary>
public enum StorageContinuityLevel
{
    /// <summary>
    /// No continuity evidence is declared or required.
    /// </summary>
    None = 0,

    /// <summary>
    /// Storage is tied to the current node, user, or filesystem view and is not safe for transparent owner movement.
    /// </summary>
    LocalOnly = 1,

    /// <summary>
    /// Contents can be restored explicitly from snapshots, but owner movement is not transparent.
    /// </summary>
    RestorableSnapshot = 2,

    /// <summary>
    /// The same backing bytes are reachable by every eligible owner through a shared storage substrate.
    /// </summary>
    SharedBackingStore = 3,

    /// <summary>
    /// Contents are duplicated symmetrically across eligible stores with provider-defined consistency semantics.
    /// </summary>
    SymmetricReplication = 4
}
