namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// Authentication mechanisms understood by the cross-process Lifecycle handshake.
/// </summary>
public enum AuthenticationMode
{
    /// <summary>
    /// No peer authentication is required. This is compatible with legacy local-only deployments.
    /// </summary>
    None = 0,

    /// <summary>
    /// Client proves knowledge of a shared secret using an HMAC over the server nonce-bearing handshake.
    /// </summary>
    SharedSecretHmac = 1,

    /// <summary>
    /// Peer trust is derived from operating-system identity or endpoint permissions.
    /// </summary>
    OsIdentity = 2,

    /// <summary>
    /// Peer trust is derived from a public-key signature over a challenge.
    /// </summary>
    SignedChallenge = 3,

    /// <summary>
    /// Authentication is supplied by a custom transport or host implementation.
    /// </summary>
    Custom = 255
}
