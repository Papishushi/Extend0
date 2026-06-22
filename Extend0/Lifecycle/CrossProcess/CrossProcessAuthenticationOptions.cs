using System.Security.Cryptography;

namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// Authentication options applied to the Lifecycle cross-process handshake.
/// </summary>
/// <remarks>
/// These options are protocol-level authentication choices. Transport security such as TLS/mTLS
/// is configured separately through transport-specific options.
/// </remarks>
public sealed record CrossProcessAuthenticationOptions
{
    private const string SignedChallengeEcdsaSha256Algorithm = "ecdsa-sha256";

    /// <summary>
    /// Gets unauthenticated compatibility mode.
    /// </summary>
    public static CrossProcessAuthenticationOptions None { get; } = new(AuthenticationMode.None);

    /// <summary>
    /// Creates shared-secret HMAC authentication options.
    /// </summary>
    public static CrossProcessAuthenticationOptions SharedSecretHmac(string sharedSecret) =>
        new(AuthenticationMode.SharedSecretHmac, sharedSecret);

    /// <summary>
    /// Creates client-side signed-challenge authentication using ECDSA/SHA-256.
    /// </summary>
    /// <remarks>
    /// The provided key is owned by the caller and must remain alive while the transport is connecting.
    /// </remarks>
    public static CrossProcessAuthenticationOptions SignedChallengeClient(string keyId, ECDsa signingKey)
    {
        ArgumentNullException.ThrowIfNull(signingKey);
        ValidateSignedChallengeKeyId(keyId);

        return new CrossProcessAuthenticationOptions(
            AuthenticationMode.SignedChallenge,
            signedChallengeKeyId: keyId,
            signedChallengeAlgorithm: SignedChallengeEcdsaSha256Algorithm,
            signChallenge: challenge => signingKey.SignData(challenge, HashAlgorithmName.SHA256));
    }

    /// <summary>
    /// Creates server-side signed-challenge authentication using a trusted ECDSA/SHA-256 public key.
    /// </summary>
    /// <remarks>
    /// The provided key is owned by the caller and must remain alive while the owner host is accepting clients.
    /// </remarks>
    public static CrossProcessAuthenticationOptions SignedChallengeServer(string keyId, ECDsa publicKey)
    {
        ArgumentNullException.ThrowIfNull(publicKey);
        ValidateSignedChallengeKeyId(keyId);

        return new CrossProcessAuthenticationOptions(
            AuthenticationMode.SignedChallenge,
            signedChallengeKeyId: keyId,
            signedChallengeAlgorithm: SignedChallengeEcdsaSha256Algorithm,
            verifyChallengeSignature: (candidateKeyId, challenge, signature) =>
                string.Equals(candidateKeyId, keyId, StringComparison.Ordinal)
                && publicKey.VerifyData(challenge, signature, HashAlgorithmName.SHA256));
    }

    /// <summary>
    /// Creates client-side signed-challenge authentication using a caller-provided signer.
    /// </summary>
    public static CrossProcessAuthenticationOptions SignedChallengeClient(
        string keyId,
        Func<byte[], byte[]> signChallenge,
        string signedChallengeAlgorithm = SignedChallengeEcdsaSha256Algorithm)
    {
        ArgumentNullException.ThrowIfNull(signChallenge);
        ValidateSignedChallengeKeyId(keyId);
        ValidateSignedChallengeAlgorithm(signedChallengeAlgorithm);

        return new CrossProcessAuthenticationOptions(
            AuthenticationMode.SignedChallenge,
            signedChallengeKeyId: keyId,
            signedChallengeAlgorithm: signedChallengeAlgorithm,
            signChallenge: signChallenge);
    }

    /// <summary>
    /// Creates server-side signed-challenge authentication using a caller-provided verifier.
    /// </summary>
    public static CrossProcessAuthenticationOptions SignedChallengeServer(
        Func<string?, byte[], byte[], bool> verifyChallengeSignature,
        string signedChallengeAlgorithm = SignedChallengeEcdsaSha256Algorithm)
    {
        ArgumentNullException.ThrowIfNull(verifyChallengeSignature);
        ValidateSignedChallengeAlgorithm(signedChallengeAlgorithm);

        return new CrossProcessAuthenticationOptions(
            AuthenticationMode.SignedChallenge,
            signedChallengeAlgorithm: signedChallengeAlgorithm,
            verifyChallengeSignature: verifyChallengeSignature);
    }

    /// <summary>
    /// Initializes a new authentication option set.
    /// </summary>
    public CrossProcessAuthenticationOptions(
        AuthenticationMode mode,
        string? sharedSecret = null,
        string? signedChallengeKeyId = null,
        string? signedChallengeAlgorithm = null,
        Func<byte[], byte[]>? signChallenge = null,
        Func<string?, byte[], byte[], bool>? verifyChallengeSignature = null)
    {
        if (mode == AuthenticationMode.SharedSecretHmac && string.IsNullOrWhiteSpace(sharedSecret))
            throw new ArgumentException("Shared-secret HMAC authentication requires a non-empty shared secret.", nameof(sharedSecret));

        if (mode == AuthenticationMode.SignedChallenge && signChallenge is null && verifyChallengeSignature is null)
            throw new ArgumentException("Signed-challenge authentication requires a signer on clients or a verifier on servers.", nameof(signChallenge));

        if (!string.IsNullOrWhiteSpace(signedChallengeKeyId))
            ValidateSignedChallengeKeyId(signedChallengeKeyId);

        if (!string.IsNullOrWhiteSpace(signedChallengeAlgorithm))
            ValidateSignedChallengeAlgorithm(signedChallengeAlgorithm);

        Mode = mode;
        SharedSecret = sharedSecret;
        SignedChallengeKeyId = signedChallengeKeyId;
        SignedChallengeAlgorithm = signedChallengeAlgorithm ?? SignedChallengeEcdsaSha256Algorithm;
        SignChallenge = signChallenge;
        VerifyChallengeSignature = verifyChallengeSignature;
    }

    /// <summary>
    /// Gets the selected authentication mode.
    /// </summary>
    public AuthenticationMode Mode { get; }

    /// <summary>
    /// Gets the shared secret used by <see cref="AuthenticationMode.SharedSecretHmac"/>.
    /// </summary>
    /// <remarks>
    /// Do not log or serialize this value. It is intentionally omitted from reports.
    /// </remarks>
    public string? SharedSecret { get; }

    /// <summary>
    /// Gets the public key identifier sent by signed-challenge clients.
    /// </summary>
    public string? SignedChallengeKeyId { get; }

    /// <summary>
    /// Gets the signed-challenge algorithm label written to the authentication line.
    /// </summary>
    public string SignedChallengeAlgorithm { get; }

    internal Func<byte[], byte[]>? SignChallenge { get; }

    internal Func<string?, byte[], byte[], bool>? VerifyChallengeSignature { get; }

    internal bool RequiresClientProof =>
        Mode is AuthenticationMode.SharedSecretHmac or AuthenticationMode.SignedChallenge;

    internal bool RequiresHandshakeAuthentication => Mode != AuthenticationMode.None;

    private static void ValidateSignedChallengeKeyId(string keyId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyId);
        if (keyId.Any(static c => char.IsWhiteSpace(c) || c == '='))
            throw new ArgumentException("Signed-challenge key ids cannot contain whitespace or '='.", nameof(keyId));
    }

    private static void ValidateSignedChallengeAlgorithm(string algorithm)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithm);
        if (algorithm.Any(static c => char.IsWhiteSpace(c) || c == '='))
            throw new ArgumentException("Signed-challenge algorithm names cannot contain whitespace or '='.", nameof(algorithm));
    }
}
