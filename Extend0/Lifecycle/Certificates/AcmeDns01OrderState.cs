using System.Text.Json;
using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Persisted state for an ACME DNS-01 certificate order.
/// </summary>
/// <remarks>
/// This state may contain ACME account private key material and certificate private key material.
/// Treat serialized instances as secret material unless protected by <see cref="AcmeDns01StateFile"/>.
/// </remarks>
/// <param name="DirectoryUrl">ACME directory URL used by the order.</param>
/// <param name="AccountLocation">ACME account URL assigned by the certificate authority.</param>
/// <param name="AccountKeyPem">PEM-encoded ACME account private key.</param>
/// <param name="AccountKeyThumbprint">Base64url JWK thumbprint for the ACME account key.</param>
/// <param name="CertificateKeyPem">PEM-encoded certificate private key used for the final CSR.</param>
/// <param name="Domains">DNS identifiers requested by the order.</param>
/// <param name="OrderUrl">ACME order URL.</param>
/// <param name="FinalizeUrl">ACME finalize URL for submitting the CSR.</param>
/// <param name="CertificateUrl">ACME certificate URL once issued, or <see langword="null"/> before issuance.</param>
/// <param name="OrderStatus">Last observed ACME order status.</param>
/// <param name="Authorizations">DNS-01 authorization states associated with the order.</param>
/// <param name="CreatedAtUtc">UTC timestamp when the local order state was created.</param>
/// <param name="UpdatedAtUtc">UTC timestamp when the local order state was last refreshed.</param>
public sealed record AcmeDns01OrderState(
    string DirectoryUrl,
    string AccountLocation,
    string AccountKeyPem,
    string AccountKeyThumbprint,
    string CertificateKeyPem,
    IReadOnlyList<string> Domains,
    string OrderUrl,
    string FinalizeUrl,
    string? CertificateUrl,
    string OrderStatus,
    IReadOnlyList<AcmeDns01AuthorizationState> Authorizations,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Returns a copy of the state with refreshed order status and authorization snapshots.
    /// </summary>
    /// <param name="orderStatus">Latest ACME order status.</param>
    /// <param name="certificateUrl">Latest ACME certificate URL, if one has been issued.</param>
    /// <param name="authorizations">Latest authorization snapshots.</param>
    /// <returns>A state instance with updated status metadata.</returns>
    public AcmeDns01OrderState WithOrderStatus(
        string orderStatus,
        string? certificateUrl,
        IReadOnlyList<AcmeDns01AuthorizationState> authorizations) =>
        this with
        {
            OrderStatus = orderStatus,
            CertificateUrl = certificateUrl,
            Authorizations = authorizations,
            UpdatedAtUtc = DateTimeOffset.UtcNow
        };

    /// <summary>
    /// Saves this state as unprotected JSON.
    /// </summary>
    /// <param name="path">Destination state file path.</param>
    /// <remarks>
    /// Prefer <see cref="AcmeDns01StateFile.Save(string, AcmeDns01OrderState, AcmeDns01StateProtectionOptions?)"/>
    /// when the state contains real account or certificate private keys.
    /// </remarks>
    public void Save(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var fullPath = Path.GetFullPath(path);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        File.WriteAllText(fullPath, JsonSerializer.Serialize(this, JsonOptions));
    }

    /// <summary>
    /// Loads an unprotected ACME DNS-01 order state file.
    /// </summary>
    /// <param name="path">Path to the state file.</param>
    /// <returns>The deserialized order state.</returns>
    /// <remarks>
    /// Prefer <see cref="AcmeDns01StateFile.Load(string, AcmeDns01StateProtectionOptions?, out AcmeDns01StateProtectionKind)"/>
    /// for callers that need to support protected state.
    /// </remarks>
    public static AcmeDns01OrderState Load(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var state = JsonSerializer.Deserialize<AcmeDns01OrderState>(File.ReadAllText(path), JsonOptions);
        return state ?? throw new InvalidDataException($"ACME DNS-01 state file '{path}' is empty or invalid.");
    }
}

/// <summary>
/// Persisted state for one ACME authorization inside a DNS-01 order.
/// </summary>
/// <param name="AuthorizationUrl">ACME authorization URL.</param>
/// <param name="Identifier">DNS identifier being authorized.</param>
/// <param name="Wildcard">Whether the authorization is for a wildcard identifier.</param>
/// <param name="Status">Last observed ACME authorization status.</param>
/// <param name="DnsChallengeUrl">ACME DNS-01 challenge URL.</param>
/// <param name="Token">ACME DNS-01 challenge token.</param>
/// <param name="TxtRecordName">Fully qualified TXT record name to publish.</param>
/// <param name="TxtRecordValue">TXT record value computed from the key authorization digest.</param>
public sealed record AcmeDns01AuthorizationState(
    string AuthorizationUrl,
    string Identifier,
    bool Wildcard,
    string Status,
    string DnsChallengeUrl,
    string Token,
    string TxtRecordName,
    string TxtRecordValue);

/// <summary>
/// Result of finalizing an ACME DNS-01 order and downloading the certificate chain.
/// </summary>
/// <param name="State">Updated order state after finalization.</param>
/// <param name="CertificateChainPem">PEM-encoded certificate chain returned by the ACME server.</param>
public sealed record AcmeDns01FinalizationResult(
    AcmeDns01OrderState State,
    string CertificateChainPem);

/// <summary>
/// File paths written by ACME certificate finalization.
/// </summary>
/// <param name="CertificateChainPath">Path to the PEM-encoded certificate chain.</param>
/// <param name="PrivateKeyPath">Path to the PEM-encoded private key.</param>
/// <param name="PfxPath">Path to the optional PFX output, or <see langword="null"/> when no PFX was requested.</param>
public sealed record AcmeCertificateFiles(
    string CertificateChainPath,
    string PrivateKeyPath,
    string? PfxPath);
