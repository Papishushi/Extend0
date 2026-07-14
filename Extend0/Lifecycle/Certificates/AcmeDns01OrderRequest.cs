namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Describes the user-facing inputs required to create an ACME DNS-01 order.
/// </summary>
/// <param name="DirectoryUrl">ACME directory URL used for account and order creation.</param>
/// <param name="Domains">DNS identifiers requested for the certificate order.</param>
/// <param name="Email">ACME account contact email address.</param>
/// <param name="AcceptTermsOfService">Whether the caller explicitly accepted the CA terms of service.</param>
/// <param name="AccountKeyBits">RSA key size used for the ACME account key.</param>
/// <param name="CertificateKeyBits">RSA key size used for the certificate private key.</param>
public sealed record AcmeDns01OrderRequest(
    string DirectoryUrl,
    IReadOnlyList<string> Domains,
    string Email,
    bool AcceptTermsOfService,
    int AccountKeyBits,
    int CertificateKeyBits)
{
    /// <summary>
    /// Default RSA key size for ACME account keys.
    /// </summary>
    public const int DefaultAccountKeyBits = 2048;

    /// <summary>
    /// Default RSA key size for certificate private keys.
    /// </summary>
    public const int DefaultCertificateKeyBits = 2048;

    /// <summary>
    /// Creates a validated request, normalizing and de-duplicating DNS identifiers.
    /// </summary>
    /// <param name="directoryUrl">ACME directory URL.</param>
    /// <param name="domains">DNS identifiers requested for the certificate.</param>
    /// <param name="email">ACME account contact email address.</param>
    /// <param name="acceptTermsOfService">Whether the caller accepted the CA terms of service.</param>
    /// <param name="accountKeyBits">RSA key size for the ACME account key.</param>
    /// <param name="certificateKeyBits">RSA key size for the certificate private key.</param>
    /// <returns>A normalized ACME DNS-01 order request.</returns>
    public static AcmeDns01OrderRequest Create(
        string directoryUrl,
        IReadOnlyList<string> domains,
        string email,
        bool acceptTermsOfService,
        int accountKeyBits = DefaultAccountKeyBits,
        int certificateKeyBits = DefaultCertificateKeyBits)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryUrl);
        ArgumentNullException.ThrowIfNull(domains);
        ArgumentException.ThrowIfNullOrWhiteSpace(email);

        if (domains.Count == 0)
            throw new ArgumentException("At least one domain is required.", nameof(domains));

        if (accountKeyBits < 2048)
            throw new ArgumentOutOfRangeException(nameof(accountKeyBits), accountKeyBits, "Account RSA key size must be at least 2048 bits.");

        if (certificateKeyBits < 2048)
            throw new ArgumentOutOfRangeException(nameof(certificateKeyBits), certificateKeyBits, "Certificate RSA key size must be at least 2048 bits.");

        return new AcmeDns01OrderRequest(
            directoryUrl.Trim(),
            domains.Select(NormalizeDomainForOrder).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
            email.Trim(),
            acceptTermsOfService,
            accountKeyBits,
            certificateKeyBits);
    }

    private static string NormalizeDomainForOrder(string domain) =>
        Dns01Challenge.CreateFromAccountThumbprint(domain, "token", "thumbprint").Domain;
}
