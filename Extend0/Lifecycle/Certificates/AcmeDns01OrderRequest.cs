namespace Extend0.Lifecycle.Certificates;

public sealed record AcmeDns01OrderRequest(
    string DirectoryUrl,
    IReadOnlyList<string> Domains,
    string Email,
    bool AcceptTermsOfService,
    int AccountKeyBits,
    int CertificateKeyBits)
{
    public const int DefaultAccountKeyBits = 2048;
    public const int DefaultCertificateKeyBits = 2048;

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
