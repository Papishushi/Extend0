namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Provides well-known ACME certificate authority directory endpoints and endpoint selection helpers.
/// </summary>
public static class AcmeCertificateAuthority
{
    /// <summary>
    /// Let's Encrypt production ACME v2 directory URL.
    /// </summary>
    public const string LetsEncryptProductionDirectoryUrl = "https://acme-v02.api.letsencrypt.org/directory";

    /// <summary>
    /// Let's Encrypt staging ACME v2 directory URL.
    /// </summary>
    public const string LetsEncryptStagingDirectoryUrl = "https://acme-staging-v02.api.letsencrypt.org/directory";

    /// <summary>
    /// Resolves the effective ACME directory URL from explicit, staging, and production options.
    /// </summary>
    /// <param name="directoryUrl">Explicit ACME directory URL, or <see langword="null"/> to use a built-in Let's Encrypt endpoint.</param>
    /// <param name="staging">Whether the staging endpoint was requested.</param>
    /// <param name="production">Whether the production endpoint was requested.</param>
    /// <returns>The ACME directory URL that should be used for the order.</returns>
    public static string ResolveDirectoryUrl(string? directoryUrl, bool staging, bool production)
    {
        if (!string.IsNullOrWhiteSpace(directoryUrl))
        {
            if (staging || production)
                throw new ArgumentException("--directory-url cannot be combined with --staging or --production.", nameof(directoryUrl));

            return directoryUrl.Trim();
        }

        if (staging && production)
            throw new ArgumentException("Choose either --staging or --production, not both.", nameof(staging));

        return production ? LetsEncryptProductionDirectoryUrl : LetsEncryptStagingDirectoryUrl;
    }
}
