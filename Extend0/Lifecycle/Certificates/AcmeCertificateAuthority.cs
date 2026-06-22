namespace Extend0.Lifecycle.Certificates;

public static class AcmeCertificateAuthority
{
    public const string LetsEncryptProductionDirectoryUrl = "https://acme-v02.api.letsencrypt.org/directory";
    public const string LetsEncryptStagingDirectoryUrl = "https://acme-staging-v02.api.letsencrypt.org/directory";

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
