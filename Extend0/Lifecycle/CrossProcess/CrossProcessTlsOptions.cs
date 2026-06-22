using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// TLS options used by <see cref="TransportKind.TlsTcpSocket"/>.
/// </summary>
/// <remarks>
/// TLS is a transport-security layer. It can provide confidentiality, integrity,
/// server authentication, and optional client-certificate authentication. It is separate
/// from the JSON-RPC handshake authentication options such as shared-secret HMAC.
/// </remarks>
public sealed record CrossProcessTlsOptions
{
    /// <summary>
    /// Creates server-side TLS options.
    /// </summary>
    public static CrossProcessTlsOptions ForServer(
        X509Certificate2 serverCertificate,
        bool requireClientCertificate = false,
        RemoteCertificateValidationCallback? clientCertificateValidationCallback = null) =>
        new()
        {
            ServerCertificate = serverCertificate ?? throw new ArgumentNullException(nameof(serverCertificate)),
            RequireClientCertificate = requireClientCertificate,
            RemoteCertificateValidationCallback = clientCertificateValidationCallback
        };

    /// <summary>
    /// Creates client-side TLS options.
    /// </summary>
    public static CrossProcessTlsOptions ForClient(
        string? targetHost = null,
        RemoteCertificateValidationCallback? serverCertificateValidationCallback = null,
        X509CertificateCollection? clientCertificates = null) =>
        new()
        {
            TargetHost = targetHost,
            RemoteCertificateValidationCallback = serverCertificateValidationCallback,
            ClientCertificates = clientCertificates
        };

    /// <summary>
    /// Certificate presented by the owner host. Required for TLS server hosts.
    /// </summary>
    public X509Certificate2? ServerCertificate { get; init; }

    /// <summary>
    /// Target host used by the client for SNI and certificate-name validation.
    /// Defaults to the TCP endpoint host when omitted.
    /// </summary>
    public string? TargetHost { get; init; }

    /// <summary>
    /// Client certificates presented by the client. Required when the server enforces mTLS.
    /// </summary>
    public X509CertificateCollection? ClientCertificates { get; init; }

    /// <summary>
    /// Whether the server requires a client certificate during TLS authentication.
    /// </summary>
    public bool RequireClientCertificate { get; init; }

    /// <summary>
    /// Certificate validation callback. On clients it validates the server certificate;
    /// on servers it validates client certificates when <see cref="RequireClientCertificate"/> is enabled.
    /// </summary>
    public RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; init; }

    /// <summary>
    /// TLS protocol versions allowed by the transport.
    /// </summary>
    public SslProtocols EnabledSslProtocols { get; init; } = SslProtocols.Tls12 | SslProtocols.Tls13;

    /// <summary>
    /// Whether certificate revocation checks are requested during TLS authentication.
    /// </summary>
    public bool CheckCertificateRevocation { get; init; } = true;
}
