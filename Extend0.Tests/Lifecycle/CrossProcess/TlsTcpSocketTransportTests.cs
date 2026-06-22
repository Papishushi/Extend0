using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Extend0.Lifecycle.CrossProcess;
using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class TlsTcpSocketTransportTests
{
    [Fact]
    public async Task TlsTcpSocketServer_AndClientTransport_RoundTrip_EndToEnd()
    {
        using var serverCertificate = CreateCertificate("localhost", clientAuthentication: false);
        var endpointName = AllocateLoopbackEndpoint();
        var implementation = new TlsTcpSocketTestService(endpointName);
        await using var server = LifecycleCrossProcessHarness.CreateTlsTcpSocketServer(
            endpointName,
            implementation,
            CancellationToken.None,
            CreateServerTls(serverCertificate));

        using var transport = CreateBuiltInTlsTcpSocketTransport(endpointName, CreateClientTls(serverCertificate));
        var proxy = RpcDispatchProxy<ITlsTcpSocketTestService>.Create(transport, CancellationToken.None);

        Assert.Equal(13, proxy.Add(6, 7));
        Assert.Equal("echo:secure", await proxy.EchoAsync("secure"));

        var info = await proxy.GetServiceInfoAsync();
        Assert.Equal(typeof(ITlsTcpSocketTestService).FullName, info.ContractName);
        Assert.Equal(endpointName, info.EndpointName);
        Assert.Equal(TransportKind.TlsTcpSocket, info.TransportKind);
    }

    [Fact]
    public async Task TlsTcpSocketClientTransport_RejectsUntrustedServerCertificate()
    {
        using var serverCertificate = CreateCertificate("localhost", clientAuthentication: false);
        var endpointName = AllocateLoopbackEndpoint();
        await using var server = LifecycleCrossProcessHarness.CreateTlsTcpSocketServer(
            endpointName,
            new TlsTcpSocketTestService(endpointName),
            CancellationToken.None,
            CreateServerTls(serverCertificate));

        var error = Assert.Throws<AuthenticationException>(() =>
        {
            using var transport = CreateBuiltInTlsTcpSocketTransport(
                endpointName,
                CrossProcessTlsOptions.ForClient("localhost") with { CheckCertificateRevocation = false });
        });

        Assert.Contains("certificate", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TlsTcpSocketServer_WithMutualTls_RequiresAndAcceptsClientCertificate()
    {
        using var serverCertificate = CreateCertificate("localhost", clientAuthentication: false);
        using var clientCertificate = CreateCertificate("extend0-client", clientAuthentication: true);
        var endpointName = AllocateLoopbackEndpoint();
        await using var server = LifecycleCrossProcessHarness.CreateTlsTcpSocketServer(
            endpointName,
            new TlsTcpSocketTestService(endpointName),
            CancellationToken.None,
            CreateServerTls(serverCertificate, requireClientCertificate: true, expectedClientCertificate: clientCertificate));

        var clientCertificates = new X509CertificateCollection { clientCertificate };
        using var transport = CreateBuiltInTlsTcpSocketTransport(
            endpointName,
            CreateClientTls(serverCertificate, clientCertificates));
        var proxy = RpcDispatchProxy<ITlsTcpSocketTestService>.Create(transport, CancellationToken.None);

        Assert.Equal("echo:mtls", await proxy.EchoAsync("mtls"));
    }

    [Fact]
    public async Task TlsTcpSocketServer_WithMutualTls_RejectsMissingClientCertificate()
    {
        using var serverCertificate = CreateCertificate("localhost", clientAuthentication: false);
        using var clientCertificate = CreateCertificate("extend0-client", clientAuthentication: true);
        var endpointName = AllocateLoopbackEndpoint();
        await using var server = LifecycleCrossProcessHarness.CreateTlsTcpSocketServer(
            endpointName,
            new TlsTcpSocketTestService(endpointName),
            CancellationToken.None,
            CreateServerTls(serverCertificate, requireClientCertificate: true, expectedClientCertificate: clientCertificate));

        var error = Assert.ThrowsAny<Exception>(() =>
        {
            using var transport = CreateBuiltInTlsTcpSocketTransport(endpointName, CreateClientTls(serverCertificate));
        });

        Assert.True(error is AuthenticationException or IOException, $"Unexpected exception type: {error.GetType()}");
    }

    private static IClientTransport CreateBuiltInTlsTcpSocketTransport(string endpointName, CrossProcessTlsOptions tls) =>
        LifecycleCrossProcessHarness.CreateBuiltInClientTransport(
            new ClientTransportFactoryContext(
                TransportKind.TlsTcpSocket,
                LifecycleCrossProcessHarness.TlsTcpSocketProtocolDescriptor,
                endpointName,
                ".",
                2000,
                Tls: tls));

    private static CrossProcessTlsOptions CreateServerTls(
        X509Certificate2 serverCertificate,
        bool requireClientCertificate = false,
        X509Certificate2? expectedClientCertificate = null) =>
        CrossProcessTlsOptions.ForServer(
            serverCertificate,
            requireClientCertificate,
            expectedClientCertificate is null ? null : (_, certificate, _, _) => CertificateMatches(certificate, expectedClientCertificate))
        with
        {
            CheckCertificateRevocation = false
        };

    private static CrossProcessTlsOptions CreateClientTls(
        X509Certificate2 expectedServerCertificate,
        X509CertificateCollection? clientCertificates = null) =>
        CrossProcessTlsOptions.ForClient(
            "localhost",
            (_, certificate, _, _) => CertificateMatches(certificate, expectedServerCertificate),
            clientCertificates)
        with
        {
            CheckCertificateRevocation = false
        };

    private static bool CertificateMatches(X509Certificate? actual, X509Certificate2 expected)
    {
        if (actual is null)
            return false;

        if (actual is X509Certificate2 actual2)
            return string.Equals(actual2.Thumbprint, expected.Thumbprint, StringComparison.OrdinalIgnoreCase);

        using var converted = new X509Certificate2(actual);
        return string.Equals(converted.Thumbprint, expected.Thumbprint, StringComparison.OrdinalIgnoreCase);
    }

    private static X509Certificate2 CreateCertificate(string commonName, bool clientAuthentication)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);

        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
            false));

        var usages = new OidCollection
        {
            new(clientAuthentication ? "1.3.6.1.5.5.7.3.2" : "1.3.6.1.5.5.7.3.1")
        };
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(usages, false));

        if (!clientAuthentication)
        {
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName("localhost");
            san.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(san.Build());
        }

        using var certificate = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddDays(1));

        return X509CertificateLoader.LoadPkcs12(
            certificate.Export(X509ContentType.Pfx),
            (string?)null,
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.DefaultKeySet);
    }

    private static string AllocateLoopbackEndpoint()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return $"127.0.0.1:{port}";
    }

    private interface ITlsTcpSocketTestService : ICrossProcessService
    {
        int Add(int a, int b);
        Task<string> EchoAsync(string value);
    }

    private sealed class TlsTcpSocketTestService(string endpointName)
        : CrossProcessServiceBase<ITlsTcpSocketTestService>, ITlsTcpSocketTestService
    {
        protected override string? EndpointName => endpointName;
        protected override string? EndpointServerName => "127.0.0.1";
        protected override TransportKind EndpointTransportKind => TransportKind.TlsTcpSocket;

        public int Add(int a, int b) => a + b;

        public Task<string> EchoAsync(string value) => Task.FromResult($"echo:{value}");
    }
}
