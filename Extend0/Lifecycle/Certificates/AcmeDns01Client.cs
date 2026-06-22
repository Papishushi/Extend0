using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace Extend0.Lifecycle.Certificates;

public sealed class AcmeDns01Client
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _httpClient;
    private string? _newNonceUrl;
    private string? _nonce;

    public AcmeDns01Client(HttpClient? httpClient = null)
    {
        _httpClient = httpClient ?? new HttpClient();
    }

    public async Task<AcmeDns01OrderState> CreateOrderAsync(
        AcmeDns01OrderRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.AcceptTermsOfService)
            throw new InvalidOperationException("ACME account creation requires explicit terms-of-service acceptance.");

        var directory = await GetDirectoryAsync(request.DirectoryUrl, cancellationToken).ConfigureAwait(false);
        using var account = AcmeAccountKey.Create(request.AccountKeyBits);
        using var certificateKey = RSA.Create(request.CertificateKeyBits);

        var accountPayload = JsonSerializer.SerializeToElement(new
        {
            termsOfServiceAgreed = true,
            contact = new[] { NormalizeMailto(request.Email) }
        }, JsonOptions);
        using var accountResponse = await SendSignedRequestAsync(
            directory.NewAccount,
            account,
            kid: null,
            payload: accountPayload,
            cancellationToken).ConfigureAwait(false);

        var accountLocation = GetRequiredHeader(accountResponse, "Location");
        var orderPayload = JsonSerializer.SerializeToElement(new
        {
            identifiers = request.Domains.Select(static domain => new { type = "dns", value = domain }).ToArray()
        }, JsonOptions);
        using var orderResponse = await SendSignedRequestAsync(
            directory.NewOrder,
            account,
            accountLocation,
            orderPayload,
            cancellationToken).ConfigureAwait(false);

        var orderUrl = GetRequiredHeader(orderResponse, "Location");
        using var orderDocument = await ReadJsonAsync(orderResponse, cancellationToken).ConfigureAwait(false);
        var order = orderDocument.RootElement;
        var finalizeUrl = order.GetProperty("finalize").GetString()
            ?? throw new InvalidDataException("ACME order did not include a finalize URL.");
        var orderStatus = order.GetProperty("status").GetString() ?? "unknown";
        var certificateUrl = TryReadString(order, "certificate");
        var authorizationUrls = ReadStringArray(order.GetProperty("authorizations"));
        var authorizations = new List<AcmeDns01AuthorizationState>(authorizationUrls.Count);
        foreach (var authorizationUrl in authorizationUrls)
        {
            authorizations.Add(await FetchAuthorizationAsync(
                account,
                accountLocation,
                authorizationUrl,
                cancellationToken).ConfigureAwait(false));
        }

        var now = DateTimeOffset.UtcNow;
        return new AcmeDns01OrderState(
            request.DirectoryUrl,
            accountLocation,
            account.ExportPrivateKeyPem(),
            account.Thumbprint,
            certificateKey.ExportRSAPrivateKeyPem(),
            request.Domains.ToArray(),
            orderUrl,
            finalizeUrl,
            certificateUrl,
            orderStatus,
            authorizations,
            now,
            now);
    }

    public async Task<AcmeDns01OrderState> RequestValidationAsync(
        AcmeDns01OrderState state,
        TimeSpan wait,
        TimeSpan pollInterval,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(state);
        ValidatePolling(wait, pollInterval);
        await EnsureDirectoryLoadedAsync(state.DirectoryUrl, cancellationToken).ConfigureAwait(false);
        using var account = AcmeAccountKey.FromPem(state.AccountKeyPem);

        foreach (var authorization in state.Authorizations)
        {
            if (!string.Equals(authorization.Status, "pending", StringComparison.OrdinalIgnoreCase))
                continue;

            using var _ = await SendSignedRequestAsync(
                authorization.DnsChallengeUrl,
                account,
                state.AccountLocation,
                JsonSerializer.SerializeToElement(new { }, JsonOptions),
                cancellationToken).ConfigureAwait(false);
        }

        return await PollOrderAsync(state, account, wait, pollInterval, waitForCertificate: false, cancellationToken).ConfigureAwait(false);
    }

    public async Task<AcmeDns01OrderState> RefreshOrderAsync(
        AcmeDns01OrderState state,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(state);
        await EnsureDirectoryLoadedAsync(state.DirectoryUrl, cancellationToken).ConfigureAwait(false);
        using var account = AcmeAccountKey.FromPem(state.AccountKeyPem);
        return await RefreshOrderAsync(state, account, cancellationToken).ConfigureAwait(false);
    }

    public async Task<AcmeDns01FinalizationResult> FinalizeAsync(
        AcmeDns01OrderState state,
        TimeSpan wait,
        TimeSpan pollInterval,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(state);
        ValidatePolling(wait, pollInterval);
        await EnsureDirectoryLoadedAsync(state.DirectoryUrl, cancellationToken).ConfigureAwait(false);
        using var account = AcmeAccountKey.FromPem(state.AccountKeyPem);
        var refreshed = await RefreshOrderAsync(state, account, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(refreshed.OrderStatus, "ready", StringComparison.OrdinalIgnoreCase)
            && string.IsNullOrWhiteSpace(refreshed.CertificateUrl))
        {
            throw new InvalidOperationException($"ACME order must be ready before finalization. Current status is '{refreshed.OrderStatus}'.");
        }

        if (string.IsNullOrWhiteSpace(refreshed.CertificateUrl))
        {
            using var certificateKey = RSA.Create();
            certificateKey.ImportFromPem(refreshed.CertificateKeyPem);
            var csr = CreateCertificateSigningRequest(refreshed.Domains, certificateKey);
            var finalizePayload = JsonSerializer.SerializeToElement(new
            {
                csr = Base64Url.Encode(csr)
            }, JsonOptions);

            using var _ = await SendSignedRequestAsync(
                refreshed.FinalizeUrl,
                account,
                refreshed.AccountLocation,
                finalizePayload,
                cancellationToken).ConfigureAwait(false);

            refreshed = await PollOrderAsync(refreshed, account, wait, pollInterval, waitForCertificate: true, cancellationToken).ConfigureAwait(false);
        }

        if (string.IsNullOrWhiteSpace(refreshed.CertificateUrl))
            throw new InvalidOperationException($"ACME order did not expose a certificate URL. Current status is '{refreshed.OrderStatus}'.");

        using var certificateResponse = await SendSignedRequestAsync(
            refreshed.CertificateUrl,
            account,
            refreshed.AccountLocation,
            payload: null,
            cancellationToken).ConfigureAwait(false);
        var certificateChainPem = await certificateResponse.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        return new AcmeDns01FinalizationResult(refreshed, certificateChainPem);
    }

    public static AcmeCertificateFiles WriteCertificateFiles(
        AcmeDns01FinalizationResult result,
        string outputDirectory,
        string? pfxPassword = null)
    {
        ArgumentNullException.ThrowIfNull(result);
        ArgumentException.ThrowIfNullOrWhiteSpace(outputDirectory);

        var fullOutputDirectory = Path.GetFullPath(outputDirectory);
        Directory.CreateDirectory(fullOutputDirectory);

        var certificateChainPath = Path.Combine(fullOutputDirectory, "certificate-chain.pem");
        var privateKeyPath = Path.Combine(fullOutputDirectory, "private-key.pem");
        File.WriteAllText(certificateChainPath, result.CertificateChainPem);
        File.WriteAllText(privateKeyPath, result.State.CertificateKeyPem);

        string? pfxPath = null;
        if (pfxPassword is not null)
        {
            using var certificate = X509Certificate2.CreateFromPem(result.CertificateChainPem, result.State.CertificateKeyPem);
            pfxPath = Path.Combine(fullOutputDirectory, "certificate.pfx");
            File.WriteAllBytes(pfxPath, certificate.Export(X509ContentType.Pkcs12, pfxPassword));
        }

        return new AcmeCertificateFiles(certificateChainPath, privateKeyPath, pfxPath);
    }

    private async Task<AcmeDirectory> GetDirectoryAsync(string directoryUrl, CancellationToken cancellationToken)
    {
        using var response = await _httpClient.GetAsync(directoryUrl, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        using var document = await ReadJsonAsync(response, cancellationToken).ConfigureAwait(false);
        var root = document.RootElement;
        var directory = new AcmeDirectory(
            root.GetProperty("newNonce").GetString() ?? throw new InvalidDataException("ACME directory is missing newNonce."),
            root.GetProperty("newAccount").GetString() ?? throw new InvalidDataException("ACME directory is missing newAccount."),
            root.GetProperty("newOrder").GetString() ?? throw new InvalidDataException("ACME directory is missing newOrder."));
        _newNonceUrl = directory.NewNonce;
        return directory;
    }

    private async Task EnsureDirectoryLoadedAsync(string directoryUrl, CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_newNonceUrl))
            return;

        _ = await GetDirectoryAsync(directoryUrl, cancellationToken).ConfigureAwait(false);
    }

    private async Task<AcmeDns01OrderState> RefreshOrderAsync(
        AcmeDns01OrderState state,
        AcmeAccountKey account,
        CancellationToken cancellationToken)
    {
        using var orderResponse = await SendSignedRequestAsync(
            state.OrderUrl,
            account,
            state.AccountLocation,
            payload: null,
            cancellationToken).ConfigureAwait(false);

        using var orderDocument = await ReadJsonAsync(orderResponse, cancellationToken).ConfigureAwait(false);
        var order = orderDocument.RootElement;
        var status = order.GetProperty("status").GetString() ?? "unknown";
        var certificateUrl = TryReadString(order, "certificate");
        var authorizationUrls = ReadStringArray(order.GetProperty("authorizations"));
        var authorizations = new List<AcmeDns01AuthorizationState>(authorizationUrls.Count);
        foreach (var authorizationUrl in authorizationUrls)
        {
            authorizations.Add(await FetchAuthorizationAsync(
                account,
                state.AccountLocation,
                authorizationUrl,
                cancellationToken).ConfigureAwait(false));
        }

        return state.WithOrderStatus(status, certificateUrl, authorizations);
    }

    private async Task<AcmeDns01OrderState> PollOrderAsync(
        AcmeDns01OrderState state,
        AcmeAccountKey account,
        TimeSpan wait,
        TimeSpan pollInterval,
        bool waitForCertificate,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + wait;
        var current = await RefreshOrderAsync(state, account, cancellationToken).ConfigureAwait(false);
        while (RequiresPolling(current, waitForCertificate) && DateTimeOffset.UtcNow < deadline)
        {
            var remaining = deadline - DateTimeOffset.UtcNow;
            var delay = remaining < pollInterval ? remaining : pollInterval;
            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

            current = await RefreshOrderAsync(current, account, cancellationToken).ConfigureAwait(false);
        }

        return current;
    }

    private static bool RequiresPolling(AcmeDns01OrderState state, bool waitForCertificate) =>
        state.OrderStatus is "pending" or "processing"
        || (waitForCertificate && state.OrderStatus == "ready" && string.IsNullOrWhiteSpace(state.CertificateUrl));

    private async Task<AcmeDns01AuthorizationState> FetchAuthorizationAsync(
        AcmeAccountKey account,
        string accountLocation,
        string authorizationUrl,
        CancellationToken cancellationToken)
    {
        using var response = await SendSignedRequestAsync(
            authorizationUrl,
            account,
            accountLocation,
            payload: null,
            cancellationToken).ConfigureAwait(false);
        using var document = await ReadJsonAsync(response, cancellationToken).ConfigureAwait(false);
        var authorization = document.RootElement;
        var status = authorization.GetProperty("status").GetString() ?? "unknown";
        var identifierValue = authorization.GetProperty("identifier").GetProperty("value").GetString()
            ?? throw new InvalidDataException("ACME authorization is missing identifier value.");
        var wildcard = authorization.TryGetProperty("wildcard", out var wildcardElement)
            && wildcardElement.ValueKind == JsonValueKind.True;
        var domain = wildcard ? $"*.{identifierValue}" : identifierValue;
        var dnsChallenge = authorization.GetProperty("challenges")
            .EnumerateArray()
            .FirstOrDefault(static challenge =>
                challenge.TryGetProperty("type", out var type)
                && string.Equals(type.GetString(), "dns-01", StringComparison.OrdinalIgnoreCase));

        if (dnsChallenge.ValueKind == JsonValueKind.Undefined)
            throw new InvalidDataException($"ACME authorization '{authorizationUrl}' does not include a dns-01 challenge.");

        var token = dnsChallenge.GetProperty("token").GetString()
            ?? throw new InvalidDataException($"ACME dns-01 challenge '{authorizationUrl}' is missing token.");
        var challengeUrl = dnsChallenge.GetProperty("url").GetString()
            ?? throw new InvalidDataException($"ACME dns-01 challenge '{authorizationUrl}' is missing URL.");
        var challenge = Dns01Challenge.CreateFromAccountThumbprint(domain, token, account.Thumbprint);

        return new AcmeDns01AuthorizationState(
            authorizationUrl,
            domain,
            wildcard,
            status,
            challengeUrl,
            token,
            challenge.TxtRecordName,
            challenge.TxtRecordValue);
    }

    private async Task<HttpResponseMessage> SendSignedRequestAsync(
        string url,
        AcmeAccountKey account,
        string? kid,
        JsonElement? payload,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var nonce = _nonce ?? await GetNonceAsync(_newNonceUrl ?? url, cancellationToken).ConfigureAwait(false);
            _nonce = null;
            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = CreateJwsContent(account, kid, nonce, url, payload)
            };

            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            CaptureNonce(response);
            if (response.IsSuccessStatusCode)
                return response;

            var errorBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            if (attempt == 0 && errorBody.Contains("urn:ietf:params:acme:error:badNonce", StringComparison.OrdinalIgnoreCase))
            {
                response.Dispose();
                continue;
            }

            throw new HttpRequestException($"ACME request to '{url}' failed with {(int)response.StatusCode} {response.ReasonPhrase}: {errorBody}");
        }

        throw new HttpRequestException($"ACME request to '{url}' failed after nonce retry.");
    }

    private async Task<string> GetNonceAsync(string url, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Head, url);
        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (response.Headers.TryGetValues("Replay-Nonce", out var values))
            return values.First();

        throw new InvalidDataException("ACME server did not provide a Replay-Nonce header.");
    }

    private static HttpContent CreateJwsContent(
        AcmeAccountKey account,
        string? kid,
        string nonce,
        string url,
        JsonElement? payload)
    {
        var protectedHeader = kid is null
            ? new Dictionary<string, object?>
            {
                ["alg"] = "RS256",
                ["jwk"] = account.Jwk,
                ["nonce"] = nonce,
                ["url"] = url
            }
            : new Dictionary<string, object?>
            {
                ["alg"] = "RS256",
                ["kid"] = kid,
                ["nonce"] = nonce,
                ["url"] = url
            };

        var protectedPart = Base64Url.EncodeString(JsonSerializer.Serialize(protectedHeader, JsonOptions));
        var payloadPart = payload.HasValue
            ? Base64Url.EncodeString(payload.Value.GetRawText())
            : string.Empty;
        var signatureInput = Encoding.ASCII.GetBytes($"{protectedPart}.{payloadPart}");
        var signature = Base64Url.Encode(account.Sign(signatureInput));
        var envelope = new Dictionary<string, string>
        {
            ["protected"] = protectedPart,
            ["payload"] = payloadPart,
            ["signature"] = signature
        };
        var content = new StringContent(JsonSerializer.Serialize(envelope, JsonOptions), Encoding.UTF8);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/jose+json");
        return content;
    }

    private static byte[] CreateCertificateSigningRequest(IReadOnlyList<string> domains, RSA certificateKey)
    {
        if (domains.Count == 0)
            throw new InvalidOperationException("Cannot create a CSR without DNS identifiers.");

        var request = new CertificateRequest(
            $"CN={domains[0]}",
            certificateKey,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        var sanBuilder = new SubjectAlternativeNameBuilder();
        foreach (var domain in domains)
            sanBuilder.AddDnsName(domain);

        request.CertificateExtensions.Add(sanBuilder.Build());
        return request.CreateSigningRequest();
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
            return;

        var errorBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new HttpRequestException($"ACME request failed with {(int)response.StatusCode} {response.ReasonPhrase}: {errorBody}");
    }

    private static async Task<JsonDocument> ReadJsonAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private static IReadOnlyList<string> ReadStringArray(JsonElement array) =>
        array.EnumerateArray()
            .Select(static item => item.GetString() ?? throw new InvalidDataException("ACME array contained a non-string value."))
            .ToArray();

    private static string? TryReadString(JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static string GetRequiredHeader(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values)
            ? values.First()
            : throw new InvalidDataException($"ACME response is missing required '{name}' header.");

    private static string NormalizeMailto(string email)
    {
        var trimmed = email.Trim();
        return trimmed.StartsWith("mailto:", StringComparison.OrdinalIgnoreCase)
            ? trimmed
            : $"mailto:{trimmed}";
    }

    private static void ValidatePolling(TimeSpan wait, TimeSpan pollInterval)
    {
        if (wait < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(wait), wait, "Wait time cannot be negative.");

        if (pollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(pollInterval), pollInterval, "Poll interval must be positive.");
    }

    private void CaptureNonce(HttpResponseMessage response)
    {
        if (response.Headers.TryGetValues("Replay-Nonce", out var values))
            _nonce = values.FirstOrDefault();
    }

    private sealed record AcmeDirectory(string NewNonce, string NewAccount, string NewOrder);

    private sealed class AcmeAccountKey : IDisposable
    {
        private readonly RSA _rsa;

        private AcmeAccountKey(RSA rsa)
        {
            _rsa = rsa;
            Jwk = CreateJwk(_rsa);
            Thumbprint = ComputeThumbprint(_rsa);
        }

        public IReadOnlyDictionary<string, string> Jwk { get; }

        public string Thumbprint { get; }

        public static AcmeAccountKey Create(int bits) => new(RSA.Create(bits));

        public static AcmeAccountKey FromPem(string pem)
        {
            var rsa = RSA.Create();
            rsa.ImportFromPem(pem);
            return new AcmeAccountKey(rsa);
        }

        public string ExportPrivateKeyPem() => _rsa.ExportRSAPrivateKeyPem();

        public byte[] Sign(byte[] data) =>
            _rsa.SignData(data, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        public void Dispose() => _rsa.Dispose();

        private static IReadOnlyDictionary<string, string> CreateJwk(RSA rsa)
        {
            var parameters = rsa.ExportParameters(includePrivateParameters: false);
            return new Dictionary<string, string>
            {
                ["e"] = Base64Url.Encode(parameters.Exponent!),
                ["kty"] = "RSA",
                ["n"] = Base64Url.Encode(parameters.Modulus!)
            };
        }

        private static string ComputeThumbprint(RSA rsa)
        {
            var parameters = rsa.ExportParameters(includePrivateParameters: false);
            var e = Base64Url.Encode(parameters.Exponent!);
            var n = Base64Url.Encode(parameters.Modulus!);
            var canonicalJwk = $"{{\"e\":\"{e}\",\"kty\":\"RSA\",\"n\":\"{n}\"}}";
            return Base64Url.Encode(SHA256.HashData(Encoding.UTF8.GetBytes(canonicalJwk)));
        }
    }
}
