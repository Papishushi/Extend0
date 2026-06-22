using System.Net;
using Extend0.Lifecycle.Certificates;

namespace Extend0.Tests.Lifecycle.Certificates;

public sealed class AcmeDns01ClientTests
{
    [Fact]
    public async Task CreateOrderAsync_CreatesOrderStateWithDns01TxtRecord()
    {
        var handler = new SequencedAcmeHandler(
            Response(HttpStatusCode.OK, DirectoryJson()),
            Response(HttpStatusCode.NoContent, string.Empty, nonce: "nonce-1"),
            Response(HttpStatusCode.Created, "{\"status\":\"valid\"}", nonce: "nonce-2", location: "https://acme.test/account/1"),
            Response(HttpStatusCode.Created, OrderJson("pending"), nonce: "nonce-3", location: "https://acme.test/order/1"),
            Response(HttpStatusCode.OK, AuthorizationJson("pending"), nonce: "nonce-4"));
        var client = new AcmeDns01Client(new HttpClient(handler));

        var state = await client.CreateOrderAsync(
            AcmeDns01OrderRequest.Create(
                "https://acme.test/directory",
                ["example.com"],
                "ops@example.com",
                acceptTermsOfService: true));

        Assert.Equal("https://acme.test/account/1", state.AccountLocation);
        Assert.Equal("https://acme.test/order/1", state.OrderUrl);
        Assert.Equal("pending", state.OrderStatus);
        Assert.Single(state.Authorizations);
        var authorization = state.Authorizations[0];
        Assert.Equal("example.com", authorization.Identifier);
        Assert.Equal("_acme-challenge.example.com", authorization.TxtRecordName);
        Assert.Equal(
            Dns01Challenge.CreateFromAccountThumbprint("example.com", "abc123", state.AccountKeyThumbprint).TxtRecordValue,
            authorization.TxtRecordValue);
        Assert.Contains(handler.Requests, static request =>
            request.Method == HttpMethod.Post
            && request.ContentType == "application/jose+json");
    }

    [Fact]
    public async Task ValidateAndFinalizeAsync_RequestsChallengeAndDownloadsCertificate()
    {
        var handler = new SequencedAcmeHandler(
            Response(HttpStatusCode.OK, DirectoryJson()),
            Response(HttpStatusCode.NoContent, string.Empty, nonce: "nonce-1"),
            Response(HttpStatusCode.Created, "{\"status\":\"valid\"}", nonce: "nonce-2", location: "https://acme.test/account/1"),
            Response(HttpStatusCode.Created, OrderJson("pending"), nonce: "nonce-3", location: "https://acme.test/order/1"),
            Response(HttpStatusCode.OK, AuthorizationJson("pending"), nonce: "nonce-4"),
            Response(HttpStatusCode.OK, "{\"type\":\"dns-01\",\"status\":\"processing\"}", nonce: "nonce-5"),
            Response(HttpStatusCode.OK, OrderJson("ready"), nonce: "nonce-6"),
            Response(HttpStatusCode.OK, AuthorizationJson("valid"), nonce: "nonce-7"),
            Response(HttpStatusCode.OK, OrderJson("ready"), nonce: "nonce-8"),
            Response(HttpStatusCode.OK, AuthorizationJson("valid"), nonce: "nonce-9"),
            Response(HttpStatusCode.OK, OrderJson("processing"), nonce: "nonce-10"),
            Response(HttpStatusCode.OK, OrderJson("valid", "https://acme.test/cert/1"), nonce: "nonce-11"),
            Response(HttpStatusCode.OK, AuthorizationJson("valid"), nonce: "nonce-12"),
            Response(HttpStatusCode.OK, FakeCertificateChainPem, nonce: "nonce-13"));
        var client = new AcmeDns01Client(new HttpClient(handler));
        var state = await client.CreateOrderAsync(
            AcmeDns01OrderRequest.Create(
                "https://acme.test/directory",
                ["example.com"],
                "ops@example.com",
                acceptTermsOfService: true));

        var validated = await client.RequestValidationAsync(
            state,
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(1));
        var finalized = await client.FinalizeAsync(
            validated,
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(1));

        Assert.Equal("ready", validated.OrderStatus);
        Assert.Equal("valid", finalized.State.OrderStatus);
        Assert.Equal("https://acme.test/cert/1", finalized.State.CertificateUrl);
        Assert.Equal(FakeCertificateChainPem, finalized.CertificateChainPem);
        Assert.Contains(handler.Requests, static request => request.Url == "https://acme.test/challenge/1");
        Assert.Contains(handler.Requests, static request => request.Url == "https://acme.test/finalize/1");
        Assert.Contains(handler.Requests, static request => request.Url == "https://acme.test/cert/1");
    }

    private static Func<HttpRequestMessage, HttpResponseMessage> Response(
        HttpStatusCode statusCode,
        string body,
        string? nonce = null,
        string? location = null) =>
        _ =>
        {
            var response = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(body)
            };
            if (nonce is not null)
                response.Headers.TryAddWithoutValidation("Replay-Nonce", nonce);
            if (location is not null)
                response.Headers.Location = new Uri(location);
            return response;
        };

    private static string DirectoryJson() =>
        """
        {
          "newNonce": "https://acme.test/new-nonce",
          "newAccount": "https://acme.test/new-account",
          "newOrder": "https://acme.test/new-order"
        }
        """;

    private static string OrderJson(string status, string? certificateUrl = null) =>
        certificateUrl is null
            ? $$"""
              {
                "status": "{{status}}",
                "authorizations": ["https://acme.test/auth/1"],
                "finalize": "https://acme.test/finalize/1"
              }
              """
            : $$"""
              {
                "status": "{{status}}",
                "authorizations": ["https://acme.test/auth/1"],
                "finalize": "https://acme.test/finalize/1",
                "certificate": "{{certificateUrl}}"
              }
              """;

    private static string AuthorizationJson(string status) =>
        $$"""
        {
          "status": "{{status}}",
          "identifier": { "type": "dns", "value": "example.com" },
          "challenges": [
            {
              "type": "dns-01",
              "url": "https://acme.test/challenge/1",
              "token": "abc123",
              "status": "{{status}}"
            }
          ]
        }
        """;

    private const string FakeCertificateChainPem =
        """
        -----BEGIN CERTIFICATE-----
        fake
        -----END CERTIFICATE-----

        """;

    private sealed class SequencedAcmeHandler : HttpMessageHandler
    {
        private readonly Queue<Func<HttpRequestMessage, HttpResponseMessage>> _responses;

        public SequencedAcmeHandler(params Func<HttpRequestMessage, HttpResponseMessage>[] responses)
        {
            _responses = new Queue<Func<HttpRequestMessage, HttpResponseMessage>>(responses);
        }

        public List<RecordedRequest> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(new RecordedRequest(
                request.Method,
                request.RequestUri?.AbsoluteUri,
                request.Content?.Headers.ContentType?.MediaType));
            if (_responses.Count == 0)
                throw new InvalidOperationException($"Unexpected request to '{request.RequestUri}'.");

            return Task.FromResult(_responses.Dequeue()(request));
        }
    }

    private sealed record RecordedRequest(HttpMethod Method, string? Url, string? ContentType);
}
