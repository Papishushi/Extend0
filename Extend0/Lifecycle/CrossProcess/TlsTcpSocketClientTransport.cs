using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Client-side JSON-RPC NDJSON transport over TCP protected by TLS.
    /// </summary>
    internal sealed class TlsTcpSocketClientTransport : IClientTransport
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _networkStream;
        private readonly SslStream _tlsStream;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;

        public TransportKind Kind => TransportKind.TlsTcpSocket;

        public TlsTcpSocketClientTransport(
            string serverName,
            string endpointName,
            int timeoutMs,
            CrossProcessProtocolDescriptor? protocol = null,
            CrossProcessAuthenticationOptions? authentication = null,
            CrossProcessTlsOptions? tls = null)
        {
            var endpoint = TcpSocketEndpoint.Parse(endpointName, serverName);
            var expectedProtocol = protocol ?? TlsTcpSocketTransportProtocol.Descriptor;
            var tlsOptions = tls ?? CrossProcessTlsOptions.ForClient();

            _client = new TcpClient();
            try
            {
                var connectTask = _client.ConnectAsync(endpoint.Host, endpoint.Port);
                if (!connectTask.Wait(timeoutMs))
                    throw new TimeoutException($"Timed out connecting to TLS TCP endpoint {endpoint.Host}:{endpoint.Port} after {timeoutMs}ms.");

                connectTask.GetAwaiter().GetResult();
                _networkStream = _client.GetStream();
                _tlsStream = new SslStream(
                    _networkStream,
                    leaveInnerStreamOpen: true,
                    tlsOptions.RemoteCertificateValidationCallback);

                AuthenticateTls(endpoint, tlsOptions);

                _reader = new StreamReader(_tlsStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1024, leaveOpen: true);
                _writer = new StreamWriter(_tlsStream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1024, leaveOpen: true)
                {
                    AutoFlush = true
                };

                var serverHello = _reader.ReadLine();
                if (serverHello is null)
                    throw new IOException("Invalid server handshake: missing greeting.");

                if (!CrossProcessHandshake.TryValidateHelloLine(serverHello, expectedProtocol, out var hello, out var handshakeError))
                    throw new IOException($"Invalid server handshake: {handshakeError}");

                var authLine = CrossProcessHandshake.CreateClientAuthenticationLine(hello!, authentication);
                if (authLine is not null)
                {
                    _writer.WriteLine(authLine);
                    var authenticationAck = _reader.ReadLine();
                    if (!CrossProcessHandshake.TryValidateAuthenticationOkLine(authenticationAck, out var authenticationError))
                        throw new IOException($"Authentication failed: {authenticationError}");
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        private void AuthenticateTls(TcpSocketEndpoint endpoint, CrossProcessTlsOptions tlsOptions)
        {
            var targetHost = string.IsNullOrWhiteSpace(tlsOptions.TargetHost)
                ? endpoint.Host
                : tlsOptions.TargetHost;

            var options = new SslClientAuthenticationOptions
            {
                TargetHost = targetHost,
                EnabledSslProtocols = tlsOptions.EnabledSslProtocols,
                CertificateRevocationCheckMode = tlsOptions.CheckCertificateRevocation
                    ? X509RevocationMode.Online
                    : X509RevocationMode.NoCheck,
                ClientCertificates = tlsOptions.ClientCertificates
            };

            _tlsStream.AuthenticateAsClient(options);
        }

        public async Task<JsonDocument> CallAsync(string method, object?[] args, Type[] paramTypes, Type declaredReturnType, CancellationToken ct)
        {
            var req = new RpcReq(method, args);
            try
            {
                await _writer.WriteLineAsync(JsonSerializer.Serialize(req)).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
            {
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Transport closed.\", \"hr\": 426}");
            }

            string? line;
            try
            {
                line = await _reader.ReadLineAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException)
            {
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Transport closed.\", \"hr\": 426}");
            }

            if (line is null)
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Transport closed.\", \"hr\": 426}");

            try
            {
                return JsonDocument.Parse(line);
            }
            catch (JsonException)
            {
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Bad transport data.\", \"hr\": 422}");
            }
        }

        public void Dispose()
        {
            try { _writer?.Dispose(); } catch { }
            try { _reader?.Dispose(); } catch { }
            try { _tlsStream?.Dispose(); } catch { }
            try { _networkStream?.Dispose(); } catch { }
            try { _client.Dispose(); } catch { }
        }

#pragma warning disable IDE1006
        private readonly record struct RpcReq(string m, object?[] a);
#pragma warning restore IDE1006
    }
}
