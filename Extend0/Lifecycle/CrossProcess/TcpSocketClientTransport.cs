using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Client-side JSON-RPC NDJSON transport over a TCP socket.
    /// </summary>
    internal sealed class TcpSocketClientTransport : IClientTransport
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;

        public TransportKind Kind => TransportKind.TcpSocket;

        public TcpSocketClientTransport(
            string serverName,
            string endpointName,
            int timeoutMs,
            CrossProcessProtocolDescriptor? protocol = null)
        {
            var endpoint = TcpSocketEndpoint.Parse(endpointName, serverName);
            var expectedProtocol = protocol ?? TcpSocketTransportProtocol.Descriptor;

            _client = new TcpClient();
            try
            {
                var connectTask = _client.ConnectAsync(endpoint.Host, endpoint.Port);
                if (!connectTask.Wait(timeoutMs))
                    throw new TimeoutException($"Timed out connecting to TCP endpoint {endpoint.Host}:{endpoint.Port} after {timeoutMs}ms.");

                connectTask.GetAwaiter().GetResult();
                _stream = _client.GetStream();
                _reader = new StreamReader(_stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1024, leaveOpen: true);
                _writer = new StreamWriter(_stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1024, leaveOpen: true)
                {
                    AutoFlush = true
                };

                var serverHello = _reader.ReadLine();
                if (serverHello is null)
                    throw new IOException("Invalid server handshake: missing greeting.");

                if (!CrossProcessHandshake.TryValidateHelloLine(serverHello, expectedProtocol, out var handshakeError))
                    throw new IOException($"Invalid server handshake: {handshakeError}");
            }
            catch
            {
                _client.Dispose();
                throw;
            }
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
            catch (IOException)
            {
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Transport closed.\", \"hr\": 426}");
            }
            catch (ObjectDisposedException)
            {
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Transport closed.\", \"hr\": 426}");
            }
            catch (InvalidOperationException)
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
            catch (IOException)
            {
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Transport closed.\", \"hr\": 426}");
            }
            catch (ObjectDisposedException)
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
            try { _writer.Dispose(); } catch { }
            try { _reader.Dispose(); } catch { }
            try { _stream.Dispose(); } catch { }
            try { _client.Dispose(); } catch { }
        }

#pragma warning disable IDE1006
        private readonly record struct RpcReq(string m, object?[] a);
#pragma warning restore IDE1006
    }
}
