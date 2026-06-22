using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Client-side JSON-RPC NDJSON transport over a Unix domain socket.
    /// </summary>
    internal sealed class UnixDomainSocketClientTransport : IClientTransport
    {
        private readonly Socket _socket;
        private readonly NetworkStream _stream;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;

        public TransportKind Kind => TransportKind.UnixDomainSocket;

        public UnixDomainSocketClientTransport(
            string endpointName,
            int timeoutMs,
            CrossProcessProtocolDescriptor? protocol = null,
            CrossProcessAuthenticationOptions? authentication = null)
        {
            var endpointNameValue = UnixDomainSocketEndpointName.Parse(endpointName);
            var endpoint = new UnixDomainSocketEndPoint(endpointNameValue.Path);
            var expectedProtocol = protocol ?? UnixDomainSocketTransportProtocol.Descriptor;

            var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            NetworkStream? stream = null;
            StreamReader? reader = null;
            StreamWriter? writer = null;

            try
            {
                var connectTask = socket.ConnectAsync(endpoint);
                if (!connectTask.Wait(timeoutMs))
                    throw new TimeoutException($"Timed out connecting to Unix domain socket endpoint {endpointNameValue.Path} after {timeoutMs}ms.");

                connectTask.GetAwaiter().GetResult();
                stream = new NetworkStream(socket, ownsSocket: false);
                reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1024, leaveOpen: true);
                writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1024, leaveOpen: true)
                {
                    AutoFlush = true
                };

                var serverHello = reader.ReadLine();
                if (serverHello is null)
                    throw new IOException("Invalid server handshake: missing greeting.");

                if (!CrossProcessHandshake.TryValidateHelloLine(serverHello, expectedProtocol, out var hello, out var handshakeError))
                    throw new IOException($"Invalid server handshake: {handshakeError}");

                var authLine = CrossProcessHandshake.CreateClientAuthenticationLine(hello!, authentication);
                if (authLine is not null)
                {
                    writer.WriteLine(authLine);
                    var authenticationAck = reader.ReadLine();
                    if (!CrossProcessHandshake.TryValidateAuthenticationOkLine(authenticationAck, out var authenticationError))
                        throw new IOException($"Authentication failed: {authenticationError}");
                }

                _socket = socket;
                _stream = stream;
                _reader = reader;
                _writer = writer;
            }
            catch
            {
                try { writer?.Dispose(); } catch { }
                try { reader?.Dispose(); } catch { }
                try { stream?.Dispose(); } catch { }
                try { socket.Dispose(); } catch { }
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
            try { _socket.Dispose(); } catch { }
        }

#pragma warning disable IDE1006
        private readonly record struct RpcReq(string m, object?[] a);
#pragma warning restore IDE1006
    }
}
