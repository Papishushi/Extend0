using System.IO.Pipes;
using System.Text;
using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Client-side transport that sends line-delimited JSON (NDJSON) requests over a
    /// <see cref="NamedPipeClientStream"/> and returns the raw JSON replies.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Protocol:
    /// </para>
    /// <list type="number">
    ///   <item>
    ///     <description>Connects to the named pipe and reads a single greeting line in the form <c>"HELLO &lt;fingerprint&gt;"</c>.</description>
    ///   </item>
    ///   <item>
    ///     <description>Sends requests as a single JSON line: <c>{"m":"&lt;method&gt;","a":[&lt;args&gt;]}</c>.</description>
    ///   </item>
    ///   <item>
    ///     <description>Receives responses as a single JSON line: success <c>{"ok":true,"r":&lt;result&gt;}</c> or error <c>{"ok":false,"e":"&lt;message&gt;"}</c>.</description>
    ///   </item>
    /// </list>
    /// <para>
    /// Thread-safety: this transport does not guarantee concurrent read/write safety. Callers should
    /// serialize access (the provided RPC proxy does this with a <c>SemaphoreSlim</c>).
    /// </para>
    /// <para>
    /// The returned <see cref="JsonDocument"/> from <see cref="CallAsync(string, object?[], System.Type[], System.Type, System.Threading.CancellationToken)"/>
    /// must be disposed by the caller.
    /// </para>
    /// </remarks>
    internal sealed class NamedPipeClientTransport : IClientTransport
    {
        private readonly NamedPipeClientStream _pipe;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;

        /// <summary>
        /// Initializes a new transport and connects to the specified named pipe on the given server.
        /// Performs a simple handshake by reading the server greeting line.
        /// </summary>
        /// <param name="serverName">
        /// The target server/machine name. Use <c>"."</c> for the local machine.
        /// On non-Windows platforms this value is ignored and the connection is always local.
        /// </param>
        /// <param name="pipeName">The OS pipe name to connect to.</param>
        /// <param name="timeoutMs">Connection timeout in milliseconds.</param>
        /// <exception cref="TimeoutException">The connection attempt timed out.</exception>
        /// <exception cref="IOException">The server handshake was invalid or the pipe I/O failed.</exception>
        /// <exception cref="ObjectDisposedException">The transport was disposed during initialization.</exception>
        /// <remarks>
        /// <para><b>Windows:</b> Remote server names are supported (e.g., <c>"MYSERVER"</c>) when permissions allow.</para>
        /// <para><b>Linux/macOS:</b> Remote server names are not supported; named pipes are local only and <paramref name="serverName"/> is ignored.</para>
        /// </remarks>
        public NamedPipeClientTransport(string serverName, string pipeName, int timeoutMs)
        {
            _pipe = new NamedPipeClientStream(serverName, pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
            _pipe.Connect(timeoutMs);

            _reader = new StreamReader(_pipe, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1024, leaveOpen: true);
            _writer = new StreamWriter(_pipe, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1024, leaveOpen: true)
            {
                AutoFlush = true
            };

            // Handshake: read server fingerprint
            var serverHello = _reader.ReadLine();
            if (serverHello is null || !serverHello.StartsWith("HELLO ", StringComparison.Ordinal))
                throw new IOException("Invalid server handshake.");
        }

        /// <summary>
        /// Sends a method invocation to the remote endpoint and returns the raw JSON response.
        /// </summary>
        /// <param name="method">Logical method name to invoke.</param>
        /// <param name="args">Positional arguments for the call (may be empty).</param>
        /// <param name="paramTypes">CLR parameter types corresponding to <paramref name="args"/> (may be ignored by the transport).</param>
        /// <param name="declaredReturnType">Declared return type (e.g., <c>typeof(void)</c>, result type for <c>Task&lt;T&gt;</c>, or a sync type).</param>
        /// <param name="ct">Cancellation token applied to the overall operation (best-effort; underlying stream APIs may not support it fully).</param>
        /// <returns>A <see cref="JsonDocument"/> representing the response envelope. The caller must dispose it.</returns>
        /// <exception cref="IOException">The server closed the connection or returned invalid data.</exception>
        /// <remarks>
        /// Request is written as one JSON line; response is read as one JSON line.
        /// The payload shape is enforced by the server implementation.
        /// </remarks>
        public async Task<JsonDocument> CallAsync(string method, object?[] args, Type[] paramTypes, Type declaredReturnType, CancellationToken ct)
        {
            // Send: {"m":"Method","a":[...]}
            var req = new RpcReq(method, args);
            try
            {
                await _writer.WriteLineAsync(JsonSerializer.Serialize(req)).ConfigureAwait(false);
            }
            catch
            {
                return JsonDocument.Parse("{\"ok\": false, \"e\": \"Server closed.\"}");
            }

            // Receive one line
            var line = await _reader.ReadLineAsync(ct).ConfigureAwait(false);
            return line is null ? throw new IOException("Server closed.") : JsonDocument.Parse(line);
        }

        /// <summary>
        /// Disposes the underlying stream writer, reader, and pipe.
        /// </summary>
        public void Dispose()
        {
            try { _writer.Dispose(); } catch { }
            try { _reader.Dispose(); } catch { }
            try { _pipe.Dispose(); } catch { }
        }

        private readonly record struct RpcReq(string m, object?[] a);
    }
}