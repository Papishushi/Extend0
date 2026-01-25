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
    /// This transport is intended to be used by a higher-level RPC proxy that
    /// serializes method calls into a minimal JSON envelope and sends them over
    /// a local or remote named pipe.
    /// </para>
    /// <para>
    /// Protocol:
    /// </para>
    /// <list type="number">
    ///   <item>
    ///     <description>
    ///       Connects to the named pipe and reads a single greeting line in the form
    ///       <c>"HELLO &lt;fingerprint&gt;"</c>. If the greeting is missing or malformed,
    ///       the constructor throws an <see cref="IOException"/>.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       Sends requests as a single JSON line: <c>{"m":"&lt;method&gt;","a":[&lt;args&gt;]}</c>.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       Receives responses as a single JSON line: success
    ///       <c>{"ok":true,"r":&lt;result&gt;}</c> or error
    ///       <c>{"ok":false,"e":"&lt;message&gt;"}</c>.
    ///     </description>
    ///   </item>
    /// </list>
    /// <para>
    /// Thread-safety: this transport does <b>not</b> guarantee concurrent read/write safety.
    /// Callers must serialize access (the provided RPC proxy does this with a
    /// <see cref="System.Threading.SemaphoreSlim"/>).
    /// </para>
    /// <para>
    /// The <see cref="JsonDocument"/> returned by
    /// <see cref="CallAsync(string, object?[], System.Type[], System.Type, System.Threading.CancellationToken)"/>
    /// <b>must</b> be disposed by the caller.
    /// </para>
    /// </remarks>
    internal sealed class NamedPipeClientTransport : IClientTransport
    {
        /// <summary>
        /// Underlying named pipe stream used to send and receive raw bytes.
        /// </summary>
        private readonly NamedPipeClientStream _pipe;

        /// <summary>
        /// Text reader bound to the pipe, used to read NDJSON response lines.
        /// </summary>
        private readonly StreamReader _reader;

        /// <summary>
        /// Text writer bound to the pipe, used to write NDJSON request lines.
        /// </summary>
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
        /// <exception cref="IOException">
        /// The server handshake was invalid (no <c>HELLO</c> line) or the pipe I/O failed.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The transport was disposed during initialization.</exception>
        /// <remarks>
        /// <para><b>Windows:</b> Remote server names are supported (e.g., <c>"MYSERVER"</c>) when permissions allow.</para>
        /// <para><b>Linux/macOS:</b> Remote server names are not supported; named pipes are local only and
        /// <paramref name="serverName"/> is ignored.</para>
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
        /// Sends a line-delimited JSON-RPC request over the underlying named-pipe transport and returns
        /// the raw JSON response envelope as a <see cref="JsonDocument"/>.
        /// </summary>
        /// <param name="method">Logical method name to invoke remotely.</param>
        /// <param name="args">Positional arguments for the call (may be empty).</param>
        /// <param name="paramTypes">
        /// CLR parameter types corresponding to <paramref name="args"/>. This transport does not use them directly,
        /// but higher layers may pass them for symmetry and diagnostics.
        /// </param>
        /// <param name="declaredReturnType">
        /// Declared return type (for example <c>typeof(void)</c>, a synchronous CLR type, or the result type of a
        /// <c>Task&lt;T&gt;</c>). This transport does not use it directly.
        /// </param>
        /// <param name="ct">
        /// Cancellation token applied to the write/read operations. Cancellation is propagated as
        /// <see cref="OperationCanceledException"/>.
        /// </param>
        /// <returns>
        /// A <see cref="JsonDocument"/> representing the response envelope. The caller must dispose it.
        /// On transport-level failures (pipe closed/disposed), a synthetic error envelope is returned with
        /// <c>hr = 426</c> to signal an upgrade/reconnect condition to higher layers.
        /// </returns>
        /// <exception cref="OperationCanceledException">
        /// Thrown when <paramref name="ct"/> is canceled while sending or receiving.
        /// </exception>
        /// <remarks>
        /// <para>
        /// Protocol: the request is written as a single JSON line <c>{"m":"Method","a":[...]}</c> and the response is read
        /// as a single JSON line containing the standard envelope <c>{"ok":true,"r":...}</c> or <c>{"ok":false,"e":"...","hr":...}</c>.
        /// </para>
        /// <para>
        /// This method treats transport shutdown as a soft error and returns a synthetic envelope instead of throwing,
        /// allowing the RPC proxy to unify “server closed / reconnect needed” handling (e.g., mapping <c>hr = 426</c>
        /// to an upgrade path).
        /// </para>
        /// <para>
        /// If the response line is malformed JSON, a synthetic error envelope is returned with a dedicated <c>hr</c>
        /// (<c>422</c>) to indicate corrupted/partial transport data.
        /// </para>
        /// </remarks>
        public async Task<JsonDocument> CallAsync(string method, object?[] args, Type[] paramTypes, Type declaredReturnType, CancellationToken ct)
        {           
            // Send: {"m":"Method","a":[...]}
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
                // Receive one line
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

        /// <summary>
        /// Releases all managed resources associated with this transport.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Disposes the underlying <see cref="StreamWriter"/>, <see cref="StreamReader"/>
        /// and <see cref="NamedPipeClientStream"/> in that order. Exceptions thrown during
        /// disposal of any individual component are swallowed to avoid masking earlier failures.
        /// </para>
        /// <para>
        /// Once disposed, the transport instance must not be used again.
        /// </para>
        /// </remarks>
        public void Dispose()
        {
            try { _writer.Dispose(); } catch { /*swallow*/ }
            try { _reader.Dispose(); } catch { /*swallow*/ }
            try { _pipe.Dispose(); } catch { /*swallow*/ }
        }

        /// <summary>
        /// Lightweight request envelope used for serialization of RPC calls.
        /// </summary>
        /// <param name="m">Logical method name.</param>
        /// <param name="a">Array of positional arguments.</param>
        /// <remarks>
        /// This record struct is serialized to JSON with property names <c>"m"</c> and <c>"a"</c>,
        /// producing documents of the form <c>{"m":"MethodName","a":[...]}</c>.
        /// </remarks>
#pragma warning disable IDE1006 // Estilos de nombres
        private readonly record struct RpcReq(string m, object?[] a);
#pragma warning restore IDE1006 // Estilos de nombres
    }
}
