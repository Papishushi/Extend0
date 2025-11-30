using System.Buffers;
using System.IO.Pipes;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Lightweight JSON-RPC host over <see cref="NamedPipeServerStream"/> for a single concurrent client.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This server accepts a client, performs a simple text handshake, and then processes line-delimited JSON
    /// requests (NDJSON). Each request must be a single JSON object with:
    /// <c>{"m":"MethodName","a":[arg0,arg1,...]}</c>.
    /// The response is a single JSON line using the envelope:
    /// <c>{"ok":true,"r":&lt;result&gt;}</c> on success or <c>{"ok":false,"e":"error message"}</c> on failure.
    /// </para>
    /// <para>
    /// Method dispatch is performed via reflection against the first interface implemented by
    /// <c>impl</c> (or the concrete type if none). Overloads are resolved by name and parameter count.
    /// Return shapes supported:
    /// <list type="bullet">
    ///   <item><description><c>void</c> → <c>{"ok":true,"r":null}</c></description></item>
    ///   <item><description><see cref="Task"/> → awaited, then <c>{"ok":true,"r":null}</c></description></item>
    ///   <item><description><see cref="Task{T}"/> → awaited, then <c>{"ok":true,"r":T}</c></description></item>
    ///   <item><description>Any other type → <c>{"ok":true,"r":value}</c></description></item>
    /// </list>
    /// </para>
    /// <para><b>Handshake:</b> after connection, the server writes a single line:
    /// <c>HELLO &lt;fingerprint&gt;</c>, where <seealso cref="CrossProcessUtils.CurrentFingerprint"/> identifies the hosting binary.</para>
    /// <para><b>Concurrency:</b> this server is configured for one connection at a time
    /// (<c>maxNumberOfServerInstances = 1</c>). It will accept a new client after the current one disconnects.</para>
    /// <para><b>Lifetime:</b> call <see cref="Dispose"/> or <see cref="DisposeAsync"/> to cancel the accept loop and
    /// stop the server gracefully. <see cref="StopAsync"/> can be used to await shutdown explicitly.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// using var cts = new CancellationTokenSource();
    /// var server = new NamedPipeServer("CPS.MyService", impl: new MyService(), ct: cts.Token);
    /// // ... later ...
    /// await server.DisposeAsync();
    /// </code>
    /// </example>
    /// <seealso cref="NamedPipeClientTransport"/>
    /// <seealso cref="CrossProcessUtils"/>
    internal sealed class NamedPipeServer : IDisposable, IAsyncDisposable
    {
        private readonly string _pipeName;
        private readonly object _impl;
        private readonly CancellationTokenSource _cts;   // linked to external token
        private readonly Task _loopTask;
        private bool _disposed;

        /// <summary>
        /// Creates and starts a named-pipe JSON-RPC server bound to <paramref name="pipeName"/>.
        /// The server begins accepting clients immediately.
        /// </summary>
        /// <param name="pipeName">Operating system pipe name (e.g., value from <see cref="CrossProcessUtils.BuildPipeName(string, string?)"/>).</param>
        /// <param name="impl">Service implementation whose public instance methods are exposed.</param>
        /// <param name="ct">External cancellation token. When signaled, the server stops.</param>
        /// <exception cref="ArgumentNullException"><paramref name="pipeName"/> or <paramref name="impl"/> is <c>null</c>.</exception>
        public NamedPipeServer(string pipeName, object impl, CancellationToken ct)
        {
            _pipeName = pipeName ?? throw new ArgumentNullException(nameof(pipeName));
            _impl     = impl     ?? throw new ArgumentNullException(nameof(impl));

            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _loopTask = AcceptLoopAsync();
        }

        /// <summary>
        /// Accepts clients and dispatches each one to a dedicated handler,
        /// allowing multiple concurrent clients. Each handler processes NDJSON requests
        /// until the client disconnects or cancellation is requested.
        /// Writes a <c>HELLO &lt;fingerprint&gt;</c> greeting on connect.
        /// </summary>
        /// <remarks>This is the server’s background loop; it completes on disposal or cancellation.</remarks>
        private async Task AcceptLoopAsync()
        {
            var token = _cts.Token;

            // Build dispatch table once; it's read-only
            var ifaceSet = _impl.GetType().GetInterfaces()
                .Where(i => typeof(ICrossProcessService).IsAssignableFrom(i))
                .ToHashSet();

            ifaceSet.Add(typeof(ICrossProcessService));
            if (ifaceSet.Count == 0)
                ifaceSet.Add(_impl.GetType());

            // Merge all methods (public instance). Keep special names so property getters work.
            var methodsByName = ifaceSet
                .Append(_impl.GetType())
                .Distinct()
                .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance))
                .GroupBy(m => m.Name)
                .ToDictionary(g => g.Key, g => g.ToArray());

            try
            {
                while (!token.IsCancellationRequested)
                {
                    var server = new NamedPipeServerStream(
                        _pipeName,
                        PipeDirection.InOut,
                        NamedPipeServerStream.MaxAllowedServerInstances,
                        PipeTransmissionMode.Byte,
                        PipeOptions.Asynchronous);

                    try
                    {
                        await server.WaitForConnectionAsync(token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        server.Dispose();
                        break;
                    }

                    // Each client is processed by an observed fire and forget handler.
                    HandleClientAsync(server, methodsByName, token).Forget();
                }
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                // swallow
            }
        }

        /// <summary>
        /// Processes NDJSON requests for a single connected client until disconnect or cancellation.
        /// </summary>
        /// <param name="server">
        /// Connected <see cref="NamedPipeServerStream"/> representing the client connection.
        /// </param>
        /// <param name="methodsByName">
        /// Map of method name to available overloads used to resolve RPC targets.
        /// </param>
        /// <param name="token">
        /// Cancellation token used to stop the processing loop (typically when the host is shutting down).
        /// </param>
        private async Task HandleClientAsync(NamedPipeServerStream server, IReadOnlyDictionary<string, MethodInfo[]> methodsByName, CancellationToken token)
        {
            using var reader = new StreamReader(
                server,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                leaveOpen: false);

            using var writer = new StreamWriter(
                server,
                new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
                bufferSize: 1024,
                leaveOpen: false)
            {
                AutoFlush = true
            };

            try
            {
                await SendHandshakeAsync(writer).ConfigureAwait(false);
                await ProcessClientLoopAsync(server, reader, writer, methodsByName, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Graceful shutdown requested.
            }
            finally
            {
                try
                {
                    server.Dispose();
                }
                catch
                {
                    // Best-effort cleanup; ignore failures during teardown.
                }
            }
        }

        /// <summary>
        /// Sends the initial textual handshake line to the connected client.
        /// </summary>
        /// <param name="writer">
        /// The <see cref="StreamWriter"/> bound to the client connection.
        /// </param>
        /// <returns>
        /// A task that completes when the handshake line has been written.
        /// </returns>
        private static Task SendHandshakeAsync(StreamWriter writer) => writer.WriteLineAsync($"HELLO {CrossProcessUtils.CurrentFingerprint}");

        /// <summary>
        /// Main NDJSON processing loop for a single client connection.
        /// </summary>
        /// <param name="server">
        /// The underlying <see cref="NamedPipeServerStream"/> used to check connection state.
        /// </param>
        /// <param name="reader">
        /// Reader used to consume NDJSON request lines from the client.
        /// </param>
        /// <param name="writer">
        /// Writer used to send JSON responses back to the client.
        /// </param>
        /// <param name="methodsByName">
        /// Map of method name to available overloads for RPC resolution.
        /// </param>
        /// <param name="token">
        /// Cancellation token used to terminate the loop.
        /// </param>
        /// <returns>
        /// A task that completes when the client disconnects or cancellation is requested.
        /// </returns>
        private async Task ProcessClientLoopAsync(NamedPipeServerStream server, StreamReader reader, StreamWriter writer, IReadOnlyDictionary<string, MethodInfo[]> methodsByName, CancellationToken token)
        {
            while (!token.IsCancellationRequested && server.IsConnected)
            {
                var line = await ReadRequestLineAsync(reader, token).ConfigureAwait(false);
                if (line is null) break;
                await ProcessRequestLineAsync(line, methodsByName, writer).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Reads a single NDJSON request line from the client, handling IO drop semantics.
        /// </summary>
        /// <param name="reader">
        /// Reader bound to the client stream.
        /// </param>
        /// <param name="token">
        /// Cancellation token to abort the read.
        /// </param>
        /// <returns>
        /// The line read from the client, or <see langword="null"/> when the client disconnects
        /// or an <see cref="IOException"/> occurs.
        /// </returns>
        private static async Task<string?> ReadRequestLineAsync(StreamReader reader, CancellationToken token)
        {
            try
            {
                return await reader.ReadLineAsync(token).ConfigureAwait(false);
            }
            catch (IOException)
            {
                // Client dropped or stream broken.
                return null;
            }
        }

        /// <summary>
        /// Parses and dispatches a single NDJSON request line.
        /// </summary>
        /// <param name="line">Raw JSON line received from the client.</param>
        /// <param name="methodsByName">Lookup of method names to candidate overloads.</param>
        /// <param name="writer">Writer used to emit JSON responses.</param>
        /// <returns>
        /// A task that completes when the request has been processed and a response written.
        /// </returns>
        private async Task ProcessRequestLineAsync(string line, IReadOnlyDictionary<string, MethodInfo[]> methodsByName, StreamWriter writer)
        {
            JsonDocument doc;
            try
            {
                doc = JsonDocument.Parse(line);
            }
            catch
            {
                await WriteErr("Bad JSON", writer).ConfigureAwait(false);
                return;
            }

            using (doc)
            {
                var root = doc.RootElement;

                if (!TryResolveMethod(root, methodsByName, out var target, out var argsElem, out var error))
                {
                    await WriteErr(error, writer).ConfigureAwait(false);
                    return;
                }

                await InvokeTargetAsync(target!, argsElem, writer).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Resolves the target method and argument array element from a JSON request payload.
        /// </summary>
        /// <param name="root">
        /// Root JSON element for the request document.
        /// </param>
        /// <param name="methodsByName">
        /// Map of RPC method names to reflectable overloads.
        /// </param>
        /// <param name="target">
        /// When this method returns <see langword="true"/>, contains the selected <see cref="MethodInfo"/>.
        /// </param>
        /// <param name="argsElem">
        /// When this method returns <see langword="true"/>, contains the JSON array element with arguments.
        /// </param>
        /// <param name="error">
        /// When this method returns <see langword="false"/>, contains a human-readable error message.
        /// </param>
        /// <returns>
        /// <see langword="true"/> when a suitable method and argument array were found;
        /// otherwise <see langword="false"/>.
        /// </returns>
        private static bool TryResolveMethod(JsonElement root, IReadOnlyDictionary<string, MethodInfo[]> methodsByName, out MethodInfo? target, out JsonElement argsElem, out string error)
        {
            target = null;
            argsElem = default;

            if (!root.TryGetProperty("m", out var mProp))
            {
                error = "Bad request: missing 'm'";
                return false;
            }

            var methodName = mProp.GetString();
            if (string.IsNullOrEmpty(methodName) || !methodsByName.TryGetValue(methodName, out var candidates))
            {
                error = $"Unknown method '{methodName}'";
                return false;
            }

            if (!root.TryGetProperty("a", out argsElem) || argsElem.ValueKind != JsonValueKind.Array)
            {
                error = "Bad request: 'a' must be array";
                return false;
            }

            var argCount = argsElem.GetArrayLength();

            // Naive overload resolution by parameter count, then fallback to first candidate.
            target = candidates.FirstOrDefault(c => c.GetParameters().Length == argCount)
                  ?? candidates.FirstOrDefault();

            if (target is null)
            {
                error = "No method overload found";
                return false;
            }

            error = string.Empty;
            return true;
        }

        /// <summary>
        /// Invokes a reflected RPC target and writes the corresponding JSON response.
        /// </summary>
        /// <param name="target">The reflected method to invoke.</param>
        /// <param name="argsElem">JSON array with the arguments for the invocation.</param>
        /// <param name="writer">Writer used to emit JSON responses.</param>
        /// <returns>
        /// A task that completes when the method has been invoked and an OK or error response has been written.
        /// </returns>
        private async Task InvokeTargetAsync(MethodInfo target, JsonElement argsElem, StreamWriter writer)
        {
            var pars = target.GetParameters();
            var argVals = new object?[pars.Length];

            try
            {
                // Materialize arguments from JSON into CLR types.
                for (int i = 0; i < argVals.Length; i++)
                {
                    var pType = pars[i].ParameterType;
                    argVals[i] = JsonSerializer.Deserialize(argsElem[i].GetRawText(), pType);
                }

                var retType = target.ReturnType;
                var callRes = target.Invoke(_impl, argVals);

                if (retType == typeof(Task))
                {
                    await (Task)callRes!;
                    await WriteOk(null, writer).ConfigureAwait(false);
                }
                else if (retType.IsGenericType && retType.GetGenericTypeDefinition() == typeof(Task<>))
                {
                    var t = (Task)callRes!;
                    await t.ConfigureAwait(false);
                    var resProp = t.GetType().GetProperty("Result")!;
                    await WriteOk(resProp.GetValue(t), writer).ConfigureAwait(false);
                }
                else
                {
                    await WriteOk(callRes, writer).ConfigureAwait(false);
                }
            }
            catch (TargetInvocationException tex)
            {
                await WriteErr(tex.InnerException?.Message ?? tex.Message, writer).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await WriteErr(ex.Message, writer).ConfigureAwait(false);
            }
        }


        private static async Task WriteErr(string e, StreamWriter writer)
        {
            var payload = JsonSerializer.Serialize(new { ok = false, e });
            await writer.WriteLineAsync(payload).ConfigureAwait(false);
        }

        private static async Task WriteOk(object? r, StreamWriter writer)
        {
            var payload = JsonSerializer.Serialize(new { ok = true, r });
            await writer.WriteLineAsync(payload).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronously stops the server and releases resources.
        /// </summary>
        /// <remarks>
        /// Cancels the accept loop and waits for it to complete. Exceptions solely due to cancellation are swallowed.
        /// </remarks>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            try
            {
                _loopTask.Wait(); // block until loop exits
            }
            catch (AggregateException ae) when (ae.InnerExceptions.All(e => e is TaskCanceledException || e is OperationCanceledException))
            {
                // expected on cancellation
            }
            finally
            {
                _cts.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Asynchronously stops the server and releases resources.
        /// </summary>
        /// <remarks>Cancels the accept loop and awaits its completion.</remarks>
        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { /* expected */ }
            finally
            {
                _cts.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Requests shutdown and returns a task that completes when the accept loop finishes.
        /// </summary>
        public Task StopAsync()
        {
            _cts.Cancel();
            return _loopTask;
        }
    }
}
