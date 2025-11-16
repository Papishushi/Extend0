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
        private async Task HandleClientAsync(
            NamedPipeServerStream server,
            IReadOnlyDictionary<string, MethodInfo[]> methodsByName,
            CancellationToken token)
        {
            using var reader = new StreamReader(server, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: false);
            using var writer = new StreamWriter(server, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1024, leaveOpen: false)
            {
                AutoFlush = true
            };

            try
            {
                // Handshake
                await writer.WriteLineAsync($"HELLO {CrossProcessUtils.CurrentFingerprint}").ConfigureAwait(false);

                while (!token.IsCancellationRequested && server.IsConnected)
                {
                    string? line;
                    try
                    {
                        line = await reader.ReadLineAsync(token).ConfigureAwait(false);
                    }
                    catch (IOException)
                    {
                        break; // client dropped
                    }

                    if (line is null) break;

                    JsonDocument doc;
                    try
                    {
                        doc = JsonDocument.Parse(line);
                    }
                    catch
                    {
                        await WriteErr("Bad JSON").ConfigureAwait(false);
                        continue;
                    }

                    using (doc)
                    {
                        if (!doc.RootElement.TryGetProperty("m", out var mProp))
                        {
                            await WriteErr("Bad request: missing 'm'").ConfigureAwait(false);
                            continue;
                        }

                        var methodName = mProp.GetString();
                        if (string.IsNullOrEmpty(methodName) || !methodsByName.TryGetValue(methodName, out var candidates))
                        {
                            await WriteErr($"Unknown method '{methodName}'").ConfigureAwait(false);
                            continue;
                        }

                        if (!doc.RootElement.TryGetProperty("a", out var argsElem) || argsElem.ValueKind != JsonValueKind.Array)
                        {
                            await WriteErr("Bad request: 'a' must be array").ConfigureAwait(false);
                            continue;
                        }

                        var argVals = new object?[argsElem.GetArrayLength()];

                        // naive overload: by parameter count
                        var target = candidates.FirstOrDefault(c => c.GetParameters().Length == argVals.Length)
                                  ?? candidates.FirstOrDefault();
                        if (target is null)
                        {
                            await WriteErr("No method overload found").ConfigureAwait(false);
                            continue;
                        }

                        var pars = target.GetParameters();
                        for (int i = 0; i < argVals.Length; i++)
                        {
                            var pType = pars[i].ParameterType;
                            argVals[i] = JsonSerializer.Deserialize(argsElem[i].GetRawText(), pType);
                        }

                        try
                        {
                            var retType = target.ReturnType;
                            var callRes = target.Invoke(_impl, argVals);

                            if (retType == typeof(Task))
                            {
                                await (Task)callRes!;
                                await WriteOk(null).ConfigureAwait(false);
                            }
                            else if (retType.IsGenericType && retType.GetGenericTypeDefinition() == typeof(Task<>))
                            {
                                var t = (Task)callRes!;
                                await t.ConfigureAwait(false);
                                var resProp = t.GetType().GetProperty("Result")!;
                                await WriteOk(resProp.GetValue(t)).ConfigureAwait(false);
                            }
                            else
                            {
                                await WriteOk(callRes).ConfigureAwait(false);
                            }
                        }
                        catch (TargetInvocationException tex)
                        {
                            await WriteErr(tex.InnerException?.Message ?? tex.Message).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            await WriteErr(ex.Message).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // shutting down
            }
            finally
            {
                try { server.Dispose(); } catch { /* ignore */ }
            }

            async Task WriteOk(object? r)
            {
                var payload = JsonSerializer.Serialize(new { ok = true, r });
                await writer.WriteLineAsync(payload).ConfigureAwait(false);
            }

            async Task WriteErr(string e)
            {
                var payload = JsonSerializer.Serialize(new { ok = false, e });
                await writer.WriteLineAsync(payload).ConfigureAwait(false);
            }
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
