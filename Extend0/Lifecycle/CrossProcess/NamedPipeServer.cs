using System.IO.Pipes;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Buffers;

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
        /// Accepts a client and processes NDJSON requests until the client disconnects or cancellation is requested.
        /// Writes a <c>HELLO &lt;fingerprint&gt;</c> greeting on connect.
        /// </summary>
        /// <remarks>This is the server’s background loop; it completes on disposal or cancellation.</remarks>
        private async Task AcceptLoopAsync()
        {
            var token = _cts.Token;

            try
            {
                while (!token.IsCancellationRequested)
                {
                    using var server = new NamedPipeServerStream(
                        _pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

                    await server.WaitForConnectionAsync(token).ConfigureAwait(false);

                    using var reader = new StreamReader(server, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
                    using var writer = new StreamWriter(server, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), leaveOpen: true)
                    { AutoFlush = true };

                    // Handshake: send host fingerprint
                    await writer.WriteLineAsync($"HELLO {CrossProcessUtils.CurrentFingerprint}").ConfigureAwait(false);

                    try
                    {
                        var iface = _impl.GetType().GetInterfaces().FirstOrDefault() ?? _impl.GetType();
                        var methodsByName = iface.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                                                 .GroupBy(m => m.Name)
                                                 .ToDictionary(g => g.Key, g => g.ToArray());

                        while (!token.IsCancellationRequested && server.IsConnected)
                        {
                            var line = await reader.ReadLineAsync().ConfigureAwait(false);
                            if (line is null) break;

                            using var doc = JsonDocument.Parse(line);
                            if (!doc.RootElement.TryGetProperty("m", out var mProp))
                            { await WriteErr("Bad request"); continue; }

                            var method = mProp.GetString();
                            if (string.IsNullOrEmpty(method) || !methodsByName.TryGetValue(method, out var candidates))
                            { await WriteErr($"Unknown method '{method}'"); continue; }

                            var argsElem = doc.RootElement.GetProperty("a");
                            var argVals = new object?[argsElem.GetArrayLength()];

                            // Simple overload resolution by parameter count
                            MethodInfo? target = candidates.FirstOrDefault(c => c.GetParameters().Length == argVals.Length)
                                              ?? candidates.FirstOrDefault();
                            if (target is null) { await WriteErr("No method overload found"); continue; }

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
                                    await WriteOk(null);
                                }
                                else if (retType.IsGenericType && retType.GetGenericTypeDefinition() == typeof(Task<>))
                                {
                                    var t = (Task)callRes!;
                                    await t;
                                    var resProp = t.GetType().GetProperty("Result")!;
                                    await WriteOk(resProp.GetValue(t));
                                }
                                else
                                {
                                    await WriteOk(callRes);
                                }
                            }
                            catch (TargetInvocationException tex)
                            {
                                await WriteErr(tex.InnerException?.Message ?? tex.Message);
                            }
                            catch (Exception ex)
                            {
                                await WriteErr(ex.Message);
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
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested) { /* shutdown */ }
                    catch (IOException) when (token.IsCancellationRequested) { /* shutdown */ }
                    catch
                    {
                        // Client dropped / transient error: keep accepting.
                    }
                }
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested) { /* graceful */ }
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
