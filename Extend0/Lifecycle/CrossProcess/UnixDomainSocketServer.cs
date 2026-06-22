using Microsoft.Extensions.Logging;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// JSON-RPC NDJSON host over Unix domain sockets.
    /// </summary>
    internal sealed class UnixDomainSocketServer : ICrossProcessServerHost
    {
        private readonly UnixDomainSocketEndpointName _endpoint;
        private readonly CrossProcessProtocolDescriptor _protocol;
        private readonly CrossProcessAuthenticationOptions _authentication;
        private readonly object _impl;
        private readonly ILogger<UnixDomainSocketServer> _logger;
        private readonly CancellationTokenSource _cts;
        private readonly Socket _listener;
        private readonly Task _loopTask;
        private bool _disposed;

        public UnixDomainSocketServer(
            string endpointName,
            object impl,
            ILogger<UnixDomainSocketServer> logger,
            CancellationToken ct,
            CrossProcessProtocolDescriptor? protocol = null,
            CrossProcessAuthenticationOptions? authentication = null)
        {
            _endpoint = UnixDomainSocketEndpointName.Parse(endpointName);
            _protocol = protocol ?? UnixDomainSocketTransportProtocol.Descriptor;
            _authentication = authentication ?? CrossProcessAuthenticationOptions.None;
            _impl = impl ?? throw new ArgumentNullException(nameof(impl));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);

            try
            {
                var directory = System.IO.Path.GetDirectoryName(_endpoint.Path);
                if (!string.IsNullOrWhiteSpace(directory))
                    Directory.CreateDirectory(directory);

                _listener.Bind(new UnixDomainSocketEndPoint(_endpoint.Path));
                _listener.Listen(backlog: 32);
                _loopTask = AcceptLoopAsync();
            }
            catch
            {
                _listener.Dispose();
                _cts.Dispose();
                throw;
            }
        }

        private async Task AcceptLoopAsync()
        {
            var token = _cts.Token;
            var methodsByName = BuildDispatchTable(_impl);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    Socket client;
                    try
                    {
                        client = await _listener.AcceptAsync(token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (ObjectDisposedException) when (token.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (SocketException) when (token.IsCancellationRequested)
                    {
                        break;
                    }

                    HandleClientAsync(client, methodsByName, token)
                        .Forget(_logger,
                            onExceptionMessage: "UnixDomainSocketServer: client handler crashed",
                            onExceptionAction: ex => _logger.LogCritical(ex, "Unix domain socket RPC handler fatal"),
                            measureDuration: true);
                }
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
        }

        private async Task HandleClientAsync(Socket client, IReadOnlyDictionary<string, MethodInfo[]> methodsByName, CancellationToken token)
        {
            NetworkStream? stream = null;
            StreamReader? reader = null;
            StreamWriter? writer = null;

            try
            {
                stream = new NetworkStream(client, ownsSocket: true);
                reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
                writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1024, leaveOpen: true)
                {
                    AutoFlush = true
                };

                var helloLine = CrossProcessHandshake.BuildHelloLine(_protocol, _authentication);
                await writer.WriteLineAsync(helloLine).ConfigureAwait(false);
                if (!await AuthenticateClientAsync(reader, writer, helloLine, token).ConfigureAwait(false))
                    return;

                await ProcessClientLoopAsync(reader, writer, methodsByName, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (IOException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            finally
            {
                try { writer?.Dispose(); } catch (IOException) { } catch (ObjectDisposedException) { }
                try { reader?.Dispose(); } catch (ObjectDisposedException) { }
                try { stream?.Dispose(); } catch (ObjectDisposedException) { }
                try { client.Dispose(); } catch (ObjectDisposedException) { }
            }
        }

        private async Task ProcessClientLoopAsync(
            StreamReader reader,
            StreamWriter writer,
            IReadOnlyDictionary<string, MethodInfo[]> methodsByName,
            CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var line = await ReadRequestLineAsync(reader, token).ConfigureAwait(false);
                if (line is null) break;
                await ProcessRequestLineAsync(line, methodsByName, writer).ConfigureAwait(false);
            }
        }

        private async Task<bool> AuthenticateClientAsync(
            StreamReader reader,
            StreamWriter writer,
            string helloLine,
            CancellationToken token)
        {
            if (!_authentication.RequiresHandshakeAuthentication)
                return true;

            if (!_authentication.RequiresClientProof)
            {
                _logger.LogWarning("Rejected cross-process client authentication for Unix domain socket endpoint {Endpoint}: authentication mode {AuthenticationMode} is not supported by this transport.", _endpoint.Path, _authentication.Mode);
                await writer.WriteLineAsync(CrossProcessHandshake.BuildAuthenticationErrorLine()).ConfigureAwait(false);
                return false;
            }

            var authenticationLine = await ReadRequestLineAsync(reader, token).ConfigureAwait(false);
            if (!CrossProcessHandshake.TryValidateClientAuthenticationLine(authenticationLine, helloLine, _authentication, out var error))
            {
                _logger.LogWarning("Rejected cross-process client authentication for Unix domain socket endpoint {Endpoint}: {Reason}", _endpoint.Path, error);
                await writer.WriteLineAsync(CrossProcessHandshake.BuildAuthenticationErrorLine()).ConfigureAwait(false);
                return false;
            }

            await writer.WriteLineAsync(CrossProcessHandshake.BuildAuthenticationOkLine()).ConfigureAwait(false);
            return true;
        }

        private static IReadOnlyDictionary<string, MethodInfo[]> BuildDispatchTable(object impl)
        {
            var ifaceSet = impl.GetType().GetInterfaces()
                .Where(i => typeof(ICrossProcessService).IsAssignableFrom(i))
                .ToHashSet();

            ifaceSet.Add(typeof(ICrossProcessService));
            if (ifaceSet.Count == 0)
                ifaceSet.Add(impl.GetType());

            return ifaceSet
                .Append(impl.GetType())
                .Distinct()
                .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance))
                .GroupBy(m => m.Name)
                .ToDictionary(g => g.Key, g => g.ToArray());
        }

        private static async Task<string?> ReadRequestLineAsync(StreamReader reader, CancellationToken token)
        {
            try
            {
                return await reader.ReadLineAsync(token).ConfigureAwait(false);
            }
            catch (IOException)
            {
                return null;
            }
        }

        private async Task ProcessRequestLineAsync(string line, IReadOnlyDictionary<string, MethodInfo[]> methodsByName, StreamWriter writer)
        {
            JsonDocument doc;
            try
            {
                doc = JsonDocument.Parse(line);
            }
            catch
            {
                await WriteErr("Bad JSON", 0xBADC0FF, writer).ConfigureAwait(false);
                return;
            }

            using (doc)
            {
                var root = doc.RootElement;
                if (!TryResolveMethod(root, methodsByName, out var target, out var argsElem, out var error))
                {
                    await WriteErr(error, 0xBADE110, writer).ConfigureAwait(false);
                    return;
                }

                await InvokeTargetAsync(target!, argsElem, writer).ConfigureAwait(false);
            }
        }

        private static bool TryResolveMethod(
            JsonElement root,
            IReadOnlyDictionary<string, MethodInfo[]> methodsByName,
            out MethodInfo? target,
            out JsonElement argsElem,
            out string error)
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

        private async Task InvokeTargetAsync(MethodInfo target, JsonElement argsElem, StreamWriter writer)
        {
            var pars = target.GetParameters();
            var argVals = new object?[pars.Length];

            try
            {
                await InvokeTargetAsyncCore(target, argsElem, writer, pars, argVals).ConfigureAwait(false);
            }
            catch (TargetInvocationException tex)
            {
                await WriteErr(tex.InnerException?.Message ?? tex.Message, tex.InnerException?.HResult ?? tex.HResult, writer).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await WriteErr(ex.Message, ex.HResult, writer).ConfigureAwait(false);
            }
        }

        private async Task InvokeTargetAsyncCore(
            MethodInfo target,
            JsonElement argsElem,
            StreamWriter writer,
            ParameterInfo[] pars,
            object?[] argVals)
        {
            for (int i = 0; i < argVals.Length; i++)
            {
                var pType = pars[i].ParameterType;
                argVals[i] = JsonSerializer.Deserialize(argsElem[i].GetRawText(), pType);
            }

            var retType = target.ReturnType;
            object? rawRes = target.Invoke(_impl, argVals);

            if (retType == typeof(void))
            {
                await WriteOk(null, writer).ConfigureAwait(false);
                return;
            }

            if (retType == typeof(Task))
            {
                await InvokeTargetTask(writer, (Task)rawRes!).ConfigureAwait(false);
                return;
            }

            if (retType.IsGenericType && retType.GetGenericTypeDefinition() == typeof(Task<>))
            {
                await InvokeTargetGenericTask(writer, (Task)rawRes!).ConfigureAwait(false);
                return;
            }

            await WriteOk(rawRes, writer).ConfigureAwait(false);
        }

        private static async Task InvokeTargetGenericTask(StreamWriter writer, Task callRes)
        {
            await callRes.ConfigureAwait(false);
            var resProp = callRes.GetType().GetProperty("Result")!;
            await WriteOk(resProp.GetValue(callRes), writer).ConfigureAwait(false);
        }

        private static async Task InvokeTargetTask(StreamWriter writer, Task callRes)
        {
            await callRes.ConfigureAwait(false);
            await WriteOk(null, writer).ConfigureAwait(false);
        }

        private static async Task WriteErr(string e, int hr, StreamWriter writer)
        {
            var payload = JsonSerializer.Serialize(new { ok = false, e, hr });
            await writer.WriteLineAsync(payload).ConfigureAwait(false);
        }

        private static async Task WriteOk(object? r, StreamWriter writer)
        {
            var payload = JsonSerializer.Serialize(new { ok = true, r });
            await writer.WriteLineAsync(payload).ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            try { _listener.Dispose(); } catch { }

            try
            {
                _loopTask.Wait();
            }
            catch (AggregateException ae) when (ae.InnerExceptions.All(e => e is TaskCanceledException or OperationCanceledException or ObjectDisposedException or SocketException))
            {
            }
            finally
            {
                _cts.Dispose();
                TryDeleteEndpointPath();
            }

            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            try { _listener.Dispose(); } catch { }

            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (SocketException) { }
            finally
            {
                _cts.Dispose();
                TryDeleteEndpointPath();
            }

            GC.SuppressFinalize(this);
        }

        public Task StopAsync()
        {
            _cts.Cancel();
            try { _listener.Dispose(); } catch { }
            return _loopTask;
        }

        private void TryDeleteEndpointPath()
        {
            try
            {
                if (File.Exists(_endpoint.Path))
                    File.Delete(_endpoint.Path);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _logger.LogDebug(ex, "Could not delete Unix domain socket endpoint path {EndpointPath}.", _endpoint.Path);
            }
        }
    }
}
