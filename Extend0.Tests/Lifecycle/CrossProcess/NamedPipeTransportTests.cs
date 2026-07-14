using System.IO.Pipes;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Extend0.Lifecycle.CrossProcess;
using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class NamedPipeTransportTests
{
    [Fact]
    public async Task NamedPipeServer_AndClientTransport_RoundTrip_EndToEnd()
    {
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-e2e-{Guid.NewGuid():N}");
        var implementation = new NamedPipeTestService(endpointName);
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            implementation,
            CancellationToken.None);

        using var transport = CreateBuiltInNamedPipeTransport(endpointName);
        var proxy = RpcDispatchProxy<INamedPipeTestService>.Create(transport, CancellationToken.None);

        proxy.Notify("hello");
        Assert.Equal("hello", implementation.LastNotification);
        Assert.Equal(7, proxy.Add(3, 4));
        Assert.Equal("echo:hi", proxy.Echo("hi"));
        await proxy.TouchAsync("task");
        Assert.Equal("task", implementation.LastNotification);
        Assert.Equal(9, await proxy.AddAsync(4, 5));
        Assert.Equal("echo:async", await proxy.EchoAsync("async"));

        var heartbeat = await proxy.PingAsync();
        Assert.False(string.IsNullOrWhiteSpace(heartbeat.Fingerprint));

        var info = await proxy.GetServiceInfoAsync();
        Assert.Equal(typeof(INamedPipeTestService).FullName, info.ContractName);
        Assert.Equal(endpointName, info.PipeName);
        Assert.Equal(endpointName, info.EndpointName);
        Assert.Equal(TransportKind.NamedPipe, info.TransportKind);

        var error = await Assert.ThrowsAsync<RemoteInvocationException>(() => proxy.ThrowAsync("boom"));
        Assert.Equal(418, error.HResult);
        Assert.Contains("boom", error.Message);
    }

    [Fact]
    public async Task NamedPipeTransport_WithSharedSecretHmac_AllowsAuthenticatedClient()
    {
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-auth-ok-{Guid.NewGuid():N}");
        var authentication = CrossProcessAuthenticationOptions.SharedSecretHmac("correct-horse-battery-staple");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None,
            authentication);

        using var transport = CreateBuiltInNamedPipeTransport(endpointName, authentication);
        var proxy = RpcDispatchProxy<INamedPipeTestService>.Create(transport, CancellationToken.None);

        Assert.Equal("echo:secure", await proxy.EchoAsync("secure"));
    }

    [Fact]
    public async Task NamedPipeTransport_WithSharedSecretHmac_RejectsUnauthenticatedClient()
    {
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-auth-missing-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None,
            CrossProcessAuthenticationOptions.SharedSecretHmac("server-secret"));

        var error = await Assert.ThrowsAsync<IOException>(() => Task.Run(() =>
        {
            using var transport = CreateBuiltInNamedPipeTransport(endpointName);
        }));

        Assert.Contains("authentication", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NamedPipeTransport_WithSharedSecretHmac_RejectsWrongSecret()
    {
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-auth-wrong-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None,
            CrossProcessAuthenticationOptions.SharedSecretHmac("server-secret"));

        var error = await Assert.ThrowsAsync<IOException>(() => Task.Run(() =>
        {
            using var transport = CreateBuiltInNamedPipeTransport(
                endpointName,
                CrossProcessAuthenticationOptions.SharedSecretHmac("client-secret"));
        }));

        Assert.Contains("Authentication failed", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NamedPipeTransport_WithSignedChallenge_AllowsAuthenticatedClient()
    {
        using var signer = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        using var verifier = ECDsa.Create(signer.ExportParameters(includePrivateParameters: false));
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-signed-ok-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None,
            CrossProcessAuthenticationOptions.SignedChallengeServer("client-1", verifier));

        using var transport = CreateBuiltInNamedPipeTransport(
            endpointName,
            CrossProcessAuthenticationOptions.SignedChallengeClient("client-1", signer));
        var proxy = RpcDispatchProxy<INamedPipeTestService>.Create(transport, CancellationToken.None);

        Assert.Equal("echo:signed", await proxy.EchoAsync("signed"));
    }

    [Fact]
    public async Task NamedPipeTransport_WithSignedChallenge_RejectsWrongSignature()
    {
        using var trustedSigner = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        using var verifier = ECDsa.Create(trustedSigner.ExportParameters(includePrivateParameters: false));
        using var untrustedSigner = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-signed-wrong-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None,
            CrossProcessAuthenticationOptions.SignedChallengeServer("client-1", verifier));

        var error = await Assert.ThrowsAsync<IOException>(() => Task.Run(() =>
        {
            using var transport = CreateBuiltInNamedPipeTransport(
                endpointName,
                CrossProcessAuthenticationOptions.SignedChallengeClient("client-1", untrustedSigner));
        }));

        Assert.Contains("Authentication failed", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NamedPipeTransport_WithSignedChallenge_RejectsUnauthenticatedClient()
    {
        using var signer = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        using var verifier = ECDsa.Create(signer.ExportParameters(includePrivateParameters: false));
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-signed-missing-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None,
            CrossProcessAuthenticationOptions.SignedChallengeServer("client-1", verifier));

        var error = await Assert.ThrowsAsync<IOException>(() => Task.Run(() =>
        {
            using var transport = CreateBuiltInNamedPipeTransport(endpointName);
        }));

        Assert.Contains("signed-challenge", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NamedPipeServer_StopAsync_CompletesAcceptLoop()
    {
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-stop-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None);

        await LifecycleCrossProcessHarness.StopNamedPipeServerAsync(server).WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task NamedPipeServer_ReturnsStructuredErrors_ForBadJsonAndBadRequest()
    {
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"named-pipe-errors-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new NamedPipeTestService(endpointName),
            CancellationToken.None);

        await using var client = new NamedPipeClientStream(".", endpointName, PipeDirection.InOut, PipeOptions.Asynchronous);
        await client.ConnectAsync(2000);

        using var reader = new StreamReader(client, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        using var writer = new StreamWriter(client, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };

        var hello = await reader.ReadLineAsync();
        Assert.NotNull(hello);
        Assert.True(LifecycleCrossProcessHarness.TryValidateHelloLine(
            hello!,
            LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor,
            out _));

        await writer.WriteLineAsync("not-json");
        using var badJson = JsonDocument.Parse(await reader.ReadLineAsync() ?? throw new InvalidOperationException("Expected bad-json response."));
        Assert.False(badJson.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("Bad JSON", badJson.RootElement.GetProperty("e").GetString());

        await writer.WriteLineAsync("""{"m":"Echo"}""");
        using var badRequest = JsonDocument.Parse(await reader.ReadLineAsync() ?? throw new InvalidOperationException("Expected bad-request response."));
        Assert.False(badRequest.RootElement.GetProperty("ok").GetBoolean());
        Assert.Contains("'a' must be array", badRequest.RootElement.GetProperty("e").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void NamedPipeServer_MethodResolution_CoversGuardAndOverloadPaths()
    {
        var methods = typeof(ResolveFixture)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .GroupBy(static method => method.Name)
            .ToDictionary(static group => group.Key, static group => group.ToArray(), StringComparer.Ordinal);

        methods["NoCandidates"] = [];

        var missingMethod = LifecycleCrossProcessHarness.ResolveNamedPipeMethod("""{"a":[]}""", methods);
        var unknownMethod = LifecycleCrossProcessHarness.ResolveNamedPipeMethod("""{"m":"Missing","a":[]}""", methods);
        var nonArrayArgs = LifecycleCrossProcessHarness.ResolveNamedPipeMethod("""{"m":"Echo","a":{}}""", methods);
        var exactOverload = LifecycleCrossProcessHarness.ResolveNamedPipeMethod("""{"m":"Overload","a":[1,2]}""", methods);
        var fallbackOverload = LifecycleCrossProcessHarness.ResolveNamedPipeMethod("""{"m":"Overload","a":[1,2,3]}""", methods);
        var noCandidates = LifecycleCrossProcessHarness.ResolveNamedPipeMethod("""{"m":"NoCandidates","a":[]}""", methods);

        Assert.False(missingMethod.Success);
        Assert.Contains("missing 'm'", missingMethod.Error, StringComparison.Ordinal);
        Assert.False(unknownMethod.Success);
        Assert.Contains("Unknown method", unknownMethod.Error, StringComparison.Ordinal);
        Assert.False(nonArrayArgs.Success);
        Assert.Contains("'a' must be array", nonArrayArgs.Error, StringComparison.Ordinal);
        Assert.True(exactOverload.Success);
        Assert.Equal(2, exactOverload.TargetParameterCount);
        Assert.Equal(2, exactOverload.ArgsLength);
        Assert.True(fallbackOverload.Success);
        Assert.Contains(fallbackOverload.TargetParameterCount, new[] { 0, 1, 2 });
        Assert.Equal(3, fallbackOverload.ArgsLength);
        Assert.False(noCandidates.Success);
        Assert.Contains("No method overload", noCandidates.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NamedPipeClientTransport_RejectsInvalidHandshake()
    {
        var pipeName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"Extend0.Tests.BadHandshake.{Guid.NewGuid():N}");
        var serverTask = RunRawServerAsync(
            pipeName,
            static async server =>
            {
                using var writer = new StreamWriter(server, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                await writer.WriteLineAsync("not-a-valid-hello");
            });

        var error = await Assert.ThrowsAsync<IOException>(() => Task.Run(() =>
        {
            using var transport = CreateBuiltInNamedPipeTransport(pipeName);
        }));

        Assert.Contains("Invalid server handshake", error.Message, StringComparison.Ordinal);
        await serverTask;
    }

    [Fact]
    public async Task NamedPipeClientTransport_RejectsMissingGreeting()
    {
        var pipeName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"Extend0.Tests.MissingHello.{Guid.NewGuid():N}");
        var serverTask = RunRawServerAsync(
            pipeName,
            static server =>
            {
                server.Dispose();
                return Task.CompletedTask;
            });

        var error = await Assert.ThrowsAsync<IOException>(() => Task.Run(() =>
        {
            using var transport = CreateBuiltInNamedPipeTransport(pipeName);
        }));

        Assert.Contains("missing greeting", error.Message, StringComparison.OrdinalIgnoreCase);
        await serverTask;
    }

    [Fact]
    public async Task NamedPipeClientTransport_ReturnsSoftErrors_ForMalformedJsonAndClosedTransport()
    {
        var malformedPipe = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"Extend0.Tests.MalformedJson.{Guid.NewGuid():N}");
        var malformedServerTask = RunRawServerAsync(
            malformedPipe,
            async server =>
            {
                using var reader = new StreamReader(server, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
                using var writer = new StreamWriter(server, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                await writer.WriteLineAsync(LifecycleCrossProcessHarness.BuildHelloLine(LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor));
                _ = await reader.ReadLineAsync();
                await writer.WriteLineAsync("not-json");
            });

        using (var malformedTransport = CreateBuiltInNamedPipeTransport(malformedPipe))
        using (var malformedResponse = await malformedTransport.CallAsync("Echo", ["x"], [typeof(string)], typeof(string), CancellationToken.None))
        {
            Assert.False(malformedResponse.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal(422, malformedResponse.RootElement.GetProperty("hr").GetInt32());
        }

        await malformedServerTask;

        var closedPipe = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"Extend0.Tests.ClosedTransport.{Guid.NewGuid():N}");
        var closedServerTask = RunRawServerAsync(
            closedPipe,
            async server =>
            {
                using var writer = new StreamWriter(server, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                await writer.WriteLineAsync(LifecycleCrossProcessHarness.BuildHelloLine(LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor));
            });

        using (var closedTransport = CreateBuiltInNamedPipeTransport(closedPipe))
        using (var closedResponse = await closedTransport.CallAsync("Echo", ["x"], [typeof(string)], typeof(string), CancellationToken.None))
        {
            Assert.False(closedResponse.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal(426, closedResponse.RootElement.GetProperty("hr").GetInt32());
        }

        await closedServerTask;

        var eofPipe = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"Extend0.Tests.EofTransport.{Guid.NewGuid():N}");
        var eofServerTask = RunRawServerAsync(
            eofPipe,
            async server =>
            {
                using var reader = new StreamReader(server, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
                using var writer = new StreamWriter(server, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                await writer.WriteLineAsync(LifecycleCrossProcessHarness.BuildHelloLine(LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor));
                _ = await reader.ReadLineAsync();
            });

        using (var eofTransport = CreateBuiltInNamedPipeTransport(eofPipe))
        using (var eofResponse = await eofTransport.CallAsync("Echo", ["x"], [typeof(string)], typeof(string), CancellationToken.None))
        {
            Assert.False(eofResponse.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal(426, eofResponse.RootElement.GetProperty("hr").GetInt32());
        }

        await eofServerTask;
    }

    [Fact]
    public async Task NamedPipeClientTransport_ReturnsSoftError_WhenCalledAfterDispose()
    {
        var pipeName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"Extend0.Tests.DisposedTransport.{Guid.NewGuid():N}");
        var serverTask = RunRawServerAsync(
            pipeName,
            async server =>
            {
                StreamReader? reader = null;
                StreamWriter? writer = null;

                try
                {
                    reader = new StreamReader(server, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
                    writer = new StreamWriter(server, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                    await writer.WriteLineAsync(LifecycleCrossProcessHarness.BuildHelloLine(LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor));
                    _ = await reader.ReadLineAsync();
                }
                catch (IOException)
                {
                    // The client intentionally disposes immediately after the handshake.
                }
                finally
                {
                    try { writer?.Dispose(); } catch (IOException) { } catch (ObjectDisposedException) { }
                    try { reader?.Dispose(); } catch (ObjectDisposedException) { }
                }
            });

        using var transport = CreateBuiltInNamedPipeTransport(pipeName);
        transport.Dispose();

        using var response = await transport.CallAsync("Echo", ["x"], [typeof(string)], typeof(string), CancellationToken.None);

        Assert.False(response.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal(426, response.RootElement.GetProperty("hr").GetInt32());

        await serverTask;
    }

    [Fact]
    public async Task NamedPipeClientTransport_PropagatesReadCancellation()
    {
        var pipeName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"Extend0.Tests.CancellableRead.{Guid.NewGuid():N}");
        var serverTask = RunRawServerAsync(
            pipeName,
            async server =>
            {
                using var reader = new StreamReader(server, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
                using var writer = new StreamWriter(server, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                await writer.WriteLineAsync(LifecycleCrossProcessHarness.BuildHelloLine(LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor));
                _ = await reader.ReadLineAsync();
                await Task.Delay(2000);
            });

        using var transport = CreateBuiltInNamedPipeTransport(pipeName);
        using var cts = new CancellationTokenSource(100);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            transport.CallAsync("Echo", ["x"], [typeof(string)], typeof(string), cts.Token));

        await serverTask;
    }

    private static IClientTransport CreateBuiltInNamedPipeTransport(
        string endpointName,
        CrossProcessAuthenticationOptions? authentication = null) =>
        LifecycleCrossProcessHarness.CreateBuiltInClientTransport(
            new ClientTransportFactoryContext(
                TransportKind.NamedPipe,
                LifecycleCrossProcessHarness.NamedPipeProtocolDescriptor,
                endpointName,
                ".",
                2000,
                authentication));

    private static Task RunRawServerAsync(string pipeName, Func<NamedPipeServerStream, Task> handler)
    {
        var server = new NamedPipeServerStream(
            pipeName,
            PipeDirection.InOut,
            1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);

        return Task.Run(async () =>
        {
            await using (server)
            {
                await server.WaitForConnectionAsync();
                await handler(server);
            }
        });
    }

    private interface INamedPipeTestService : ICrossProcessService
    {
        void Notify(string value);
        int Add(int a, int b);
        string Echo(string value);
        Task TouchAsync(string value);
        Task<int> AddAsync(int a, int b);
        Task<string> EchoAsync(string value);
        Task ThrowAsync(string message);
    }

    private static class ResolveFixture
    {
        public static string Echo(string value) => value;
        public static int Overload() => 0;
        public static int Overload(int value) => value;
        public static int Overload(int left, int right) => left + right;
    }

    private sealed class NamedPipeTestService(string endpointName)
        : CrossProcessServiceBase<INamedPipeTestService>, INamedPipeTestService
    {
        protected override string? PipeName => endpointName;
        protected override string? EndpointName => endpointName;
        protected override string? EndpointServerName => ".";

        public string? LastNotification { get; private set; }

        public void Notify(string value)
        {
            LastNotification = value;
        }

        public int Add(int a, int b) => a + b;

        public string Echo(string value) => $"echo:{value}";

        public Task TouchAsync(string value)
        {
            LastNotification = value;
            return Task.CompletedTask;
        }

        public Task<int> AddAsync(int a, int b) => Task.FromResult(a + b);

        public Task<string> EchoAsync(string value) => Task.FromResult($"echo:{value}");

        public Task ThrowAsync(string message) => throw new NamedPipeTestException(message);
    }

    private sealed class NamedPipeTestException : Exception
    {
        public NamedPipeTestException(string message) : base(message)
        {
            HResult = 418;
        }
    }
}
