using System.Reflection;
using System.Text.Json;
using System.Buffers;
using System.Collections.Concurrent;

namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// DispatchProxy-based client-side RPC proxy for a cross-process service contract <typeparamref name="TService"/>.
/// </summary>
/// <typeparam name="TService">
/// The service interface contract. Must be an interface and implement <see cref="ICrossProcessService"/>.
/// </typeparam>
/// <remarks>
/// <para>
/// This proxy serializes method invocations over an <see cref="IClientTransport"/>. It caches per-method
/// metadata (parameters and return-kind) to minimize reflection overhead and uses a <see cref="SemaphoreSlim"/>
/// to serialize I/O, since most pipe/socket transports are not thread-safe for concurrent reads/writes.
/// </para>
/// <para>
/// Return types supported:
/// <list type="bullet">
/// <item><description><c>void</c> — one-way call, waits for server acknowledgment.</description></item>
/// <item><description><see cref="Task"/> — async one-way; completes when the server acknowledges.</description></item>
/// <item><description><see cref="Task{TResult}"/> — async call returning a JSON value deserialized to <c>TResult</c>.</description></item>
/// <item><description>Sync result — call returning a JSON value deserialized to the declared return type.</description></item>
/// </list>
/// </para>
/// <para>
/// The server is expected to return a JSON envelope: <c>{ "ok": true, "r": ... }</c> on success,
/// or <c>{ "ok": false, "e": "error message" }</c> on failure. Remote failures are surfaced as
/// <see cref="RemoteInvocationException"/>.
/// </para>
/// </remarks>
/// <seealso cref="IClientTransport"/>
/// <seealso cref="RemoteInvocationException"/>
internal class RpcDispatchProxy<TService> : DispatchProxy where TService : ICrossProcessService
{
    /// <summary>
    /// Cache of per-method metadata (name, parameter types, return kind).
    /// </summary>
    private static readonly ConcurrentDictionary<MethodInfo, MethodShape> _shapes = new();

    /// <summary>
    /// Cache of the closed generic <see cref="CallTaskTAsync{TRes}"/> method per result type.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, MethodInfo> _callTaskTCache = new();

    /// <summary>
    /// Serializes transport I/O; most transports are not safe for concurrent operations.
    /// </summary>
    private readonly SemaphoreSlim _ioLock = new(1, 1);

    private IClientTransport _transport = default!;
    private CancellationToken _ct;

    /// <summary>Return kind classification for fast-path dispatch.</summary>
    private enum RetKind { Void, Task, TaskOfT, SyncResult }

    /// <summary>
    /// Immutable shape for a method: name, parameter types, return kind and (if applicable) task result type.
    /// </summary>
    private sealed class MethodShape
    {
        /// <summary>Method name to send over the transport.</summary>
        public required string Name;

        /// <summary>Parameter CLR types, used for argument serialization hints.</summary>
        public required Type[] ParamTypes;

        /// <summary>Classified return kind.</summary>
        public required RetKind Kind;

        /// <summary>Result type when <see cref="Kind"/> is <see cref="RetKind.TaskOfT"/>.</summary>
        public Type? TaskResultType;
    }

    /// <summary>
    /// Creates a proxy instance that implements <typeparamref name="TService"/> and forwards calls
    /// through the provided <paramref name="transport"/>.
    /// </summary>
    /// <param name="transport">The client transport used to perform RPC calls.</param>
    /// <param name="ct">A cancellation token applied to transport operations.</param>
    /// <returns>An object implementing <typeparamref name="TService"/>.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <typeparamref name="TService"/> is not an interface.
    /// </exception>
    public static TService Create(IClientTransport transport, CancellationToken ct)
    {
        if (!typeof(TService).IsInterface)
            throw new InvalidOperationException($"{typeof(TService).Name} must be an interface.");

        var core = Create<TService, RpcDispatchProxy<TService>>();
        // cast legal: the runtime type implements TService and derives from RpcDispatchProxy<TService>
        var raw = (RpcDispatchProxy<TService>)(object)core!;
        raw.Init(transport, ct);
        return core!;
    }

    /// <summary>
    /// Initializes the proxy with the transport and cancellation token.
    /// </summary>
    /// <param name="transport">The transport to use.</param>
    /// <param name="ct">Cancellation token for calls.</param>
    /// <exception cref="ArgumentNullException"><paramref name="transport"/> is <c>null</c>.</exception>
    private void Init(IClientTransport transport, CancellationToken ct)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _ct = ct;
    }

    /// <summary>
    /// Intercepts a call to a method on <typeparamref name="TService"/> and performs an RPC.
    /// </summary>
    /// <param name="targetMethod">The method being invoked.</param>
    /// <param name="args">The argument values (may be <c>null</c> or empty for no args).</param>
    /// <returns>The method return value, if any.</returns>
    /// <exception cref="RemoteInvocationException">The remote endpoint returned an error envelope.</exception>
    /// <exception cref="OperationCanceledException">The call was canceled via the proxy's token.</exception>
    /// <remarks>
    /// This method is called by the <see cref="DispatchProxy"/> infrastructure and is not intended to be invoked directly.
    /// </remarks>
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod, nameof(targetMethod));
        args ??= [];

        var shape = _shapes.GetOrAdd(targetMethod, BuildShape);

        switch (shape.Kind)
        {
            case RetKind.Void:
                {
                    // synchronous, no result
                    _ioLock.Wait(_ct);
                    try
                    {
                        using var doc = _transport.CallAsync(shape.Name, args, shape.ParamTypes, typeof(void), _ct)
                                                  .GetAwaiter().GetResult();
                        ThrowIfError(doc);
                        return null;
                    }
                    finally { _ioLock.Release(); }
                }

            case RetKind.Task:
                {
                    // Task-returning (no result value)
                    return CallTaskAsync(shape, args);
                }

            case RetKind.TaskOfT:
                {
                    // Task<T>-returning
                    var mi = _callTaskTCache.GetOrAdd(
                        shape.TaskResultType!,
                        resType => typeof(RpcDispatchProxy<TService>)
                            .GetMethod(nameof(CallTaskTAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
                            .MakeGenericMethod(resType));

                    return mi.Invoke(this, new object[] { shape, args })!;
                }

            default: // SyncResult
                {
                    _ioLock.Wait(_ct);
                    try
                    {
                        using var doc = _transport.CallAsync(shape.Name, args, shape.ParamTypes, targetMethod.ReturnType, _ct)
                                                  .GetAwaiter().GetResult();
                        var root = ThrowIfError(doc);
                        return JsonSerializer.Deserialize(root.GetProperty("r").GetRawText(), targetMethod.ReturnType)!;
                    }
                    finally { _ioLock.Release(); }
                }
        }
    }

    /// <summary>
    /// Builds and caches the method shape (parameters and return kind) for a contract method.
    /// </summary>
    /// <param name="m">The method to analyze.</param>
    /// <returns>A <see cref="MethodShape"/> representing the method signature.</returns>
    private static MethodShape BuildShape(MethodInfo m)
    {
        var ret = m.ReturnType;
        var pars = m.GetParameters();
        var s = new MethodShape
        {
            Name = m.Name,
            ParamTypes = pars.Select(p => p.ParameterType).ToArray(),
            Kind = RetKind.SyncResult
        };

        if (ret == typeof(void))
            s.Kind = RetKind.Void;
        else if (ret == typeof(Task))
            s.Kind = RetKind.Task;
        else if (ret.IsGenericType && ret.GetGenericTypeDefinition() == typeof(Task<>))
        {
            s.Kind = RetKind.TaskOfT;
            s.TaskResultType = ret.GetGenericArguments()[0];
        }
        else
            s.Kind = RetKind.SyncResult;

        return s;
    }

    /// <summary>
    /// Performs an async RPC for a method that returns <see cref="Task"/> (no result payload).
    /// </summary>
    /// <param name="shape">The cached method shape.</param>
    /// <param name="args">Argument values.</param>
    /// <returns>A task that completes when the remote call has been acknowledged.</returns>
    /// <exception cref="RemoteInvocationException">The remote endpoint returned an error envelope.</exception>
    private async Task CallTaskAsync(MethodShape shape, object?[] args)
    {
        await _ioLock.WaitAsync(_ct).ConfigureAwait(false);
        try
        {
            using var doc = await _transport.CallAsync(shape.Name, args, shape.ParamTypes, typeof(Task), _ct)
                                            .ConfigureAwait(false);
            ThrowIfError(doc);
        }
        finally { _ioLock.Release(); }
    }

    /// <summary>
    /// Performs an async RPC for a method that returns <see cref="Task{TRes}"/>.
    /// </summary>
    /// <typeparam name="TRes">The declared result type.</typeparam>
    /// <param name="shape">The cached method shape.</param>
    /// <param name="args">Argument values.</param>
    /// <returns>A task producing the deserialized result value.</returns>
    /// <exception cref="RemoteInvocationException">The remote endpoint returned an error envelope.</exception>
    private async Task<TRes> CallTaskTAsync<TRes>(MethodShape shape, object?[] args)
    {
        await _ioLock.WaitAsync(_ct).ConfigureAwait(false);
        try
        {
            using var doc = await _transport.CallAsync(shape.Name, args, shape.ParamTypes, typeof(TRes), _ct)
                                            .ConfigureAwait(false);
            var root = ThrowIfError(doc);
            return JsonSerializer.Deserialize<TRes>(root.GetProperty("r").GetRawText())!;
        }
        finally { _ioLock.Release(); }
    }

    /// <summary>
    /// Validates the standard JSON envelope and throws if it represents a remote error.
    /// </summary>
    /// <param name="doc">The JSON document returned by the transport.</param>
    /// <returns>The root element for further processing when the envelope is successful.</returns>
    /// <exception cref="RemoteInvocationException">
    /// Thrown when <c>ok</c> is <c>false</c>. The exception message contains the remote error string, if provided.
    /// </exception>
    private static JsonElement ThrowIfError(JsonDocument doc)
    {
        var root = doc.RootElement;
        if (!root.GetProperty("ok").GetBoolean())
            throw new RemoteInvocationException(root.GetProperty("e").GetString() ?? "Remote error");
        return root;
    }
}
