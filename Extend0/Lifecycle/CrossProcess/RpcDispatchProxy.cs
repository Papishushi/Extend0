using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// DispatchProxy-based client-side RPC proxy for a cross-process service contract <typeparamref name="TService"/>.
/// </summary>
/// <typeparam name="TService">
/// The service interface contract. Must be an interface and implement <see cref="ICrossProcessService"/>.
/// </typeparam>
/// <remarks>
/// <para>
/// This proxy serializes method invocations over an <see cref="IClientTransport"/> using a simple JSON-RPC
/// envelope. It caches per-method metadata (name, parameter types, and return-kind) to minimize reflection
/// overhead, and uses a <see cref="SemaphoreSlim"/> to serialize transport I/O, since most pipe/socket
/// transports are not safe for concurrent read/write operations.
/// </para>
///
/// <para>
/// Supported return shapes:
/// <list type="bullet">
/// <item><description><c>void</c> — one-way call; completes when the server acknowledges.</description></item>
/// <item><description><see cref="Task"/> — asynchronous one-way call; completes on server acknowledgment.</description></item>
/// <item><description><see cref="Task{TResult}"/> — asynchronous call returning a JSON value deserialized to <c>TResult</c>.</description></item>
/// <item><description>
/// Synchronous result type — call returning a JSON value deserialized to the declared return type.
/// </description></item>
/// </list>
/// </para>
///
/// <para>
/// The server is expected to return a JSON envelope:
/// <c>{ "ok": true, "r": &lt;result&gt; }</c> on success, or
/// <c>{ "ok": false, "e": "error message" }</c> on failure.
/// Remote failures are surfaced as <see cref="RemoteInvocationException"/>.
/// </para>
///
/// <para>
/// Upgrade handling:
/// When the remote endpoint signals an upgrade condition (for example by returning an envelope that maps
/// to a <see cref="RemoteInvocationException"/> with <c>HResult == 426</c>, such as <c>"Server closed."</c>),
/// and <see cref="UpgradeHandler"/> is configured, the proxy will:
/// <list type="number">
/// <item><description>Invoke <see cref="UpgradeHandler"/> exactly once for that call.</description></item>
/// <item><description>
/// If the handler returns <c>true</c>, transparently perform a single retry of the original RPC using the
/// (potentially) recovered connection or singleton.
/// </description></item>
/// <item><description>
/// If the handler returns <c>false</c> or the retry also fails, the corresponding
/// <see cref="RemoteInvocationException"/> is propagated to the caller.
/// </description></item>
/// </list>
/// This mechanism avoids infinite retry loops and centralizes cross-process upgrade/recovery logic outside
/// of application call sites.
/// </para>
///
/// <para>
/// This type is intended to be used indirectly via <see cref="DispatchProxy.Create{T,TProxy}"/> and higher-level
/// factory helpers; it is not expected to be instantiated directly by user code.
/// </para>
/// </remarks>
/// <seealso cref="IClientTransport"/>
/// <seealso cref="RemoteInvocationException"/>
public class RpcDispatchProxy<TService> : DispatchProxy where TService : class, ICrossProcessService
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
    /// Intercepts calls to the <typeparamref name="TService"/> contract and executes them as RPC
    /// invocations over the underlying <see cref="IClientTransport"/>.
    /// </summary>
    /// <param name="targetMethod">
    /// The contract method being invoked. Must be a method defined on <typeparamref name="TService"/>.
    /// </param>
    /// <param name="args">
    /// The argument values for the invocation. May be <c>null</c> or empty when the method has no parameters.
    /// </param>
    /// <returns>
    /// The result of the remote call:
    /// <list type="bullet">
    /// <item><description><c>null</c> for <c>void</c>-returning methods.</description></item>
    /// <item><description>
    /// A <see cref="Task"/> instance for <c>Task</c>-returning methods, representing the asynchronous RPC.
    /// </description></item>
    /// <item><description>
    /// A <see cref="Task{TResult}"/> instance for <c>Task&lt;T&gt;</c>-returning methods, producing the
    /// deserialized remote result.
    /// </description></item>
    /// <item><description>
    /// A deserialized value of the declared return type for synchronous result methods.
    /// </description></item>
    /// </list>
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="targetMethod"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="RemoteInvocationException">
    /// Thrown when the remote endpoint returns an error envelope or an upgrade cannot be successfully
    /// handled. If the remote endpoint signals an upgrade condition (for example, <c>HResult == 426</c>)
    /// and <see cref="UpgradeHandler"/> is configured, the proxy will invoke the handler once and
    /// transparently retry the call a single time; if that retry also fails, the exception is propagated.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown when the call is canceled via the proxy's cancellation token.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This method is invoked by the <see cref="DispatchProxy"/> infrastructure and is not intended
    /// to be called directly from user code.
    /// </para>
    /// <para>
    /// Method metadata is cached to reduce reflection overhead. Calls are serialized via an internal
    /// <see cref="SemaphoreSlim"/> to ensure safe use of transports that do not support concurrent I/O.
    /// </para>
    /// <para>
    /// When <see cref="UpgradeHandler"/> is set and an upgrade-eligible <see cref="RemoteInvocationException"/>
    /// is observed (e.g. server closed / version change), the handler is responsible for performing any
    /// necessary recovery (such as recreating a singleton or reconnecting). If it returns <c>true</c>,
    /// the proxy issues a single retry of the failed RPC; otherwise, the original exception is rethrown.
    /// </para>
    /// </remarks>
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod, nameof(targetMethod));
        args ??= [];

        var shape = _shapes.GetOrAdd(targetMethod, BuildShape);

        switch (shape.Kind)
        {
            // Synchronous no result
            case RetKind.Void:
                {
                    return InvokeVoid(args, shape);
                }

            // Synchronous with result
            case RetKind.SyncResult:
                {
                    return InvokeSyncResult(targetMethod, args, shape);
                }

            // Task-returning (no result value)
            case RetKind.Task:
                {
                    return InvokeTaskAsync(shape, args);
                }

            // Task<T>-returning
            case RetKind.TaskOfT:
                {
                    return InvokeTaskOfT(args, shape);
                }

            default:
                throw new InvalidOperationException("Unsupported return kind.");
        }
    }

    /// <summary>
    /// Invokes a <c>Task&lt;T&gt;</c>-returning contract method using the cached
    /// generic <see cref="CallTaskTAsync{TResult}(MethodShape, object?[])"/> helper.
    /// </summary>
    /// <param name="args">
    /// The argument values for the RPC invocation. May be <c>null</c> or empty when the method has no parameters.
    /// </param>
    /// <param name="shape">
    /// Cached metadata describing the target method (name, parameter types, return kind, task result type, etc.).
    /// </param>
    /// <returns>
    /// A boxed <see cref="Task{TResult}"/> instance representing the asynchronous RPC.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Uses <c>_callTaskTCache</c> to cache the closed generic <see cref="MethodInfo"/> for
    /// <see cref="CallTaskTAsync{TResult}(MethodShape, object?[])"/> per result type in order
    /// to avoid repeated <see cref="MethodInfo.MakeGenericMethod(Type[])"/> calls.
    /// </para>
    /// </remarks>
    private object InvokeTaskOfT(object?[] args, MethodShape shape)
    {
        var mi = _callTaskTCache.GetOrAdd(
            shape.TaskResultType!,
            resType => typeof(RpcDispatchProxy<TService>)
                .GetMethod(nameof(CallTaskTAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(resType));

        return mi.Invoke(this, [shape, args])!;
    }

    /// <summary>
    /// Invokes a synchronous contract method that returns a value, performing a remote call,
    /// deserializing the result, and handling a single upgrade retry if necessary.
    /// </summary>
    /// <param name="targetMethod">
    /// The original contract method being invoked. Used to obtain the declared return type.
    /// </param>
    /// <param name="args">
    /// The argument values for the invocation. May be <c>null</c> or empty when the method has no parameters.
    /// </param>
    /// <param name="shape">
    /// Cached metadata describing the method (RPC name, parameter types, return kind, etc.).
    /// </param>
    /// <returns>
    /// A deserialized instance of the method's declared return type.
    /// </returns>
    /// <exception cref="RemoteInvocationException">
    /// Thrown when the remote endpoint returns an error envelope and the call is not recoverable
    /// via <see cref="UpgradeHandler"/> or the single retry also fails.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown when the underlying cancellation token is signaled while waiting for the RPC.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The method acquires the internal I/O lock to serialize access to the underlying transport,
    /// performs the RPC, validates the response via <see cref="ThrowIfError(JsonDocument)"/>,
    /// and deserializes the <c>"r"</c> field into the declared return type.
    /// </para>
    /// <para>
    /// When an upgrade-eligible <see cref="RemoteInvocationException"/> is observed (for example,
    /// <c>HResult == 426</c>) and <see cref="UpgradeHandler"/> is configured, the handler is invoked
    /// once; if it returns <c>true</c>, the call is retried a single time.
    /// </para>
    /// </remarks>
    private object InvokeSyncResult(MethodInfo targetMethod, object?[] args, MethodShape shape)
    {
        var returnType = targetMethod.ReturnType;

        return RunWithUpgradeRetry(() =>
        {
            _ioLock.Wait(_ct);
            try
            {
                using var doc = _transport
                    .CallAsync(shape.Name, args, shape.ParamTypes, returnType, _ct)
                    .GetAwaiter().GetResult();

                var root = ThrowIfError(doc);
                return JsonSerializer.Deserialize(root.GetProperty("r").GetRawText(), returnType)!;
            }
            finally
            {
                _ioLock.Release();
            }
        });
    }

    /// <summary>
    /// Invokes a synchronous <c>void</c>-returning contract method as an RPC call,
    /// handling error envelopes and a single upgrade retry when applicable.
    /// </summary>
    /// <param name="args">
    /// The argument values for the invocation. May be <c>null</c> or empty when the method has no parameters.
    /// </param>
    /// <param name="shape">
    /// Cached metadata describing the method (RPC name, parameter types, return kind, etc.).
    /// </param>
    /// <returns>
    /// Always <see langword="null"/> for <c>void</c>-returning methods.
    /// </returns>
    /// <exception cref="RemoteInvocationException">
    /// Thrown when the remote endpoint returns an error envelope and the call is not recoverable
    /// via <see cref="UpgradeHandler"/> or the single retry also fails.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown when the underlying cancellation token is signaled while waiting for the RPC.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The method serializes access to the underlying transport via the internal I/O lock,
    /// performs the RPC with a <c>void</c> return type, and validates the response using
    /// <see cref="ThrowIfError(JsonDocument)"/>. No deserialization of a result payload is performed.
    /// </para>
    /// <para>
    /// When an upgrade-eligible <see cref="RemoteInvocationException"/> is observed (for example,
    /// <c>HResult == 426</c>) and <see cref="UpgradeHandler"/> is configured, the handler is invoked
    /// once; if it returns <c>true</c>, the call is retried a single time before the exception is propagated.
    /// </para>
    /// </remarks>
    private object? InvokeVoid(object?[] args, MethodShape shape)
    {
        return RunWithUpgradeRetry<object?>(() =>
        {
            _ioLock.Wait(_ct);
            try
            {
                using var doc = _transport
                    .CallAsync(shape.Name, args, shape.ParamTypes, typeof(void), _ct)
                    .GetAwaiter().GetResult();

                ThrowIfError(doc);
                return null;
            }
            finally
            {
                _ioLock.Release();
            }
        });
    }

    /// <summary>
    /// Optional callback used to handle remote upgrade scenarios (e.g. when the server closes or
    /// signals that a newer instance must take over).
    /// </summary>
    /// <remarks>
    /// The delegate receives the <see cref="RemoteInvocationException"/> that triggered the upgrade,
    /// performs any required recovery (recreate singleton, promote to server, reconnect, etc.),
    /// and returns <c>true</c> if the call can be safely retried, or <c>false</c> to propagate
    /// the original error.
    /// </remarks>
    public static Func<RemoteInvocationException, ValueTask<bool>>? UpgradeHandler { get; set; }

    /// <summary>
    /// Attempts to handle an upgrade scenario asynchronously using <see cref="UpgradeHandler"/>.
    /// </summary>
    /// <param name="ex">
    /// The <see cref="RemoteInvocationException"/> that indicates an upgrade condition
    /// (typically with <c>HResult == 426</c> or equivalent).
    /// </param>
    /// <returns>
    /// <c>true</c> if the upgrade handler completed successfully and the original RPC call
    /// may be retried; otherwise, <c>false</c>.
    /// </returns>
    private static async ValueTask<bool> TryHandleUpgradeAsync(RemoteInvocationException ex)
    {
        var handler = UpgradeHandler;
        if (handler is null)
            return false;

        try
        {
            return await handler(ex).ConfigureAwait(false);
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Synchronous wrapper for <see cref="TryHandleUpgradeAsync"/> intended for use in
    /// synchronous call paths.
    /// </summary>
    /// <param name="ex">
    /// The <see cref="RemoteInvocationException"/> that indicates an upgrade condition.
    /// </param>
    /// <returns>
    /// <c>true</c> if the upgrade handler completed successfully and the original RPC call
    /// may be retried; otherwise, <c>false</c>.
    /// </returns>
    /// <remarks>
    /// This method blocks on the asynchronous handler and should be used only from
    /// synchronous contexts (e.g. non-async <see cref="DispatchProxy.Invoke"/> branches).
    /// </remarks>
    private static bool TryHandleUpgrade(RemoteInvocationException ex)
    {
        var handler = UpgradeHandler;
        if (handler is null)
            return false;

        try
        {
            var vt = handler(ex);

            // Fast-path si ya está completado sin fallo.
            if (vt.IsCompletedSuccessfully)
                return vt.Result;

            // Si no, lo materializamos como Task de forma segura.
            return vt.AsTask().GetAwaiter().GetResult();
        }
        catch
        {
            return false;
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
            ParamTypes = [.. pars.Select(p => p.ParameterType)],
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
    private async Task InvokeTaskAsync(MethodShape shape, object?[] args)
    {
        _ = await RunWithUpgradeRetryAsync<object?>(async ct =>
        {
            await _ioLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                using var doc = await _transport
                    .CallAsync(shape.Name, args, shape.ParamTypes, typeof(Task), ct)
                    .ConfigureAwait(false);

                ThrowIfError(doc);
                return default;
            }
            finally
            {
                _ioLock.Release();
            }
        });
        return;
    }

    /// <summary>
    /// Performs an async RPC for a method that returns <see cref="Task{TRes}"/>.
    /// </summary>
    /// <typeparam name="TRes">The declared result type.</typeparam>
    /// <param name="shape">The cached method shape.</param>
    /// <param name="args">Argument values.</param>
    /// <returns>A task producing the deserialized result value.</returns>
    /// <exception cref="RemoteInvocationException">The remote endpoint returned an error envelope.</exception>
    private Task<TRes?> CallTaskTAsync<TRes>(MethodShape shape, object?[] args)
    {
        return RunWithUpgradeRetryAsync<TRes?>(async ct =>
        {
            await _ioLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                using var doc = await _transport
                    .CallAsync(shape.Name, args, shape.ParamTypes, typeof(TRes), ct)
                    .ConfigureAwait(false);

                var root = ThrowIfError(doc);
                return JsonSerializer.Deserialize<TRes?>(root.GetProperty("r").GetRawText())!;
            }
            finally
            {
                _ioLock.Release();
            }
        });
    }

    /// <summary>
    /// Validates a standard RPC JSON response envelope and throws a <see cref="RemoteInvocationException"/>
    /// when the envelope represents a remote or transport-level failure.
    /// </summary>
    /// <param name="doc">The JSON document returned by the transport.</param>
    /// <returns>
    /// The root <see cref="JsonElement"/> when the envelope indicates success (either no <c>ok</c> flag is present,
    /// or <c>ok</c> is <see langword="true"/>).
    /// </returns>
    /// <exception cref="RemoteInvocationException">
    /// Thrown when the response contains <c>"ok": false</c>. The exception message is taken from <c>"e"</c> when present.
    /// The exception <see cref="Exception.HResult"/> is taken from <c>"hr"</c> when present.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Expected envelope format:
    /// <c>{"ok": true, "r": &lt;result&gt;}</c> on success, or
    /// <c>{"ok": false, "e": "error message", "hr": &lt;hresult&gt;}</c> on failure.
    /// </para>
    /// <para>
    /// Special-case: a transport shutdown / upgrade condition is surfaced as <c>HResult = 426</c>.
    /// This method treats either a well-known message (e.g. <c>"Server closed."</c>) or an explicit
    /// <c>"hr": 426</c> as upgrade-eligible.
    /// </para>
    /// <para>
    /// Note: if the envelope does not contain an <c>ok</c> property, it is treated as success and the root is returned.
    /// Higher layers may still validate result shape as needed.
    /// </para>
    /// </remarks>
    private static JsonElement ThrowIfError(JsonDocument doc)
    {
        var root = doc.RootElement;

        // Success fast-path: missing "ok" or ok == true.
        if (!root.TryGetProperty("ok", out var okProp) || okProp.GetBoolean())
            return root;

        var msg = root.TryGetProperty("e", out var eProp) ? eProp.GetString() : null;
        var hr = root.TryGetProperty("hr", out var hrProp) ? hrProp.GetInt32() : 0;

        // Upgrade/transport-closed condition: either explicit hr==426 or known message.
        if (hr == 426)
        {
            throw new RemoteInvocationException(msg ?? "Upgrade required")
            {
                HResult = 426
            };
        }

        if (hr == 422)
        {
            throw new RemoteInvocationException(msg ?? "Corrupted transport messages")
            {
                HResult = 422
            };
        }

        throw new RemoteInvocationException(msg ?? "Remote error")
        {
            HResult = hr
        };
    }

    /// <summary>
    /// Core sync helper that executes a single RPC attempt with upgrade-aware retry.
    /// </summary>
    private static TOut RunWithUpgradeRetry<TOut>(Func<TOut> attempt)
    {
        try
        {
            // First attempt
            return attempt();
        }
        catch (RemoteInvocationException rEx) when (!IsUpgradeEligible(rEx))
        {
            // Not an upgrade case → propagate
            throw;
        }
        catch (RemoteInvocationException rEx)
        {
            // Upgrade path (426-ish)
            if (!TryHandleUpgrade(rEx))
                throw;

            // Single retry after successful upgrade
            return attempt();
        }
    }

    /// <summary>
    /// Core async helper that executes a single RPC attempt with upgrade-aware retry.
    /// </summary>
    private async Task<TOut> RunWithUpgradeRetryAsync<TOut>(Func<CancellationToken, Task<TOut>> attempt)
    {
        try
        {
            // First attempt
            return await attempt(_ct).ConfigureAwait(false);
        }
        catch (RemoteInvocationException rEx) when (!IsUpgradeEligible(rEx))
        {
            // Not an upgrade case → propagate
            throw;
        }
        catch (RemoteInvocationException rEx)
        {
            var handled = await TryHandleUpgradeAsync(rEx).ConfigureAwait(false);
            if (!handled)
                throw;

            // Single retry after successful upgrade
            return await attempt(_ct).ConfigureAwait(false);
        }
    }

    private static bool IsUpgradeEligible(RemoteInvocationException ex)
    => ex.HResult is 426 or 422;

}