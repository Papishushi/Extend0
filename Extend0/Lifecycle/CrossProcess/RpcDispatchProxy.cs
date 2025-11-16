using System.Buffers;
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
                    _ioLock.Wait(_ct);
                    try
                    {
                        // First attempt
                        using var doc = _transport
                            .CallAsync(shape.Name, args, shape.ParamTypes, typeof(void), _ct)
                            .GetAwaiter().GetResult();

                        ThrowIfError(doc);
                        return null;
                    }
                    catch (RemoteInvocationException rEx) when (rEx.HResult != 426)
                    {
                        throw;
                    }
                    catch (RemoteInvocationException rEx)
                    {
                        if (!TryHandleUpgrade(rEx))
                            throw;

                        // Single retry
                        using var doc2 = _transport
                            .CallAsync(shape.Name, args, shape.ParamTypes, typeof(void), _ct)
                            .GetAwaiter().GetResult();

                        ThrowIfError(doc2);
                        return null;
                    }
                    finally
                    {
                        _ioLock.Release();
                    }
                }

            // Synchronous with result
            case RetKind.SyncResult:
                {
                    var returnType = targetMethod.ReturnType;

                    _ioLock.Wait(_ct);
                    try
                    {
                        // First attempt
                        using var doc = _transport
                            .CallAsync(shape.Name, args, shape.ParamTypes, returnType, _ct)
                            .GetAwaiter().GetResult();

                        var root = ThrowIfError(doc);
                        return JsonSerializer.Deserialize(root.GetProperty("r").GetRawText(), returnType)!;
                    }
                    catch (RemoteInvocationException rEx) when (rEx.HResult != 426)
                    {
                        throw;
                    }
                    catch (RemoteInvocationException rEx)
                    {
                        if (!TryHandleUpgrade(rEx))
                            throw;

                        // Single retry
                        using var doc2 = _transport
                            .CallAsync(shape.Name, args, shape.ParamTypes, returnType, _ct)
                            .GetAwaiter().GetResult();

                        var root2 = ThrowIfError(doc2);
                        return JsonSerializer.Deserialize(root2.GetProperty("r").GetRawText(), returnType)!;
                    }
                    finally
                    {
                        _ioLock.Release();
                    }
                }

            // Task-returning (no result value)
            case RetKind.Task:
                {
                    return CallTaskAsync(shape, args);
                }

            // Task<T>-returning
            case RetKind.TaskOfT:
                {
                    var mi = _callTaskTCache.GetOrAdd(
                        shape.TaskResultType!,
                        resType => typeof(RpcDispatchProxy<TService>)
                            .GetMethod(nameof(CallTaskTAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
                            .MakeGenericMethod(resType));

                    return mi.Invoke(this, [shape, args])!;
                }

            default:
                throw new InvalidOperationException("Unsupported return kind.");
        }
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
            using var doc = await _transport
                .CallAsync(shape.Name, args, shape.ParamTypes, typeof(Task), _ct)
                .ConfigureAwait(false);

            ThrowIfError(doc);
        }
        catch (RemoteInvocationException rEx) when (rEx.HResult != 426)
        {
            throw;
        }
        catch (RemoteInvocationException rEx)
        {
            var handled = await TryHandleUpgradeAsync(rEx).ConfigureAwait(false);
            if (!handled)
                throw;

            // Single retry after successful upgrade
            using var doc2 = await _transport
                .CallAsync(shape.Name, args, shape.ParamTypes, typeof(Task), _ct)
                .ConfigureAwait(false);

            ThrowIfError(doc2);
        }
        finally
        {
            _ioLock.Release();
        }
    }


    /// <summary>
    /// Performs an async RPC for a method that returns <see cref="Task{TRes}"/>.
    /// </summary>
    /// <typeparam name="TRes">The declared result type.</typeparam>
    /// <param name="shape">The cached method shape.</param>
    /// <param name="args">Argument values.</param>
    /// <returns>A task producing the deserialized result value.</returns>
    /// <exception cref="RemoteInvocationException">The remote endpoint returned an error envelope.</exception>
    private async Task<TRes?> CallTaskTAsync<TRes>(MethodShape shape, object?[] args)
    {
        await _ioLock.WaitAsync(_ct).ConfigureAwait(false);
        try
        {
            // First attempt
            using var doc = await _transport
                .CallAsync(shape.Name, args, shape.ParamTypes, typeof(TRes), _ct)
                .ConfigureAwait(false);

            var root = ThrowIfError(doc);
            return JsonSerializer.Deserialize<TRes>(root.GetProperty("r").GetRawText())!;
        }
        catch (RemoteInvocationException rEx) when (rEx.HResult == 426)
        {
            var handled = await TryHandleUpgradeAsync(rEx).ConfigureAwait(false);
            if (!handled)
                throw;

            // Single retry after successful upgrade
            using var doc2 = await _transport
                .CallAsync(shape.Name, args, shape.ParamTypes, typeof(TRes), _ct)
                .ConfigureAwait(false);

            var root2 = ThrowIfError(doc2);
            return JsonSerializer.Deserialize<TRes>(root2.GetProperty("r").GetRawText())!;
        }
        finally
        {
            _ioLock.Release();
        }
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

        if (root.TryGetProperty("ok", out var okProp) && !okProp.GetBoolean())
        {
            var msg = root.TryGetProperty("e", out var eProp) ? eProp.GetString() : null;

            if (string.Equals(msg, "Server closed.", StringComparison.OrdinalIgnoreCase))
            {
                throw new RemoteInvocationException(msg ?? "Upgrade required")
                {
                    HResult = 426
                };
            }

            throw new RemoteInvocationException(msg ?? "Remote error");
        }

        return root;
    }

}