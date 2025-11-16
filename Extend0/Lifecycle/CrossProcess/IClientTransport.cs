using System.Text.Json;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Abstraction for client-side IPC transport used by cross-process proxies.
    /// Implementations send a method call with arguments to a remote host and return
    /// the JSON reply as a <see cref="JsonDocument"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Expected envelope from the remote endpoint:
    /// <c>{ "ok": true, "r": &lt;result&gt; }</c> on success, or
    /// <c>{ "ok": false, "e": "&lt;error message&gt;" }</c> on failure.
    /// </para>
    /// <para>
    /// Thread-safety: transports are not required to support concurrent calls.
    /// Callers (e.g., the RPC proxy) may serialize access.
    /// </para>
    /// <para>
    /// Lifetime: the caller is responsible for disposing the returned
    /// <see cref="JsonDocument"/>.
    /// </para>
    /// </remarks>
    public interface IClientTransport : IDisposable
    {
        /// <summary>
        /// Invokes a remote method over the transport and returns the raw JSON reply.
        /// </summary>
        /// <param name="method">Logical method name to invoke on the remote service.</param>
        /// <param name="args">Argument values in positional order. May be empty for parameterless methods.</param>
        /// <param name="paramTypes">
        /// CLR parameter types corresponding to <paramref name="args"/>. Provided for serializers
        /// that need type hints (may be ignored by some transports).
        /// </param>
        /// <param name="declaredReturnType">
        /// The declared return type of the method (e.g., <c>typeof(void)</c>, <c>typeof(Task)</c>,
        /// <c>typeof(Task&lt;T&gt;)</c>’s <c>T</c>, or a synchronous result type). Provided as a hint for
        /// serialization; transports may ignore it.
        /// </param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>
        /// A <see cref="JsonDocument"/> containing the remote envelope. Caller must dispose it.
        /// </returns>
        /// <exception cref="OperationCanceledException">The operation was canceled via <paramref name="ct"/>.</exception>
        /// <exception cref="ObjectDisposedException">The transport has been disposed.</exception>
        /// <exception cref="InvalidOperationException">The transport is not connected or not initialized.</exception>
        /// <exception cref="System.IO.IOException">Underlying I/O error (e.g., broken pipe/connection).</exception>
        /// <exception cref="JsonException">Malformed JSON received from the remote endpoint.</exception>
        Task<JsonDocument> CallAsync(
            string method,
            object?[] args,
            Type[] paramTypes,
            Type declaredReturnType,
            CancellationToken ct);
    }
}
