namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Common contract for all cross-process services.
    /// Derive your service interfaces from this to get standard diagnostics/utilities.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Contract constraints (important):</b> Service contracts must be composed only of
    /// JSON-serializable data and <see cref="System.Threading.Tasks.Task"/>/ <see cref="System.Threading.Tasks.Task{TResult}"/> returns.
    /// You <b>must not</b> include any pointer/handle or non-serializable runtime resources in method
    /// parameters or return types.
    /// </para>
    /// <para><b>Do NOT use (non-exhaustive):</b></para>
    /// <list type="bullet">
    ///   <item><description><see cref="System.IntPtr"/>, <see cref="System.UIntPtr"/>, <c>nint</c>, <c>nuint</c>, unmanaged pointers (<c>void*</c>, etc.).</description></item>
    ///   <item><description><c>ref struct</c> types and stack-only types: <see cref="System.Span{T}"/>, <see cref="System.ReadOnlySpan{T}"/>.</description></item>
    ///   <item><description>Handles/OS resources: <see cref="System.Runtime.InteropServices.HandleRef"/>, <see cref="System.Threading.WaitHandle"/>, <see cref="System.Threading.Mutex"/>, <see cref="System.Threading.Semaphore"/>, <see cref="Microsoft.Win32.SafeHandles.SafeHandle"/> and derivatives.</description></item>
    ///   <item><description>Streams and live I/O abstractions: <see cref="System.IO.Stream"/>, <see cref="System.IO.Pipelines.PipeReader"/>, <see cref="System.IO.Pipelines.PipeWriter"/>.</description></item>
    ///   <item><description><see cref="System.Threading.CancellationToken"/> (tokens are not sent over IPC; use method-specific flags/timeout args instead).</description></item>
    ///   <item><description>Delegates containing unmanaged function pointers.</description></item>
    /// </list>
    /// <para><b>Safe choices:</b> primitives, <see cref="string"/>, <see cref="System.Decimal"/>, <see cref="System.Guid"/>,
    /// <see cref="System.DateTimeOffset"/>, enums, POCO/record DTOs with public getters/setters, arrays,
    /// <see cref="System.Collections.Generic.List{T}"/>, and dictionaries with string keys.</para>
    /// <para>
    /// Rationale: the wire format is JSON; pointer/handle-like values cannot be serialized meaningfully
    /// and would fail (e.g., <see cref="System.Text.Json"/> does not support <see cref="System.IntPtr"/>).
    /// </para>
    /// </remarks>
    public interface ICrossProcessService
    {
        /// <summary>
        /// Gets the logical contract name for this service, as reported by <see cref="GetServiceInfoAsync"/>.
        /// </summary>
        string ContractName { get; }

        /// <summary>
        /// Performs a lightweight liveness probe against the service.
        /// </summary>
        /// <returns>
        /// A task that completes with a <see cref="Heartbeat"/> snapshot describing
        /// the current time, uptime and service fingerprint.
        /// </returns>
        Task<Heartbeat> PingAsync();

        /// <summary>
        /// Retrieves diagnostic and identity information about the hosted service instance.
        /// </summary>
        /// <returns>
        /// A task that completes with a <see cref="ServiceInfo"/> instance containing
        /// contract, implementation, assembly, process and hosting details.
        /// </returns>
        Task<ServiceInfo> GetServiceInfoAsync();

        /// <summary>
        /// Stops hosting this service instance over its named pipe and releases
        /// all associated server-side resources.
        /// </summary>
        /// <remarks>
        /// Implementations should be idempotent so that multiple calls are safe.
        /// </remarks>
        void StopHosting();

        /// <summary>
        /// Probes the configured named pipe to determine whether a server is
        /// currently listening for connections.
        /// </summary>
        /// <returns>
        /// A task whose result is <c>true</c> if a connection to the pipe can be
        /// established within a short timeout; otherwise, <c>false</c>.
        /// </returns>
        Task<bool> CanConnectAsync();
    }
}
