namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Lifetime handle for a cross-process service instance of <typeparamref name="TService"/>.
    /// </summary>
    /// <typeparam name="TService">
    /// The service contract. Must implement <see cref="ICrossProcessService"/>.
    /// For non-owners, <see cref="Service"/> is a proxy that forwards calls via IPC.
    /// For owners, <see cref="Service"/> is the real implementation.
    /// </typeparam>
    /// <remarks>
    /// <para>
    /// The handle encapsulates the resources needed to host or access a cross-process singleton:
    /// the service instance/proxy, an optional named-pipe server (owner only), a process-wide
    /// mutex (owner only), and a client transport (non-owner only).
    /// </para>
    /// <para>
    /// Disposing the handle tears down these resources in a safe order:
    /// client transport → cancel hosting token → dispose server → release mutex → dispose service (owner only).
    /// Both <see cref="Dispose"/> and <see cref="DisposeAsync"/> are idempotent.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Start or connect to the cross-process service:
    /// using var handle = CrossProcess&lt;IMyService&gt;.GetOrStart(() =&gt; new MyService());
    /// var svc = handle.Service; // proxy or real implementation
    /// var hb  = await svc.PingAsync();
    /// </code>
    /// </example>
    public sealed class CrossProcessHandle<TService> : IDisposable, IAsyncDisposable
        where TService : ICrossProcessService
    {
        /// <summary>
        /// Gets the service instance exposed to consumers.
        /// For owners this is the real implementation; for non-owners this is an IPC proxy.
        /// </summary>
        public TService Service { get; }

        /// <summary>
        /// Gets a value indicating whether this process owns (hosts) the cross-process instance.
        /// </summary>
        public bool IsOwner { get; }

        private readonly Mutex? _mutex;
        private readonly CancellationTokenSource? _cts;
        private readonly NamedPipeServer? _server;
        private readonly IClientTransport? _transport;
        private bool _disposed;

        /// <summary>
        /// Initializes a new handle.
        /// </summary>
        /// <param name="service">
        /// The service instance to expose. For owners, the real implementation; for non-owners, the proxy.
        /// </param>
        /// <param name="isOwner">Whether this handle represents the hosting (owner) side.</param>
        /// <param name="mutex">The process-wide mutex used to establish ownership (owner only).</param>
        /// <param name="cts">Cancellation token source used to stop the server loop (owner only).</param>
        /// <param name="server">The named-pipe server hosting the service (owner only).</param>
        /// <param name="transport">The client transport used by the proxy (non-owner only).</param>
        /// <exception cref="ArgumentNullException"><paramref name="service"/> is <c>null</c>.</exception>
        internal CrossProcessHandle(
            TService service, bool isOwner, Mutex? mutex, CancellationTokenSource? cts,
            NamedPipeServer? server, IClientTransport? transport)
        {
            Service    = service ?? throw new ArgumentNullException(nameof(service));
            IsOwner    = isOwner;
            _mutex     = mutex;
            _cts       = cts;
            _server    = server;
            _transport = transport;
        }

        /// <summary>
        /// Synchronously releases resources held by the handle.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The method disposes the client transport (if any), cancels the hosting token (owner),
        /// disposes the server (owner), releases and disposes the mutex (owner), and finally
        /// disposes the underlying service if this handle is the owner and the service implements
        /// <see cref="IDisposable"/>.
        /// </para>
        /// <para>This method is idempotent and may be called multiple times.</para>
        /// </remarks>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            DisposeTransport();
            CancelCts();

            DisposeServerSync();

            ReleaseMutexSafe();
            DisposeMutexSafe();

            DisposeServiceSync();
        }

        /// <summary>
        /// Disposes the hosted server instance synchronously, if present.
        /// </summary>
        /// <remarks>
        /// This helper only acts when <see cref="_server"/> implements <see cref="IDisposable"/>.
        /// Any exception thrown during server disposal is swallowed, as teardown is best-effort.
        /// </remarks>
        private void DisposeServerSync()
        {
            if (_server is IDisposable syncServer)
            {
                try
                {
                    syncServer.Dispose();
                }
                catch
                {
                    // swallow
                }
            }
        }

        /// <summary>
        /// Disposes the underlying service instance synchronously when this handle
        /// represents the owner side and the service implements <see cref="IDisposable"/>.
        /// </summary>
        /// <remarks>
        /// Non-owner handles do not dispose the service. Disposal failures are ignored to
        /// avoid surfacing teardown errors from <see cref="Dispose"/>.
        /// </remarks>
        private void DisposeServiceSync()
        {
            if (!IsOwner)
                return;

            if (Service is IDisposable syncService)
            {
                try
                {
                    syncService.Dispose();
                }
                catch
                {
                    // swallow
                }
            }
        }

        /// <summary>
        /// Asynchronously releases resources held by the handle.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Preferred when the hosted server or service supports asynchronous disposal.
        /// Performs the same steps as <see cref="Dispose"/> but awaits
        /// <see cref="IAsyncDisposable.DisposeAsync"/> for the server/service when available.
        /// </para>
        /// <para>
        /// This method is idempotent: only the first call performs disposal; subsequent calls return immediately.
        /// </para>
        /// </remarks>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, true)) return;

            DisposeTransport();
            CancelCts();

            await DisposeServerAsync().ConfigureAwait(false);

            ReleaseMutexSafe();
            DisposeMutexSafe();

            await DisposeServiceAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes the client transport, if any.
        /// </summary>
        /// <remarks>
        /// This is the first step in teardown to stop outgoing IPC calls from this handle.
        /// </remarks>
        private void DisposeTransport()
        {
            _transport?.Dispose();
        }

        /// <summary>
        /// Attempts to cancel the hosting cancellation token source, if present.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Used on the owner side to request that the server loop stops. Any exception
        /// (e.g., CTS already disposed) is swallowed because cancellation is best-effort
        /// during teardown.
        /// </para>
        /// </remarks>
        private void CancelCts()
        {
            try
            {
                _cts?.Cancel();
            }
            catch
            {
                // Cancellation is best-effort; teardown failures are ignored.
            }
        }

        /// <summary>
        /// Asynchronously disposes the hosted server instance, if present.
        /// </summary>
        /// <remarks>
        /// Prefers <see cref="IAsyncDisposable.DisposeAsync"/> when supported by the server;
        /// otherwise falls back to <see cref="IDisposable.Dispose"/>. Any disposal failure
        /// is swallowed because the handle is already tearing down.
        /// </remarks>
        private async ValueTask DisposeServerAsync()
        {
            if (_server is IAsyncDisposable asyncServer)
            {
                try
                {
                    await asyncServer.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Async server disposal failed; nothing sensible to do here.
                }
            }
            else if (_server is IDisposable syncServer)
            {
                try
                {
                    syncServer.Dispose();
                }
                catch
                {
                    // Same rationale as in async case: ignore teardown errors.
                }
            }
        }

        /// <summary>
        /// Safely releases the ownership mutex, if any.
        /// </summary>
        /// <remarks>
        /// Called near the end of teardown to release the cross-process ownership lock.
        /// Any exception (for example, releasing a mutex not owned by the current thread)
        /// is swallowed because it is non-fatal once the handle is being disposed.
        /// </remarks>
        private void ReleaseMutexSafe()
        {
            try
            {
                _mutex?.ReleaseMutex();
            }
            catch
            {
                // Mutex state issues during shutdown are non-fatal for the process.
            }
        }

        /// <summary>
        /// Safely disposes the underlying mutex instance, if any.
        /// </summary>
        /// <remarks>
        /// Disposing the OS handle for the mutex is the final cleanup step.
        /// Failures are ignored because they are not actionable at this stage.
        /// </remarks>
        private void DisposeMutexSafe()
        {
            try
            {
                _mutex?.Dispose();
            }
            catch
            {
                // Non-actionable failure when releasing OS handle during teardown.
            }
        }

        /// <summary>
        /// Asynchronously disposes the underlying service instance when this handle
        /// represents the owner side.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If <typeparamref name="TService"/> implements <see cref="IAsyncDisposable"/>,
        /// its <see cref="IAsyncDisposable.DisposeAsync"/> method is awaited. Otherwise,
        /// if it implements <see cref="IDisposable"/>, <see cref="IDisposable.Dispose"/>
        /// is called.
        /// </para>
        /// <para>
        /// Non-owner handles do not dispose the service. All disposal errors are swallowed
        /// to keep <see cref="DisposeAsync"/> best-effort and non-throwing during teardown.
        /// </para>
        /// </remarks>
        private async ValueTask DisposeServiceAsync()
        {
            if (!IsOwner) return;

            if (Service is IAsyncDisposable asyncService)
            {
                try
                {
                    await asyncService.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Service-specific async disposal errors are swallowed.
                }
            }
            else if (Service is IDisposable syncService)
            {
                try
                {
                    syncService.Dispose();
                }
                catch
                {
                    // Same idea: do not surface teardown errors.
                }
            }
        }
    }
}
