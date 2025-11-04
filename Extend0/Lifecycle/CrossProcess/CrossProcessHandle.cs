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
    /// using var handle = CrossProcess&lt;IMyService&gt;.GetOrStart(() => new MyService());
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

            // Client first
            _transport?.Dispose();

            // Host: stop server and wait for loop end (server.Dispose handles its own wait)
            try { _cts?.Cancel(); } catch { }
            try { (_server as IDisposable)?.Dispose(); } catch { }

            // Mutex at the end
            try { _mutex?.ReleaseMutex(); } catch { }
            try { _mutex?.Dispose(); } catch { }

            // Dispose real service only if we are the owner
            if (IsOwner && Service is IDisposable d)
                try { d.Dispose(); } catch { }
        }

        /// <summary>
        /// Asynchronously releases resources held by the handle.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Preferred when the hosted server supports asynchronous disposal.
        /// Performs the same steps as <see cref="Dispose"/> but awaits <see cref="IAsyncDisposable.DisposeAsync"/>
        /// for the server/service when available.
        /// </para>
        /// <para>This method is idempotent and may be called multiple times.</para>
        /// </remarks>
        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            _transport?.Dispose();

            try { _cts?.Cancel(); } catch { }
            if (_server is IAsyncDisposable ad)
                try { await ad.DisposeAsync().ConfigureAwait(false); } catch { }
            else
            {
                try { (_server as IDisposable)?.Dispose(); } catch { }
            }

            try { _mutex?.ReleaseMutex(); } catch { }
            try { _mutex?.Dispose(); } catch { }

            if (IsOwner && Service is IAsyncDisposable ad2)
                try { await ad2.DisposeAsync().ConfigureAwait(false); } catch { }
            else if (IsOwner && Service is IDisposable d)
            {
                try { d.Dispose(); } catch { }
            }
        }
    }
}
