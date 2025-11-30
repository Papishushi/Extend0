namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// A singleton that can run in-process or as a cross-process singleton,
/// exposing a static <see cref="Service"/> property for consumers.
/// </summary>
/// <typeparam name="TService">
/// The service contract exposed to consumers. Prefer an interface when using cross-process mode.
/// </typeparam>
/// <remarks>
/// <para>
/// This type keeps a single live instance per <typeparamref name="TService"/> in the current process,
/// and optionally enforces a single owner across processes. Consumers access the instance via
/// <see cref="Service"/>:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>Cross-process owner → <see cref="Service"/> is the real implementation created by the factory.</description>
///   </item>
///   <item>
///     <description>Cross-process non-owner → <see cref="Service"/> is an IPC proxy forwarding calls to the owner.</description>
///   </item>
///   <item>
///     <description>In-process mode → <see cref="Service"/> is the real implementation (no IPC).</description>
///   </item>
/// </list>
/// <para>
/// The <see cref="Service"/> property is static per closed generic type
/// (i.e., one per <c>CrossProcessSingleton&lt;SomeContract&gt;</c>).
/// Construct one instance at application startup to initialize the static state.
/// </para>
/// </remarks>
public class CrossProcessSingleton<TService> : Singleton where TService : class, ICrossProcessService
{
    private static readonly Lock _staticGate = new();
    private static CrossProcessHandle<TService>? _handle;
    private static TService? _service;

    private readonly CrossProcessHandle<TService>? _crossHandleInstance;

    /// <summary>
    /// Gets the service instance available to consumers. Throws if the singleton
    /// has not been initialized (i.e., no instance has been constructed yet).
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when accessed before the singleton has been initialized.
    /// </exception>
    public static TService Service
    {
        get
        {
            var s = _service;
            return s is null
                ? throw new InvalidOperationException("The singleton has not been initialized. Construct a CrossProcessSingleton<TService> at application startup.")
                : s;
        }
    }

    /// <summary>
    /// Indicates whether the current process owns the cross-process instance.
    /// Always <see langword="true"/> when running in in-process mode.
    /// </summary>
    public static bool IsOwner { get; private set; }

    /// <summary>
    /// Initializes the singleton and sets up the static <see cref="Service"/>:
    /// becomes the cross-process owner (hosting the service) or connects to an existing owner,
    /// or simply creates a local instance in in-process mode.
    /// </summary>
    /// <param name="factory">
    /// Factory that creates the real <typeparamref name="TService"/> implementation.
    /// In cross-process mode, this instance is hosted only by the owner process.
    /// </param>
    /// <param name="options">
    /// Singleton registration and orchestration options (overwrite behavior, mode, endpoint name, timeouts, logging).
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the static service is already initialized and <see cref="SingletonOptions.Overwrite"/> is <see langword="false"/>.
    /// </exception>
    public CrossProcessSingleton(Func<TService> factory, CrossProcessSingletonOptions options) : base(options) => InitializeStatic(factory, options, out _crossHandleInstance);

    /// <summary>
    /// Releases managed resources held by this singleton. In cross-process mode, disposes the
    /// underlying cross-process handle (server/proxy/mutex). In in-process mode, disposes
    /// <see cref="Service"/> if it implements <see cref="IDisposable"/>. Also clears the static state.
    /// </summary>
    protected override void DisposeManaged()
    {
        lock (_staticGate)
        {
            if (_handle is not null)
            {
                try { _handle.Dispose(); } catch { /* swallow on shutdown */ }
                _handle = null;
            }
            if (_crossHandleInstance is not null)
                try { _crossHandleInstance.Dispose(); } catch { /* swallow on shutdown */ }
            else if (_service is IDisposable d)
            {
                try { d.Dispose(); } catch { /* swallow on shutdown */ }
            }

            _service = null;
            IsOwner = false;
        }
    }

    /// <summary>
    /// Initializes or reinitializes the static singleton instance and returns an optional
    /// cross-process handle for the current caller.
    /// </summary>
    /// <param name="factory">
    /// Factory that creates the concrete <typeparamref name="TService"/> implementation.
    /// Used for both in-process and cross-process modes.
    /// </param>
    /// <param name="options">
    /// Singleton configuration controlling overwrite behavior and whether the instance
    /// is hosted in-process or as a cross-process singleton.
    /// </param>
    /// <param name="instanceHandle">
    /// When this method returns, contains the <see cref="CrossProcessHandle{TService}"/> associated
    /// with the current caller when running in cross-process mode; otherwise <see langword="null"/>.
    /// The handle can be used to explicitly dispose the cross-process resources for this instance.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="factory"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a singleton is already initialized and <see cref="CrossProcessSingletonOptions.Overwrite"/>
    /// is <c>false</c>.
    /// </exception>
    private static void InitializeStatic(Func<TService> factory, CrossProcessSingletonOptions options, out CrossProcessHandle<TService>? instanceHandle)
    {
        ArgumentNullException.ThrowIfNull(factory);
        instanceHandle = null;

        lock (_staticGate)
        {
            EnsureOverwriteAllowed(options);
            TearDownPreviousState(options);

            _service = null;
            IsOwner = false;

            InitializeNewInstance(factory, options, out instanceHandle);
        }
    }

    /// <summary>
    /// Validates that the current singleton can be overwritten under the provided options.
    /// </summary>
    /// <param name="options">
    /// Singleton configuration that includes the <see cref="CrossProcessSingletonOptions.Overwrite"/> flag.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a singleton instance already exists and overwriting is not allowed.
    /// </exception>
    private static void EnsureOverwriteAllowed(CrossProcessSingletonOptions options)
    {
        if (_service is not null && !options.Overwrite)
            throw new InvalidOperationException($"A singleton service for '{typeof(TService).FullName}' is already initialized.");
    }

    /// <summary>
    /// Tears down any previously initialized singleton state, including an existing cross-process handle
    /// or an in-process service instance, according to the overwrite options.
    /// </summary>
    /// <param name="options">
    /// Singleton configuration that determines whether an existing in-process service should be disposed
    /// when no cross-process handle is present.
    /// </param>
    /// <remarks>
    /// Disposal is best-effort: teardown exceptions are swallowed because callers cannot act on them
    /// during reinitialization.
    /// </remarks>
    private static void TearDownPreviousState(CrossProcessSingletonOptions options)
    {
        if (_handle is not null)
        {
            try
            {
                _handle.Dispose();
            }
            catch
            {
                // Best-effort teardown of previous cross-process handle.
            }

            _handle = null;
        }
        else if (_service is IDisposable d && options.Overwrite)
        {
            try
            {
                d.Dispose();
            }
            catch
            {
                // Failure to dispose the previous in-process service during overwrite is non-recoverable.
            }
        }
    }

    /// <summary>
    /// Creates and registers a new singleton instance according to the configured mode
    /// (in-process or cross-process), updating static state and returning an optional handle.
    /// </summary>
    /// <param name="factory">
    /// Factory that creates the concrete <typeparamref name="TService"/> implementation.
    /// </param>
    /// <param name="options">
    /// Singleton configuration that selects between <see cref="SingletonMode.InProcess"/> and
    /// <see cref="SingletonMode.CrossProcess"/> and carries cross-process settings.
    /// </param>
    /// <param name="instanceHandle">
    /// When running in cross-process mode, receives the created <see cref="CrossProcessHandle{TService}"/>;
    /// otherwise <see langword="null"/>.
    /// </param>
    /// <remarks>
    /// This method assumes previous state has already been validated and torn down.
    /// It updates <c>_service</c>, <c>_handle</c> and <see cref="IsOwner"/> atomically under the static lock.
    /// </remarks>
    private static void InitializeNewInstance(Func<TService> factory, CrossProcessSingletonOptions options, out CrossProcessHandle<TService>? instanceHandle)
    {
        instanceHandle = null;

        if (options.Mode == SingletonMode.CrossProcess)
        {
            var h = CrossProcessOrchestator<TService>.GetOrStart(factory, serverName: options.CrossProcessServer, name: options.CrossProcessName, connectTimeoutMs: options.CrossProcessConnectTimeoutMs);

            _handle = h;
            _service = h.Service;
            IsOwner = h.IsOwner;

            // keep a per-instance reference so DisposeManaged() can dispose gracefully
            instanceHandle = h;
        }
        else
        {
            _service = factory();
            IsOwner = true;
        }
    }
}
