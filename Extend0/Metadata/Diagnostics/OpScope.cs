using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata.Diagnostics
{
    /// <summary>
    /// Disposable helper that wraps a structured logging scope, a stopwatch and an optional
    /// <see cref="Activity"/> for a named MetaDB operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Typical usage:
    /// </para>
    /// <code>
    /// using var op = logger.BeginOp("ImportUsers", new { batchId });
    /// try
    /// {
    ///     // work…
    /// }
    /// catch (Exception ex)
    /// {
    ///     op.Fail(ex);
    ///     throw;
    /// }
    /// </code>
    /// <para>
    /// On dispose it logs a <c>RunEnd</c> event with elapsed time; <see cref="Fail"/> logs
    /// a <c>RunFail</c> event and marks the associated activity as error.
    /// </para>
    /// </remarks>
    public struct OpScope : IDisposable
    {
        private ILogger? _log;
        private string? _name;
        private IDisposable? _scope;
        private Stopwatch? _sw;
        private Activity? _activity;
        private readonly bool _enabled;
        private bool _disposed;

        /// <summary>
        /// Initializes a new <see cref="OpScope"/> for a named operation, optionally
        /// creating a structured logging scope, starting a stopwatch and creating
        /// an <see cref="Activity"/> for OpenTelemetry tracing.
        /// </summary>
        /// <param name="log">
        /// Logger used to emit <c>RunStart</c>, <c>RunEnd</c> and <c>RunFail</c> events
        /// and to create the structured logging scope. If <see langword="null"/>,
        /// logging is effectively disabled for this scope.
        /// </param>
        /// <param name="name">
        /// Human-readable operation name. Appears in logs and as the activity name
        /// when OpenTelemetry is enabled.
        /// </param>
        /// <param name="state">
        /// Optional structured state attached to the logging scope and the activity.
        /// Typically an anonymous object, e.g. <c>new { tenant = "acme", batch = 42 }</c>.
        /// </param>
        /// <param name="level">
        /// Minimum log level that must be enabled on <paramref name="log"/> for
        /// timing and start/end logs to be emitted. Defaults to <see cref="LogLevel.Information"/>.
        /// </param>
        /// <param name="activitySource">
        /// Optional <see cref="ActivitySource"/> from which to create the activity.
        /// If <see langword="null"/>, <see cref="MetaDBManager.ActivitySrc"/> is used.
        /// </param>
        /// <param name="activityKind">
        /// Kind of activity to create for OpenTelemetry. Defaults to
        /// <see cref="ActivityKind.Internal"/>.
        /// </param>
        /// <remarks>
        /// This constructor is usually not called directly; prefer the
        /// <c>BeginOp</c> helper extension which handles null loggers and
        /// typical parameter defaults.
        /// </remarks>
        public OpScope(
            ILogger? log,
            string name,
            object? state = null,
            LogLevel level = LogLevel.Information,
            ActivitySource? activitySource = null,
            ActivityKind activityKind = ActivityKind.Internal)
        {
            _log = log;
            _name = name;
            _enabled = log?.IsEnabled(level) == true;
            _disposed = false;

            // Scope estructurado sin Dictionary
            var scopeState = new OpScopeState(name, state);
            _scope = log?.BeginScope(scopeState);

            // Solo cronometra si el nivel está activo
            _sw = _enabled ? Stopwatch.StartNew() : null;

            ConfigureOpenTelemetry(name, state, activitySource, activityKind);

            if (_enabled && _log is not null)
                Log.RunStart(_log, name);
        }

        /// <summary>
        /// Configures the optional OpenTelemetry <see cref="Activity"/> associated
        /// with this operation, including basic tags for name and state.
        /// </summary>
        /// <param name="name">Operation name used for the activity.</param>
        /// <param name="state">Structured state to attach as tags, if present.</param>
        /// <param name="activitySource">
        /// Activity source to use. If <see langword="null"/>, <see cref="MetaDBManager.ActivitySrc"/> is used.
        /// </param>
        /// <param name="activityKind">Kind of the activity (internal, server, client, etc.).</param>
        private void ConfigureOpenTelemetry(string name, object? state, ActivitySource? activitySource, ActivityKind activityKind)
        {
            // Activity opcional (OpenTelemetry)
            var src = activitySource ?? MetaDBManager.ActivitySrc;
            _activity = src.StartActivity(name, activityKind);
            if (state is not null)
            {
                _activity?.SetTag("op", name);
                _activity?.SetTag("state", state);
            }
        }

        /// <summary>
        /// Marks the operation as failed, logging an error with elapsed time and flagging
        /// the underlying activity (if any) as <see cref="ActivityStatusCode.Error"/>.
        /// </summary>
        /// <param name="ex">Exception that caused the failure.</param>
        /// <remarks>
        /// <para>
        /// This method is intended to be called from a <c>catch</c> block before
        /// rethrowing or handling the exception. It preserves accurate timings
        /// and error metadata in logs and traces.
        /// </para>
        /// <para>
        /// If the scope has already been disposed, or if no logger was provided,
        /// the call is a no-op.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void Fail(Exception ex)
        {
            if (_disposed) return;
            if (_log is null) return;
            var ms = _sw?.Elapsed.TotalMilliseconds ?? 0d;
            Log.RunFail(_log, _name!, ms, ex);
            _activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        }

        /// <summary>
        /// Completes the operation scope, logging a <c>RunEnd</c> event with the elapsed
        /// time (when enabled), stopping the associated activity and disposing the
        /// underlying logging scope.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is idempotent: subsequent calls after the first have no effect.
        /// </para>
        /// <para>
        /// It is normally invoked via a <c>using</c> statement, ensuring that every
        /// started operation scope is eventually closed even in the presence of exceptions.
        /// </para>
        /// </remarks>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            var ms = _sw?.Elapsed.TotalMilliseconds ?? 0d;
            if (_enabled && _log is not null)
                Log.RunEnd(_log, _name!, ms);

            _activity?.Stop();
            _scope?.Dispose();

            // liberar refs
            _log = null;
            _name = null;
            _scope = null;
            _sw = null;
            _activity = null;
        }
    }
}
