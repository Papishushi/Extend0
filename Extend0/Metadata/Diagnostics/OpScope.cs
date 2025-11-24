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
            _scope = log?.BeginScope(new OpScopeState(name, state));

            // Solo cronometra si el nivel está activo
            _sw = _enabled ? Stopwatch.StartNew() : null;

            // Activity opcional (OpenTelemetry)
            var src = activitySource ?? MetaDBManager.ActivitySrc;
            _activity = src.StartActivity(name, activityKind);
            if (state is not null)
            {
                _activity?.SetTag("op", name);
                _activity?.SetTag("state", state);
            }

            if (_enabled && _log is not null)
                Log.RunStart(_log, name);
        }

        /// <summary>
        /// Marks the operation as failed, logging an error with elapsed time and flagging
        /// the underlying activity (if any) as <see cref="ActivityStatusCode.Error"/>.
        /// </summary>
        /// <param name="ex">Exception that caused the failure.</param>
        /// <remarks>
        /// Should be called from a <c>catch</c> block before rethrowing to preserve
        /// accurate timings and error metadata in logs and traces.
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
