using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Extend0
{
    /// <summary>
    /// Extension methods for <see cref="Task"/>
    /// </summary>
    public static class TaskExtensions
    {
        /// <summary>
        /// Observes the given <paramref name="task"/> to avoid unobserved exceptions,
        /// logs any failure via the provided <paramref name="logger"/> (if any),
        /// invokes an optional callback on exception, and optionally measures and logs duration.
        /// </summary>
        /// <param name="task">
        /// The <see cref="Task"/> to execute in fire-and-forget mode.
        /// </param>
        /// <param name="logger">
        /// An optional <see cref="Microsoft.Extensions.Logging.ILogger"/>
        /// used to log errors and trace information. If <c>null</c>, no logging is performed.
        /// </param>
        /// <param name="onExceptionMessage">
        /// An optional custom message to include when logging an exception.
        /// If <c>null</c>, a default failure message is used.
        /// </param>
        /// <param name="onExceptionAction">
        /// An optional <see cref="Action{Exception}"/> callback that is invoked
        /// with the exception when <paramref name="task"/> faults.
        /// </param>
        /// <param name="finallyAction">
        /// An optional <see cref="Action"/> callback that is invoked
        /// when <paramref name="task"/> ends.
        /// </param>
        /// <param name="measureDuration">
        /// If <c>true</c>, measures the elapsed time of <paramref name="task"/>
        /// and logs it as a trace message; otherwise, duration is not measured.
        /// </param>
        /// <remarks>
        /// Internally this method awaits the task using <c>ConfigureAwait(false)</c>
        /// to avoid capturing the synchronization context, and ensures that any
        /// exceptions are observed rather than causing an <see cref="TaskScheduler.UnobservedTaskException"/>.
        /// </remarks>
        /// <example>
        /// <![CDATA[
        /// SomeAsyncOperation()
        ///     .Forget(_logger,
        ///             onExceptionMessage: "Oops, something went wrong!",
        ///             onExceptionAction: ex => Telemetry.TrackException(ex),
        ///             measureDuration: true);
        /// ]]>
        /// </example>
        public static void Forget(
              this Task task,
              ILogger? logger = null,
              string? onExceptionMessage = null,
              Action<Exception>? onExceptionAction = null,
              Action? finallyAction = null,
              bool measureDuration = false)
        {
            if (task.IsCompleted && !task.IsFaulted && !task.IsCanceled)
                return;

            _ = ForgetAwaited(task, logger, onExceptionMessage, onExceptionAction, finallyAction, measureDuration);

            static async Task ForgetAwaited(
                Task task,
                ILogger? logger,
                string? msg,
                Action<Exception>? callback,
                Action? finallyCallback,
                bool measure)
            {
                Stopwatch? sw = measure ? Stopwatch.StartNew() : null;
                try
                {
                    await task.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var fmsg = msg ?? "Fire-and-forget task failed in Forget()";
                    // 1. Log
                    logger?.LogError(ex, "{fmsg}", fmsg);
                    // 2. Callback extra
                    callback?.Invoke(ex);
                }
                finally
                {
                    if (measure && sw is not null)
                    {
                        sw.Stop();
                        logger?.LogTrace(
                            "Fire-and-forget task duration: {ElapsedMilliseconds}ms",
                            sw.ElapsedMilliseconds);
                    }
                    finallyCallback?.Invoke();
                }
            }
        }
    }
}
