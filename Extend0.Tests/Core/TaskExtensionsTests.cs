using Extend0.Tests.TestUtilities;
using Microsoft.Extensions.Logging;

namespace Extend0.Tests.Core;

public sealed class TaskExtensionsTests
{
    [Fact]
    public async Task Forget_InvokesCallbackAndLogs_WhenTaskFaults()
    {
        var logger = new ListLogger();
        var callbackSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finallySignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var exception = new InvalidOperationException("boom");

        Task.FromException(exception).Forget(
            logger,
            onExceptionMessage: "expected failure",
            onExceptionAction: _ => callbackSignal.TrySetResult(),
            finallyAction: () => finallySignal.TrySetResult(),
            measureDuration: true);

        await callbackSignal.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await finallySignal.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("expected failure", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Trace && entry.Message.Contains("duration", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Forget_InvokesFinally_WhenTaskCompletesSuccessfully()
    {
        var finallySignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task.Delay(10).Forget(finallyAction: () => finallySignal.TrySetResult());

        await finallySignal.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }
}
