using Extend0.Lifecycle;
using Extend0.Tests.TestUtilities;
using Microsoft.Extensions.Logging;

namespace Extend0.Tests.Lifecycle;

public sealed class SingletonTests
{
    [Fact]
    public void Singleton_Throws_WhenSecondInstanceIsCreatedWithoutOverwrite()
    {
        using var first = new TestSingleton(new SingletonOptions { Overwrite = false });

        var ex = Assert.Throws<InvalidOperationException>(() => new TestSingleton(new SingletonOptions { Overwrite = false }));

        Assert.Contains("already a singleton instance", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Singleton_Overwrite_ReplacesAndDisposesPreviousInstance()
    {
        var logger = new ListLogger();
        using var first = new TestSingleton(new SingletonOptions { Overwrite = false });
        using var second = new TestSingleton(new SingletonOptions { Overwrite = true, Logger = logger });

        Assert.True(first.DisposeManagedCallCount > 0);
        Assert.True(Singleton.TryGet<TestSingleton>(out var current));
        Assert.Same(second, current);
    }

    private sealed class TestSingleton(SingletonOptions options) : Singleton(options)
    {
        public int DisposeManagedCallCount { get; private set; }

        protected override void DisposeManaged()
        {
            DisposeManagedCallCount++;
        }
    }
}
