using Extend0.Metadata.Diagnostics;
using Extend0.Metadata.Storage;
using Extend0.Tests.TestUtilities;
using Microsoft.Extensions.Logging;

namespace Extend0.Tests.Metadata.Diagnostics;

public sealed class MetadataDiagnosticsTests
{
    [Fact]
    public void MetadataTableLockedException_UsesInnerHResult_WhenAvailable()
    {
        var inner = new IOException("locked") { HResult = 12345 };

        var ex = new MetadataTableLockedException("table locked", inner);

        Assert.Equal(12345, ex.HResult);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void MetadataTableLockedException_UsesDefaultLockViolationHResult_WhenInnerIsMissing()
    {
        var ex = new MetadataTableLockedException("table locked", null);

        Assert.Equal(unchecked((int)0x80070021), ex.HResult);
    }

    [Fact]
    public void OpScopeState_EnumeratesExpectedStructuredFields()
    {
        var state = new OpScopeState("import-users", new { batch = 42 });

        Assert.Equal(3, state.Count);
        Assert.Equal("import-users", state.Op);
        Assert.Equal("op", state[0].Key);
        Assert.Equal("ts", state[1].Key);
        Assert.Equal("state", state[2].Key);

        var entries = state.ToArray();
        Assert.Equal(3, entries.Length);
    }

    [Fact]
    public void OpScopeState_CoversNoStateEqualityEnumeratorResetAndBounds()
    {
        var left = new OpScopeState("cleanup", null);
        var right = left;
        var different = new OpScopeState("cleanup", new { value = 1 });

        Assert.Equal(2, left.Count);
        Assert.Null(left.State);
        Assert.True(left.Ts <= DateTimeOffset.UtcNow);
        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left.Equals((object)right));
        Assert.True(left == right);
        Assert.True(left != different);
        Assert.NotEqual(0, different.GetHashCode());
        Assert.False(left.Equals("not-a-state"));
        Assert.Null(left[2].Value);
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = left[3]);

        var enumerator = left.GetEnumerator();
        Assert.Equal(-1, enumerator.Index);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(0, enumerator.Index);
        Assert.Equal("op", enumerator.Current.Key);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(1, enumerator.Index);
        Assert.Equal("ts", enumerator.Current.Key);
        Assert.False(enumerator.MoveNext());
        enumerator.Reset();
        Assert.Equal(-1, enumerator.Index);
        Assert.True(enumerator.MoveNext());
        enumerator.Dispose();

        var nongeneric = ((System.Collections.IEnumerable)left).GetEnumerator();
        Assert.True(nongeneric.MoveNext());
        Assert.IsType<KeyValuePair<string, object?>>(nongeneric.Current);
    }

    [Fact]
    public void BeginOp_LogsStartAndEnd()
    {
        var logger = new ListLogger();

        using (logger.BeginOp("sync-users"))
        {
        }

        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Message.Contains("START", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Message.Contains("END", StringComparison.Ordinal));
    }

    [Fact]
    public void OpScope_Fail_LogsFailure()
    {
        var logger = new ListLogger();
        using var op = logger.BeginOp("sync-users");

        op.Fail(new InvalidOperationException("boom"));

        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("FAILED", StringComparison.Ordinal));
    }

    [Fact]
    public void PrecompiledLogHelpers_EmitExpectedLevelsAndMessages()
    {
        var logger = new ListLogger();
        var childId = Guid.NewGuid();
        var exception = new InvalidOperationException("boom");

        Log.GrowHookOverwrite(logger);
        Log.TableRegisteredLazy(logger, "Users", childId, "users.map");
        Log.TableCreatedNow(logger, "Users", childId, "users.map");
        Log.TableRegistering(logger, "Users");
        Log.TableNameDuplicate(logger, "Users");
        Log.TableOpened(logger, "Users", "users.map", childId);
        Log.FillColumnStart(logger, "Users", 0, 5, "Int32", CapacityPolicy.None);
        Log.FillColumnEnd(logger, "Users", "Int32", 1.5);
        Log.FillColumnValueTooSmall(logger, "Users", "Int32", 1, 4);
        Log.FillRawStart(logger, "Users", 0, 5, CapacityPolicy.AutoGrowZeroInit);
        Log.FillRawEnd(logger, "Users", 2.5);
        Log.CopyStart(logger, "Users", 0, "UsersCopy", 1, 10, CapacityPolicy.Throw);
        Log.CopyEnd(logger, "Users", "UsersCopy", 3.5);
        Log.CopySizeMismatch(logger, 4, 16, 8);
        Log.EnsureRefVecInit(logger, "Users", 2, 3);
        Log.RefCellTooSmall(logger, 7);
        Log.LinkRefAdded(logger, "Users", 3, childId, 4, 5);
        Log.RefsFull(logger);
        Log.ChildReused(logger, "Users", 3, childId);
        Log.ChildCreatedLinked(logger, "Users", 3, "Child", childId);
        Log.CapacityOk(logger, "Users", 0, 10);
        Log.CapacityGrow(logger, "Users", 0, 20, CapacityPolicy.AutoGrowZeroInit);
        Log.CapacityGrowHookMissing(logger);
        Log.CapacityGrowFailed(logger, "Users", 0, 20);
        Log.EnsureRowCapacityProbeFailed(logger, "Users", 0, 30, exception);
        Log.RunStart(logger, "sync");
        Log.RunEnd(logger, "sync", 6.7);
        Log.RunFail(logger, "sync", 8.9, exception);

        Assert.Equal(28, logger.Entries.Count);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Message.Contains("already configured", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Message.Contains("lazily registered", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Debug && entry.Message.Contains("Trying to register table", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("VALUE 1 < sizeof(4)", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("Refs vector is full", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("EnsureRowCapacityProbe failed", StringComparison.Ordinal) && ReferenceEquals(entry.Exception, exception));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Message.Contains("Run sync END", StringComparison.Ordinal));
    }

    [Fact]
    public void PrecompiledLogHelpers_ExposeStructuredStateItems()
    {
        var logger = new StructuredStateLogger();
        var childId = Guid.NewGuid();

        Log.FillColumnStart(logger, "Users", 0, 5, "Int32", CapacityPolicy.None);
        Log.FillColumnValueTooSmall(logger, "Users", "Int32", 1, 4);
        Log.LinkRefAdded(logger, "Users", 3, childId, 4, 5);

        Assert.NotEmpty(logger.StructuredEntries);
        Assert.Contains(logger.StructuredEntries, entry => entry.Any(kv => kv.Key == "Table" && Equals(kv.Value, "Users")));
        Assert.Contains(logger.StructuredEntries, entry => entry.Any(kv => kv.Key == "Type" && Equals(kv.Value, "Int32")));
        Assert.Contains(logger.StructuredEntries, entry => entry.Any(kv => kv.Key == "ChildId" && Equals(kv.Value, childId)));
    }

    private sealed class StructuredStateLogger : ILogger
    {
        public List<List<KeyValuePair<string, object?>>> StructuredEntries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            _ = formatter(state, exception);

            if (state is not IReadOnlyList<KeyValuePair<string, object?>> fields)
                return;

            var snapshot = new List<KeyValuePair<string, object?>>(fields.Count);
            for (var i = 0; i < fields.Count; i++)
                snapshot.Add(fields[i]);

            StructuredEntries.Add(snapshot);
        }
    }
}
