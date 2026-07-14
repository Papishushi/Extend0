using Extend0.Testing.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessUtilsTests
{
    [Fact]
    public void CurrentFingerprint_Is32HexCharacters()
    {
        var fingerprint = CrossProcessUtilsHarness.CurrentFingerprint;

        Assert.Equal(32, fingerprint.Length);
        Assert.Matches("^[0-9a-fA-F]{32}$", fingerprint);
    }

    [Fact]
    public void BuildNameFor_UsesTypeFingerprintAndOptionalSuffix()
    {
        var withSuffix = CrossProcessUtilsHarness.BuildNameFor<CrossProcessUtilsTests>("demo");
        var withoutSuffix = CrossProcessUtilsHarness.BuildNameFor<CrossProcessUtilsTests>(" ");

        Assert.Contains("CPS:", withSuffix, StringComparison.Ordinal);
        Assert.Contains(typeof(CrossProcessUtilsTests).FullName!, withSuffix, StringComparison.Ordinal);
        Assert.EndsWith(":demo", withSuffix, StringComparison.Ordinal);
        Assert.DoesNotContain(": ", withoutSuffix, StringComparison.Ordinal);
        Assert.False(withoutSuffix.EndsWith(":", StringComparison.Ordinal));
    }

    [Fact]
    public void BuildPipeName_UsesSafeCharacters_AndHashesLongInputs()
    {
        var shortName = CrossProcessUtilsHarness.BuildPipeName("hello/world", "P.");
        var longInput = new string('x', 2000);
        var longName = CrossProcessUtilsHarness.BuildPipeName(longInput, "P.");

        Assert.StartsWith("P.", shortName, StringComparison.Ordinal);
        Assert.Matches("^[A-Za-z0-9._-]+$", shortName);
        Assert.StartsWith("P.", longName, StringComparison.Ordinal);
        Assert.Contains('.', longName);
        Assert.Equal(shortName.Length, longName.Length);
        Assert.True(longName.Length <= 36);
    }

    [Fact]
    public void BuildPipeName_AllowsNullPrefix_AndRejectsNullBaseName()
    {
        var withoutPrefix = CrossProcessUtilsHarness.BuildPipeName("hello/world", prefix: null);

        Assert.DoesNotContain("/", withoutPrefix);
        Assert.DoesNotContain("+", withoutPrefix);

        Assert.Throws<ArgumentNullException>(() => CrossProcessUtilsHarness.BuildPipeName(null!, "P."));
    }

    [Fact]
    public void CreateOwned_RejectsBlankName_AndCanReuseLocalMutex()
    {
        Assert.Throws<ArgumentException>(() => LifecycleCrossProcessHarness.CreateOwnedMutex(" ", preferGlobal: false, out _, out _));

        var name = $"Extend0.Tests.LocalMutex.{Guid.NewGuid():N}";
        using var first = LifecycleCrossProcessHarness.CreateOwnedMutex(name, preferGlobal: false, out var firstCreated, out var firstIsGlobal);
        using var second = LifecycleCrossProcessHarness.CreateOwnedMutex(name, preferGlobal: false, out var secondCreated, out var secondIsGlobal);

        Assert.True(firstCreated);
        Assert.False(firstIsGlobal);
        Assert.False(secondCreated);
        Assert.False(secondIsGlobal);

        first.ReleaseMutex();
    }

    [Fact]
    public void CreateOwned_LocalMutex_EnforcesOwnershipUntilReleased()
    {
        var name = $"Extend0.Tests.LocalMutexOwnership.{Guid.NewGuid():N}";
        using var first = LifecycleCrossProcessHarness.CreateOwnedMutex(name, preferGlobal: false, out var firstCreated, out _);
        using var contenderReady = new ManualResetEventSlim();
        using var releaseOwner = new ManualResetEventSlim();
        var contenderCreated = true;
        var contenderCouldAcquireBeforeRelease = true;
        var contenderCouldAcquireAfterRelease = false;
        Exception? contenderFailure = null;

        var contenderThread = new Thread(() =>
        {
            try
            {
                using var contender = LifecycleCrossProcessHarness.CreateOwnedMutex(name, preferGlobal: false, out contenderCreated, out _);
                contenderCouldAcquireBeforeRelease = contender.WaitOne(0);
                contenderReady.Set();

                releaseOwner.Wait();

                contenderCouldAcquireAfterRelease = contender.WaitOne(TimeSpan.FromSeconds(2));
                if (contenderCouldAcquireAfterRelease)
                    contender.ReleaseMutex();
            }
            catch (Exception ex)
            {
                contenderFailure = ex;
                contenderReady.Set();
            }
        });

        contenderThread.Start();

        Assert.True(contenderReady.Wait(TimeSpan.FromSeconds(2)));

        Assert.True(firstCreated);
        Assert.False(contenderCreated);
        Assert.False(contenderCouldAcquireBeforeRelease);

        first.ReleaseMutex();
        releaseOwner.Set();

        Assert.True(contenderThread.Join(TimeSpan.FromSeconds(3)));
        Assert.Null(contenderFailure);
        Assert.True(contenderCouldAcquireAfterRelease);
    }
}
