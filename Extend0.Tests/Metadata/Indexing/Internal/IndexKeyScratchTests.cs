using Extend0.Testing.Metadata.Indexing.Internal;

namespace Extend0.Tests.Metadata.Indexing.Internal;

public sealed class IndexKeyScratchTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(256)]
    [InlineData(512)]
    public void GetScratch_ReusesCommonThreadLocalBuffers(int size)
    {
        var first = IndexKeyScratchHarness.GetScratch(size);
        var second = IndexKeyScratchHarness.GetScratch(size);

        Assert.Same(first, second);
        Assert.Equal(size, first.Length);
    }

    [Fact]
    public void GetScratch_CachesUncommonSizes_Separately()
    {
        var first = IndexKeyScratchHarness.GetScratch(7);
        var second = IndexKeyScratchHarness.GetScratch(7);
        var third = IndexKeyScratchHarness.GetScratch(9);

        Assert.Same(first, second);
        Assert.NotSame(first, third);
        Assert.Equal(7, first.Length);
        Assert.Equal(9, third.Length);
    }

    [Fact]
    public void Fill_CopiesKey_AndClearsTrailingBytes()
    {
        var scratch = new byte[] { 1, 2, 3, 4, 5, 6 };
        var returned = IndexKeyScratchHarness.Fill(scratch, [0x41, 0x42, 0x43]);

        Assert.Same(scratch, returned);
        Assert.Equal([0x41, 0x42, 0x43, 0, 0, 0], scratch);
    }
}
