using Extend0.Metadata.Indexing.Internal;

namespace Extend0.Testing.Metadata.Indexing.Internal;

public static class IndexKeyScratchHarness
{
    public static byte[] GetScratch(int size) => IndexKeyScratch.GetScratch(size);

    public static byte[] Fill(byte[] scratch, ReadOnlySpan<byte> key) => IndexKeyScratch.Fill(scratch, key);
}
