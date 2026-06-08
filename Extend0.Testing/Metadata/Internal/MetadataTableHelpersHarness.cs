using Extend0.Metadata.Internal;

namespace Extend0.Testing.Metadata.Internal;

public static class MetadataTableHelpersHarness
{
    public static string Border(uint columnCount, int[] widths) =>
        MetadataTableHelpers.Border(columnCount, widths);

    public static string Pad(string value, int width) =>
        MetadataTableHelpers.Pad(value, width);

    public static string Preview(ReadOnlySpan<byte> value, int maxChars) =>
        MetadataTableHelpers.Preview(value, maxChars);
}
