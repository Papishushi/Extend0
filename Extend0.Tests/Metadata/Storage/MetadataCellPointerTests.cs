using Extend0.Metadata.Storage;

namespace Extend0.Tests.Metadata.Storage;

public sealed class MetadataCellPointerTests
{
    [Fact]
    public void Formatting_Parsing_AndPacking_RoundTrip()
    {
        var pointer = new MetadataCellPointer(row: 12, column: 34);

        Assert.Equal("12:34", pointer.ToString());
        Assert.Equal("12,34", pointer.ToString("C", null));
        Assert.Equal("C:22", pointer.ToString("X", null));
        Assert.True(MetadataCellPointer.TryParse("12:34", out var parsed));
        Assert.Equal(pointer, parsed);
        Assert.Equal(pointer, MetadataCellPointer.Parse("12,34"));
        Assert.Equal(pointer, MetadataCellPointer.FromPacked(pointer.ToPacked()));
        Assert.Equal((ulong)pointer, pointer.ToPacked());
        Assert.Equal(pointer, (MetadataCellPointer)(ulong)pointer);
    }

    [Fact]
    public void TryFormat_ReturnsFalse_WhenDestinationIsTooSmall()
    {
        var pointer = new MetadataCellPointer(row: 1234567890, column: 42);
        Span<char> chars = stackalloc char[2];
        Span<byte> bytes = stackalloc byte[2];

        var charOk = pointer.TryFormat(chars, out var writtenChars, default, provider: null);
        var byteOk = pointer.TryFormat(bytes, out var writtenBytes, default, provider: null);

        Assert.False(charOk);
        Assert.Equal(0, writtenChars);
        Assert.False(byteOk);
        Assert.Equal(0, writtenBytes);
    }

    [Fact]
    public void TryParse_ReturnsFalse_ForInvalidShapes()
    {
        Assert.False(MetadataCellPointer.TryParse("12", out _));
        Assert.False(MetadataCellPointer.TryParse(":12", out _));
        Assert.False(MetadataCellPointer.TryParse("12:", out _));
        Assert.False(MetadataCellPointer.TryParse("abc:def", out _));
        Assert.False(MetadataCellPointer.TryParse("0x10:0xZZ", out _));
        Assert.False(TryParseViaInterface(null, out _));
    }

    [Fact]
    public void Utf8TryParse_SupportsDecimalAndHex()
    {
        Assert.True(MetadataCellPointer.TryParse(" 15:16 "u8, provider: null, out var decimalValue));
        Assert.True(MetadataCellPointer.TryParse("0xA,0xF"u8, provider: null, out var hexValue));
        Assert.True(MetadataCellPointer.TryParse("0x1a:0X2B"u8, provider: null, out var mixedHexValue));

        Assert.Equal(new MetadataCellPointer(15, 16), decimalValue);
        Assert.Equal(new MetadataCellPointer(10, 15), hexValue);
        Assert.Equal(new MetadataCellPointer(26, 43), mixedHexValue);
        Assert.Equal(new MetadataCellPointer(10, 15), MetadataCellPointer.Parse("0xA,0xF"u8, provider: null));
    }

    [Fact]
    public void Utf8TryParse_ReturnsFalse_ForEmptyHexAndInvalidDecimal()
    {
        Assert.False(MetadataCellPointer.TryParse("0x:1"u8, provider: null, out _));
        Assert.False(MetadataCellPointer.TryParse("1:0X"u8, provider: null, out _));
        Assert.False(MetadataCellPointer.TryParse("1a:2"u8, provider: null, out _));
        Assert.False(MetadataCellPointer.TryParse("   "u8, provider: null, out _));
    }

    [Fact]
    public void Comparison_Equality_AndTupleConversions_Work()
    {
        var a = new MetadataCellPointer(1, 2);
        var b = new MetadataCellPointer(1, 3);
        (uint row, uint column) tuple = a;
        var fromTuple = (MetadataCellPointer)(row: 1u, column: 2u);
        a.Deconstruct(out var row, out var column);

        Assert.True(a == fromTuple);
        Assert.True(a != b);
        Assert.True(a.Equals((object)fromTuple));
        Assert.True(a.CompareTo(b) < 0);
        Assert.Equal((uint)1, tuple.row);
        Assert.Equal((uint)2, tuple.column);
        Assert.Equal((uint)1, row);
        Assert.Equal((uint)2, column);
    }

    [Fact]
    public void Parse_ThrowsForInvalidText()
    {
        Assert.Throws<FormatException>(() => MetadataCellPointer.Parse("bad"));
        Assert.Throws<FormatException>(() => ParseViaInterface("bad"));
        Assert.Throws<FormatException>(() => ParseSpanViaInterface("bad".AsSpan()));
    }

    private static bool TryParseViaInterface(string? value, out MetadataCellPointer result) =>
        TryParseViaGeneric(value, out result);

    private static MetadataCellPointer ParseViaInterface(string value) =>
        ParseViaGeneric<MetadataCellPointer>(value);

    private static MetadataCellPointer ParseSpanViaInterface(ReadOnlySpan<char> value) =>
        ParseSpanViaGeneric<MetadataCellPointer>(value);

    private static bool TryParseViaGeneric<T>(string? value, out T result)
        where T : IParsable<T>
    {
        if (T.TryParse(value, provider: null, out var parsed))
        {
            result = parsed;
            return true;
        }

        result = default!;
        return false;
    }

    private static T ParseViaGeneric<T>(string value)
        where T : IParsable<T> =>
        T.Parse(value, provider: null);

    private static T ParseSpanViaGeneric<T>(ReadOnlySpan<char> value)
        where T : ISpanParsable<T> =>
        T.Parse(value, provider: null);
}
