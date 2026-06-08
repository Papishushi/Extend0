namespace Extend0.Tests.Core;

public sealed class ByteArrayComparerTests
{
    [Fact]
    public void Ordinal_Equals_ReturnsTrue_ForEqualArrays()
    {
        var left = new byte[] { 1, 2, 3, 4 };
        var right = new byte[] { 1, 2, 3, 4 };

        Assert.True(ByteArrayComparer.Ordinal.Equals(left, right));
        Assert.Equal(ByteArrayComparer.Ordinal.GetHashCode(left), ByteArrayComparer.Ordinal.GetHashCode(right));
    }

    [Fact]
    public void Ordinal_Equals_ReturnsFalse_ForDifferentArrays()
    {
        var left = new byte[] { 1, 2, 3, 4 };
        var right = new byte[] { 1, 2, 3, 5 };

        Assert.False(ByteArrayComparer.Ordinal.Equals(left, right));
    }

    [Fact]
    public void Ordinal_GetHashCode_Throws_ForNullArray()
    {
        Assert.Throws<ArgumentNullException>(() => ByteArrayComparer.Ordinal.GetHashCode((byte[])null!));
    }

    [Fact]
    public void GetHashCode64_IsStable_ForSameInput()
    {
        ReadOnlySpan<byte> bytes = [9, 8, 7, 6, 5];

        var first = ByteArrayComparer.GetHashCode64(bytes);
        var second = ByteArrayComparer.GetHashCode64(bytes);

        Assert.Equal(first, second);
    }

    [Fact]
    public void Span_AndReadOnlySpan_Overloads_CompareAndHash_ByValue()
    {
        Span<byte> left = [1, 2, 3];
        Span<byte> same = [1, 2, 3];
        Span<byte> different = [1, 2, 4];
        ReadOnlySpan<byte> readOnlyLeft = left;
        ReadOnlySpan<byte> readOnlySame = same;
        ReadOnlySpan<byte> readOnlyDifferent = different;

        Assert.True(ByteArrayComparer.Ordinal.Equals(left, same));
        Assert.False(ByteArrayComparer.Ordinal.Equals(left, different));
        Assert.True(ByteArrayComparer.Ordinal.Equals(readOnlyLeft, readOnlySame));
        Assert.False(ByteArrayComparer.Ordinal.Equals(readOnlyLeft, readOnlyDifferent));

        Assert.Equal(ByteArrayComparer.Ordinal.GetHashCode(left), ByteArrayComparer.Ordinal.GetHashCode(same));
        Assert.Equal(ByteArrayComparer.Ordinal.GetHashCode(readOnlyLeft), ByteArrayComparer.Ordinal.GetHashCode(readOnlySame));
    }
}
