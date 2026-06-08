using Extend0.Metadata.Contract;
using Extend0.Metadata.CodeGen;
using System.Reflection;

namespace Extend0.Tests.Metadata.CodeGen;

public sealed class MetadataCellGeneratedCoverageTests
{
    public static IEnumerable<object[]> AllGeneratedEntrySizes()
        => Enum.GetValues<MetadataEntrySize>().Select(static size => new object[] { size });

    [Theory]
    [MemberData(nameof(AllGeneratedEntrySizes))]
    public void MetadataCell_RoundTripKeyValue_AndEquality_Work_ForEveryGeneratedShape(MetadataEntrySize size)
    {
        var key = "key"u8.ToArray();
        var wrongKey = "zzz"u8.ToArray();
        const string value = "v";

        var cell = new MetadataCell(size);
        try
        {
            Assert.True(cell.KeySize > 0);
            Assert.True(cell.ValueSize > 0);

            Assert.True(cell.TrySetKey(key));
            Assert.True(cell.HasKeyRaw());
            Assert.True(cell.KeyEquals(key));
            Assert.False(cell.KeyEquals(wrongKey));

            Assert.True(cell.TrySetValue(value));
            Assert.True(cell.HasAnyValueRaw());

            Assert.True(cell.TryGetKeyUtf8(out string? keyUtf8));
            Assert.Equal("key", keyUtf8);

            Assert.True(cell.TryGetValueUtf8("key", out string? valueUtf8));
            Assert.Equal(value, valueUtf8);

            Assert.True(cell.TryGetKeyRaw(out var keyRaw));
            Assert.Equal((byte)'k', keyRaw[0]);

            Assert.True(cell.TryGetValueRaw(out var valueRaw));
            Assert.Equal((byte)'v', valueRaw[0]);

            Assert.True(cell.KeySize >= key.Length);
            Assert.True(cell.ValueSize >= 1);
        }
        finally
        {
            cell.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(AllGeneratedEntrySizes))]
    public void MetadataCell_RejectsOversizedKeyAndValue_ForEveryGeneratedShape(MetadataEntrySize size)
    {
        var cell = new MetadataCell(size);
        try
        {
            var tooLargeKey = new byte[cell.KeySize + 1];
            var tooLargeValue = new byte[cell.ValueSize + 1];

            Assert.False(cell.TrySetKey(tooLargeKey));
            Assert.False(cell.TrySetValue(tooLargeValue));
            Assert.False(cell.HasKeyRaw());
            Assert.False(cell.HasAnyValueRaw());
        }
        finally
        {
            cell.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(AllGeneratedEntrySizes))]
    public void MetadataEntryStruct_DirectApi_Works_ForEveryGeneratedShape(MetadataEntrySize size)
    {
        var generatedType = typeof(MetadataCell).Assembly.GetType(size.GetGeneratedTypeName(), throwOnError: true)!;
        var assertion = typeof(MetadataCellGeneratedCoverageTests)
            .GetMethod(nameof(AssertGeneratedEntryShape), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(generatedType);

        assertion.Invoke(null, [size]);
    }

    private static unsafe void AssertGeneratedEntryShape<TEntry>(MetadataEntrySize size)
        where TEntry : unmanaged, IMetadataEntry
    {
        Assert.Equal(size.GetKeySize(), TEntry.KeyCapacity);
        Assert.Equal(size.GetValueSize(), TEntry.ValueCapacity);

        var entry = Activator.CreateInstance<TEntry>();

        Assert.True(entry.TrySetKey("key"));
        Assert.True(entry.TrySetValue("value"));
        Assert.True(entry.TrySetKey("bytes"u8));
        Assert.True(entry.TrySetValue("payload"u8));
        Assert.False(entry.TrySetKey(new byte[TEntry.KeyCapacity]));
        Assert.False(entry.TrySetValue(new byte[TEntry.ValueCapacity]));
        Assert.False(entry.TrySetKey(new string('k', TEntry.KeyCapacity)));
        Assert.False(entry.TrySetValue(new string('v', TEntry.ValueCapacity)));

        Assert.True(entry.TrySetKey("key"));
        Assert.True(entry.TrySetValue("value"));

        var entryPtr = &entry;
        var entryBytes = (byte*)entryPtr;
        var valuePtr = TEntry.GetValuePointer(entryPtr);

        Assert.Equal(TEntry.KeyCapacity, (int)(valuePtr - entryBytes));
        Assert.Equal((byte)'v', *valuePtr);
        Assert.True(TEntry.KeyEquals(entryPtr, "key"u8));
        Assert.True(TEntry.KeyEquals(entryPtr, "key"));
        Assert.False(TEntry.KeyEquals(entryPtr, "wrong"u8));
        Assert.False(TEntry.KeyEquals(entryPtr, "wrong"));
        Assert.False(TEntry.KeyEquals(entryPtr, new byte[TEntry.KeyCapacity]));
        Assert.False(TEntry.KeyEquals(entryPtr, new string('k', TEntry.KeyCapacity)));
    }
}
