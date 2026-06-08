using System.Runtime.CompilerServices;
using Extend0.Metadata.Refs;

namespace Extend0.Tests.Metadata.Refs;

public sealed class MetadataTableRefTests
{
    [Fact]
    public void MetadataTableRef_HasExpectedPackedSize()
    {
        Assert.Equal(32, Unsafe.SizeOf<MetadataTableRef>());
    }

    [Fact]
    public void MetadataTableRef_Equality_UsesAllFields()
    {
        var tableId = Guid.NewGuid();
        var left = new MetadataTableRef(tableId, 1, 2, 3);
        var same = new MetadataTableRef(tableId, 1, 2, 3);
        var different = new MetadataTableRef(tableId, 1, 2, 4);

        Assert.Equal(left, same);
        Assert.NotEqual(left, different);
        Assert.Equal(left.GetHashCode(), same.GetHashCode());
    }
}
