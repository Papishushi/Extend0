using Extend0.Metadata.Refs;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;
using Extend0.Testing.Metadata.Internal;

namespace Extend0.Tests.Metadata.Refs;

public sealed class MetadataRefLinkerTests
{
    [Fact]
    public void EnsureLinkRefNoDup_IsIdempotentByTriple()
    {
        using var parent = MetadataTableHarness.CreateInMemoryTable(
            "Parents",
            TableSpec.Helpers.RefsColumn(4));

        var childTableId = Guid.NewGuid();

        MetadataRefLinker.EnsureLinkRefNoDup(parent, refsCol: 0, parentRow: 0, childTableId, childCol: 1, childRow: 2, childKey: 7, policy: CapacityPolicy.Throw);
        MetadataRefLinker.EnsureLinkRefNoDup(parent, refsCol: 0, parentRow: 0, childTableId, childCol: 1, childRow: 2, childKey: 99, policy: CapacityPolicy.Throw);
        MetadataRefLinker.EnsureLinkRefNoDup(parent, refsCol: 0, parentRow: 0, childTableId, childCol: 1, childRow: 3, childKey: 8, policy: CapacityPolicy.Throw);

        Assert.Equal(2, MetaDBManagerHelpersHarness.GetRefCount(parent, 0, 0));
        Assert.True(MetaDBManagerHelpersHarness.TryHasRef(parent, 0, 0, new MetadataTableRef(childTableId, 1, 2, 7)));
        Assert.True(MetaDBManagerHelpersHarness.TryHasRef(parent, 0, 0, new MetadataTableRef(childTableId, 1, 3, 8)));
    }

    [Fact]
    public void EnsureLinkRefNoDupByKey_AndEnsureRefVec_AreIdempotent()
    {
        using var parent = MetadataTableHarness.CreateInMemoryTable(
            "Parents",
            TableSpec.Helpers.RefsColumn(4));

        var childA = Guid.NewGuid();
        var childB = Guid.NewGuid();

        MetadataRefLinker.EnsureRefVec(parent, refsCol: 0, parentRow: 0, policy: CapacityPolicy.Throw);
        MetadataRefLinker.EnsureRefVec(parent, refsCol: 0, parentRow: 0, policy: CapacityPolicy.Throw);

        MetadataRefLinker.EnsureLinkRefNoDupByKey(parent, refsCol: 0, parentRow: 0, childA, childCol: 1, childRow: 2, childKey: 77, policy: CapacityPolicy.Throw);
        MetadataRefLinker.EnsureLinkRefNoDupByKey(parent, refsCol: 0, parentRow: 0, childB, childCol: 9, childRow: 9, childKey: 77, policy: CapacityPolicy.Throw);

        Assert.Equal(1, MetaDBManagerHelpersHarness.GetRefCount(parent, 0, 0));
        Assert.True(MetaDBManagerHelpersHarness.TryHasRef(parent, 0, 0, new MetadataTableRef(childA, 1, 2, 77)));
        Assert.True(MetadataRefLinker.TryFindChildByKey(parent, refsCol: 0, parentRow: 0, childKey: 77, out var foundChildId));
        Assert.Equal(childA, foundChildId);
    }
}
