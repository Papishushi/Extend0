using Extend0.Metadata.Refs;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Typed;
using Extend0.Testing.Metadata.Internal;

namespace Extend0.Tests.Metadata.Typed;

public sealed class MetadataTypedColumnTests
{
    [Fact]
    public void TypedColumns_ReadWriteValuesUtf8AndReferenceVectors()
    {
        using var table = MetadataTableHarness.CreateInMemoryTable(
            "ClusterNodes",
            TableSpec.Helpers.Column<Guid>("node_id", capacity: 2, keyBytes: 0),
            TableSpec.Helpers.Column<int>("connection_count", capacity: 2, keyBytes: 0),
            TableSpec.Helpers.Column("node_id_name_pair", capacity: 2, keyBytes: 16, valueBytes: 64),
            TableSpec.Helpers.RefsColumn(capacity: 2, name: "services", keyBytes: 16, refsPerCell: 2));

        var nodeId = new MetadataValueColumn<Guid>(table, column: 0, name: "node_id", keySize: 0, valueSize: 16);
        var connections = new MetadataValueColumn<int>(table, column: 1, name: "connection_count", keySize: 0, valueSize: 4);
        var nodeName = new MetadataUtf8Column(table, column: 2, name: "node_id_name_pair", keySize: 16, valueSize: 64);
        var services = new MetadataRefsColumn(table, column: 3, name: "services", keySize: 16, valueSize: TableSpec.Helpers.RefSize * 2);

        var id = Guid.NewGuid();
        nodeId.Set(row: 0, id);
        connections.Set(row: 0, 42);
        nodeName.Set(row: 0, "vallecas-worker-01");
        Assert.True(nodeName.TrySetKey(row: 0, "node-a"));

        Assert.Equal(id, nodeId.Get(row: 0));
        Assert.Equal(42, connections.Get(row: 0));
        Assert.Equal("vallecas-worker-01", nodeName.Get(row: 0));
        Assert.True(nodeName.TryGetCell(row: 0, out var nodeNameCell));
        Assert.True(nodeNameCell.KeyEquals("node-a"u8));

        var childTableId = Guid.NewGuid();
        Assert.True(services.Init(row: 0));
        Assert.True(services.TryAdd(row: 0, childTableId, childColumn: 2, childRow: 7));
        Assert.Equal(1, services.Count(row: 0));
        Assert.Equal(0, services.Find(row: 0, childTableId, childColumn: 2, childRow: 7));

        var reference = services.Get(row: 0, index: 0);
        Assert.Equal(new MetadataTableRef(childTableId, column: 2, row: 7, reserved: 0), reference);

        Assert.True(services.TryRemoveAt(row: 0, index: 0));
        Assert.Equal(0, services.Count(row: 0));
    }

    [Fact]
    public void TypedColumnConstructor_RejectsSpecMismatch()
    {
        using var table = MetadataTableHarness.CreateInMemoryTable(
            "Mismatch",
            TableSpec.Helpers.Column<Guid>("actual_id", capacity: 1, keyBytes: 0));

        var ex = Assert.Throws<InvalidOperationException>(
            () => new MetadataValueColumn<Guid>(table, column: 0, name: "node_id", keySize: 0, valueSize: 16));

        Assert.Contains("expected 'node_id'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
