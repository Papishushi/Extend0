using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.Contract;
using Extend0.Metadata.CrossProcess.Internal;
using Extend0.Metadata.Internal;
using Extend0.Metadata.Schema;
using Extend0.Metadata.Storage;

namespace Extend0.Testing.Metadata.CrossProcess;

public static class MetaDBManagerRpcServiceHarness
{
    public static IMetaDBManagerRPCCompatible CreateInMemoryService(CapacityPolicy capacityPolicy = CapacityPolicy.Throw) =>
        new MetaDBManagerRPCCompatible(
            logger: null,
            factory: spec =>
            {
                var effective = spec is null
                    ? new TableSpec("Default", MapPath: null!, [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)])
                    : new TableSpec(spec.Value.Name, MapPath: null!, spec.Value.Columns);

                return new MetadataTable(effective);
            },
            capacityPolicy: capacityPolicy);
}
