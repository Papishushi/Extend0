using Xunit;

namespace Extend0.Tests.Metadata.Storage;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MappedStorageCollection
{
    public const string Name = "MappedStorage";
}
