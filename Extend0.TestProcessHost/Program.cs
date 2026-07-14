using Extend0.Metadata.Schema;
using Extend0.Testing.Metadata.Storage;

if (args.Length != 4 || !string.Equals(args[0], "hold-mapped-store", StringComparison.Ordinal))
{
    Console.Error.WriteLine("Usage: Extend0.TestProcessHost hold-mapped-store <map-path> <ready-path> <release-path>");
    return 64;
}

var mapPath = args[1];
var readyPath = args[2];
var releasePath = args[3];
var spec = new TableSpec(
    "CrossProcessLease",
    mapPath,
    [TableSpec.Helpers.Column("Value", 1, valueBytes: 64)]);

using var store = MetadataStorageHarness.CreateMappedStore(spec);
File.WriteAllText(readyPath, Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));

while (!File.Exists(releasePath))
    await Task.Delay(10).ConfigureAwait(false);

return 0;
