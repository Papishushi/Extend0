using Extend0.Metadata;
using Extend0.Metadata.Schema;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;

var expectedVersion = args.Length == 1
    ? args[0]
    : throw new ArgumentException("Expected exactly one package-version argument.");
var productVersion = typeof(MetaDB).Assembly
    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
    ?.InformationalVersion
    .Split('+', 2)[0] ?? "unknown";
var tempRoot = Path.Combine(Path.GetTempPath(), "Extend0.ReleaseSmoke", Guid.NewGuid().ToString("N"));
var mapPath = Path.Combine(tempRoot, "arm64-smoke.meta");
var specPath = mapPath + ".tablespec.json";

try
{
    if (!string.Equals(productVersion, expectedVersion, StringComparison.Ordinal))
        throw new InvalidOperationException($"Core version '{productVersion}' does not match expected version '{expectedVersion}'.");
    if (RuntimeInformation.OSArchitecture != Architecture.Arm64)
        throw new PlatformNotSupportedException($"Expected ARM64, observed {RuntimeInformation.OSArchitecture}.");

    Directory.CreateDirectory(tempRoot);
    var spec = new TableSpec(
        "Arm64ReleaseSmoke",
        mapPath,
        [TableSpec.Helpers.Column<int>("Value", capacity: 4, keyBytes: 0)]);
    spec.SaveToFile(specPath);

    using (var manager = MetaDB.CreateManager())
    {
        var id = manager.RegisterTable(spec, createNow: true);
        manager.FillColumn<int>(id, column: 0, rows: 4, static row => checked((int)row + 1));
        if (!manager.CloseStrict(id))
            throw new InvalidOperationException("The created table could not be closed.");
    }

    using (var restartedManager = MetaDB.CreateManager())
    {
        var reopened = restartedManager.Open(mapPath, forceRelocation: true);
        if (!string.Equals(reopened.Table.Spec.Name, spec.Name, StringComparison.Ordinal))
            throw new InvalidOperationException("The reopened table did not preserve its specification.");
        if (!restartedManager.CloseStrict(reopened.Id))
            throw new InvalidOperationException("The reopened table could not be closed.");
    }

    File.Delete(mapPath);
    File.Delete(specPath);
    if (File.Exists(mapPath) || File.Exists(specPath))
        throw new IOException("MetaDB smoke files remain after cleanup.");

    Console.WriteLine(JsonSerializer.Serialize(new
    {
        version = productVersion,
        runtime_identifier = RuntimeInformation.RuntimeIdentifier,
        architecture = RuntimeInformation.OSArchitecture.ToString(),
        metadb_ready = true,
        create = true,
        restart = true,
        cleanup = true
    }));
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine(JsonSerializer.Serialize(new
    {
        version = productVersion,
        runtime_identifier = RuntimeInformation.RuntimeIdentifier,
        architecture = RuntimeInformation.OSArchitecture.ToString(),
        metadb_ready = false,
        error = ex.Message
    }));
    return 1;
}
finally
{
    if (Directory.Exists(tempRoot))
        Directory.Delete(tempRoot, recursive: true);
}
