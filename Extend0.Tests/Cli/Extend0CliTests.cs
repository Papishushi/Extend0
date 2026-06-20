using System.Text.Json;
using System.Xml.Linq;
using Extend0.Cli;
using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.Schema;
using Extend0.Testing.Lifecycle.CrossProcess;
using Extend0.Testing.Metadata.Storage;

namespace Extend0.Tests.Cli;

public sealed class Extend0CliTests
{
    [Fact]
    public async Task Help_PrintsUsage()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(["--help"], output, error, Directory.GetCurrentDirectory());

        Assert.Equal(0, exitCode);
        Assert.Contains("extend0 doctor", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("extend0 lifecycle probe", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("extend0 metadb validate", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("extend0 metadb schema", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("extend0 metadb snapshot", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("extend0 metadb restore", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("extend0 ontology inspect", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("extend0 ontology validate", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(string.Empty, error.ToString());
    }

    [Fact]
    public async Task UnknownCommand_ReturnsUsageError()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(["wat"], output, error, Directory.GetCurrentDirectory());

        Assert.Equal(2, exitCode);
        Assert.Contains("Unknown command", error.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CliProject_IsConfiguredAsDotnetTool()
    {
        var projectPath = Path.Combine(FindRepositoryRoot(), "Extend0.Cli", "Extend0.Cli.csproj");
        var document = XDocument.Load(projectPath);

        Assert.Equal("true", GetProjectProperty(document, "IsPackable"), ignoreCase: true);
        Assert.Equal("true", GetProjectProperty(document, "PackAsTool"), ignoreCase: true);
        Assert.Equal("Extend0.Cli", GetProjectProperty(document, "PackageId"));
        Assert.Equal("extend0", GetProjectProperty(document, "ToolCommandName"));
    }

    [Fact]
    public async Task LifecycleProbe_WithDefaultNamedPipe_PrintsResolvedProtocolAndEndpoint()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(["lifecycle", "probe"], output, error, Directory.GetCurrentDirectory());

        var text = output.ToString();
        Assert.Equal(0, exitCode);
        Assert.Contains("Extend0 lifecycle probe", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Transport: NamedPipe", text, StringComparison.Ordinal);
        Assert.Contains("Protocol: extend0-jsonrpc-ndjson v1", text, StringComparison.Ordinal);
        Assert.Contains("Ownership: not acquired", text, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(string.Empty, error.ToString());
    }

    [Fact]
    public async Task LifecycleProbe_WithCustomTransportAndExplicitProtocol_CanEmitJson()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(
            [
                "lifecycle", "probe",
                "--transport", "Custom",
                "--allow-custom",
                "--protocol-id", "custom-wire",
                "--protocol-version", "2",
                "--name", "extend0.custom.service",
                "--json"
            ],
            output,
            error,
            Directory.GetCurrentDirectory());

        using var document = JsonDocument.Parse(output.ToString());
        Assert.Equal(0, exitCode);
        Assert.Equal("Custom", document.RootElement.GetProperty("TransportKind").GetString());
        Assert.Equal("extend0.custom.service", document.RootElement.GetProperty("EndpointName").GetString());
        Assert.Equal("custom-wire", document.RootElement.GetProperty("ProtocolId").GetString());
        Assert.Equal(2, document.RootElement.GetProperty("ProtocolVersion").GetInt32());
        Assert.Equal(0, document.RootElement.GetProperty("ErrorCount").GetInt32());
        Assert.Equal(string.Empty, error.ToString());
    }

    [Fact]
    public async Task LifecycleProbe_WithUnsupportedBuiltInTransport_ReturnsFailureReport()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(
            ["lifecycle", "probe", "--transport", "WebSocket"],
            output,
            error,
            Directory.GetCurrentDirectory());

        Assert.Equal(1, exitCode);
        Assert.Contains("does not have a built-in protocol descriptor", output.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(string.Empty, error.ToString());
    }

    [Fact]
    public async Task LifecycleProbe_WithTcpSocketEndpoint_ResolvesBuiltInTransport()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(
            ["lifecycle", "probe", "--transport", "TcpSocket", "--endpoint", "127.0.0.1:43001", "--json"],
            output,
            error,
            Directory.GetCurrentDirectory());

        using var document = JsonDocument.Parse(output.ToString());
        Assert.Equal(0, exitCode);
        Assert.Equal("TcpSocket", document.RootElement.GetProperty("TransportKind").GetString());
        Assert.Equal("127.0.0.1:43001", document.RootElement.GetProperty("EndpointName").GetString());
        Assert.Equal("extend0-jsonrpc-ndjson", document.RootElement.GetProperty("ProtocolId").GetString());
        Assert.True(document.RootElement.GetProperty("BuiltInClientAvailable").GetBoolean());
        Assert.Equal(0, document.RootElement.GetProperty("ErrorCount").GetInt32());
        Assert.Equal(string.Empty, error.ToString());
    }

    [Fact]
    public async Task LifecycleProbe_WithUnixDomainSocket_ResolvesBuiltInTransport()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(
            ["lifecycle", "probe", "--transport", "UnixDomainSocket", "--json"],
            output,
            error,
            Directory.GetCurrentDirectory());

        using var document = JsonDocument.Parse(output.ToString());
        Assert.Equal(0, exitCode);
        Assert.Equal("UnixDomainSocket", document.RootElement.GetProperty("TransportKind").GetString());
        Assert.EndsWith(".sock", document.RootElement.GetProperty("EndpointName").GetString(), StringComparison.Ordinal);
        Assert.Equal("extend0-jsonrpc-ndjson", document.RootElement.GetProperty("ProtocolId").GetString());
        Assert.True(document.RootElement.GetProperty("BuiltInClientAvailable").GetBoolean());
        Assert.Equal(0, document.RootElement.GetProperty("ErrorCount").GetInt32());
        Assert.Equal(string.Empty, error.ToString());
    }

    [Fact]
    public async Task LifecycleProbe_WithLiveNamedPipeOwner_ConnectsAndValidatesHandshake()
    {
        var endpointName = LifecycleCrossProcessHarness.BuildNamedPipeEndpointName($"cli-probe-{Guid.NewGuid():N}");
        await using var server = LifecycleCrossProcessHarness.CreateNamedPipeServer(
            endpointName,
            new CliProbeService(endpointName),
            CancellationToken.None);

        using var output = new StringWriter();
        using var error = new StringWriter();

        var exitCode = await Extend0Cli.RunAsync(
            ["lifecycle", "probe", "--endpoint", endpointName, "--connect", "--json"],
            output,
            error,
            Directory.GetCurrentDirectory());

        using var document = JsonDocument.Parse(output.ToString());
        Assert.Equal(0, exitCode);
        Assert.True(document.RootElement.GetProperty("ConnectAttempted").GetBoolean());
        Assert.True(document.RootElement.GetProperty("Connected").GetBoolean());
        Assert.Equal(0, document.RootElement.GetProperty("ErrorCount").GetInt32());
        Assert.Equal(string.Empty, error.ToString());
    }

    [Fact]
    public async Task Doctor_WithHealthyRepo_PrintsSuccessfulHumanReport()
    {
        var root = CreateHealthyRepository();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["doctor", "--repo", root], output, error, root);

            var text = output.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains("Extend0 doctor", text, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("[ok] solution", text, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("0 errors", text, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Doctor_WithHealthyRepo_CanEmitJson()
    {
        var root = CreateHealthyRepository();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["doctor", "--repo", root, "--json"], output, error, root);

            using var document = JsonDocument.Parse(output.ToString());
            Assert.Equal(0, exitCode);
            Assert.Equal(0, document.RootElement.GetProperty("ErrorCount").GetInt32());
            Assert.True(document.RootElement.GetProperty("PassCount").GetInt32() > 0);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Doctor_WithMissingCoreFiles_ReturnsFailure()
    {
        var root = CreateTempDirectory();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["doctor", "--repo", root], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("[error] solution", output.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Doctor_WhenCliToolPackagingIsBroken_ReturnsFailure()
    {
        var root = CreateHealthyRepository();
        try
        {
            Write(root, Path.Combine("Extend0.Cli", "Extend0.Cli.csproj"), """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net10.0</TargetFramework>
                    <IsPackable>true</IsPackable>
                    <PackAsTool>false</PackAsTool>
                    <PackageId>Extend0.Cli</PackageId>
                    <ToolCommandName>extend0</ToolCommandName>
                  </PropertyGroup>
                </Project>
                """);

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["doctor", "--repo", root], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("cli-tool-packaging", output.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbInspect_WithSidecarSpec_PrintsColumnReport()
    {
        var root = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(root, "settings.meta");
            var spec = new TableSpec("Settings", mapPath,
            [
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 64, valueBytes: 512)
            ]);
            spec.SaveToFile(mapPath + ".tablespec.json");

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "inspect", mapPath], output, error, root);

            var text = output.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains("Extend0 MetaDB inspect", text, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Name: Settings", text, StringComparison.Ordinal);
            Assert.Contains("[0] Entries: key=64, value=512", text, StringComparison.Ordinal);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbInspect_WithChunkedDirectory_CanEmitJson()
    {
        var root = CreateTempDirectory();
        try
        {
            var tableDirectory = Path.Combine(root, "chunked-settings");
            var spec = new TableSpec("ChunkedSettings", tableDirectory,
            [
                TableSpec.Helpers.Column("Value", capacity: 2, keyBytes: 16, valueBytes: 64)
            ])
            {
                Storage = TableStorageOptions.Chunked(chunkSize: 1024)
            };
            spec.SaveToFile(Path.Combine(tableDirectory, "tablespec.json"));

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "inspect", tableDirectory, "--json"], output, error, root);

            using var document = JsonDocument.Parse(output.ToString());
            Assert.Equal(0, exitCode);
            Assert.Equal("ChunkedSettings", document.RootElement.GetProperty("Name").GetString());
            Assert.Equal(1, document.RootElement.GetProperty("ColumnCount").GetInt32());
            Assert.Equal("Chunked", document.RootElement.GetProperty("Storage").GetProperty("Layout").GetString());
            Assert.Equal(1024, document.RootElement.GetProperty("Storage").GetProperty("ChunkSize").GetInt32());
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbInspect_WhenSpecIsMissing_ReturnsFailure()
    {
        var root = CreateTempDirectory();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "inspect", "missing.meta"], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("No TableSpec found", error.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbValidate_WithHealthySidecarSpec_PrintsSuccessfulReport()
    {
        var root = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(root, "settings.meta");
            var spec = new TableSpec("Settings", mapPath,
            [
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 16, valueBytes: 64)
            ]);
            spec.SaveToFile(mapPath + ".tablespec.json");

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "validate", mapPath], output, error, root);

            var text = output.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains("Extend0 MetaDB validate", text, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Estimated logical bytes: 320", text, StringComparison.Ordinal);
            Assert.Contains("0 errors", text, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbValidate_WithDuplicateColumns_ReturnsFailure()
    {
        var root = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(root, "settings.meta");
            var spec = new TableSpec("Settings", mapPath,
            [
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 16, valueBytes: 64),
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 16, valueBytes: 128)
            ]);
            spec.SaveToFile(mapPath + ".tablespec.json");

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "validate", mapPath], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("duplicate-column-name", output.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbValidate_WithChunkSmallerThanEntry_ReturnsFailure()
    {
        var root = CreateTempDirectory();
        try
        {
            var tableDirectory = Path.Combine(root, "chunked-settings");
            var spec = new TableSpec("ChunkedSettings", tableDirectory,
            [
                TableSpec.Helpers.Column("Huge", capacity: 1, keyBytes: 16, valueBytes: 128)
            ])
            {
                Storage = TableStorageOptions.Chunked(chunkSize: 64)
            };
            spec.SaveToFile(Path.Combine(tableDirectory, "tablespec.json"));

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "validate", tableDirectory], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("chunk-entry-size", output.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbValidate_WithMaterializedSingleFileTable_ValidatesRuntimeHeader()
    {
        var root = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(root, "runtime.meta");
            var spec = new TableSpec("RuntimeSettings", mapPath,
            [
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 16, valueBytes: 64)
            ]);

            using (MetadataStorageHarness.CreateMappedStore(spec))
            {
            }

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "validate", mapPath, "--json"], output, error, root);

            using var document = JsonDocument.Parse(output.ToString());
            Assert.Equal(0, exitCode);
            Assert.True(document.RootElement.GetProperty("RuntimeStorage").GetProperty("Exists").GetBoolean());
            Assert.True(document.RootElement.GetProperty("RuntimeStorage").GetProperty("PhysicalBytes").GetInt64() > 0);
            Assert.Equal(0, document.RootElement.GetProperty("ErrorCount").GetInt32());
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbValidate_WithMissingChunkFile_ReturnsFailure()
    {
        var root = CreateTempDirectory();
        try
        {
            var tableDirectory = Path.Combine(root, "chunked-runtime");
            var spec = new TableSpec("ChunkedRuntime", tableDirectory,
            [
                TableSpec.Helpers.Column("Value", capacity: 1, keyBytes: 16, valueBytes: 64)
            ])
            {
                Storage = TableStorageOptions.Chunked(chunkSize: 256)
            };

            using (MetadataStorageHarness.CreateSegmentedMappedStore(spec))
            {
            }

            File.Delete(Path.Combine(tableDirectory, "chunks", "c0000_000000.chk"));

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "validate", tableDirectory], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("chunked-chunk-missing", output.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbSchema_WithAddedColumnAndVersionBump_PrintsMigrationPlan()
    {
        var root = CreateTempDirectory();
        try
        {
            var sourcePath = Path.Combine(root, "source.tablespec.json");
            var targetPath = Path.Combine(root, "target.tablespec.json");
            var source = new TableSpec("Settings", Path.Combine(root, "settings.meta"),
            [
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 16, valueBytes: 64)
            ]);
            var target = source with
            {
                SchemaVersion = 2,
                Columns =
                [
                    .. source.Columns,
                    TableSpec.Helpers.Column("Labels", capacity: 4, keyBytes: 16, valueBytes: 64)
                ]
            };
            source.SaveToFile(sourcePath);
            target.SaveToFile(targetPath);

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "schema", sourcePath, targetPath], output, error, root);

            var text = output.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains("Compatibility: RequiresMigration", text, StringComparison.Ordinal);
            Assert.Contains("AddColumn", text, StringComparison.Ordinal);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbSchema_WithSameVersionStructuralChange_ReturnsFailure()
    {
        var root = CreateTempDirectory();
        try
        {
            var sourcePath = Path.Combine(root, "source.tablespec.json");
            var targetPath = Path.Combine(root, "target.tablespec.json");
            var source = new TableSpec("Settings", Path.Combine(root, "settings.meta"),
            [
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 16, valueBytes: 64)
            ]);
            var target = source with
            {
                Columns =
                [
                    .. source.Columns,
                    TableSpec.Helpers.Column("Labels", capacity: 4, keyBytes: 16, valueBytes: 64)
                ]
            };
            source.SaveToFile(sourcePath);
            target.SaveToFile(targetPath);

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["metadb", "schema", sourcePath, targetPath], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("same-version-structural-change", output.ToString(), StringComparison.Ordinal);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MetaDbSnapshotAndRestore_WithSingleFileTable_RoundTripsRuntimeStorage()
    {
        var root = CreateTempDirectory();
        try
        {
            var mapPath = Path.Combine(root, "runtime.meta");
            var spec = new TableSpec("RuntimeSettings", mapPath,
            [
                TableSpec.Helpers.Column("Entries", capacity: 4, keyBytes: 16, valueBytes: 64)
            ])
            {
                SchemaVersion = 2
            };

            using (MetadataStorageHarness.CreateMappedStore(spec))
            {
            }

            var snapshotDirectory = Path.Combine(root, "snapshot");
            using (var output = new StringWriter())
            using (var error = new StringWriter())
            {
                var snapshotExitCode = await Extend0Cli.RunAsync(
                    ["metadb", "snapshot", mapPath, "--out", snapshotDirectory, "--label", "cli-test"],
                    output,
                    error,
                    root);

                Assert.Equal(0, snapshotExitCode);
                Assert.Contains("Runtime storage captured: True", output.ToString(), StringComparison.Ordinal);
                Assert.Equal(string.Empty, error.ToString());
            }

            var restoreMapPath = Path.Combine(root, "restored.meta");
            using (var output = new StringWriter())
            using (var error = new StringWriter())
            {
                var restoreExitCode = await Extend0Cli.RunAsync(
                    ["metadb", "restore", snapshotDirectory, "--map-path", restoreMapPath],
                    output,
                    error,
                    root);

                Assert.Equal(0, restoreExitCode);
                Assert.Contains("Extend0 MetaDB restore", output.ToString(), StringComparison.OrdinalIgnoreCase);
                Assert.Equal(string.Empty, error.ToString());
            }

            Assert.True(File.Exists(restoreMapPath));
            Assert.True(File.Exists(restoreMapPath + ".tablespec.json"));
            Assert.True(MetadataStorageHarness.TryLoadMappedColumns(restoreMapPath, out var columns));
            Assert.Single(columns);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task OntologyInspect_WithHealthyRepo_PrintsTBoxSummary()
    {
        var root = CreateHealthyRepository();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["ontology", "inspect", "--repo", root], output, error, root);

            var text = output.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains("Extend0 ontology inspect", text, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Version: 1.2.3", text, StringComparison.Ordinal);
            Assert.Contains("Classes: 9", text, StringComparison.Ordinal);
            Assert.Contains("Object properties: 1", text, StringComparison.Ordinal);
            Assert.Contains("- Extend0Concept", text, StringComparison.Ordinal);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task OntologyInspect_WithHealthyRepo_CanEmitJson()
    {
        var root = CreateHealthyRepository();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["ontology", "inspect", "--repo", root, "--json"], output, error, root);

            using var document = JsonDocument.Parse(output.ToString());
            Assert.Equal(0, exitCode);
            Assert.True(document.RootElement.GetProperty("TBox").GetProperty("Exists").GetBoolean());
            Assert.Equal("https://extend0.se777en.fyi/ns#", document.RootElement.GetProperty("TBox").GetProperty("Namespace").GetString());
            Assert.Equal("1.2.3", document.RootElement.GetProperty("TBox").GetProperty("Version").GetString());
            Assert.Equal(9, document.RootElement.GetProperty("TBox").GetProperty("ClassCount").GetInt32());
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task OntologyInspect_WhenTBoxIsMissing_ReturnsFailure()
    {
        var root = CreateTempDirectory();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["ontology", "inspect", "--repo", root], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("[error] TBox missing", output.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task OntologyValidate_WithHealthyRepo_PrintsSuccessfulReport()
    {
        var root = CreateHealthyRepository();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["ontology", "validate", "--repo", root], output, error, root);

            var text = output.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains("Extend0 ontology validate", text, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("governsAccessTo ranges over AccessSurface", text, StringComparison.Ordinal);
            Assert.Contains("0 errors", text, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task OntologyValidate_WithHealthyRepo_CanEmitJson()
    {
        var root = CreateHealthyRepository();
        try
        {
            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["ontology", "validate", "--repo", root, "--json"], output, error, root);

            using var document = JsonDocument.Parse(output.ToString());
            Assert.Equal(0, exitCode);
            Assert.Equal(0, document.RootElement.GetProperty("ErrorCount").GetInt32());
            Assert.True(document.RootElement.GetProperty("InfoCount").GetInt32() > 0);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task OntologyValidate_WhenGovernsAccessRangeIsWrong_ReturnsFailure()
    {
        var root = CreateHealthyRepository();
        try
        {
            Write(root, Path.Combine("ontology", "tbox", "extend0.owl"), """
                <rdf:RDF
                    xmlns="https://extend0.se777en.fyi/ns#"
                    xml:base="https://extend0.se777en.fyi/ns"
                    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                    xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
                    xmlns:owl="http://www.w3.org/2002/07/owl#">
                  <owl:Ontology rdf:about="">
                    <owl:versionInfo>1.2.3</owl:versionInfo>
                  </owl:Ontology>
                  <owl:Class rdf:about="#Extend0Concept" />
                  <owl:Class rdf:about="#AccessSurface" />
                  <owl:ObjectProperty rdf:about="#governsAccessTo">
                    <rdfs:range rdf:resource="#Extend0Concept" />
                  </owl:ObjectProperty>
                </rdf:RDF>
                """);

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["ontology", "validate", "--repo", root], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("governsAccessTo must range over AccessSurface", output.ToString(), StringComparison.Ordinal);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task OntologyValidate_WhenRequiredConceptIsMissing_ReturnsFailure()
    {
        var root = CreateHealthyRepository();
        try
        {
            var tboxPath = Path.Combine(root, "ontology", "tbox", "extend0.owl");
            var tbox = File.ReadAllText(tboxPath)
                .Replace("""<owl:Class rdf:about="#HeartbeatSignal" />""", string.Empty, StringComparison.Ordinal);
            File.WriteAllText(tboxPath, tbox);

            using var output = new StringWriter();
            using var error = new StringWriter();

            var exitCode = await Extend0Cli.RunAsync(["ontology", "validate", "--repo", root], output, error, root);

            Assert.Equal(1, exitCode);
            Assert.Contains("Required concept 'HeartbeatSignal' is missing", output.ToString(), StringComparison.Ordinal);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static string CreateHealthyRepository()
    {
        var root = CreateTempDirectory();

        Write(root, "Extend0.sln", "solution");
        Write(root, "README.md", "The library currently targets net10.0.");
        Write(root, Path.Combine("Extend0", "Extend0.csproj"), """
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net10.0</TargetFramework>
              </PropertyGroup>
            </Project>
            """);
        Write(root, Path.Combine("Extend0.Cli", "Extend0.Cli.csproj"), """
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net10.0</TargetFramework>
                <IsPackable>true</IsPackable>
                <PackAsTool>true</PackAsTool>
                <PackageId>Extend0.Cli</PackageId>
                <ToolCommandName>extend0</ToolCommandName>
              </PropertyGroup>
            </Project>
            """);
        Write(root, Path.Combine("docs", "ADR.md"), "# ADR");
        Write(root, Path.Combine("docs", "ADR", "1-000-EXTEND0-ADR-DEFINE-EXTEND0-MAJOR-VERSION-1.md"), "# ADR 000");
        Write(root, Path.Combine("docs", "ADR", "1-010-ARCHITECTURE-ADR-ADOPT-CLI-AS-PLATFORM-DIAGNOSTIC-SURFACE.md"), "# ADR 010");
        Write(root, Path.Combine("docs", "Runtime", "CLI.md"), "# CLI");
        Write(root, Path.Combine("ontology", "tbox", "extend0.owl"), """
            <rdf:RDF
                xmlns="https://extend0.se777en.fyi/ns#"
                xml:base="https://extend0.se777en.fyi/ns"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
                xmlns:owl="http://www.w3.org/2002/07/owl#">
              <owl:Ontology rdf:about="">
                <owl:versionInfo>1.2.3</owl:versionInfo>
              </owl:Ontology>
              <owl:Class rdf:about="#Extend0Concept" />
              <owl:Class rdf:about="#LifecycleSystem" />
              <owl:Class rdf:about="#MetaDBSystem" />
              <owl:Class rdf:about="#Transport" />
              <owl:Class rdf:about="#ServiceIdentity" />
              <owl:Class rdf:about="#AccessSurface" />
              <owl:Class rdf:about="#OwnershipClaim" />
              <owl:Class rdf:about="#Lease" />
              <owl:Class rdf:about="#HeartbeatSignal" />
              <owl:ObjectProperty rdf:about="#governsAccessTo">
                <rdfs:range rdf:resource="#AccessSurface" />
              </owl:ObjectProperty>
              <owl:NamedIndividual rdf:about="#ExampleIndividual" />
            </rdf:RDF>
            """);
        Write(root, Path.Combine("ontology", "abox", "abox-schema.ttl"), """
            @prefix ns: <https://extend0.se777en.fyi/ns#> .
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <https://extend0.se777en.fyi/abox#> .

            ex:SystemShape
                sh:targetClass ns:System ;
                sh:property [
                    sh:path ns:governsAccessTo ;
                    sh:class ns:AccessSurface
                ] .
            """);
        Write(root, Path.Combine("ontology", "abox", "example-abox.ttl"), "@prefix ns: <https://extend0.se777en.fyi/ns#> .");
        Write(root, Path.Combine("ontology", "abox", "IRI-CONVENTIONS.md"), "# IRI conventions");
        Write(root, Path.Combine("ontology", "skills", "ontology-query", "query.py"), "print('ok')");
        Write(root, Path.Combine("ontology", "diagnostics", "README.md"), "# Diagnostics");
        Write(root, Path.Combine("ontology", "tests", "truth_questions.py"), "TRUTH_QUESTIONS = []");
        Write(root, Path.Combine("Extend0.Tests", "Extend0.Tests.csproj"), "<Project />");
        Write(root, Path.Combine("Extend0.Testing", "Extend0.Testing.csproj"), "<Project />");

        return root;
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Cli.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void Write(string root, string relativePath, string contents)
    {
        var path = Path.Combine(root, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, contents);
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Extend0.Cli", "Extend0.Cli.csproj")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the repository root from the test output directory.");
    }

    private static string GetProjectProperty(XDocument document, string name) =>
        document
            .Descendants(name)
            .FirstOrDefault()
            ?.Value
            .Trim()
        ?? string.Empty;

    private interface ICliProbeService : ICrossProcessService
    {
        Task<string> EchoAsync(string value);
    }

    private sealed class CliProbeService(string endpointName)
        : CrossProcessServiceBase<ICliProbeService>, ICliProbeService
    {
        protected override string? PipeName => endpointName;

        protected override string? EndpointName => endpointName;

        protected override string? EndpointServerName => ".";

        public Task<string> EchoAsync(string value) => Task.FromResult(value);
    }
}
