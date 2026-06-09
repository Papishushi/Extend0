using System.Text.Json;
using Extend0.Cli;
using Extend0.Metadata.Schema;

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
        Assert.Contains("extend0 ontology inspect", output.ToString(), StringComparison.OrdinalIgnoreCase);
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
            Assert.Contains("Classes: 2", text, StringComparison.Ordinal);
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
            Assert.Equal(2, document.RootElement.GetProperty("TBox").GetProperty("ClassCount").GetInt32());
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
        Write(root, Path.Combine("docs", "ADR.md"), "# ADR");
        Write(root, Path.Combine("docs", "ADR", "1-000-EXTEND0-ADR-DEFINE-EXTEND0-MAJOR-VERSION-1.md"), "# ADR 000");
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
                <rdfs:range rdf:resource="#AccessSurface" />
              </owl:ObjectProperty>
              <owl:NamedIndividual rdf:about="#ExampleIndividual" />
            </rdf:RDF>
            """);
        Write(root, Path.Combine("ontology", "abox", "abox-schema.ttl"), "@prefix ns: <https://extend0.se777en.fyi/ns#> .");
        Write(root, Path.Combine("ontology", "abox", "example-abox.ttl"), "@prefix ns: <https://extend0.se777en.fyi/ns#> .");
        Write(root, Path.Combine("ontology", "skills", "ontology-query", "query.py"), "print('ok')");
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
}
