using System.Text.Json;
using Extend0.Cli;

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
            <rdf:RDF>
              <owl:ObjectProperty rdf:about="#governsAccessTo">
                <rdfs:range rdf:resource="#AccessSurface"/>
              </owl:ObjectProperty>
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
