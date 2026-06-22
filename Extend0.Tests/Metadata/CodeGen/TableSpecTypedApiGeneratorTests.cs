using System.Text;
using Extend0.MetadataEntry.Generator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace Extend0.Tests.Metadata.CodeGen;

public sealed class TableSpecTypedApiGeneratorTests
{
    [Fact]
    public void Generator_EmitsStrongWrapperFromTypedTableSpec()
    {
        const string source = """
            namespace Consumer;

            public static class Probe
            {
            }
            """;
        const string typedTableSpec = """
            {
              "name": "ClusterNodes",
              "typedApiNamespace": "Consumer.Generated",
              "typedApiName": "ClusterNodesTable",
              "schemaVersion": 1,
              "schemaId": "cluster-nodes",
              "schemaDescription": "Cluster node registry used by MetaDB demos and platform diagnostics.",
              "columns": [
                {
                  "name": "node_id",
                  "valueType": "System.Guid",
                  "keyBytes": 0,
                  "valueBytes": 16,
                  "initialCapacity": 256
                },
                {
                  "name": "connection_count",
                  "valueType": "int",
                  "keyBytes": 0,
                  "initialCapacity": 256
                },
                {
                  "name": "node_id_name_pair",
                  "propertyName": "NodeName",
                  "kind": "utf8",
                  "keyBytes": 16,
                  "valueBytes": 64,
                  "initialCapacity": 256
                },
                {
                  "name": "services",
                  "kind": "refs",
                  "keyBytes": 16,
                  "refsPerCell": 2,
                  "initialCapacity": 256
                }
              ]
            }
            """;

        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var compilation = CSharpCompilation.Create(
            assemblyName: "Consumer.TypedMetaDB",
            syntaxTrees: [CSharpSyntaxTree.ParseText(source, parseOptions)],
            references: CreateMetadataReferences(),
            options: new CSharpCompilationOptions(
                OutputKind.DynamicallyLinkedLibrary,
                allowUnsafe: true,
                optimizationLevel: OptimizationLevel.Release));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            generators: [new TableSpecTypedApiGenerator().AsSourceGenerator()],
            additionalTexts: [new InMemoryAdditionalText("ClusterNodes.typed.tablespec.json", typedTableSpec)],
            parseOptions: parseOptions);

        driver = driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out var outputCompilation,
            out var generatorDiagnostics);

        Assert.Empty(generatorDiagnostics.Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error));
        Assert.Empty(outputCompilation.GetDiagnostics().Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error));

        var result = driver.GetRunResult();
        var generatedSource = Assert.Single(result.Results.Single().GeneratedSources);
        var generatedText = generatedSource.SourceText.ToString();

        Assert.Equal("ClusterNodesTable.g.cs", generatedSource.HintName);
        Assert.Contains("public global::Extend0.Metadata.Typed.MetadataValueColumn<global::System.Guid> NodeId", generatedText);
        Assert.Contains("public global::Extend0.Metadata.Typed.MetadataValueColumn<int> ConnectionCount", generatedText);
        Assert.Contains("public global::Extend0.Metadata.Typed.MetadataUtf8Column NodeName", generatedText);
        Assert.Contains("public global::Extend0.Metadata.Typed.MetadataRefsColumn Services", generatedText);
        Assert.Contains("public static global::Extend0.Metadata.Schema.TableSpec CreateSpec", generatedText);
        Assert.Contains("global::Extend0.Metadata.Schema.TableSpec.Helpers.PackColumnSize", generatedText);
        Assert.DoesNotContain("PackUnchecked", generatedText);

        var wrapperType = outputCompilation.GetTypeByMetadataName("Consumer.Generated.ClusterNodesTable");
        Assert.NotNull(wrapperType);
        Assert.NotEmpty(wrapperType!.GetMembers("NodeId"));
        Assert.NotEmpty(wrapperType.GetMembers("Services"));
        Assert.NotEmpty(wrapperType.GetMembers("CreateSpec"));
    }

    private static IEnumerable<MetadataReference> CreateMetadataReferences()
    {
        var trustedPlatformAssemblies = ((string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES"))?
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            ?? [];

        foreach (var assemblyPath in trustedPlatformAssemblies)
            yield return MetadataReference.CreateFromFile(assemblyPath);

        yield return MetadataReference.CreateFromFile(typeof(Extend0.Metadata.Contract.IMetadataEntry).Assembly.Location);
    }

    private sealed class InMemoryAdditionalText(string path, string text) : AdditionalText
    {
        public override string Path { get; } = path;

        public override SourceText GetText(CancellationToken cancellationToken = default) =>
            SourceText.From(text, Encoding.UTF8);
    }
}
