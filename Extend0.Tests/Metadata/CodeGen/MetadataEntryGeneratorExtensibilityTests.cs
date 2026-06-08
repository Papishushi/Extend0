using Extend0.MetadataEntry.Generator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Extend0.Tests.Metadata.CodeGen;

public sealed class MetadataEntryGeneratorExtensibilityTests
{
    [Fact]
    public void Generator_EmitsConsumerDeclaredCustomShape()
    {
        const string source = """
            using Extend0.Metadata.CodeGen;

            [assembly: GenerateMetadataEntry(24, 96)]

            namespace Consumer;

            public static class ConsumerEntryProbe
            {
                public static unsafe int UseGeneratedShape()
                {
                    var entry = new MetadataEntry24x96();
                    if (!entry.TrySetKey("abc") || !entry.TrySetValue("payload"))
                        return -1;

                    var cell = new MetadataCell(MetadataEntrySize.Entry24x96);
                    try
                    {
                        if (!cell.TrySetKey("abc") || !cell.TrySetValue("payload"))
                            return -2;

                        return MetadataEntrySize.Entry24x96.GetKeySize()
                            + MetadataEntrySize.Entry24x96.GetValueSize();
                    }
                    finally
                    {
                        cell.Dispose();
                    }
                }
            }
            """;

        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var compilation = CSharpCompilation.Create(
            assemblyName: "Consumer.GeneratedMetadataEntries",
            syntaxTrees: [CSharpSyntaxTree.ParseText(source, parseOptions)],
            references: CreateMetadataReferences(),
            options: new CSharpCompilationOptions(
                OutputKind.DynamicallyLinkedLibrary,
                allowUnsafe: true,
                optimizationLevel: OptimizationLevel.Release));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new MetadataEntryGenerator().AsSourceGenerator()],
            parseOptions: parseOptions);

        driver = driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out var outputCompilation,
            out var generatorDiagnostics);

        Assert.Empty(generatorDiagnostics.Where(static d => d.Severity == DiagnosticSeverity.Error));
        Assert.Empty(outputCompilation.GetDiagnostics().Where(static d => d.Severity == DiagnosticSeverity.Error));

        var result = driver.GetRunResult();
        var generatedSources = result.Results.Single().GeneratedSources;

        Assert.Contains(generatedSources, static source => source.HintName == "MetadataEntry_24x96.g.cs");
        Assert.Contains(generatedSources, static source => source.HintName == "MetadataEntrySize.g.cs");
        Assert.Contains(generatedSources, static source => source.HintName == "MetadataCell.g.cs");
        Assert.DoesNotContain(generatedSources, static source => source.HintName == "MetadataEntry_16x64.g.cs");

        var entryType = outputCompilation.GetTypeByMetadataName("Extend0.Metadata.CodeGen.MetadataEntry24x96");
        var enumType = outputCompilation.GetTypeByMetadataName("Extend0.Metadata.CodeGen.MetadataEntrySize");
        var probeType = outputCompilation.GetTypeByMetadataName("Consumer.ConsumerEntryProbe");

        Assert.NotNull(entryType);
        Assert.NotNull(enumType);
        Assert.NotNull(probeType);
        Assert.NotEmpty(enumType!.GetMembers("Entry24x96"));
        Assert.NotNull(probeType!.GetMembers("UseGeneratedShape").OfType<IMethodSymbol>().SingleOrDefault());
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
}
