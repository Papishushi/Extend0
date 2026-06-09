using System.Text.Json;
using System.Xml.Linq;

namespace Extend0.Cli;

public static class OntologyInspectCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    public static Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentException.ThrowIfNullOrWhiteSpace(workingDirectory);

        var parse = OntologyInspectOptions.TryParse(args, workingDirectory, out var options, out var parseError);
        if (!parse)
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteHelp(error);
            return Task.FromResult(2);
        }

        if (options.ShowHelp)
        {
            WriteHelp(output);
            return Task.FromResult(0);
        }

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var report = BuildReport(options.RepositoryRoot);
            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanReport(output, report);

            return Task.FromResult(report.TBox.Exists ? 0 : 1);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not inspect ontology: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    private static OntologyInspectReport BuildReport(string repositoryRoot)
    {
        var root = Path.GetFullPath(repositoryRoot);
        var tboxPath = Path.Combine(root, "ontology", "tbox", "extend0.owl");
        var aboxSchemaPath = Path.Combine(root, "ontology", "abox", "abox-schema.ttl");
        var exampleAboxPath = Path.Combine(root, "ontology", "abox", "example-abox.ttl");
        var queryToolPath = Path.Combine(root, "ontology", "skills", "ontology-query", "query.py");
        var diagnosticsPath = Path.Combine(root, "ontology", "diagnostics");

        var tbox = InspectTBox(tboxPath);
        return new OntologyInspectReport(
            root,
            tbox,
            new OntologyFileReport(aboxSchemaPath, File.Exists(aboxSchemaPath), TryGetFileLength(aboxSchemaPath)),
            new OntologyFileReport(exampleAboxPath, File.Exists(exampleAboxPath), TryGetFileLength(exampleAboxPath)),
            new OntologyFileReport(queryToolPath, File.Exists(queryToolPath), TryGetFileLength(queryToolPath)),
            Directory.Exists(diagnosticsPath));
    }

    private static TBoxInspectReport InspectTBox(string tboxPath)
    {
        if (!File.Exists(tboxPath))
            return TBoxInspectReport.Missing(tboxPath);

        XNamespace owl = "http://www.w3.org/2002/07/owl#";
        XNamespace rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

        var doc = XDocument.Load(tboxPath);
        var root = doc.Root ?? throw new InvalidDataException("TBox document does not have a root element.");
        var classes = ExtractLocalNames(doc.Descendants(owl + "Class"), rdf).ToArray();
        var objectProperties = ExtractLocalNames(doc.Descendants(owl + "ObjectProperty"), rdf).ToArray();
        var datatypeProperties = ExtractLocalNames(doc.Descendants(owl + "DatatypeProperty"), rdf).ToArray();
        var annotationProperties = ExtractLocalNames(doc.Descendants(owl + "AnnotationProperty"), rdf).ToArray();
        var individuals = ExtractLocalNames(doc.Descendants(owl + "NamedIndividual"), rdf).ToArray();
        var version = doc.Descendants(owl + "versionInfo").FirstOrDefault()?.Value.Trim();

        return new TBoxInspectReport(
            tboxPath,
            Exists: true,
            Namespace: root.GetDefaultNamespace().NamespaceName,
            XmlBase: root.Attribute(XNamespace.Xml + "base")?.Value,
            Version: version,
            ClassCount: classes.Length,
            ObjectPropertyCount: objectProperties.Length,
            DatatypePropertyCount: datatypeProperties.Length,
            AnnotationPropertyCount: annotationProperties.Length,
            IndividualCount: individuals.Length,
            SampleClasses: classes.Take(12).ToArray(),
            SampleObjectProperties: objectProperties.Take(12).ToArray());
    }

    private static IEnumerable<string> ExtractLocalNames(IEnumerable<XElement> elements, XNamespace rdf)
    {
        foreach (var element in elements)
        {
            var about = element.Attribute(rdf + "about")?.Value;
            if (string.IsNullOrWhiteSpace(about))
                continue;

            yield return about.StartsWith('#')
                ? about[1..]
                : about;
        }
    }

    private static long? TryGetFileLength(string path) =>
        File.Exists(path) ? new FileInfo(path).Length : null;

    private static void WriteHumanReport(TextWriter output, OntologyInspectReport report)
    {
        output.WriteLine("Extend0 ontology inspect");
        output.WriteLine($"Root: {report.RepositoryRoot}");
        output.WriteLine();

        if (!report.TBox.Exists)
        {
            output.WriteLine($"[error] TBox missing: {report.TBox.Path}");
            return;
        }

        output.WriteLine($"TBox: {report.TBox.Path}");
        output.WriteLine($"Namespace: {report.TBox.Namespace}");
        output.WriteLine($"XML base: {report.TBox.XmlBase}");
        output.WriteLine($"Version: {report.TBox.Version}");
        output.WriteLine($"Classes: {report.TBox.ClassCount}");
        output.WriteLine($"Object properties: {report.TBox.ObjectPropertyCount}");
        output.WriteLine($"Datatype properties: {report.TBox.DatatypePropertyCount}");
        output.WriteLine($"Annotation properties: {report.TBox.AnnotationPropertyCount}");
        output.WriteLine($"Individuals: {report.TBox.IndividualCount}");
        output.WriteLine($"ABox schema: {FormatFile(report.ABoxSchema)}");
        output.WriteLine($"Example ABox: {FormatFile(report.ExampleABox)}");
        output.WriteLine($"Query tool: {FormatFile(report.QueryTool)}");
        output.WriteLine($"Diagnostics directory: {(report.DiagnosticsDirectoryExists ? "present" : "missing")}");

        if (report.TBox.SampleClasses.Count > 0)
        {
            output.WriteLine();
            output.WriteLine("Sample classes:");
            foreach (var name in report.TBox.SampleClasses)
                output.WriteLine($"- {name}");
        }
    }

    private static string FormatFile(OntologyFileReport file) =>
        file.Exists
            ? $"present ({file.LengthBytes} bytes)"
            : "missing";

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 ontology inspect [--repo <path>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --repo <path>    Repository root to inspect. Defaults to the current working directory.");
        writer.WriteLine("  --json           Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help       Show command help.");
    }
}

public sealed record OntologyInspectReport(
    string RepositoryRoot,
    TBoxInspectReport TBox,
    OntologyFileReport ABoxSchema,
    OntologyFileReport ExampleABox,
    OntologyFileReport QueryTool,
    bool DiagnosticsDirectoryExists);

public sealed record TBoxInspectReport(
    string Path,
    bool Exists,
    string? Namespace,
    string? XmlBase,
    string? Version,
    int ClassCount,
    int ObjectPropertyCount,
    int DatatypePropertyCount,
    int AnnotationPropertyCount,
    int IndividualCount,
    IReadOnlyList<string> SampleClasses,
    IReadOnlyList<string> SampleObjectProperties)
{
    public static TBoxInspectReport Missing(string path) =>
        new(
            path,
            Exists: false,
            Namespace: null,
            XmlBase: null,
            Version: null,
            ClassCount: 0,
            ObjectPropertyCount: 0,
            DatatypePropertyCount: 0,
            AnnotationPropertyCount: 0,
            IndividualCount: 0,
            SampleClasses: [],
            SampleObjectProperties: []);
}

public sealed record OntologyFileReport(string Path, bool Exists, long? LengthBytes);
