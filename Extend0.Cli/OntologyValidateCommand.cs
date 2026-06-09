using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace Extend0.Cli;

public static class OntologyValidateCommand
{
    private const string ExpectedNamespace = "https://extend0.se777en.fyi/ns#";
    private const string ExpectedXmlBase = "https://extend0.se777en.fyi/ns";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    static OntologyValidateCommand()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

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

        var parse = OntologyValidateOptions.TryParse(args, workingDirectory, out var options, out var parseError);
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

            return Task.FromResult(report.ErrorCount > 0 ? 1 : 0);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not validate ontology: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    private static OntologyValidateReport BuildReport(string repositoryRoot)
    {
        var inspect = OntologyInspectCommand.BuildReport(repositoryRoot);
        var findings = new List<ValidationFinding>();

        ValidateTBox(inspect, findings);
        ValidateABoxSchema(inspect, findings);
        ValidateFile(inspect.ExampleABox, "example-abox", required: false, findings, "Example ABox fixture is present.");
        ValidateFile(inspect.QueryTool, "query-tool", required: false, findings, "Ontology query tool is present.");
        ValidateRepositoryScaffolds(inspect.RepositoryRoot, inspect.DiagnosticsDirectoryExists, findings);

        return OntologyValidateReport.Create(inspect.RepositoryRoot, inspect, findings);
    }

    private static void ValidateTBox(OntologyInspectReport inspect, List<ValidationFinding> findings)
    {
        if (!inspect.TBox.Exists)
        {
            findings.Add(ValidationFinding.Error("tbox-exists", $"Missing TBox: {inspect.TBox.Path}."));
            return;
        }

        findings.Add(ValidationFinding.Info("tbox-exists", $"Found TBox '{inspect.TBox.Path}'."));

        if (string.Equals(inspect.TBox.Namespace, ExpectedNamespace, StringComparison.Ordinal))
            findings.Add(ValidationFinding.Info("tbox-namespace", $"TBox namespace is {ExpectedNamespace}."));
        else
            findings.Add(ValidationFinding.Error("tbox-namespace", $"TBox namespace is '{inspect.TBox.Namespace ?? "<missing>"}', expected '{ExpectedNamespace}'."));

        if (string.Equals(inspect.TBox.XmlBase, ExpectedXmlBase, StringComparison.Ordinal))
            findings.Add(ValidationFinding.Info("tbox-xml-base", $"TBox xml:base is {ExpectedXmlBase}."));
        else
            findings.Add(ValidationFinding.Error("tbox-xml-base", $"TBox xml:base is '{inspect.TBox.XmlBase ?? "<missing>"}', expected '{ExpectedXmlBase}'."));

        if (string.IsNullOrWhiteSpace(inspect.TBox.Version))
            findings.Add(ValidationFinding.Error("tbox-version", "TBox is missing owl:versionInfo."));
        else
            findings.Add(ValidationFinding.Info("tbox-version", $"TBox version is {inspect.TBox.Version}."));

        if (inspect.TBox.ClassCount == 0)
            findings.Add(ValidationFinding.Error("tbox-classes", "TBox must define at least one owl:Class."));
        else
            findings.Add(ValidationFinding.Info("tbox-classes", $"TBox defines {inspect.TBox.ClassCount} classes."));

        if (inspect.TBox.ObjectPropertyCount == 0)
            findings.Add(ValidationFinding.Error("tbox-object-properties", "TBox must define at least one owl:ObjectProperty."));
        else
            findings.Add(ValidationFinding.Info("tbox-object-properties", $"TBox defines {inspect.TBox.ObjectPropertyCount} object properties."));

        if (TBoxPropertyHasRange(inspect.TBox.Path, "governsAccessTo", "AccessSurface"))
            findings.Add(ValidationFinding.Info("governs-access-range", "governsAccessTo ranges over AccessSurface."));
        else
            findings.Add(ValidationFinding.Error("governs-access-range", "governsAccessTo must range over AccessSurface."));
    }

    private static void ValidateABoxSchema(OntologyInspectReport inspect, List<ValidationFinding> findings)
    {
        ValidateFile(inspect.ABoxSchema, "abox-schema", required: true, findings, "ABox SHACL schema is present.");
        if (!inspect.ABoxSchema.Exists)
            return;

        var schema = Regex.Replace(File.ReadAllText(inspect.ABoxSchema.Path), @"\s+", " ");
        var mentionsPath = schema.Contains("sh:path ns:governsAccessTo", StringComparison.OrdinalIgnoreCase);
        var mentionsClass = schema.Contains("sh:class ns:AccessSurface", StringComparison.OrdinalIgnoreCase);

        if (mentionsPath && mentionsClass)
            findings.Add(ValidationFinding.Info("abox-governs-access-shape", "ABox SHACL schema constrains governsAccessTo to AccessSurface."));
        else
            findings.Add(ValidationFinding.Error("abox-governs-access-shape", "ABox SHACL schema should constrain governsAccessTo to AccessSurface."));
    }

    private static void ValidateRepositoryScaffolds(string root, bool diagnosticsDirectoryExists, List<ValidationFinding> findings)
    {
        var iriConventions = Path.Combine(root, "ontology", "abox", "IRI-CONVENTIONS.md");
        if (File.Exists(iriConventions))
            findings.Add(ValidationFinding.Info("iri-conventions", "IRI conventions document is present."));
        else
            findings.Add(ValidationFinding.Error("iri-conventions", "Missing ontology/abox/IRI-CONVENTIONS.md."));

        var testsDirectory = Path.Combine(root, "ontology", "tests");
        if (Directory.Exists(testsDirectory))
            findings.Add(ValidationFinding.Info("truth-question-harness", "Ontology truth-question harness directory is present."));
        else
            findings.Add(ValidationFinding.Warning("truth-question-harness", "Missing ontology/tests truth-question harness directory."));

        if (diagnosticsDirectoryExists)
            findings.Add(ValidationFinding.Info("diagnostics", "Ontology diagnostics directory is present."));
        else
            findings.Add(ValidationFinding.Warning("diagnostics", "Missing ontology/diagnostics scaffold directory."));
    }

    private static void ValidateFile(
        OntologyFileReport file,
        string id,
        bool required,
        List<ValidationFinding> findings,
        string presentMessage)
    {
        if (file.Exists)
        {
            findings.Add(ValidationFinding.Info(id, presentMessage));
            return;
        }

        var missingMessage = $"Missing {file.Path}.";
        findings.Add(required
            ? ValidationFinding.Error(id, missingMessage)
            : ValidationFinding.Warning(id, missingMessage));
    }

    private static bool TBoxPropertyHasRange(string path, string propertyName, string expectedRange)
    {
        XNamespace owl = "http://www.w3.org/2002/07/owl#";
        XNamespace rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        XNamespace rdfs = "http://www.w3.org/2000/01/rdf-schema#";

        var doc = XDocument.Load(path);
        var property = doc
            .Descendants(owl + "ObjectProperty")
            .FirstOrDefault(element => string.Equals(LocalName(element.Attribute(rdf + "about")?.Value), propertyName, StringComparison.Ordinal));

        if (property is null)
            return false;

        return property
            .Elements(rdfs + "range")
            .Any(element => string.Equals(LocalName(element.Attribute(rdf + "resource")?.Value), expectedRange, StringComparison.Ordinal));
    }

    private static string? LocalName(string? iri)
    {
        if (string.IsNullOrWhiteSpace(iri))
            return null;

        if (iri[0] == '#')
            return iri[1..];

        var hash = iri.LastIndexOf('#');
        if (hash >= 0 && hash + 1 < iri.Length)
            return iri[(hash + 1)..];

        var slash = iri.LastIndexOf('/');
        return slash >= 0 && slash + 1 < iri.Length
            ? iri[(slash + 1)..]
            : iri;
    }

    private static void WriteHumanReport(TextWriter output, OntologyValidateReport report)
    {
        output.WriteLine("Extend0 ontology validate");
        output.WriteLine($"Root: {report.RepositoryRoot}");
        output.WriteLine($"TBox: {report.Inspect.TBox.Path}");
        output.WriteLine($"Namespace: {report.Inspect.TBox.Namespace ?? "<missing>"}");
        output.WriteLine($"XML base: {report.Inspect.TBox.XmlBase ?? "<missing>"}");
        output.WriteLine($"Version: {report.Inspect.TBox.Version ?? "<missing>"}");
        output.WriteLine();

        foreach (var finding in report.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.InfoCount} info, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static string FormatSeverity(ValidationSeverity severity) =>
        severity switch
        {
            ValidationSeverity.Info => "info",
            ValidationSeverity.Warning => "warn",
            ValidationSeverity.Error => "error",
            _ => severity.ToString().ToLowerInvariant()
        };

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 ontology validate [--repo <path>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --repo <path>    Repository root to validate. Defaults to the current working directory.");
        writer.WriteLine("  --json           Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help       Show command help.");
    }
}

public sealed record OntologyValidateReport(
    string RepositoryRoot,
    OntologyInspectReport Inspect,
    IReadOnlyList<ValidationFinding> Findings,
    int InfoCount,
    int WarningCount,
    int ErrorCount)
{
    public static OntologyValidateReport Create(
        string repositoryRoot,
        OntologyInspectReport inspect,
        IReadOnlyList<ValidationFinding> findings) =>
        new(
            repositoryRoot,
            inspect,
            findings,
            findings.Count(static f => f.Severity == ValidationSeverity.Info),
            findings.Count(static f => f.Severity == ValidationSeverity.Warning),
            findings.Count(static f => f.Severity == ValidationSeverity.Error));
}
