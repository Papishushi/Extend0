using Extend0.Metadata.CodeGen;

namespace Extend0.Metadata.Schema;

/// <summary>
/// Compatibility classification for two versions of a <see cref="TableSpec"/>.
/// </summary>
public enum TableSpecCompatibilityLevel
{
    /// <summary>
    /// The target schema can be treated as the same runtime contract.
    /// </summary>
    Compatible = 0,

    /// <summary>
    /// The target schema is a valid evolution, but existing storage should be migrated or rewritten.
    /// </summary>
    RequiresMigration = 1,

    /// <summary>
    /// The target schema is not a safe evolution without an explicit manual data transform.
    /// </summary>
    Incompatible = 2
}

/// <summary>
/// Severity of a compatibility finding.
/// </summary>
public enum TableSpecCompatibilitySeverity
{
    /// <summary>
    /// Informational finding that does not require migration by itself.
    /// </summary>
    Info = 0,

    /// <summary>
    /// Warning finding that usually requires migration or operator attention.
    /// </summary>
    Warning = 1,

    /// <summary>
    /// Error finding that makes the target schema incompatible without manual intervention.
    /// </summary>
    Error = 2
}

/// <summary>
/// Options that tune how strict <see cref="TableSpecCompatibility"/> should be.
/// </summary>
/// <param name="AllowSameVersionStructuralChanges">
/// Allows structural changes without bumping <see cref="TableSpec.SchemaVersion"/>.
/// The default is strict because schema versioning is the migration boundary.
/// </param>
public sealed record TableSpecCompatibilityOptions(bool AllowSameVersionStructuralChanges = false);

/// <summary>
/// A single compatibility observation.
/// </summary>
/// <param name="Severity">Severity assigned to the compatibility observation.</param>
/// <param name="Id">Stable diagnostic identifier for the finding.</param>
/// <param name="Message">Human-readable explanation of the finding.</param>
public sealed record TableSpecCompatibilityFinding(
    TableSpecCompatibilitySeverity Severity,
    string Id,
    string Message)
{
    /// <summary>
    /// Creates an informational compatibility finding.
    /// </summary>
    public static TableSpecCompatibilityFinding Info(string id, string message) =>
        new(TableSpecCompatibilitySeverity.Info, id, message);

    /// <summary>
    /// Creates a warning compatibility finding.
    /// </summary>
    public static TableSpecCompatibilityFinding Warning(string id, string message) =>
        new(TableSpecCompatibilitySeverity.Warning, id, message);

    /// <summary>
    /// Creates an error compatibility finding.
    /// </summary>
    public static TableSpecCompatibilityFinding Error(string id, string message) =>
        new(TableSpecCompatibilitySeverity.Error, id, message);
}

/// <summary>
/// Result of comparing a source and target <see cref="TableSpec"/>.
/// </summary>
/// <param name="Source">Source schema currently represented by storage or code.</param>
/// <param name="Target">Target schema that should become the new contract.</param>
/// <param name="Level">Overall compatibility classification.</param>
/// <param name="Findings">Detailed compatibility observations.</param>
public sealed record TableSpecCompatibilityReport(
    TableSpec Source,
    TableSpec Target,
    TableSpecCompatibilityLevel Level,
    IReadOnlyList<TableSpecCompatibilityFinding> Findings)
{
    /// <summary>
    /// Gets whether the target schema is compatible with the source schema.
    /// </summary>
    public bool IsCompatible => Level == TableSpecCompatibilityLevel.Compatible;

    /// <summary>
    /// Gets whether the target schema is valid but requires migration.
    /// </summary>
    public bool RequiresMigration => Level == TableSpecCompatibilityLevel.RequiresMigration;

    /// <summary>
    /// Gets whether the target schema is incompatible without explicit manual intervention.
    /// </summary>
    public bool IsIncompatible => Level == TableSpecCompatibilityLevel.Incompatible;
}

/// <summary>
/// Compares two table specs using MetaDB major 1 schema-evolution rules.
/// </summary>
public static class TableSpecCompatibility
{
    /// <summary>
    /// Validates whether a target table spec is a safe evolution of a source table spec.
    /// </summary>
    /// <param name="source">Source schema currently represented by storage or code.</param>
    /// <param name="target">Target schema that should become the new contract.</param>
    /// <param name="options">Optional compatibility validation options.</param>
    /// <returns>A compatibility report containing the overall level and findings.</returns>
    public static TableSpecCompatibilityReport Validate(
        TableSpec source,
        TableSpec target,
        TableSpecCompatibilityOptions? options = null)
    {
        source.Validate();
        target.Validate();

        options ??= new TableSpecCompatibilityOptions();
        var findings = new List<TableSpecCompatibilityFinding>();

        CompareVersion(source, target, findings);
        CompareIdentity(source, target, findings);
        CompareStorage(source, target, findings);
        CompareColumns(source, target, findings);

        if (!options.AllowSameVersionStructuralChanges
            && source.EffectiveSchemaVersion == target.EffectiveSchemaVersion
            && HasStructuralSchemaChange(source, target))
        {
            findings.Add(TableSpecCompatibilityFinding.Error(
                "same-version-structural-change",
                $"Schema version {source.EffectiveSchemaVersion} changed structurally. Bump the target schema version before planning migration."));
        }

        if (!findings.Any(static f => f.Severity != TableSpecCompatibilitySeverity.Info))
        {
            findings.Add(TableSpecCompatibilityFinding.Info(
                "schema-compatible",
                $"Target schema version {target.EffectiveSchemaVersion} is compatible with source version {source.EffectiveSchemaVersion}."));
        }

        var level = findings.Any(static f => f.Severity == TableSpecCompatibilitySeverity.Error)
            ? TableSpecCompatibilityLevel.Incompatible
            : findings.Any(static f => f.Severity == TableSpecCompatibilitySeverity.Warning)
                ? TableSpecCompatibilityLevel.RequiresMigration
                : TableSpecCompatibilityLevel.Compatible;

        return new TableSpecCompatibilityReport(source, target, level, findings);
    }

    private static void CompareVersion(
        TableSpec source,
        TableSpec target,
        List<TableSpecCompatibilityFinding> findings)
    {
        if (target.EffectiveSchemaVersion < source.EffectiveSchemaVersion)
        {
            findings.Add(TableSpecCompatibilityFinding.Error(
                "schema-version-regression",
                $"Target schema version {target.EffectiveSchemaVersion} is lower than source version {source.EffectiveSchemaVersion}."));
            return;
        }

        if (target.EffectiveSchemaVersion > source.EffectiveSchemaVersion)
        {
            findings.Add(TableSpecCompatibilityFinding.Info(
                "schema-version-bump",
                $"Target schema version advances from {source.EffectiveSchemaVersion} to {target.EffectiveSchemaVersion}."));
        }
    }

    private static void CompareIdentity(
        TableSpec source,
        TableSpec target,
        List<TableSpecCompatibilityFinding> findings)
    {
        if (!string.Equals(source.Name, target.Name, StringComparison.Ordinal))
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "table-name-change",
                $"Table name changes from '{source.Name}' to '{target.Name}'."));
        }

        if (!string.Equals(source.MapPath, target.MapPath, StringComparison.Ordinal))
        {
            findings.Add(TableSpecCompatibilityFinding.Info(
                "map-path-change",
                "MapPath changes; this is treated as relocation metadata rather than schema incompatibility."));
        }

        if (!string.Equals(source.SchemaId, target.SchemaId, StringComparison.Ordinal))
        {
            if (!string.IsNullOrWhiteSpace(source.SchemaId) && !string.IsNullOrWhiteSpace(target.SchemaId))
            {
                findings.Add(TableSpecCompatibilityFinding.Warning(
                    "schema-id-change",
                    $"SchemaId changes from '{source.SchemaId}' to '{target.SchemaId}'."));
            }
            else if (!string.IsNullOrWhiteSpace(target.SchemaId))
            {
                findings.Add(TableSpecCompatibilityFinding.Info(
                    "schema-id-assigned",
                    $"Target assigns SchemaId '{target.SchemaId}'."));
            }
            else if (!string.IsNullOrWhiteSpace(source.SchemaId))
            {
                findings.Add(TableSpecCompatibilityFinding.Warning(
                    "schema-id-removed",
                    $"Target removes SchemaId '{source.SchemaId}'."));
            }
        }
    }

    private static void CompareStorage(
        TableSpec source,
        TableSpec target,
        List<TableSpecCompatibilityFinding> findings)
    {
        var sourceStorage = source.Storage.Normalize();
        var targetStorage = target.Storage.Normalize();

        if (sourceStorage.Layout != targetStorage.Layout)
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "storage-layout-change",
                $"Storage layout changes from {sourceStorage.Layout} to {targetStorage.Layout}."));
        }

        if (sourceStorage.ChunkSize != targetStorage.ChunkSize)
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "storage-chunk-size-change",
                $"Storage chunk size changes from {sourceStorage.ChunkSize} to {targetStorage.ChunkSize}."));
        }

        if (source.Protection != target.Protection)
        {
            findings.Add(TableSpecCompatibilityFinding.Info(
                "storage-protection-policy-change",
                $"Storage protection policy changes from '{source.Protection.RequiredLevel}' to '{target.Protection.RequiredLevel}'."));
        }

        if (source.Continuity != target.Continuity)
        {
            findings.Add(TableSpecCompatibilityFinding.Info(
                "storage-continuity-policy-change",
                $"Storage continuity policy changes from '{source.Continuity.RequiredLevel}' to '{target.Continuity.RequiredLevel}'."));
        }

        if (source.Attestation != target.Attestation)
        {
            findings.Add(TableSpecCompatibilityFinding.Info(
                "hardware-attestation-policy-change",
                $"Hardware attestation policy changes from '{source.Attestation.RequiredLevel}' to '{target.Attestation.RequiredLevel}'."));
        }
    }

    private static void CompareColumns(
        TableSpec source,
        TableSpec target,
        List<TableSpecCompatibilityFinding> findings)
    {
        var common = Math.Min(source.Columns.Length, target.Columns.Length);

        for (var i = 0; i < common; i++)
            CompareColumn(i, source.Columns[i], target.Columns[i], findings);

        for (var i = common; i < source.Columns.Length; i++)
        {
            findings.Add(TableSpecCompatibilityFinding.Error(
                "column-removed",
                $"Column {i} '{source.Columns[i].Name}' is removed by the target schema."));
        }

        for (var i = common; i < target.Columns.Length; i++)
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "column-added",
                $"Column {i} '{target.Columns[i].Name}' is added by the target schema."));
        }
    }

    private static void CompareColumn(
        int index,
        ColumnConfiguration source,
        ColumnConfiguration target,
        List<TableSpecCompatibilityFinding> findings)
    {
        if (!string.Equals(source.Name, target.Name, StringComparison.Ordinal))
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "column-name-change",
                $"Column {index} changes name from '{source.Name}' to '{target.Name}'."));
        }

        var sourceKey = source.Size.GetKeySize();
        var sourceValue = source.Size.GetValueSize();
        var targetKey = target.Size.GetKeySize();
        var targetValue = target.Size.GetValueSize();

        if (targetKey < sourceKey || targetValue < sourceValue)
        {
            findings.Add(TableSpecCompatibilityFinding.Error(
                "column-shape-shrink",
                $"Column {index} '{source.Name}' shrinks from ({sourceKey},{sourceValue}) to ({targetKey},{targetValue}) bytes."));
        }
        else if (targetKey != sourceKey || targetValue != sourceValue)
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "column-shape-grow",
                $"Column {index} '{source.Name}' grows from ({sourceKey},{sourceValue}) to ({targetKey},{targetValue}) bytes."));
        }

        if (target.InitialCapacity < source.InitialCapacity)
        {
            findings.Add(TableSpecCompatibilityFinding.Error(
                "column-capacity-shrink",
                $"Column {index} '{source.Name}' lowers initial capacity from {source.InitialCapacity} to {target.InitialCapacity}."));
        }
        else if (target.InitialCapacity > source.InitialCapacity)
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "column-capacity-grow",
                $"Column {index} '{source.Name}' raises initial capacity from {source.InitialCapacity} to {target.InitialCapacity}."));
        }

        if (source.ReadOnly != target.ReadOnly)
        {
            findings.Add(TableSpecCompatibilityFinding.Warning(
                "column-readonly-change",
                $"Column {index} '{source.Name}' changes ReadOnly from {source.ReadOnly} to {target.ReadOnly}."));
        }
    }

    private static bool HasStructuralSchemaChange(TableSpec source, TableSpec target)
    {
        if (!string.Equals(source.Name, target.Name, StringComparison.Ordinal))
            return true;

        if (!string.Equals(source.SchemaId, target.SchemaId, StringComparison.Ordinal))
            return true;

        if (source.Storage.Normalize() != target.Storage.Normalize())
            return true;

        if (source.Columns.Length != target.Columns.Length)
            return true;

        return !source.Columns.AsSpan().SequenceEqual(target.Columns);
    }
}
