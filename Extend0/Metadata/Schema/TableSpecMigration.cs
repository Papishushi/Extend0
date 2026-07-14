using Extend0.Metadata.CodeGen;

namespace Extend0.Metadata.Schema;

/// <summary>
/// Kind of schema migration step needed to evolve one <see cref="TableSpec"/> into another.
/// </summary>
public enum TableSpecMigrationStepKind
{
    /// <summary>
    /// No schema migration is required.
    /// </summary>
    NoOp = 0,

    /// <summary>
    /// The target schema advances or changes the table schema version.
    /// </summary>
    BumpSchemaVersion = 1,

    /// <summary>
    /// The logical table name changes.
    /// </summary>
    RenameTable = 2,

    /// <summary>
    /// The physical table storage layout changes.
    /// </summary>
    ChangeStorageLayout = 3,

    /// <summary>
    /// The physical storage chunk size changes.
    /// </summary>
    ChangeChunkSize = 4,

    /// <summary>
    /// A column is added by the target schema.
    /// </summary>
    AddColumn = 5,

    /// <summary>
    /// A column is removed by the target schema.
    /// </summary>
    RemoveColumn = 6,

    /// <summary>
    /// A column name changes.
    /// </summary>
    RenameColumn = 7,

    /// <summary>
    /// A column key/value byte shape changes.
    /// </summary>
    ChangeColumnShape = 8,

    /// <summary>
    /// A column initial capacity changes.
    /// </summary>
    ChangeColumnCapacity = 9,

    /// <summary>
    /// A column read-only flag changes.
    /// </summary>
    ChangeColumnReadOnlyFlag = 10,

    /// <summary>
    /// The migration requires a manual data transform.
    /// </summary>
    ManualDataTransform = 11,

    /// <summary>
    /// The storage protection policy changes.
    /// </summary>
    ChangeStorageProtectionPolicy = 12,

    /// <summary>
    /// The storage continuity policy changes.
    /// </summary>
    ChangeStorageContinuityPolicy = 13,

    /// <summary>
    /// The hardware-attestation policy changes.
    /// </summary>
    ChangeHardwareAttestationPolicy = 14
}

/// <summary>
/// Operational impact of a migration step.
/// </summary>
public enum TableSpecMigrationImpact
{
    /// <summary>
    /// The step has no operational impact.
    /// </summary>
    None = 0,

    /// <summary>
    /// The step only changes schema metadata.
    /// </summary>
    MetadataOnly = 1,

    /// <summary>
    /// The step requires rewriting or recreating physical storage.
    /// </summary>
    StorageRewrite = 2,

    /// <summary>
    /// The step requires transforming existing row data.
    /// </summary>
    DataTransform = 3,

    /// <summary>
    /// The step is not supported by automatic migration.
    /// </summary>
    Unsupported = 4
}

/// <summary>
/// A planned step for evolving a source schema into a target schema.
/// </summary>
/// <param name="Kind">Kind of migration operation represented by the step.</param>
/// <param name="Impact">Operational impact of applying the step.</param>
/// <param name="Description">Human-readable explanation of the step.</param>
/// <param name="ColumnIndex">Optional zero-based column index affected by the step.</param>
/// <param name="ColumnName">Optional column name affected by the step.</param>
public sealed record TableSpecMigrationStep(
    TableSpecMigrationStepKind Kind,
    TableSpecMigrationImpact Impact,
    string Description,
    int? ColumnIndex = null,
    string? ColumnName = null);

/// <summary>
/// Migration plan produced from a source and target <see cref="TableSpec"/>.
/// </summary>
/// <param name="Source">Source schema currently represented by storage or code.</param>
/// <param name="Target">Target schema that should become the new contract.</param>
/// <param name="Compatibility">Compatibility report used to classify the migration.</param>
/// <param name="Steps">Ordered migration steps needed to evolve source into target.</param>
public sealed record TableSpecMigrationPlan(
    TableSpec Source,
    TableSpec Target,
    TableSpecCompatibilityReport Compatibility,
    IReadOnlyList<TableSpecMigrationStep> Steps)
{
    /// <summary>
    /// Gets whether the plan contains only a no-op step.
    /// </summary>
    public bool IsNoOp => Steps.Count == 1 && Steps[0].Kind == TableSpecMigrationStepKind.NoOp;

    /// <summary>
    /// Gets whether applying this plan requires explicit data transformation by the caller.
    /// </summary>
    public bool RequiresManualDataTransform =>
        Steps.Any(static s => s.Impact is TableSpecMigrationImpact.DataTransform or TableSpecMigrationImpact.Unsupported);

    /// <summary>
    /// Gets whether the plan can be applied by automatic migration tooling.
    /// </summary>
    public bool CanApplyAutomatically =>
        Compatibility.Level != TableSpecCompatibilityLevel.Incompatible && !RequiresManualDataTransform;
}

/// <summary>
/// Builds migration plans for MetaDB table specs.
/// </summary>
public static class TableSpecMigration
{
    /// <summary>
    /// Creates a migration plan that describes how to evolve one table spec into another.
    /// </summary>
    /// <param name="source">Source schema currently represented by storage or code.</param>
    /// <param name="target">Target schema that should become the new contract.</param>
    /// <param name="compatibilityOptions">Optional compatibility validation options.</param>
    /// <returns>A migration plan with compatibility findings and ordered steps.</returns>
    public static TableSpecMigrationPlan CreatePlan(
        TableSpec source,
        TableSpec target,
        TableSpecCompatibilityOptions? compatibilityOptions = null)
    {
        var compatibility = TableSpecCompatibility.Validate(source, target, compatibilityOptions);
        var steps = new List<TableSpecMigrationStep>();

        AddVersionStep(source, target, steps);
        AddIdentitySteps(source, target, steps);
        AddStorageSteps(source, target, steps);
        AddColumnSteps(source, target, steps);

        if (compatibility.IsIncompatible && !steps.Any(static s => s.Kind == TableSpecMigrationStepKind.ManualDataTransform))
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ManualDataTransform,
                TableSpecMigrationImpact.Unsupported,
                "Compatibility report contains errors; explicit manual migration policy is required."));
        }

        if (steps.Count == 0)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.NoOp,
                TableSpecMigrationImpact.None,
                "Source and target schemas require no migration steps."));
        }

        return new TableSpecMigrationPlan(source, target, compatibility, steps);
    }

    private static void AddVersionStep(TableSpec source, TableSpec target, List<TableSpecMigrationStep> steps)
    {
        if (target.EffectiveSchemaVersion == source.EffectiveSchemaVersion)
            return;

        steps.Add(new TableSpecMigrationStep(
            TableSpecMigrationStepKind.BumpSchemaVersion,
            target.EffectiveSchemaVersion > source.EffectiveSchemaVersion
                ? TableSpecMigrationImpact.MetadataOnly
                : TableSpecMigrationImpact.Unsupported,
            $"Schema version changes from {source.EffectiveSchemaVersion} to {target.EffectiveSchemaVersion}."));
    }

    private static void AddIdentitySteps(TableSpec source, TableSpec target, List<TableSpecMigrationStep> steps)
    {
        if (!string.Equals(source.Name, target.Name, StringComparison.Ordinal))
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.RenameTable,
                TableSpecMigrationImpact.MetadataOnly,
                $"Table name changes from '{source.Name}' to '{target.Name}'."));
        }
    }

    private static void AddStorageSteps(TableSpec source, TableSpec target, List<TableSpecMigrationStep> steps)
    {
        var sourceStorage = source.Storage.Normalize();
        var targetStorage = target.Storage.Normalize();

        if (sourceStorage.Layout != targetStorage.Layout)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeStorageLayout,
                TableSpecMigrationImpact.StorageRewrite,
                $"Storage layout changes from {sourceStorage.Layout} to {targetStorage.Layout}."));
        }

        if (sourceStorage.ChunkSize != targetStorage.ChunkSize)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeChunkSize,
                TableSpecMigrationImpact.StorageRewrite,
                $"Storage chunk size changes from {sourceStorage.ChunkSize} to {targetStorage.ChunkSize}."));
        }

        if (source.Protection != target.Protection)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeStorageProtectionPolicy,
                TableSpecMigrationImpact.MetadataOnly,
                $"Storage protection policy changes from '{source.Protection.RequiredLevel}' to '{target.Protection.RequiredLevel}'."));
        }

        if (source.Continuity != target.Continuity)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeStorageContinuityPolicy,
                TableSpecMigrationImpact.MetadataOnly,
                $"Storage continuity policy changes from '{source.Continuity.RequiredLevel}' to '{target.Continuity.RequiredLevel}'."));
        }

        if (source.Attestation != target.Attestation)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeHardwareAttestationPolicy,
                TableSpecMigrationImpact.MetadataOnly,
                $"Hardware attestation policy changes from '{source.Attestation.RequiredLevel}' to '{target.Attestation.RequiredLevel}'."));
        }
    }

    private static void AddColumnSteps(TableSpec source, TableSpec target, List<TableSpecMigrationStep> steps)
    {
        var common = Math.Min(source.Columns.Length, target.Columns.Length);

        for (var i = 0; i < common; i++)
            AddColumnChangeSteps(i, source.Columns[i], target.Columns[i], steps);

        for (var i = common; i < source.Columns.Length; i++)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.RemoveColumn,
                TableSpecMigrationImpact.Unsupported,
                $"Column {i} '{source.Columns[i].Name}' would be removed.",
                i,
                source.Columns[i].Name));
        }

        for (var i = common; i < target.Columns.Length; i++)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.AddColumn,
                TableSpecMigrationImpact.StorageRewrite,
                $"Column {i} '{target.Columns[i].Name}' would be added.",
                i,
                target.Columns[i].Name));
        }
    }

    private static void AddColumnChangeSteps(
        int index,
        ColumnConfiguration source,
        ColumnConfiguration target,
        List<TableSpecMigrationStep> steps)
    {
        if (!string.Equals(source.Name, target.Name, StringComparison.Ordinal))
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.RenameColumn,
                TableSpecMigrationImpact.DataTransform,
                $"Column {index} changes name from '{source.Name}' to '{target.Name}'. Existing row keys may still contain the old name.",
                index,
                target.Name));
        }

        var sourceKey = source.Size.GetKeySize();
        var sourceValue = source.Size.GetValueSize();
        var targetKey = target.Size.GetKeySize();
        var targetValue = target.Size.GetValueSize();

        if (sourceKey != targetKey || sourceValue != targetValue)
        {
            var impact = targetKey < sourceKey || targetValue < sourceValue
                ? TableSpecMigrationImpact.Unsupported
                : TableSpecMigrationImpact.StorageRewrite;

            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeColumnShape,
                impact,
                $"Column {index} '{source.Name}' shape changes from ({sourceKey},{sourceValue}) to ({targetKey},{targetValue}) bytes.",
                index,
                source.Name));
        }

        if (source.InitialCapacity != target.InitialCapacity)
        {
            var impact = target.InitialCapacity < source.InitialCapacity
                ? TableSpecMigrationImpact.Unsupported
                : TableSpecMigrationImpact.StorageRewrite;

            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeColumnCapacity,
                impact,
                $"Column {index} '{source.Name}' initial capacity changes from {source.InitialCapacity} to {target.InitialCapacity}.",
                index,
                source.Name));
        }

        if (source.ReadOnly != target.ReadOnly)
        {
            steps.Add(new TableSpecMigrationStep(
                TableSpecMigrationStepKind.ChangeColumnReadOnlyFlag,
                TableSpecMigrationImpact.MetadataOnly,
                $"Column {index} '{source.Name}' ReadOnly changes from {source.ReadOnly} to {target.ReadOnly}.",
                index,
                source.Name));
        }
    }
}
