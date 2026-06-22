using Extend0.Metadata.CodeGen;

namespace Extend0.Metadata.Schema;

/// <summary>
/// Kind of schema migration step needed to evolve one <see cref="TableSpec"/> into another.
/// </summary>
public enum TableSpecMigrationStepKind
{
    NoOp = 0,
    BumpSchemaVersion = 1,
    RenameTable = 2,
    ChangeStorageLayout = 3,
    ChangeChunkSize = 4,
    AddColumn = 5,
    RemoveColumn = 6,
    RenameColumn = 7,
    ChangeColumnShape = 8,
    ChangeColumnCapacity = 9,
    ChangeColumnReadOnlyFlag = 10,
    ManualDataTransform = 11,
    ChangeStorageProtectionPolicy = 12,
    ChangeStorageContinuityPolicy = 13,
    ChangeHardwareAttestationPolicy = 14
}

/// <summary>
/// Operational impact of a migration step.
/// </summary>
public enum TableSpecMigrationImpact
{
    None = 0,
    MetadataOnly = 1,
    StorageRewrite = 2,
    DataTransform = 3,
    Unsupported = 4
}

/// <summary>
/// A planned step for evolving a source schema into a target schema.
/// </summary>
public sealed record TableSpecMigrationStep(
    TableSpecMigrationStepKind Kind,
    TableSpecMigrationImpact Impact,
    string Description,
    int? ColumnIndex = null,
    string? ColumnName = null);

/// <summary>
/// Migration plan produced from a source and target <see cref="TableSpec"/>.
/// </summary>
public sealed record TableSpecMigrationPlan(
    TableSpec Source,
    TableSpec Target,
    TableSpecCompatibilityReport Compatibility,
    IReadOnlyList<TableSpecMigrationStep> Steps)
{
    public bool IsNoOp => Steps.Count == 1 && Steps[0].Kind == TableSpecMigrationStepKind.NoOp;

    public bool RequiresManualDataTransform =>
        Steps.Any(static s => s.Impact is TableSpecMigrationImpact.DataTransform or TableSpecMigrationImpact.Unsupported);

    public bool CanApplyAutomatically =>
        Compatibility.Level != TableSpecCompatibilityLevel.Incompatible && !RequiresManualDataTransform;
}

/// <summary>
/// Builds migration plans for MetaDB table specs.
/// </summary>
public static class TableSpecMigration
{
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
