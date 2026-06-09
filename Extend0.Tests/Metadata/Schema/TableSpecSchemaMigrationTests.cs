using System.Text.Json;
using Extend0.Metadata.Schema;
using Extend0.Testing.Metadata.Storage;

namespace Extend0.Tests.Metadata.Schema;

public sealed class TableSpecSchemaMigrationTests
{
    [Fact]
    public void SaveToFile_PersistsDefaultSchemaVersion()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var path = Path.Combine(tempRoot, "spec.json");
            var spec = CreateSpec("Users", Path.Combine(tempRoot, "users.meta"));

            spec.SaveToFile(path);

            using var document = JsonDocument.Parse(File.ReadAllText(path));
            Assert.Equal(1, document.RootElement.GetProperty("schemaVersion").GetInt32());
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void LoadFromFile_TreatsMissingSchemaVersionAsMajorOne()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var path = Path.Combine(tempRoot, "legacy-spec.json");
            var spec = CreateSpec("Users", Path.Combine(tempRoot, "users.meta"));
            spec.SaveToFile(path);

            var legacyJson = string.Join(
                Environment.NewLine,
                File.ReadAllLines(path).Where(static line => !line.Contains("\"schemaVersion\"", StringComparison.Ordinal)));
            File.WriteAllText(path, legacyJson);

            var loaded = TableSpec.Helpers.LoadFromFile(path);

            Assert.Equal(0, loaded.SchemaVersion);
            Assert.Equal(1, loaded.EffectiveSchemaVersion);
            Assert.Equal(spec, loaded);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void Compatibility_WithOnlyRelocatedMapPath_IsCompatible()
    {
        var source = CreateSpec("Users", "users.meta");
        var target = source with { MapPath = "relocated-users.meta" };

        var report = TableSpecCompatibility.Validate(source, target);

        Assert.Equal(TableSpecCompatibilityLevel.Compatible, report.Level);
        Assert.Contains(report.Findings, static f => f.Id == "map-path-change");
    }

    [Fact]
    public void Compatibility_WithAddedColumnAndVersionBump_RequiresMigration()
    {
        var source = CreateSpec("Users", "users.meta");
        var target = source with
        {
            SchemaVersion = 2,
            Columns =
            [
                .. source.Columns,
                TableSpec.Helpers.Column("Email", capacity: 4, keyBytes: 16, valueBytes: 128)
            ]
        };

        var report = TableSpecCompatibility.Validate(source, target);
        var plan = TableSpecMigration.CreatePlan(source, target);

        Assert.Equal(TableSpecCompatibilityLevel.RequiresMigration, report.Level);
        Assert.Contains(report.Findings, static f => f.Id == "column-added");
        Assert.Contains(plan.Steps, static s => s.Kind == TableSpecMigrationStepKind.AddColumn);
        Assert.True(plan.CanApplyAutomatically);
    }

    [Fact]
    public void Compatibility_WithStructuralChangeAtSameVersion_IsIncompatible()
    {
        var source = CreateSpec("Users", "users.meta");
        var target = source with
        {
            Columns =
            [
                .. source.Columns,
                TableSpec.Helpers.Column("Email", capacity: 4, keyBytes: 16, valueBytes: 128)
            ]
        };

        var report = TableSpecCompatibility.Validate(source, target);
        var plan = TableSpecMigration.CreatePlan(source, target);

        Assert.Equal(TableSpecCompatibilityLevel.Incompatible, report.Level);
        Assert.Contains(report.Findings, static f => f.Id == "same-version-structural-change");
        Assert.Contains(plan.Steps, static s => s.Kind == TableSpecMigrationStepKind.ManualDataTransform);
        Assert.False(plan.CanApplyAutomatically);
    }

    [Fact]
    public void Compatibility_WithRemovedColumn_IsIncompatible()
    {
        var source = CreateSpec("Users", "users.meta") with
        {
            SchemaVersion = 1,
            Columns =
            [
                TableSpec.Helpers.Column("Name", capacity: 4, keyBytes: 16, valueBytes: 64),
                TableSpec.Helpers.Column("Email", capacity: 4, keyBytes: 16, valueBytes: 128)
            ]
        };
        var target = source with
        {
            SchemaVersion = 2,
            Columns =
            [
                TableSpec.Helpers.Column("Name", capacity: 4, keyBytes: 16, valueBytes: 64)
            ]
        };

        var report = TableSpecCompatibility.Validate(source, target);
        var plan = TableSpecMigration.CreatePlan(source, target);

        Assert.Equal(TableSpecCompatibilityLevel.Incompatible, report.Level);
        Assert.Contains(report.Findings, static f => f.Id == "column-removed");
        Assert.Contains(plan.Steps, static s => s.Kind == TableSpecMigrationStepKind.RemoveColumn && s.Impact == TableSpecMigrationImpact.Unsupported);
    }

    [Fact]
    public void SnapshotAndRestore_RoundTripSingleFileStorage()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var sourceMapPath = Path.Combine(tempRoot, "source", "users.meta");
            var source = CreateSpec("Users", sourceMapPath) with
            {
                SchemaId = "extend0.tests.users",
                SchemaVersion = 2
            };

            using (MetadataStorageHarness.CreateMappedStore(source))
            {
            }

            var snapshotDirectory = Path.Combine(tempRoot, "snapshot");
            var manifest = MetaDbSnapshot.Create(source, snapshotDirectory, label: "before-upgrade");
            var restoreMapPath = Path.Combine(tempRoot, "restore", "users.meta");
            var restored = MetaDbSnapshot.Restore(snapshotDirectory, restoreMapPath);

            Assert.True(manifest.ContainsRuntimeStorage);
            Assert.True(File.Exists(restoreMapPath));
            Assert.True(File.Exists(restoreMapPath + ".tablespec.json"));
            Assert.Equal(Path.GetFullPath(restoreMapPath), restored.MapPath);
            Assert.Equal(2, restored.EffectiveSchemaVersion);
            Assert.Equal("extend0.tests.users", restored.SchemaId);

            var loaded = TableSpec.Helpers.LoadFromFile(restoreMapPath + ".tablespec.json");
            Assert.Equal(restored, loaded);
            Assert.True(MetadataStorageHarness.TryLoadMappedColumns(restoreMapPath, out var columns));
            Assert.Single(columns);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void SnapshotAndRestore_RoundTripChunkedStorage()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var sourceDirectory = Path.Combine(tempRoot, "source", "chunked-users");
            var source = CreateSpec("ChunkedUsers", sourceDirectory) with
            {
                SchemaVersion = 2,
                Storage = TableStorageOptions.Chunked(chunkSize: 256)
            };

            using (MetadataStorageHarness.CreateSegmentedMappedStore(source))
            {
            }

            var snapshotDirectory = Path.Combine(tempRoot, "snapshot");
            var manifest = MetaDbSnapshot.Create(source, snapshotDirectory);
            var restoreDirectory = Path.Combine(tempRoot, "restore", "chunked-users");
            var restored = MetaDbSnapshot.Restore(snapshotDirectory, restoreDirectory);

            Assert.True(manifest.ContainsRuntimeStorage);
            Assert.True(File.Exists(Path.Combine(restoreDirectory, "tablespec.json")));
            Assert.True(File.Exists(Path.Combine(restoreDirectory, "manifest.json")));
            Assert.True(Directory.EnumerateFiles(Path.Combine(restoreDirectory, "chunks"), "*.chk").Any());
            Assert.Equal(Path.GetFullPath(restoreDirectory), restored.MapPath);
            Assert.Equal(TableStorageLayout.Chunked, restored.Storage.Layout);

            var loaded = TableSpec.Helpers.LoadFromFile(Path.Combine(restoreDirectory, "tablespec.json"));
            Assert.Equal(restored, loaded);
            Assert.True(MetadataStorageHarness.TryLoadSegmentedColumns(restoreDirectory, out var columns));
            Assert.Single(columns);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static TableSpec CreateSpec(string name, string mapPath) =>
        new(
            name,
            mapPath,
            [
                TableSpec.Helpers.Column("Name", capacity: 4, keyBytes: 16, valueBytes: 64)
            ]);

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Schema.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
