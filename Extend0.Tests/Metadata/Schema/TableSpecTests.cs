using Extend0.Metadata.Schema;

namespace Extend0.Tests.Metadata.Schema;

public sealed class TableSpecTests
{
    [Fact]
    public void Validate_Throws_WhenNameIsMissing()
    {
        var spec = new TableSpec("", "table.map", [TableSpec.Helpers.Column<int>("Id", 4)]);

        var ex = Assert.Throws<ArgumentException>(() => spec.Validate());

        Assert.Contains("Name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SaveToFile_AndLoadFromFile_RoundTrip()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var path = Path.Combine(tempRoot, "spec.json");
            var spec = new TableSpec(
                "Users",
                "users.map",
                [TableSpec.Helpers.Column<int>("Id", 4), TableSpec.Helpers.RefsColumn(4)]);

            spec.SaveToFile(path);
            var loaded = TableSpec.Helpers.LoadFromFile(path);

            Assert.Equal(spec, loaded);
            Assert.Equal(spec.Name, loaded.Name);
            Assert.Equal(spec.MapPath, loaded.MapPath);
            Assert.Equal(spec.Columns, loaded.Columns);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void SaveToDirectory_SanitizesName_AndLowercasesFileName()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var spec = new TableSpec("User Profile/Config", "users.map", [TableSpec.Helpers.Column<int>("Id", 4)]);

            var path = spec.SaveToDirectory(tempRoot);

            Assert.EndsWith("user_profile_config.meta.tablespec.json", path, StringComparison.Ordinal);
            Assert.True(File.Exists(path));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void SaveToDirectory_RespectsCustomExtension_AndStructurallyHashesEqualSpecs()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            ColumnConfiguration[] columns =
            [
                TableSpec.Helpers.Column<int>("Id", 4),
                TableSpec.Helpers.RefsColumn(4)
            ];
            var left = new TableSpec("Users", "users.map", columns);
            var right = new TableSpec("Users", "users.map", [.. columns]);
            var differentName = new TableSpec("Orders", "users.map", [.. columns]);
            var differentPath = new TableSpec("Users", "orders.map", [.. columns]);
            var differentColumns = new TableSpec("Users", "users.map", [TableSpec.Helpers.Column<long>("Id", 4)]);

            var path = left.SaveToDirectory(tempRoot, extension: ".SPEC.JSON");

            Assert.EndsWith("users.spec.json", path, StringComparison.Ordinal);
            Assert.Equal(left, right);
            Assert.Equal(left.GetHashCode(), right.GetHashCode());
            Assert.NotEqual(left, differentName);
            Assert.NotEqual(left, differentPath);
            Assert.NotEqual(left, differentColumns);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void SaveMany_AndLoadMany_RoundTrip()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var path = Path.Combine(tempRoot, "specs.json");
            var specs =
                new[]
                {
                    new TableSpec("Users", "users.map", [TableSpec.Helpers.Column<int>("Id", 4)]),
                    new TableSpec("Orders", "orders.map", [TableSpec.Helpers.Column<long>("OrderId", 8)])
                };

            TableSpec.Helpers.SaveMany(path, specs);
            var loaded = TableSpec.Helpers.LoadMany(path);

            Assert.Equal(specs.Length, loaded.Count);
            for (int i = 0; i < specs.Length; i++)
            {
                Assert.Equal(specs[i], loaded[i]);
                Assert.Equal(specs[i].Name, loaded[i].Name);
                Assert.Equal(specs[i].MapPath, loaded[i].MapPath);
                Assert.Equal(specs[i].Columns.Length, loaded[i].Columns.Length);
                Assert.True(specs[i].Columns.SequenceEqual(loaded[i].Columns));
            }
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void TryLoadFromFile_ReturnsFalse_WhenFileIsInvalid()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var path = Path.Combine(tempRoot, "broken.json");
            File.WriteAllText(path, "{not-json}");

            var ok = TableSpec.Helpers.TryLoadFromFile(path, out var spec);

            Assert.False(ok);
            Assert.Null(spec);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void SaveToFile_AndSaveMany_RespectOverwriteFlag()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var singlePath = Path.Combine(tempRoot, "single.json");
            var manyPath = Path.Combine(tempRoot, "many.json");
            var spec = new TableSpec("Users", "users.map", [TableSpec.Helpers.Column<int>("Id", 4)]);

            spec.SaveToFile(singlePath);
            TableSpec.Helpers.SaveMany(manyPath, [spec]);

            Assert.Throws<IOException>(() => spec.SaveToFile(singlePath, overwrite: false));
            Assert.Throws<IOException>(() => TableSpec.Helpers.SaveMany(manyPath, [spec], overwrite: false));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void SaveToFile_CreatesMissingDirectory_AndValidateRejectsMissingColumns()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var spec = new TableSpec("Users", "users.map", [TableSpec.Helpers.Column<int>("Id", 4)]);
            var nested = Path.Combine(tempRoot, "nested", "spec.json");

            spec.SaveToFile(nested);

            Assert.True(File.Exists(nested));

            var missingColumns = new TableSpec("Users", "users.map", []);
            var error = Assert.Throws<ArgumentException>(() => missingColumns.Validate());
            Assert.Contains("Columns", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [Fact]
    public void LoadMany_Throws_WhenFileIsMissing()
    {
        var tempRoot = CreateTempDirectory();
        try
        {
            var path = Path.Combine(tempRoot, "missing.json");

            Assert.Throws<FileNotFoundException>(() => TableSpec.Helpers.LoadMany(path));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "Extend0.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
