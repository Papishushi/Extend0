using System.Text;
using System.Text.Json;

namespace Extend0.Metadata.Schema
{
    /// <summary>
    /// Describes a logical metadata table (name, backing path and columns) and
    /// provides helpers for column configuration and JSON-based persistence.
    /// </summary>
    /// <param name="Name">Logical name of the table.</param>
    /// <param name="MapPath">Path to the backing metadata file (for mapped tables).</param>
    /// <param name="Columns">Column layout of the table.</param>
    public readonly partial record struct TableSpec(string Name, string MapPath, ColumnConfiguration[] Columns)
    {
        /// <summary>
        /// Current semantic schema version used when a persisted legacy spec omits <see cref="SchemaVersion"/>.
        /// </summary>
        public const int CurrentSchemaVersion = 1;

        /// <summary>
        /// Semantic version of this table schema. Legacy JSON files that omit the value are treated as version 1.
        /// </summary>
        public int SchemaVersion { get; init; }

        /// <summary>
        /// Optional stable logical identifier for a schema family across map-path relocations.
        /// </summary>
        public string? SchemaId { get; init; }

        /// <summary>
        /// Optional human-readable schema note. This is documentation metadata, not a compatibility boundary.
        /// </summary>
        public string? SchemaDescription { get; init; }

        /// <summary>
        /// Gets the semantic schema version after applying the legacy default.
        /// </summary>
        public int EffectiveSchemaVersion => SchemaVersion <= 0 ? CurrentSchemaVersion : SchemaVersion;

        /// <summary>
        /// Gets storage-level options for mapped tables. The default layout is <see cref="TableStorageLayout.SingleFile"/>.
        /// </summary>
        public TableStorageOptions Storage { get; init; }

        /// <summary>
        /// Performs basic validation of the <see cref="TableSpec"/> fields.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <see cref="Name"/>, <see cref="MapPath"/> or <see cref="Columns"/>
        /// are missing or invalid.
        /// </exception>
        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
                throw new ArgumentException("TableSpec.Name cannot be empty.");
            if (string.IsNullOrWhiteSpace(MapPath))
                throw new ArgumentException("TableSpec.MapPath cannot be empty.");
            if (Columns is null || Columns.Length == 0)
                throw new ArgumentException("TableSpec.Columns must contain at least one column.");
            if (SchemaVersion < 0)
                throw new ArgumentOutOfRangeException(nameof(SchemaVersion), "TableSpec.SchemaVersion cannot be negative.");
            if (SchemaId is not null && string.IsNullOrWhiteSpace(SchemaId))
                throw new ArgumentException("TableSpec.SchemaId cannot be empty when provided.");
            if (SchemaDescription is not null && string.IsNullOrWhiteSpace(SchemaDescription))
                throw new ArgumentException("TableSpec.SchemaDescription cannot be empty when provided.");

            Storage.Validate();
        }

        /// <summary>
        /// Serializes this <see cref="TableSpec"/> instance to JSON and writes it
        /// to the specified file path.
        /// </summary>
        /// <param name="specPath">Target file path for the JSON representation.</param>
        /// <param name="overwrite">
        /// <see langword="true"/> to overwrite the file if it already exists;
        /// <see langword="false"/> to throw if the file is present.
        /// </param>
        /// <exception cref="IOException">
        /// Thrown when <paramref name="overwrite"/> is <see langword="false"/> and
        /// the file already exists.
        /// </exception>
        public void SaveToFile(string specPath, bool overwrite = true)
        {
            var normalized = NormalizeForPersistence();
            normalized.Validate();
            var dir = Path.GetDirectoryName(specPath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            if (!overwrite && File.Exists(specPath))
                throw new IOException($"File already exists: {specPath}");

            var json = JsonSerializer.Serialize(normalized, Helpers.Json);
            File.WriteAllText(specPath, json, Encoding.UTF8);
        }

        /// <summary>
        /// Saves this <see cref="TableSpec"/> into a directory using a sanitized
        /// file name derived from <see cref="Name"/>.
        /// </summary>
        /// <param name="directory">Target directory where the file will be written.</param>
        /// <param name="extension">
        /// File extension to use. Defaults to <c>".meta.tablespec.json"</c>.
        /// </param>
        /// <param name="overwrite">
        /// <see langword="true"/> to overwrite the file if it already exists;
        /// <see langword="false"/> to throw if the file is present.
        /// </param>
        /// <returns>The full path of the created (or overwritten) file.</returns>
        public string SaveToDirectory(string directory, string extension = ".meta.tablespec.json", bool overwrite = true)
        {
            if (Storage.Normalize().Layout == TableStorageLayout.Chunked)
            {
                var tableDirectory = Path.Combine(directory, Helpers.SanitizeFileName(Name).ToLowerInvariant());
                Directory.CreateDirectory(tableDirectory);
                var specPath = Path.Combine(tableDirectory, "tablespec.json");
                SaveToFile(specPath, overwrite);
                return specPath;
            }

            var fileName = Helpers.SanitizeFileName(Name) + extension;
            var full = Path.Combine(directory, fileName.ToLowerInvariant());
            SaveToFile(full, overwrite);
            return full;
        }

        /// <summary>
        /// Compares two <see cref="TableSpec"/> values structurally, including their column layouts by value.
        /// </summary>
        public bool Equals(TableSpec other)
        {
            if (!string.Equals(Name, other.Name, StringComparison.Ordinal))
                return false;

            if (!string.Equals(MapPath, other.MapPath, StringComparison.Ordinal))
                return false;

            if (Storage.Normalize() != other.Storage.Normalize())
                return false;

            if (EffectiveSchemaVersion != other.EffectiveSchemaVersion)
                return false;

            if (!string.Equals(SchemaId, other.SchemaId, StringComparison.Ordinal))
                return false;

            if (!string.Equals(SchemaDescription, other.SchemaDescription, StringComparison.Ordinal))
                return false;

            if (ReferenceEquals(Columns, other.Columns))
                return true;

            if (Columns is null || other.Columns is null || Columns.Length != other.Columns.Length)
                return false;

            return Columns.AsSpan().SequenceEqual(other.Columns);
        }

        /// <summary>
        /// Computes a structural hash code for this <see cref="TableSpec"/>, including all column definitions.
        /// </summary>
        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(Name, StringComparer.Ordinal);
            hash.Add(MapPath, StringComparer.Ordinal);
            hash.Add(Storage.Normalize());
            hash.Add(EffectiveSchemaVersion);
            hash.Add(SchemaId, StringComparer.Ordinal);
            hash.Add(SchemaDescription, StringComparer.Ordinal);

            if (Columns is not null)
            {
                foreach (var column in Columns)
                    hash.Add(column);
            }

            return hash.ToHashCode();
        }

        internal TableSpec NormalizeForPersistence() =>
            SchemaVersion <= 0
                ? this with { SchemaVersion = CurrentSchemaVersion }
                : this;
    }
}
