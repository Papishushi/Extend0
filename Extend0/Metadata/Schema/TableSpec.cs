using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Extend0.Metadata.CodeGen;

namespace Extend0.Metadata.Schema
{
    /// <summary>
    /// Describes a logical metadata table (name, backing path and columns) and
    /// provides helpers for column configuration and JSON-based persistence.
    /// </summary>
    /// <param name="Name">Logical name of the table.</param>
    /// <param name="MapPath">Path to the backing metadata file (for mapped tables).</param>
    /// <param name="Columns">Column layout of the table.</param>
    public readonly record struct TableSpec(string Name, string MapPath, ColumnConfiguration[] Columns)
    {
        /// <summary>
        /// Base size, in bytes, reserved for a reference cell.
        /// </summary>
        public const int REF_SIZE = 0x80;

        // ─────────────────────────────────────────────────────────────────────
        // Column factories
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Creates a column configuration for blittable structs of type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">Unmanaged struct type to be stored in the column.</typeparam>
        /// <param name="name">Logical column name.</param>
        /// <param name="capacity">Initial row capacity for the column.</param>
        /// <param name="keyBits">
        /// Number of bits reserved for the key portion of each entry.
        /// Defaults to 32 bits.
        /// </param>
        /// <param name="valueBytes">
        /// Optional override for the value size in bytes. When <see langword="null"/>,
        /// <see cref="Unsafe.SizeOf{T}"/> is used.
        /// </param>
        /// <param name="readOnly">
        /// Whether the column should be treated as read-only from the metadata layer.
        /// </param>
        /// <returns>
        /// A <see cref="ColumnConfiguration"/> describing a fixed-size column whose
        /// value size is <c>sizeof(T)</c> (or <paramref name="valueBytes"/> when provided).
        /// </returns>
        public static ColumnConfiguration Column<T>(
            string name,
            uint capacity,
            int keyBits = 32,
            int? valueBytes = null,
            bool readOnly = false
        ) where T : unmanaged
        {
            int sz = valueBytes ?? Unsafe.SizeOf<T>();
            return new(MetadataEntrySizeExtensions.PackUnchecked(keyBits, sz), name, readOnly, capacity);
        }

        /// <summary>
        /// Creates a fixed-size blob column configuration.
        /// </summary>
        /// <param name="name">Logical column name.</param>
        /// <param name="capacity">Initial row capacity for the column.</param>
        /// <param name="valueBytes">Value size in bytes for each cell.</param>
        /// <param name="keyBits">
        /// Number of bits reserved for the key portion of each entry.
        /// Defaults to 32 bits.
        /// </param>
        /// <param name="readOnly">
        /// Whether the column should be treated as read-only from the metadata layer.
        /// </param>
        /// <returns>
        /// A <see cref="ColumnConfiguration"/> describing a fixed-size blob column.
        /// </returns>
        public static ColumnConfiguration Column(
            string name,
            uint capacity,
            int valueBytes,
            int keyBits = 32,
            bool readOnly = false
        )
            => new(MetadataEntrySizeExtensions.PackUnchecked(keyBits, valueBytes), name, readOnly, capacity);

        /// <summary>
        /// Creates a reference column configuration, modeled as a fixed-size blob.
        /// </summary>
        /// <param name="capacity">Initial row capacity for the column.</param>
        /// <param name="name">
        /// Logical column name. Defaults to <c>"Refs"</c>.
        /// </param>
        /// <param name="keyBits">
        /// Number of bits reserved for the key portion of each entry.
        /// Defaults to 32 bits.
        /// </param>
        /// <param name="refsPerCell">
        /// Number of reference slots stored in each cell. The resulting value
        /// size is <see cref="REF_SIZE"/> × <paramref name="refsPerCell"/>.
        /// </param>
        /// <param name="readOnly">
        /// Whether the column should be treated as read-only from the metadata layer.
        /// </param>
        /// <returns>
        /// A <see cref="ColumnConfiguration"/> suitable for storing references (IDs, pointers, etc.).
        /// </returns>
        public static ColumnConfiguration RefsColumn(
            uint capacity,
            string name = "Refs",
            int keyBits = 32,
            int refsPerCell = 1,
            bool readOnly = false
        )
            => new(MetadataEntrySizeExtensions.PackUnchecked(keyBits, REF_SIZE * refsPerCell), name, readOnly, capacity);

        // ─────────────────────────────────────────────────────────────────────
        // Persistence (JSON)
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Shared JSON serializer options used to persist <see cref="TableSpec"/> instances.
        /// </summary>
        private static readonly JsonSerializerOptions Json = new()
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true
        };

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
            Validate();
            var dir = Path.GetDirectoryName(specPath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            if (!overwrite && File.Exists(specPath))
                throw new IOException($"File already exists: {specPath}");

            var json = JsonSerializer.Serialize(this, Json);
            File.WriteAllText(specPath, json, Encoding.UTF8);
        }

        /// <summary>
        /// Loads a <see cref="TableSpec"/> from a JSON file.
        /// </summary>
        /// <param name="specPath">Path to the JSON file containing the table specification.</param>
        /// <returns>A fully validated <see cref="TableSpec"/> instance.</returns>
        /// <exception cref="FileNotFoundException">
        /// Thrown when the file at <paramref name="specPath"/> does not exist.
        /// </exception>
        /// <exception cref="InvalidDataException">
        /// Thrown when the JSON content cannot be deserialized into a valid <see cref="TableSpec"/>.
        /// </exception>
        public static TableSpec LoadFromFile(string specPath)
        {
            if (!File.Exists(specPath))
                throw new FileNotFoundException("TableSpec file not found.", specPath);

            var json = File.ReadAllText(specPath, Encoding.UTF8);
            var spec = JsonSerializer.Deserialize<TableSpec?>(json, Json)
                       ?? throw new InvalidDataException("Invalid JSON for TableSpec.");
            spec.Validate();
            return spec;
        }

        /// <summary>
        /// Attempts to load a <see cref="TableSpec"/> from a JSON file, returning
        /// a boolean value indicating success.
        /// </summary>
        /// <param name="specPath">Path to the JSON file containing the table specification.</param>
        /// <param name="spec">
        /// When this method returns <see langword="true"/>, contains the loaded
        /// <see cref="TableSpec"/>; otherwise, <see langword="null"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the spec was loaded and validated successfully;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        public static bool TryLoadFromFile(string specPath, out TableSpec? spec)
        {
            spec = null;
            try
            {
                spec = LoadFromFile(specPath);
                return true;
            }
            catch
            {
                return false;
            }
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
            var fileName = SanitizeFileName(Name) + extension;
            var full = Path.Combine(directory, fileName.ToLowerInvariant());
            SaveToFile(full, overwrite);
            return full;
        }

        /// <summary>
        /// Saves a sequence of <see cref="TableSpec"/> instances into a single JSON file
        /// as an array.
        /// </summary>
        /// <param name="path">Target file path for the JSON array.</param>
        /// <param name="specs">Sequence of table specifications to persist.</param>
        /// <param name="overwrite">
        /// <see langword="true"/> to overwrite the file if it already exists;
        /// <see langword="false"/> to throw if the file is present.
        /// </param>
        /// <exception cref="IOException">
        /// Thrown when <paramref name="overwrite"/> is <see langword="false"/> and
        /// the file already exists.
        /// </exception>
        public static void SaveMany(string path, IEnumerable<TableSpec> specs, bool overwrite = true)
        {
            if (!overwrite && File.Exists(path))
                throw new IOException($"File already exists: {path}");

            // Validate all specs before writing
            foreach (var s in specs) s.Validate();

            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            var json = JsonSerializer.Serialize(specs, Json);
            File.WriteAllText(path, json, Encoding.UTF8);
        }

        /// <summary>
        /// Loads an array of <see cref="TableSpec"/> instances from a JSON file.
        /// </summary>
        /// <param name="path">Path to the JSON file containing the array of specs.</param>
        /// <returns>
        /// A read-only list of validated <see cref="TableSpec"/> instances.
        /// </returns>
        /// <exception cref="FileNotFoundException">
        /// Thrown when the file at <paramref name="path"/> does not exist.
        /// </exception>
        /// <exception cref="InvalidDataException">
        /// Thrown when the JSON content cannot be deserialized into a valid list.
        /// </exception>
        public static IReadOnlyList<TableSpec> LoadMany(string path)
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("TableSpecs file not found.", path);

            var json = File.ReadAllText(path, Encoding.UTF8);
            var list = JsonSerializer.Deserialize<List<TableSpec>>(json, Json)
                       ?? throw new InvalidDataException("Invalid JSON for TableSpec list.");
            foreach (var s in list) s.Validate();
            return list;
        }

        // ─────────────────────────────────────────────────────────────────────
        // Utilities
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Produces a file-system-safe version of the specified table name.
        /// </summary>
        /// <param name="name">Original table name.</param>
        /// <returns>
        /// A sanitized string that can safely be used as a file name, where
        /// non alphanumeric characters (except '-' and '_') are replaced by '_'.
        /// </returns>
        private static string SanitizeFileName(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return "table";
            var sb = new StringBuilder(name.Length);
            foreach (var ch in name)
            {
                sb.Append(char.IsLetterOrDigit(ch) || ch is '-' or '_' ? ch : '_');
            }
            return sb.ToString();
        }
    }
}
