using Extend0.Metadata.CodeGen;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Extend0.Metadata.Schema
{
    public readonly partial record struct TableSpec
    {
        public class Helpers
        {
            /// <summary>
            /// Base size, in bytes, reserved for a reference cell.
            /// </summary>
            public static int RefSize => 0x80;

            // ─────────────────────────────────────────────────────────────────────
            // Column factories
            // ─────────────────────────────────────────────────────────────────────

            /// <summary>
            /// Creates a column configuration for blittable structs of type <typeparamref name="T"/>.
            /// </summary>
            /// <typeparam name="T">Unmanaged struct type to be stored in the column.</typeparam>
            /// <param name="name">Logical column name.</param>
            /// <param name="capacity">Initial row capacity for the column.</param>
            /// <param name="keyBytes">
            /// Number of bytes reserved for the key portion of each entry.
            /// Defaults to 32 bytes.
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
                int keyBytes = 32,
                int? valueBytes = null,
                bool readOnly = false
            ) where T : unmanaged
            {
                int sz = valueBytes ?? Unsafe.SizeOf<T>();
                return new(MetadataEntrySizeExtensions.PackUnchecked(keyBytes, sz), name, readOnly, capacity);
            }

            /// <summary>
            /// Creates a fixed-size blob column configuration.
            /// </summary>
            /// <param name="name">Logical column name.</param>
            /// <param name="capacity">Initial row capacity for the column.</param>
            /// <param name="valueBytes">Value size in bytes for each cell.</param>
            /// <param name="keyBytes">
            /// Number of bytes reserved for the key portion of each entry.
            /// Defaults to 32 bytes.
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
                int keyBytes = 32,
                bool readOnly = false
            )
                => new(MetadataEntrySizeExtensions.PackUnchecked(keyBytes, valueBytes), name, readOnly, capacity);

            /// <summary>
            /// Creates a reference column configuration, modeled as a fixed-size blob.
            /// </summary>
            /// <param name="capacity">Initial row capacity for the column.</param>
            /// <param name="name">
            /// Logical column name. Defaults to <c>"Refs"</c>.
            /// </param>
            /// <param name="keyBytes">
            /// Number of bytes reserved for the key portion of each entry.
            /// Defaults to 32 bytes.
            /// </param>
            /// <param name="refsPerCell">
            /// Number of reference slots stored in each cell. The resulting value
            /// size is <see cref="RefSize"/> × <paramref name="refsPerCell"/>.
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
                int keyBytes = 32,
                int refsPerCell = 1,
                bool readOnly = false
            )
                => new(MetadataEntrySizeExtensions.PackUnchecked(keyBytes, RefSize * refsPerCell), name, readOnly, capacity);

            // ─────────────────────────────────────────────────────────────────────
            // Persistence (JSON)
            // ─────────────────────────────────────────────────────────────────────

            /// <summary>
            /// Shared JSON serializer options used to persist <see cref="TableSpec"/> instances.
            /// </summary>
            internal static readonly JsonSerializerOptions Json = new()
            {
                WriteIndented = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                PropertyNameCaseInsensitive = true
            };

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
            internal static string SanitizeFileName(string name)
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
}
