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
            public const string DefaultSpecExtension = ".meta.tablespec.json";

            public const string ChunkedSpecFileName = "tablespec.json";

            private const long MaxSpecCandidateBytes = 16L * 1024L * 1024L;

            /// <summary>
            /// Base size, in bytes, reserved for a reference cell.
            /// </summary>
            public static int RefSize => 0x80;

            /// <summary>
            /// Maximum key or value segment size that can be represented by the current
            /// 32-bit <see cref="MetadataEntrySize"/> encoding.
            /// </summary>
            public const int MaxPackedSegmentBytes = ushort.MaxValue;

            /// <summary>
            /// Maximum number of reference slots that fit in one value segment with the
            /// current packed size encoding.
            /// </summary>
            public static int MaxRefsPerCell => MaxPackedSegmentBytes / RefSize;

            // ─────────────────────────────────────────────────────────────────────
            // Column factories
            // ─────────────────────────────────────────────────────────────────────

            /// <summary>
            /// Packs key and value segment sizes after validating that the current
            /// 16-bit-per-segment encoding can represent them without truncation.
            /// </summary>
            public static MetadataEntrySize PackColumnSize(int keyBytes, int valueBytes)
            {
                if (keyBytes < 0)
                    throw new ArgumentOutOfRangeException(nameof(keyBytes), keyBytes, "Key size cannot be negative.");
                if (keyBytes > MaxPackedSegmentBytes)
                    throw new ArgumentOutOfRangeException(nameof(keyBytes), keyBytes, $"Key size cannot exceed {MaxPackedSegmentBytes} bytes.");
                if (valueBytes <= 0)
                    throw new ArgumentOutOfRangeException(nameof(valueBytes), valueBytes, "Value size must be positive.");
                if (valueBytes > MaxPackedSegmentBytes)
                    throw new ArgumentOutOfRangeException(nameof(valueBytes), valueBytes, $"Value size cannot exceed {MaxPackedSegmentBytes} bytes.");

                return MetadataEntrySizeExtensions.PackUnchecked(keyBytes, valueBytes);
            }

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
                return new(PackColumnSize(keyBytes, sz), name, readOnly, capacity);
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
                => new(PackColumnSize(keyBytes, valueBytes), name, readOnly, capacity);

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
            {
                if (refsPerCell <= 0)
                    throw new ArgumentOutOfRangeException(nameof(refsPerCell), refsPerCell, "Reference columns must reserve at least one reference slot per cell.");
                if (refsPerCell > MaxRefsPerCell)
                    throw new ArgumentOutOfRangeException(nameof(refsPerCell), refsPerCell, $"The current MetadataEntrySize encoding supports at most {MaxRefsPerCell} references per cell.");

                return new(PackColumnSize(keyBytes, checked(RefSize * refsPerCell)), name, readOnly, capacity);
            }

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
            /// Computes the path used by <see cref="TableSpec.SaveToDirectory(string, string, bool)"/>.
            /// </summary>
            public static string GetSpecPathInDirectory(
                string directory,
                string tableName,
                TableStorageOptions storage = default,
                string extension = DefaultSpecExtension)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(directory);

                var normalizedExtension = NormalizeSpecExtension(extension);
                var fileName = SanitizeFileName(tableName).ToLowerInvariant();
                if (storage.Normalize().Layout == TableStorageLayout.Chunked)
                {
                    var tableDirectory = Path.Combine(directory, fileName);
                    var chunkedFileName = string.Equals(normalizedExtension, DefaultSpecExtension, StringComparison.OrdinalIgnoreCase)
                        ? ChunkedSpecFileName
                        : ("tablespec" + normalizedExtension).ToLowerInvariant();
                    return Path.Combine(tableDirectory, chunkedFileName);
                }

                return Path.Combine(directory, (fileName + normalizedExtension).ToLowerInvariant());
            }

            /// <summary>
            /// Resolves a TableSpec path from a direct spec path, table directory, map path, or missing map path.
            /// The resolver accepts custom JSON spec extensions by loading candidate files and matching their MapPath.
            /// </summary>
            public static bool TryResolveSpecPath(string inputPath, out string specPath, out string error)
            {
                var fullInput = Path.GetFullPath(inputPath);

                if (File.Exists(fullInput))
                {
                    if (TryLoadSpecCandidate(fullInput, out _))
                    {
                        specPath = fullInput;
                        error = string.Empty;
                        return true;
                    }

                    if (TryResolveFromMapPath(fullInput, out specPath, out error))
                        return true;

                    return false;
                }

                if (Directory.Exists(fullInput))
                {
                    var chunkedSpec = Path.Combine(fullInput, ChunkedSpecFileName);
                    if (TryLoadSpecCandidate(chunkedSpec, out _))
                    {
                        specPath = chunkedSpec;
                        error = string.Empty;
                        return true;
                    }

                    var candidates = LoadSpecCandidates(Directory.EnumerateFiles(fullInput)).ToArray();
                    return SelectCandidate(
                        candidates,
                        fullInput,
                        allowUniqueFallback: true,
                        noCandidateError: $"No TableSpec found in directory '{fullInput}'.",
                        multipleCandidateError: $"Multiple TableSpec files found in directory '{fullInput}'. Pass one explicitly.",
                        out specPath,
                        out error);
                }

                if (TryResolveFromMapPath(fullInput, out specPath, out error))
                    return true;

                return false;
            }

            /// <summary>
            /// Attempts to infer whether a spec path can be produced by SaveToDirectory for the supplied spec.
            /// </summary>
            public static bool TryInferSaveToDirectoryExtension(
                TableSpec spec,
                string directory,
                string specPath,
                out string extension)
            {
                extension = string.Empty;
                if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(specPath))
                    return false;

                var fullDirectory = Path.GetFullPath(directory);
                var fullSpecPath = Path.GetFullPath(specPath);
                var specDirectory = Path.GetDirectoryName(fullSpecPath);
                if (string.IsNullOrWhiteSpace(specDirectory))
                    return false;

                var sanitizedName = SanitizeFileName(spec.Name).ToLowerInvariant();
                var fileName = Path.GetFileName(fullSpecPath);

                if (spec.Storage.Normalize().Layout == TableStorageLayout.Chunked)
                {
                    var expectedTableDirectory = Path.Combine(fullDirectory, sanitizedName);
                    if (!PathsEqual(specDirectory, expectedTableDirectory))
                        return false;

                    if (string.Equals(fileName, ChunkedSpecFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        extension = DefaultSpecExtension;
                        return true;
                    }

                    if (fileName.StartsWith("tablespec.", StringComparison.OrdinalIgnoreCase))
                    {
                        extension = fileName["tablespec".Length..];
                        return extension.Length > 1;
                    }

                    return false;
                }

                if (!PathsEqual(specDirectory, fullDirectory))
                    return false;

                if (!fileName.StartsWith(sanitizedName, StringComparison.OrdinalIgnoreCase))
                    return false;

                extension = fileName[sanitizedName.Length..];
                return extension.Length > 0;
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

                var normalizedSpecs = specs
                    .Select(static s =>
                    {
                        var normalized = s.NormalizeForPersistence();
                        normalized.Validate();
                        return normalized;
                    })
                    .ToArray();

                var dir = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(dir))
                    Directory.CreateDirectory(dir);

                var json = JsonSerializer.Serialize(normalizedSpecs, Json);
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

            private static string NormalizeSpecExtension(string extension)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(extension);
                var trimmed = extension.Trim();
                return trimmed.StartsWith(".", StringComparison.Ordinal) ? trimmed : "." + trimmed;
            }

            private static bool TryResolveFromMapPath(string mapPath, out string specPath, out string error)
            {
                var directSidecar = mapPath + ".tablespec.json";
                if (TryLoadSpecCandidate(directSidecar, out _))
                {
                    specPath = directSidecar;
                    error = string.Empty;
                    return true;
                }

                var chunkedSpec = Path.Combine(mapPath, ChunkedSpecFileName);
                if (TryLoadSpecCandidate(chunkedSpec, out _))
                {
                    specPath = chunkedSpec;
                    error = string.Empty;
                    return true;
                }

                var directory = Path.GetDirectoryName(mapPath);
                if (!string.IsNullOrWhiteSpace(directory) && Directory.Exists(directory))
                {
                    var candidates = LoadSpecCandidates(Directory.EnumerateFiles(directory)).ToArray();
                    return SelectCandidate(
                        candidates,
                        mapPath,
                        allowUniqueFallback: false,
                        noCandidateError: $"No TableSpec found for '{mapPath}'. Input file is not a TableSpec and no matching TableSpec exists in '{directory}'.",
                        multipleCandidateError: $"No TableSpec found for '{mapPath}'. Input file has no direct sidecar and no sibling TableSpec declares that MapPath.",
                        out specPath,
                        out error);
                }

                specPath = string.Empty;
                error = $"No TableSpec found for '{mapPath}'.";
                return false;
            }

            private static bool SelectCandidate(
                IReadOnlyList<SpecCandidate> candidates,
                string expectedMapPath,
                bool allowUniqueFallback,
                string noCandidateError,
                string multipleCandidateError,
                out string specPath,
                out string error)
            {
                if (candidates.Count == 0)
                {
                    specPath = string.Empty;
                    error = noCandidateError;
                    return false;
                }

                var matches = candidates
                    .Where(candidate => SpecMapPathMatches(candidate, expectedMapPath))
                    .ToArray();

                if (matches.Length == 1)
                {
                    specPath = matches[0].Path;
                    error = string.Empty;
                    return true;
                }

                if (matches.Length > 1)
                {
                    specPath = string.Empty;
                    error = $"Multiple TableSpec files match '{expectedMapPath}'. Pass one explicitly.";
                    return false;
                }

                if (allowUniqueFallback && candidates.Count == 1)
                {
                    specPath = candidates[0].Path;
                    error = string.Empty;
                    return true;
                }

                specPath = string.Empty;
                error = multipleCandidateError;
                return false;
            }

            private static bool SpecMapPathMatches(SpecCandidate candidate, string expectedMapPath)
            {
                var specDirectory = Path.GetDirectoryName(candidate.Path);
                if (string.IsNullOrWhiteSpace(specDirectory))
                    return false;

                var candidateMapPath = Path.IsPathRooted(candidate.Spec.MapPath)
                    ? Path.GetFullPath(candidate.Spec.MapPath)
                    : Path.GetFullPath(Path.Combine(specDirectory, candidate.Spec.MapPath));

                return PathsEqual(candidateMapPath, expectedMapPath);
            }

            private static IEnumerable<SpecCandidate> LoadSpecCandidates(IEnumerable<string> paths)
            {
                foreach (var path in paths)
                {
                    if (!CouldBeSpecCandidate(path))
                        continue;

                    if (TryLoadSpecCandidate(path, out var spec) && spec is { } loaded)
                        yield return new SpecCandidate(Path.GetFullPath(path), loaded);
                }
            }

            private static bool TryLoadSpecCandidate(string path, out TableSpec? spec)
            {
                spec = null;
                if (!File.Exists(path) || !CouldBeSpecCandidate(path))
                    return false;

                try
                {
                    spec = LoadFromFile(path);
                    return true;
                }
                catch
                {
                    spec = null;
                    return false;
                }
            }

            private static bool CouldBeSpecCandidate(string path)
            {
                if (!File.Exists(path))
                    return false;

                try
                {
                    if (new FileInfo(path).Length > MaxSpecCandidateBytes)
                        return false;
                }
                catch
                {
                    return false;
                }

                return true;
            }

            private static bool PathsEqual(string left, string right) =>
                string.Equals(Path.GetFullPath(left), Path.GetFullPath(right), StringComparison.OrdinalIgnoreCase);

            private sealed record SpecCandidate(string Path, TableSpec Spec);
        }
    }
}
