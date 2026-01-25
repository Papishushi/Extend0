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

            var json = JsonSerializer.Serialize(this, Helpers.Json);
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
            var fileName = Helpers.SanitizeFileName(Name) + extension;
            var full = Path.Combine(directory, fileName.ToLowerInvariant());
            SaveToFile(full, overwrite);
            return full;
        }
    }
}
