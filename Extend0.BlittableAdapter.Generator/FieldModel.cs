namespace Extend0.BlittableAdapter.Generator
{
    /// <summary>
    /// Describes a single field in a blittable adapter definition.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="FieldModel"/> represents one logical member of the source record
    /// (e.g. a property in a DTO) and its mapping to a fixed or variable-size
    /// segment in the underlying metadata column.
    /// </para>
    /// <para>
    /// The <see cref="Kind"/> hint is used by the generator to choose an encoding
    /// strategy (e.g. GUID, date/time, UTF-8 text, raw binary, etc.).
    /// </para>
    /// </remarks>
    public sealed class FieldModel
    {
        /// <summary>
        /// Logical name of the field, typically matching a record or DTO property name.
        /// </summary>
        public string Name { get; set; } = default!;

        /// <summary>
        /// Optional semantic kind of the field (e.g. <c>"Guid"</c>, <c>"DateTimeOffset"</c>,
        /// <c>"Utf8"</c>, <c>"Binary"</c>).
        /// </summary>
        /// <remarks>
        /// When <see langword="null"/> or empty, the generator may assume a default
        /// encoding (commonly a raw binary blob).
        /// </remarks>
        public string? Kind { get; set; }          // "Guid", "DateTimeOffset", "Utf8", "Binary", etc.

        /// <summary>
        /// Optional maximum number of bytes reserved for this field in the underlying column.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This limit is primarily used for variable-size encodings such as UTF-8 text
        /// or opaque binary buffers. Fixed-size kinds (e.g. GUID, 64-bit integers) may
        /// ignore this value.
        /// </para>
        /// <para>
        /// When <see langword="null"/>, the generator may fall back to a default size
        /// depending on the <see cref="Kind"/>.
        /// </para>
        /// </remarks>
        public int? MaxBytes { get; set; }         // sólo para Utf8 / Binary
    }
}
