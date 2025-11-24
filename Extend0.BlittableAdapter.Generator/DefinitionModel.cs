namespace Extend0.BlittableAdapter.Generator
{
    /// <summary>
    /// High-level model for a blittable adapter definition consumed by the source generator.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="DefinitionModel"/> ties together the logical record shape
    /// (<see cref="RecordName"/> and <see cref="Fields"/>) with the target namespace
    /// for generated adapters and the backing metadata column that will store the data.
    /// </para>
    /// <para>
    /// Typically this is produced by <c>SimpleBlitParser</c> from a small JSON
    /// configuration file and then used to drive the code generation step.
    /// </para>
    /// </remarks>
    public sealed class DefinitionModel
    {
        /// <summary>
        /// Name of the logical record type the adapter is generated for.
        /// </summary>
        /// <remarks>
        /// This usually corresponds to a C# <c>record</c> or <c>class</c> name that
        /// will be mapped to a single row in a metadata table.
        /// </remarks>
        public string RecordName { get; set; } = default!;

        /// <summary>
        /// Target namespace where the generated adapter types will be emitted.
        /// </summary>
        /// <remarks>
        /// If not explicitly provided in the input definition, this commonly defaults
        /// to something under <c>Extend0.Metadata.Generated</c>.
        /// </remarks>
        public string AdapterNamespace { get; set; } = default!;

        /// <summary>
        /// Name of the metadata column backing this record’s serialized representation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is typically used to locate or create the <c>MetadataTable</c> column
        /// that will store the raw bytes for the generated adapter.
        /// </para>
        /// <para>
        /// When omitted in the source definition, it usually defaults to the
        /// <see cref="RecordName"/>.
        /// </para>
        /// </remarks>
        public string ColumnName { get; set; } = default!;

        /// <summary>
        /// Ordered list of fields that compose the record layout.
        /// </summary>
        /// <remarks>
        /// The generator walks this array to allocate offsets, compute sizes and emit
        /// read/write helpers for each <see cref="FieldModel"/> according to its
        /// <see cref="FieldModel.Kind"/> and <see cref="FieldModel.MaxBytes"/>.
        /// </remarks>
        public FieldModel[] Fields { get; set; } = [];
    }
}