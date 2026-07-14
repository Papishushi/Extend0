namespace Extend0.Metadata.Schema
{
    /// <summary>
    /// Physical storage layout used by a mapped metadata table.
    /// </summary>
    public enum TableStorageLayout
    {
        /// <summary>
        /// A single memory-mapped file stores the full table.
        /// </summary>
        SingleFile = 0,

        /// <summary>
        /// A table directory stores a manifest and one memory-mapped file per column chunk.
        /// </summary>
        Chunked = 1
    }

    /// <summary>
    /// Storage-level options for mapped metadata tables.
    /// </summary>
    /// <param name="Layout">Physical table layout.</param>
    /// <param name="ChunkSize">
    /// Target chunk size in bytes. For <see cref="TableStorageLayout.SingleFile"/>, values greater
    /// than zero opt into chunk-aligned slab growth. For <see cref="TableStorageLayout.Chunked"/>,
    /// this is the exact byte length of each physical chunk file; zero means <see cref="DefaultChunkSize"/>.
    /// </param>
    public readonly record struct TableStorageOptions(TableStorageLayout Layout, int ChunkSize)
    {
        /// <summary>
        /// Default chunk size used by chunked metadata table storage.
        /// </summary>
        public const int DefaultChunkSize = 4 * 1024 * 1024;

        /// <summary>
        /// Creates options for the single-file storage layout.
        /// </summary>
        /// <param name="chunkSize">Optional chunk-alignment size for slab growth; zero disables explicit chunk sizing.</param>
        /// <returns>Single-file storage options.</returns>
        public static TableStorageOptions SingleFile(int chunkSize = 0) =>
            new(TableStorageLayout.SingleFile, chunkSize);

        /// <summary>
        /// Creates options for the chunked storage layout.
        /// </summary>
        /// <param name="chunkSize">Physical chunk size in bytes, or zero to use <see cref="DefaultChunkSize"/> after normalization.</param>
        /// <returns>Chunked storage options.</returns>
        public static TableStorageOptions Chunked(int chunkSize = DefaultChunkSize) =>
            new(TableStorageLayout.Chunked, chunkSize);

        /// <summary>
        /// Returns equivalent options with implicit defaults made explicit.
        /// </summary>
        /// <returns>Normalized storage options.</returns>
        public TableStorageOptions Normalize() =>
            Layout switch
            {
                TableStorageLayout.SingleFile => this,
                TableStorageLayout.Chunked => ChunkSize == 0 ? this with { ChunkSize = DefaultChunkSize } : this,
                _ => throw new ArgumentOutOfRangeException(nameof(Layout), Layout, "Unknown metadata table storage layout.")
            };

        /// <summary>
        /// Validates that the storage options describe a supported physical layout.
        /// </summary>
        public void Validate()
        {
            var normalized = Normalize();
            if (normalized.ChunkSize < 0)
                throw new ArgumentOutOfRangeException(nameof(ChunkSize), "Chunk size cannot be negative.");

            if (normalized.Layout == TableStorageLayout.Chunked && normalized.ChunkSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(ChunkSize), "Chunked storage requires a positive chunk size.");
        }
    }
}
