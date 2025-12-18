namespace Extend0.Metadata.CrossProcess
{
    /// <summary>
    /// Serializable, cross-process safe representation of a single MetaDB cell snapshot.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This DTO is designed for RCP / IPC boundaries where returning a <c>MetadataCell</c> (or any view/pointer-backed
    /// structure) is unsafe. It captures the <em>meaning</em> of a cell at a point in time (layout, emptiness signals,
    /// and optional payload copies) without exposing process-dependent state such as pointers, memory-mapped views,
    /// or ownership semantics.
    /// </para>
    ///
    /// <para>
    /// <b>Cell layout model</b>:
    /// a MetaDB entry is a fixed-size record composed of two contiguous segments:
    /// <c>[ KEY ][ VALUE ]</c>. The segment capacities are derived from <see cref="EntrySize"/>:
    /// <list type="bullet">
    ///   <item><description><see cref="KeyCapacity"/> bytes for KEY.</description></item>
    ///   <item><description><see cref="ValueCapacity"/> bytes for VALUE.</description></item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// <b>Column modes</b>:
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>Key/Value columns</b>: <see cref="IsKeyValue"/> is <see langword="true"/> (i.e., <see cref="KeyCapacity"/> &gt; 0).
    ///     A cell is typically considered "present" when it stores a non-empty key (<see cref="HasKey"/>).
    ///   </description></item>
    ///   <item><description>
    ///     <b>Value-only columns</b>: <see cref="IsKeyValue"/> is <see langword="false"/> (i.e., <see cref="KeyCapacity"/> == 0).
    ///     Emptiness is best-effort: <see cref="HasAnyValue"/> can be used to detect non-zero bytes in VALUE.
    ///   </description></item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// <b>String decoding and length hints</b>:
    /// MetaDB uses "C-string-like" semantics for textual payloads: the first <c>0</c> byte acts as a terminator.
    /// <see cref="KeyUtf8LengthHint"/> and <see cref="ValueUtf8LengthHint"/> report the number of bytes up to the first
    /// <c>0</c> (or the segment capacity when no terminator is present). These are <em>hints</em> and should not be treated
    /// as canonical lengths for arbitrary binary payloads.
    /// </para>
    ///
    /// <para>
    /// <b>Payload strategy</b>:
    /// The DTO can carry UTF-8 decoded strings, raw byte copies, or both, controlled by <see cref="Mode"/>.
    /// This allows callers to trade off size vs. fidelity, particularly for bulk reads such as <c>ReadColumn</c>
    /// or <c>ReadBlock</c>.
    /// </para>
    ///
    /// <para>
    /// <b>Preview</b>:
    /// <see cref="Preview"/> is intended for diagnostics and UI inspection. Implementations may populate it using a
    /// best-effort strategy (e.g., printable UTF-8 when possible, otherwise hexadecimal), possibly truncating long data.
    /// </para>
    /// </remarks>
    public readonly record struct CellResultDTO(
        /// <summary>
        /// Indicates whether the underlying cell existed and could be read.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When <see langword="false"/>, the remaining fields should be treated as default values and any payload members
        /// are expected to be <see langword="null"/>. This allows APIs to distinguish "missing/unreadable cell" from
        /// "present but empty".
        /// </para>
        /// </remarks>
        bool HasCell,

        /// <summary>
        /// Entry size variant describing the fixed KEY and VALUE capacities.
        /// </summary>
        CodeGen.MetadataEntrySize EntrySize,

        /// <summary>
        /// Maximum capacity in bytes for the KEY segment of the entry.
        /// </summary>
        /// <remarks>
        /// <para>
        /// A value of 0 indicates a value-only column layout (no KEY segment).
        /// </para>
        /// </remarks>
        int KeyCapacity,

        /// <summary>
        /// Maximum capacity in bytes for the VALUE segment of the entry.
        /// </summary>
        int ValueCapacity,

        /// <summary>
        /// Indicates whether this cell uses a key/value layout (<see cref="KeyCapacity"/> &gt; 0).
        /// </summary>
        bool IsKeyValue,

        /// <summary>
        /// Indicates whether a non-empty key is stored in the KEY segment.
        /// </summary>
        /// <remarks>
        /// <para>
        /// For key/value columns, this is a common "presence" signal. For value-only columns this should be <see langword="false"/>.
        /// </para>
        /// </remarks>
        bool HasKey,

        /// <summary>
        /// Indicates whether the VALUE segment contains any non-zero byte.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is primarily used for value-only columns to detect "empty" rows where VALUE is all zeros.
        /// For key/value columns this is optional and may be used as an additional signal.
        /// </para>
        /// </remarks>
        bool HasAnyValue,

        /// <summary>
        /// Best-effort hint for the UTF-8 key length in bytes, measured up to the first <c>0</c> byte terminator.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is only meaningful when the KEY segment stores a textual UTF-8 payload using terminator semantics.
        /// For arbitrary binary keys, treat this as an implementation detail.
        /// </para>
        /// </remarks>
        int KeyUtf8LengthHint,

        /// <summary>
        /// Best-effort hint for the UTF-8 value length in bytes, measured up to the first <c>0</c> byte terminator.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is only meaningful when the VALUE segment stores a textual UTF-8 payload using terminator semantics.
        /// For arbitrary binary payloads, this hint can be misleading.
        /// </para>
        /// </remarks>
        int ValueUtf8LengthHint,

        /// <summary>
        /// Specifies which payload representations are included in this DTO.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Implementations should honor this selection to avoid duplicating data in bulk reads.
        /// </para>
        /// </remarks>
        CellPayloadMode Mode,

        /// <summary>
        /// UTF-8 decoded key string when available and requested by <see cref="Mode"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// For value-only columns this is expected to be <see langword="null"/>. For key/value columns,
        /// it may still be <see langword="null"/> when the key is absent, not requested, or not decodable.
        /// </para>
        /// </remarks>
        string? KeyUtf8,

        /// <summary>
        /// UTF-8 decoded value string when available and requested by <see cref="Mode"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This may be <see langword="null"/> when the value is empty, not requested, or not decodable as printable UTF-8.
        /// For binary payloads, prefer <see cref="ValueRaw"/>.
        /// </para>
        /// </remarks>
        string? ValueUtf8,

        /// <summary>
        /// Raw byte copy of the key payload when requested by <see cref="Mode"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Implementations typically return a trimmed payload (up to <see cref="KeyUtf8LengthHint"/>),
        /// but may choose to return the full KEY segment capacity for strict binary fidelity.
        /// </para>
        /// </remarks>
        byte[]? KeyRaw,

        /// <summary>
        /// Raw byte copy of the value payload when requested by <see cref="Mode"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Implementations may return a full VALUE segment copy (binary-safe and fixed-size) or a trimmed copy
        /// (up to <see cref="ValueUtf8LengthHint"/>) depending on the API contract and performance goals.
        /// </para>
        /// </remarks>
        byte[]? ValueRaw,

        /// <summary>
        /// Human-readable best-effort preview of the cell payload, intended for diagnostics and UI inspection.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Implementations may populate this using a strategy such as: printable UTF-8 when possible, otherwise hexadecimal,
        /// typically truncating long payloads. This field should not be used for round-tripping data.
        /// </para>
        /// </remarks>
        string? Preview
    );
}
