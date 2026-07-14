namespace Extend0.Metadata.CrossProcess.DTO;

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
/// <param name="HasCell">Indicates whether the underlying cell existed and could be read.</param>
/// <param name="EntrySize">Entry size variant describing the fixed key and value capacities.</param>
/// <param name="KeyCapacity">Maximum capacity in bytes for the key segment of the entry.</param>
/// <param name="ValueCapacity">Maximum capacity in bytes for the value segment of the entry.</param>
/// <param name="IsKeyValue">Indicates whether this cell uses a key/value layout.</param>
/// <param name="HasKey">Indicates whether a non-empty key is stored in the key segment.</param>
/// <param name="HasAnyValue">Indicates whether the value segment contains any non-zero byte.</param>
/// <param name="KeyUtf8LengthHint">Best-effort UTF-8 key length hint, measured up to the first zero byte terminator.</param>
/// <param name="ValueUtf8LengthHint">Best-effort UTF-8 value length hint, measured up to the first zero byte terminator.</param>
/// <param name="Mode">Specifies which payload representations are included in this DTO.</param>
/// <param name="KeyUtf8">UTF-8 decoded key string when available and requested by <paramref name="Mode"/>.</param>
/// <param name="ValueUtf8">UTF-8 decoded value string when available and requested by <paramref name="Mode"/>.</param>
/// <param name="KeyRaw">Raw byte copy of the key payload when requested by <paramref name="Mode"/>.</param>
/// <param name="ValueRaw">Raw byte copy of the value payload when requested by <paramref name="Mode"/>.</param>
/// <param name="Preview">Human-readable best-effort preview of the cell payload.</param>
public readonly record struct CellResultDTO(
    bool HasCell,
    CodeGen.MetadataEntrySize EntrySize,
    int KeyCapacity,
    int ValueCapacity,
    bool IsKeyValue,
    bool HasKey,
    bool HasAnyValue,
    int KeyUtf8LengthHint,
    int ValueUtf8LengthHint,
    CellPayloadModeDTO Mode,
    string? KeyUtf8,
    string? ValueUtf8,
    byte[]? KeyRaw,
    byte[]? ValueRaw,
    string? Preview
);
