namespace Extend0.Metadata.CrossProcess.DTO
{
    /// <summary>
    /// Specifies which representations of a MetaDB cell payload are included in a returned snapshot.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is primarily used by cross-process / RCP-safe APIs that must return serializable data
    /// instead of exposing pointer-backed structures (e.g., <c>MetadataCell</c> or memory-mapped views).
    /// </para>
    ///
    /// <para>
    /// <b>Trade-off</b>:
    /// <list type="bullet">
    ///   <item><description><see cref="Utf8Only"/> minimizes payload size but may lose fidelity for binary or non-printable data.</description></item>
    ///   <item><description><see cref="RawOnly"/> preserves exact bytes but is larger and requires caller-side decoding.</description></item>
    ///   <item><description><see cref="Both"/> returns both forms for convenience at the cost of extra allocations/serialization.</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public enum CellPayloadModeDTO : byte
    {
        /// <summary>
        /// Returns only UTF-8 decoded strings (best-effort), omitting raw byte copies.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Implementations may return <see langword="null"/> for the UTF-8 fields when the underlying bytes are empty,
        /// not terminated, or not decodable/printable as UTF-8. This mode is ideal for diagnostics and UI inspection.
        /// </para>
        /// </remarks>
        Utf8Only,

        /// <summary>
        /// Returns only raw byte copies, omitting UTF-8 decoded strings.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This mode is binary-safe and preserves exact payload content, but increases payload size and pushes decoding
        /// responsibility to the caller. Prefer this for stable round-tripping and non-textual data.
        /// </para>
        /// </remarks>
        RawOnly,

        /// <summary>
        /// Returns both UTF-8 decoded strings (best-effort) and raw byte copies.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Use this when you want convenient text plus binary fidelity in a single response and you can afford the extra
        /// allocations and serialization overhead.
        /// </para>
        /// </remarks>
        Both
    }

}
