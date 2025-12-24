namespace Extend0.Metadata.Indexing.Internal;

/// <summary>
/// Provides per-thread reusable scratch buffers for fixed-size key lookups in indexes,
/// allowing span-based queries to be performed without allocating.
/// </summary>
/// <remarks>
/// <para>
/// Many built-in indexes store keys as fixed-size <see cref="byte"/> arrays whose length is
/// dictated by schema (<c>KeySize</c>). When a lookup starts from a variable-length
/// <see cref="ReadOnlySpan{byte}"/>, the lookup key must be materialized into an array with the
/// <em>exact</em> same size and padding rules as the stored keys, otherwise the comparer would not
/// see the same bytes.
/// </para>
/// <para>
/// This type maintains thread-static buffers for common key sizes (1/2/4/8/16/32/64/128/256/512)
/// and a per-thread cache for uncommon sizes. Buffers are reused across calls on the same thread,
/// minimizing GC pressure in hot lookup paths.
/// </para>
/// <para>
/// <b>Important:</b> the arrays returned by <see cref="GetScratch"/> are scratch space. They are not safe
/// to keep, store in collections, or return to any pool. Use them only as ephemeral lookup keys
/// within the scope of a single operation.
/// </para>
/// <para>
/// Thread safety: buffers are thread-local by design; there is no cross-thread synchronization.
/// </para>
/// </remarks>
internal static class IndexKeyScratch
{
    [ThreadStatic] private static byte[]? _b1;
    [ThreadStatic] private static byte[]? _b2;
    [ThreadStatic] private static byte[]? _b4;
    [ThreadStatic] private static byte[]? _b8;
    [ThreadStatic] private static byte[]? _b16;
    [ThreadStatic] private static byte[]? _b32;
    [ThreadStatic] private static byte[]? _b64;
    [ThreadStatic] private static byte[]? _b128;
    [ThreadStatic] private static byte[]? _b256;
    [ThreadStatic] private static byte[]? _b512;
    [ThreadStatic] private static byte[]?[] _temp;

    static IndexKeyScratch()
    {
        var env = Environment.GetEnvironmentVariable(nameof(IndexKeyScratch));
        if (string.IsNullOrEmpty(env)) _temp = new byte[256][];
        else _temp = new byte[int.Parse(env)][];
    }

    /// <summary>
    /// Gets a thread-local scratch buffer of the requested exact size.
    /// </summary>
    /// <param name="size">Exact key size to obtain.</param>
    /// <returns>
    /// A thread-local <see cref="byte"/> array of length <paramref name="size"/> for common sizes,
    /// or a cached per-thread array for uncommon sizes.
    /// </returns>
    /// <remarks>
    /// <para>
    /// The returned buffer is reused on the calling thread. Its contents are undefined until you fill it.
    /// </para>
    /// <para>
    /// Do not store the returned instance or use it beyond the immediate lookup operation. A subsequent call
    /// on the same thread may overwrite its contents.
    /// </para>
    /// <para>
    /// For uncommon sizes, this method caches the allocated array in a thread-local slot keyed by
    /// <paramref name="size"/>. Callers must ensure <paramref name="size"/> is non-negative and within the
    /// bounds of the configured cache; otherwise an <see cref="IndexOutOfRangeException"/> may be thrown.
    /// </para>
    /// </remarks>
    public static byte[] GetScratch(int size) => size switch
    {
        1 => _b1 ??= new byte[1],
        2 => _b2 ??= new byte[2],
        4 => _b4 ??= new byte[4],
        8 => _b8 ??= new byte[8],
        16 => _b16 ??= new byte[16],
        32 => _b32 ??= new byte[32],
        64 => _b64 ??= new byte[64],
        128 => _b128 ??= new byte[128],
        256 => _b256 ??= new byte[256],
        512 => _b512 ??= new byte[512],
        _ => _temp[size] ??= new byte[size], // uncommon size: allocate and cache
    };

    /// <summary>
    /// Copies <paramref name="key"/> into <paramref name="scratch"/> and clears any trailing bytes.
    /// </summary>
    /// <param name="scratch">Destination fixed-size buffer used as a lookup key.</param>
    /// <param name="key">Source key bytes.</param>
    /// <returns>The same <paramref name="scratch"/> instance for convenience.</returns>
    /// <remarks>
    /// <para>
    /// Fixed-size key dictionaries typically compare the entire array. If <paramref name="key"/> is shorter
    /// than <paramref name="scratch"/>, the remainder must be set to zero so the padded representation matches
    /// keys that were stored using the same rule (copy + zero-fill).
    /// </para>
    /// <para>
    /// Precondition: <paramref name="key"/> length must be less than or equal to <paramref name="scratch"/> length.
    /// If not, <see cref="ReadOnlySpan{byte}.CopyTo(Span{byte})"/> throws.
    /// </para>
    /// <para>
    /// This method always overwrites the entire buffer (by copy then clear), so it is safe to call even when the
    /// scratch buffer previously contained unrelated bytes.
    /// </para>
    /// </remarks>
    public static byte[] Fill(byte[] scratch, ReadOnlySpan<byte> key)
    {
        key.CopyTo(scratch);
        scratch.AsSpan(key.Length).Clear();
        return scratch;
    }
}
