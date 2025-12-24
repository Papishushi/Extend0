namespace Extend0.Metadata.Indexing.Internal;

/// <summary>
/// Very small, fixed-size byte[] pool.
/// </summary>
/// <remarks>
/// <para>
/// This pool groups buffers by <see cref="Array.Length"/> and stores them in LIFO stacks.
/// It is designed to mitigate allocations when you need to materialize fixed-size UTF-8 keys
/// (e.g., column key sizes declared by schema) and keep them alive as dictionary keys.
/// </para>
/// <para>
/// Thread-safety: all operations are protected by a single gate lock.
/// This intentionally favors simplicity over throughput, since the pool is normally hit
/// during rebuild/clear phases rather than in the hottest lookup paths.
/// </para>
/// <para>
/// Ownership: callers that rent buffers are responsible for returning them. Returning a buffer
/// does not clear its contents; consumers must overwrite/clear as needed before reuse.
/// </para>
/// <para>
/// Note: this is not a general-purpose pool like <see cref="System.Buffers.ArrayPool{T}"/>.
/// It is deliberately minimal and keyed by exact length.
/// </para>
/// </remarks>
internal sealed class IndexFixedSizeByteArrayPool
{
    private readonly Dictionary<int, Stack<byte[]>> _bins = [];
    private readonly Lock _gate = new();

    /// <summary>
    /// Rents a buffer of exactly <paramref name="size"/> bytes.
    /// </summary>
    /// <param name="size">Exact buffer length requested.</param>
    /// <returns>
    /// A <see cref="byte"/>[] of length exactly <paramref name="size"/>. It may be newly allocated
    /// if the pool has no available buffers of that size.
    /// </returns>
    /// <remarks>
    /// Returned buffers are not cleared. Callers should treat the contents as undefined.
    /// </remarks>
    public byte[] RentExact(int size)
    {
        lock (_gate)
        {
            if (_bins.TryGetValue(size, out var stack) && stack.Count != 0)
                return stack.Pop();
        }

        return new byte[size];
    }

    /// <summary>
    /// Returns a previously rented buffer to the pool.
    /// </summary>
    /// <param name="buffer">Buffer to return.</param>
    /// <remarks>
    /// The buffer is stored in the bin matching its <see cref="Array.Length"/>. Contents are not cleared.
    /// </remarks>
    public void Return(byte[] buffer)
    {
        lock (_gate)
        {
            if (!_bins.TryGetValue(buffer.Length, out var stack))
                _bins[buffer.Length] = stack = new Stack<byte[]>();

            stack.Push(buffer);
        }
    }
}
