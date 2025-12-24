using Extend0.Metadata.Indexing.Contract;

namespace Extend0.Metadata.Indexing.Definitions
{
    /// <summary>
    /// Base type for table index implementations.
    /// </summary>
    /// <typeparam name="TKey">Index key type.</typeparam>
    /// <typeparam name="TValue">Index value type.</typeparam>
    /// <remarks>
    /// <para>
    /// This class centralizes the common infrastructure shared by index implementations:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>A <see cref="ReaderWriterLockSlim"/> for thread-safety.</description></item>
    ///   <item><description>Basic index metadata (<see cref="Name"/>, <see cref="KeyType"/>, <see cref="ValueType"/>).</description></item>
    ///   <item><description>Dispose tracking and a helper guard (<see cref="ThrowIfDisposed"/>).</description></item>
    /// </list>
    /// <para>
    /// Derived types implement the actual storage and operations, and should call
    /// <see cref="ThrowIfDisposed"/> before accessing state.
    /// </para>
    /// </remarks>
    public abstract class IndexBase<TKey, TValue>(string name) : ITableIndex, IDisposable where TKey : notnull
    {
        /// <summary>
        /// Reader/writer lock used to guard index state.
        /// </summary>
        /// <remarks>
        /// Implementations typically use a write lock for mutating operations and a read lock for lookups.
        /// </remarks>
        protected readonly ReaderWriterLockSlim Rwls = new(LockRecursionPolicy.NoRecursion);

        private int _disposed; // 0 = alive, 1 = disposed

        /// <summary>
        /// Gets the logical name of this index.
        /// </summary>
        public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));

        /// <summary>
        /// Gets the runtime type of the keys stored in this index.
        /// </summary>
        public Type KeyType => typeof(TKey);

        /// <summary>
        /// Gets the runtime type of the values stored in this index.
        /// </summary>
        public Type ValueType => typeof(TValue);

        /// <summary>
        /// Removes all entries from the index.
        /// </summary>
        public abstract void Clear();

        /// <summary>
        /// Materializes a fixed-size lookup key into a per-thread scratch buffer, suitable for
        /// dictionary lookups against indexes that store keys as fixed-size <see cref="byte"/> arrays.
        /// </summary>
        /// <param name="key">
        /// The source key bytes to look up. If shorter than <paramref name="fixedSize"/>, the key is
        /// copied and the remaining trailing bytes are zero-filled so the padded representation matches
        /// the stored key shape.
        /// </param>
        /// <param name="fixedSize">
        /// The exact key size (in bytes) used by the index (typically dictated by schema <c>KeySize</c>).
        /// The returned array length is always exactly this value.
        /// </param>
        /// <returns>
        /// A thread-local scratch <see cref="byte"/> array of length <paramref name="fixedSize"/> containing
        /// <paramref name="key"/> followed by zero padding.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method exists to enable allocation-free lookups when the index uses <c>byte[]</c> keys but
        /// the query key is provided as a <see cref="ReadOnlySpan{T}"/>. Since fixed-size key indexes compare
        /// the whole array, any unused tail bytes must be zeroed to ensure equality matches the persisted
        /// representation (copy + zero-fill).
        /// </para>
        /// <para>
        /// <b>Scratch lifetime:</b> the returned array is <em>ephemeral</em> and reused on the calling thread.
        /// Do not store it, return it, or use it beyond the immediate lookup. A subsequent call on the same
        /// thread may overwrite its contents.
        /// </para>
        /// <para>
        /// <b>Preconditions:</b> <paramref name="key"/> length must be less than or equal to
        /// <paramref name="fixedSize"/>. If not, the underlying copy will throw.
        /// </para>
        /// <para>
        /// Thread safety: the buffer is thread-local by design; no cross-thread synchronization is performed.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="fixedSize"/> is not a valid positive size for a fixed key.
        /// </exception>
        protected static byte[] GetScratchLookupKey(ReadOnlySpan<byte> key, int fixedSize)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(fixedSize);
            var lookupKey = Internal.IndexKeyScratch.GetScratch(fixedSize);
            Internal.IndexKeyScratch.Fill(lookupKey, key);
            return lookupKey;
        }

        /// <summary>
        /// Releases resources used by the index.
        /// </summary>
        /// <remarks>
        /// This disposes the underlying <see cref="ReaderWriterLockSlim"/> and marks the index as disposed.
        /// Subsequent calls to operations guarded by <see cref="ThrowIfDisposed"/> will throw
        /// <see cref="ObjectDisposedException"/>.
        /// </remarks>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            Rwls.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Throws an <see cref="ObjectDisposedException"/> if the index has been disposed.
        /// </summary>
        /// <remarks>
        /// Call this at the beginning of public operations in derived types to enforce correct usage.
        /// </remarks>
        protected void ThrowIfDisposed()
            => ObjectDisposedException.ThrowIf(
                Volatile.Read(ref _disposed) != 0,
                $"{GetType().Name}('{Name}')");
    }
}
