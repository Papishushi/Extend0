using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Extend0
{
    /// <summary>
    /// Provides a value-based equality comparer for <see cref="byte"/> sequences.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Equality is exact: when hash codes collide, the dictionary still confirms by calling
    /// the equality comparer again for sequence comparison.
    /// </para>
    /// <para>
    /// <b>Important:</b> Arrays used as keys must not be mutated after insertion into a hash-based collection.
    /// </para>
    /// <para>
    /// Hashing uses FNV-1a (64-bit) internally and folds to 32-bit as required by
    /// <see cref="IEqualityComparer{T}.GetHashCode(T)"/>. Collisions are possible and only affect performance.
    /// </para>
    /// </remarks>
    public sealed class ByteArrayComparer :
        IEqualityComparer<byte[]>,
        IEqualityComparer<Span<byte>>,
        IEqualityComparer<ReadOnlySpan<byte>>
    {
        // FNV-1a 64-bit
        private const ulong offset64 = 14695981039346656037ul;
        private const ulong prime64 = 1099511628211ul;

        /// <summary>
        /// Gets a shared instance of <see cref="ByteArrayComparer"/> that performs ordinal (byte-by-byte) comparison.
        /// </summary>
        public static readonly ByteArrayComparer Ordinal = new();

        /// <summary>
        /// Determines whether two byte arrays contain the same bytes in the same order.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(byte[]? x, byte[]? y)
            => ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        /// <summary>
        /// Determines whether two writable byte spans contain the same bytes in the same order.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Span<byte> x, Span<byte> y)
            => x.SequenceEqual(y);

        /// <summary>
        /// Determines whether two read-only byte spans contain the same bytes in the same order.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
            => x.SequenceEqual(y);

        /// <summary>
        /// Computes a stable 32-bit FNV-1a-derived hash code for a byte array.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode([DisallowNull] byte[] obj)
        {
            ArgumentNullException.ThrowIfNull(obj);
            return FoldToInt32(GetHashCode64(obj));
        }

        /// <summary>
        /// Computes a stable 32-bit FNV-1a-derived hash code for a writable byte span.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(Span<byte> obj)
            => FoldToInt32(GetHashCode64(obj));

        /// <summary>
        /// Computes a stable 32-bit FNV-1a-derived hash code for a read-only byte span.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(ReadOnlySpan<byte> obj)
            => FoldToInt32(GetHashCode64(obj));

        /// <summary>
        /// Computes a 64-bit FNV-1a hash for the provided bytes.
        /// Useful when you want a real 64-bit key in a custom hashtable or cache.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong GetHashCode64(ReadOnlySpan<byte> bytes)
        {
            unchecked
            {
                ulong hash = offset64;
                for (int i = 0; i < bytes.Length; i++)
                    hash = (hash ^ bytes[i]) * prime64;

                return hash;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FoldToInt32(ulong h)
            => (int)(h ^ (h >> 32));
    }
}
