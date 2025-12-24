using System.Runtime.CompilerServices;

namespace Extend0
{
    /// <summary>
    /// Provides a value-based equality comparer for <see cref="byte"/> arrays.
    /// </summary>
    /// <remarks>
    /// This comparer treats two byte arrays as equal when:
    /// <list type="bullet">
    ///   <item><description>They are the same reference instance, or</description></item>
    ///   <item><description>Their lengths are equal and all bytes match in order (ordinal comparison).</description></item>
    /// </list>
    /// It is intended for use as a key comparer in dictionaries and hash-based collections
    /// where <c>byte[]</c> instances represent immutable keys (for example, UTF-8 keys or
    /// binary identifiers).
    /// <b>Important:</b> Arrays used as keys must not be mutated after insertion into a hash-based collection.
    /// </para>
    /// </remarks>
    public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        /// <summary>
        /// Gets a shared instance of <see cref="ByteArrayComparer"/> that performs
        /// ordinal (byte-by-byte) comparison.
        /// </summary>
        public static readonly ByteArrayComparer Ordinal = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(byte[]? x, byte[]? y)
            => ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        /// <inheritdoc/>
        /// <remarks>
        /// Uses FNV-1a (32-bit) over the full array.
        /// This implementation assumes that the contents of <paramref name="obj"/> do not
        /// change while it is used as a key in a hash-based collection. Mutating the array
        /// after insertion may lead to undefined behavior.
        /// </remarks>
        /// <exception cref="ArgumentNullException">
        /// Thrown if <paramref name="obj"/> is <see langword="null"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(byte[] obj)
        {
            ArgumentNullException.ThrowIfNull(obj);

            unchecked
            {
                // FNV-1a 32-bit
                const uint offset = 2166136261u;
                const uint prime = 16777619u;

                uint hash = offset;
                for (int i = 0; i < obj.Length; i++)
                    hash = (hash ^ obj[i]) * prime;

                return (int)hash;
            }
        }
    }
}
