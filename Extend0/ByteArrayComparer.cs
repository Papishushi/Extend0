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
    /// </remarks>
    public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        /// <summary>
        /// Gets a shared instance of <see cref="ByteArrayComparer"/> that performs
        /// ordinal (byte-by-byte) comparison.
        /// </summary>
        public static readonly ByteArrayComparer Ordinal = new();

        /// <summary>
        /// Determines whether two <see cref="byte"/> arrays are equal.
        /// </summary>
        /// <param name="x">The first byte array to compare.</param>
        /// <param name="y">The second byte array to compare.</param>
        /// <returns>
        /// <see langword="true"/> if both arrays reference the same instance, or if both are
        /// non-<see langword="null"/> and contain the same bytes in the same order;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        public bool Equals(byte[]? x, byte[]? y)
            => ReferenceEquals(x, y) || x is not null && y is not null && x.AsSpan().SequenceEqual(y);

        /// <summary>
        /// Returns a hash code for the specified <see cref="byte"/> array.
        /// </summary>
        /// <param name="obj">The byte array for which to compute a hash code.</param>
        /// <returns>
        /// A hash code that combines all bytes of <paramref name="obj"/> using a simple
        /// multiplicative hash (starting from 17 and multiplying by 31 for each byte).
        /// </returns>
        /// <remarks>
        /// This implementation assumes that the contents of <paramref name="obj"/> do not
        /// change while it is used as a key in a hash-based collection. Mutating the array
        /// after insertion may lead to undefined behavior.
        /// </remarks>
        /// <exception cref="ArgumentNullException">
        /// Thrown if <paramref name="obj"/> is <see langword="null"/>.
        /// </exception>
        public int GetHashCode(byte[] obj)
        {
            ArgumentNullException.ThrowIfNull(obj);

            unchecked
            {
                int h = 17;
                foreach (byte b in obj) h = h * 31 + b;
                return h;
            }
        }
    }
}
