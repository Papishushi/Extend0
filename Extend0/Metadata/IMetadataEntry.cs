namespace Extend0.Metadata
{
    /// <summary>
    /// Contract for generated metadata entry structs that expose a fixed key/value layout.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implementations of <see cref="IMetadataEntry"/> are blittable value types that
    /// represent a single key/value entry with a compile-time known layout and capacity.
    /// </para>
    /// <para>
    /// The interface is split into:
    /// </para>
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       <b>Static members</b> that operate on raw pointers (<c>void*</c>) and are used
    ///       by low-level infrastructure (e.g. <c>MetadataCell</c>, column blocks).
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <b>Instance members</b> for convenient, type-safe initialization from
    ///       <see cref="ReadOnlySpan{T}"/> and <see cref="string"/>.
    ///     </description>
    ///   </item>
    /// </list>
    /// <para>
    /// Generated types are expected to be <c>unsafe struct</c>s with inline buffers for the
    /// key and value and no managed references, so they can be used directly inside
    /// <see cref="MetadataTable"/> value cells without allocations.
    /// </para>
    /// </remarks>
    public unsafe interface IMetadataEntry
    {
        /// <summary>
        /// Returns a pointer to the beginning of the value region for the given entry.
        /// </summary>
        /// <param name="entry">Pointer to an instance of the implementing entry type.</param>
        /// <returns>
        /// A non-null pointer to the first byte of the value buffer.
        /// </returns>
        /// <remarks>
        /// The caller is responsible for not reading or writing beyond
        /// <see cref="ValueCapacity"/> bytes from the returned pointer.
        /// </remarks>
        static abstract unsafe byte* GetValuePointer(void* entry);

        /// <summary>
        /// Compares the entry's key with a UTF-8 byte span.
        /// </summary>
        /// <param name="entry">Pointer to an instance of the implementing entry type.</param>
        /// <param name="key">UTF-8 bytes to compare with the stored key.</param>
        /// <returns>
        /// <see langword="true"/> if the stored key is equal to <paramref name="key"/>;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This comparison is typically ordinal and length-sensitive.
        /// Implementations must not allocate.
        /// </remarks>
        static abstract unsafe bool KeyEquals(void* entry, ReadOnlySpan<byte> key);

        /// <summary>
        /// Compares the entry's key with a UTF-16 string.
        /// </summary>
        /// <param name="entry">Pointer to an instance of the implementing entry type.</param>
        /// <param name="key">String to compare with the stored key.</param>
        /// <returns>
        /// <see langword="true"/> if the stored key is equal to <paramref name="key"/>;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// Implementations are expected to encode <paramref name="key"/> to UTF-8 and
        /// compare it against the internal key buffer without allocations when possible.
        /// </remarks>
        static abstract unsafe bool KeyEquals(void* entry, string key);

        /// <summary>
        /// Gets the maximum number of bytes available for the key.
        /// </summary>
        /// <remarks>
        /// This is the fixed capacity of the key buffer and is typically used by callers
        /// to validate that a key fits before attempting to write it.
        /// </remarks>
        static abstract int KeyCapacity { get; }

        /// <summary>
        /// Gets the maximum number of bytes available for the value.
        /// </summary>
        /// <remarks>
        /// This is the fixed capacity of the value buffer and is typically used by callers
        /// to validate that a value fits before attempting to write it.
        /// </remarks>
        static abstract int ValueCapacity { get; }

        /// <summary>
        /// Attempts to set the key from a UTF-8 byte span.
        /// </summary>
        /// <param name="key">Key bytes in UTF-8 encoding.</param>
        /// <returns>
        /// <see langword="true"/> if the key was written successfully;
        /// <see langword="false"/> if it does not fit within <see cref="KeyCapacity"/>.
        /// </returns>
        bool TrySetKey(ReadOnlySpan<byte> key);

        /// <summary>
        /// Attempts to set the value from a byte span.
        /// </summary>
        /// <param name="str">Value bytes (encoding is defined by the entry type).</param>
        /// <returns>
        /// <see langword="true"/> if the value was written successfully;
        /// <see langword="false"/> if it does not fit within <see cref="ValueCapacity"/>.
        /// </returns>
        bool TrySetValue(ReadOnlySpan<byte> str);

        /// <summary>
        /// Attempts to set the key from a UTF-16 string.
        /// </summary>
        /// <param name="key">Key text to encode and store.</param>
        /// <returns>
        /// <see langword="true"/> if the key was written successfully;
        /// <see langword="false"/> if the encoded key would exceed
        /// <see cref="KeyCapacity"/>.
        /// </returns>
        bool TrySetKey(string key);

        /// <summary>
        /// Attempts to set the value from a UTF-16 string.
        /// </summary>
        /// <param name="str">Value text to encode and store.</param>
        /// <returns>
        /// <see langword="true"/> if the value was written successfully;
        /// <see langword="false"/> if the encoded value would exceed
        /// <see cref="ValueCapacity"/>.
        /// </returns>
        bool TrySetValue(string str);
    }
}
