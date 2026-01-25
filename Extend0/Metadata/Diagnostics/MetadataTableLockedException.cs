namespace Extend0.Metadata.Diagnostics
{
    /// <summary>
    /// Exception thrown when a metadata table file cannot be opened or resized because it is locked,
    /// in use by another process, or otherwise unavailable due to an OS-level sharing/locking/access error.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is typically raised by parsing an underlying <see cref="IOException"/> (or a platform-specific I/O exception)
    /// and rethrowing a domain-specific error that callers can catch to implement retry, backoff, or user feedback.
    /// </para>
    /// <para>
    /// The <see cref="Exception.HResult"/> of this exception is copied from <paramref name="innerException"/> when available.
    /// If no inner exception is provided, it defaults to <c>0x80070021</c> (LOCK_VIOLATION).
    /// </para>
    /// </remarks>
    public class MetadataTableLockedException : InvalidOperationException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataTableLockedException"/> class.
        /// </summary>
        /// <param name="message">A human-readable error message.</param>
        /// <param name="innerException">The underlying exception that caused the lock failure.</param>
        public MetadataTableLockedException(string? message, Exception? innerException)
            : base(message, innerException)
        {
            HResult = innerException?.HResult ?? unchecked((int)0x80070021);
        }
    }

}
