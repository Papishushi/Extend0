namespace Extend0.Metadata.CrossProcess
{
    /// <summary>
    /// Coarse-grained RPC error classification used to encode failures into a stable <c>HRESULT</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Values are intentionally small (1 byte) because they are packed into the low byte of the
    /// <c>HRESULT</c> code produced by <see cref="MakeRpcHResult"/>.
    /// </para>
    /// <para>
    /// Keep this list conservative and version-stable. New values can be added, but existing values
    /// should not be renumbered because callers may rely on them for telemetry and retry policy.
    /// </para>
    /// </remarks>
    public enum RpcErr : byte
    {
        /// <summary>
        /// The caller provided invalid input (null/empty/out-of-range/format).
        /// </summary>
        InvalidArg = 0x01,

        /// <summary>
        /// The requested resource was not found (e.g., table/index/key missing).
        /// </summary>
        NotFound = 0x02,

        /// <summary>
        /// The requested operation conflicts with existing state (e.g., name already registered).
        /// </summary>
        AlreadyExists = 0x03,

        /// <summary>
        /// The requested feature/operation is not supported by this backend/version.
        /// </summary>
        NotSupported = 0x04,

        /// <summary>
        /// The operation is forbidden due to protection rules (e.g., built-in/protected entity).
        /// </summary>
        Protected = 0x05,

        /// <summary>
        /// The target instance has been disposed and cannot be used.
        /// </summary>
        Disposed = 0x06,

        /// <summary>
        /// An unexpected error occurred (fallback bucket).
        /// </summary>
        Unexpected = 0xFF
    }

}