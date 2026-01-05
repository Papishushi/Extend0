namespace Extend0.Metadata.CrossProcess.HResult
{
    /// <summary>
    /// Utilities for decoding <c>HRESULT</c> values produced by <c>MetaDBManagerRPCCompatible</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// These helpers are intended specifically for parsing the stable RPC <c>HRESULT</c> shape emitted by
    /// the MetaDBManager cross-process boundary (<c>MetaDBManagerRPCCompatible</c>).
    /// </para>
    /// <para>
    /// The encoding uses a failure <c>HRESULT</c> (severity=1) with facility <c>ITF</c> (4), and packs a 16-bit code:
    /// <c>code16 = (opLow8 &lt;&lt; 8) | err</c>.
    /// </para>
    /// <para>
    /// <b>Important:</b> only the low 8 bits of the operation id are preserved by the current encoding.
    /// </para>
    /// </remarks>
    public static class MetaDBHResult
    {
        private const int FacilityItf = 4;

        /// <summary>
        /// Determines whether a given <c>HRESULT</c> matches the MetaDBManager RPC encoding header:
        /// failure (severity=1) with facility <c>ITF</c> (4).
        /// </summary>
        /// <param name="hresult">The <c>HRESULT</c> to test.</param>
        /// <returns>
        /// <see langword="true"/> if the value matches the RPC header shape; otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This check validates only the severity/facility header. It does not validate that the decoded operation and error
        /// are defined enum members.
        /// </remarks>
        public static bool IsMetaDBHResult(int hresult)
        {
            // Severity is bit 31 (1 = failure). Facility is bits 16..26 (11 bits).
            if (hresult >= 0) return false; // not a failing HRESULT
            int facility = hresult >> 16 & 0x7FF;
            return facility == FacilityItf;
        }

        /// <summary>
        /// Attempts to decode a MetaDBManager RPC <c>HRESULT</c> into the packed operation and coarse error classification.
        /// </summary>
        /// <param name="hresult">The <c>HRESULT</c> to decode.</param>
        /// <param name="op">
        /// When this method returns <see langword="true"/>, contains the decoded operation identifier.
        /// Only the low 8 bits are recoverable with the current encoding.
        /// </param>
        /// <param name="err">
        /// When this method returns <see langword="true"/>, contains the decoded coarse error classification.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if <paramref name="hresult"/> matches the MetaDBManager RPC encoding header and could be decoded;
        /// otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Encoding: <c>code16 = (opLow8 &lt;&lt; 8) | err</c>, stored in the low 16 bits of the <c>HRESULT</c>.
        /// Therefore, only <c>opLow8</c> can be recovered from the current format.
        /// </para>
        /// </remarks>
        public static bool TryDecode(int hresult, out RpcOp op, out RpcErr err)
        {
            op = default;
            err = default;

            if (!IsMetaDBHResult(hresult))
                return false;

            int code16 = hresult & 0xFFFF;
            int opLow8 = code16 >> 8 & 0xFF;
            int err8 = code16 & 0xFF;

            op = (RpcOp)opLow8;
            err = (RpcErr)err8;

            return true;
        }

        /// <summary>
        /// Decodes a MetaDBManager RPC <c>HRESULT</c> into the packed operation and error classification.
        /// </summary>
        /// <param name="hresult">The <c>HRESULT</c> to decode.</param>
        /// <returns>
        /// A tuple containing the decoded <see cref="RpcOp"/> and <see cref="RpcErr"/>.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="hresult"/> does not match the MetaDBManager RPC encoding header.
        /// </exception>
        public static (RpcOp Op, RpcErr Err) Decode(int hresult)
        {
            if (!TryDecode(hresult, out var op, out var err))
                throw new ArgumentException("The provided HRESULT is not a MetaDBManagerRPCCompatible RPC HRESULT.", nameof(hresult));

            return (op, err);
        }

        /// <summary>
        /// Attempts to decode a MetaDBManager RPC <c>HRESULT</c> and validates that the decoded values are defined enum members.
        /// </summary>
        /// <param name="hresult">The <c>HRESULT</c> to decode.</param>
        /// <param name="op">
        /// When this method returns <see langword="true"/>, contains the decoded operation identifier.
        /// </param>
        /// <param name="err">
        /// When this method returns <see langword="true"/>, contains the decoded coarse error classification.
        /// </param>
        /// <returns>
        /// <see langword="true"/> when the value matches the MetaDBManager RPC encoding header and both decoded values are defined;
        /// otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This is useful for telemetry: unknown newer operations/errors can be detected as "not defined" without throwing.
        /// </remarks>
        public static bool TryDecodeDefined(int hresult, out RpcOp op, out RpcErr err)
        {
            if (!TryDecode(hresult, out op, out err))
                return false;

            return Enum.IsDefined(op)
                && Enum.IsDefined(err);
        }
    }
}
