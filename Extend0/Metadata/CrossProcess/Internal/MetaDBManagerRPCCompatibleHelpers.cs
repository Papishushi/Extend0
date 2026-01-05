using Extend0.Metadata.CodeGen;
using Extend0.Metadata.Contract;
using Extend0.Metadata.CrossProcess.DTO;
using Extend0.Metadata.CrossProcess.HResult;
using Extend0.Metadata.Indexing.Contract;
using Extend0.Metadata.Storage;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace Extend0.Metadata.CrossProcess.Internal
{
    internal static partial class MetaDBManagerRPCCompatibleHelpers
    {
        /// <summary>
        /// Encodes a managed string to an exact-length UTF-8 byte array and invokes a callback with the result.
        /// </summary>
        /// <param name="s">Input string to encode.</param>
        /// <param name="fn">Callback that receives an exact-length UTF-8 byte array.</param>
        /// <returns>The callback result.</returns>
        /// <remarks>
        /// <para>
        /// Uses a <c>stackalloc</c> fast-path for small payloads and <see cref="ArrayPool{T}"/> for larger ones.
        /// The callback always receives a new exact-length array (never a pooled buffer) to avoid leaking pooled memory.
        /// </para>
        /// <para>
        /// This helper intentionally performs a single allocation for the exact-length array to keep the public API
        /// (<c>Func&lt;byte[], ...&gt;</c>) safe and simple.
        /// </para>
        /// </remarks>
        internal static IndexLookupResultDTO WithUtf8(string s, Func<byte[], IndexLookupResultDTO> fn)
        {
            const int STACK_LIMIT = 512;
            int byteCount = Encoding.UTF8.GetByteCount(s);

            if (byteCount <= STACK_LIMIT)
            {
                Span<byte> tmp = stackalloc byte[byteCount];
                Encoding.UTF8.GetBytes(s.AsSpan(), tmp);

                // 1 alloc exacto
                var arr = new byte[byteCount];
                tmp.CopyTo(arr);
                return fn(arr);
            }

            byte[] rented = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                int written = Encoding.UTF8.GetBytes(s.AsSpan(), rented);
                var arr = new byte[written];
                Buffer.BlockCopy(rented, 0, arr, 0, written);
                return fn(arr);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented, clearArray: false);
            }
        }

        /// <summary>
        /// Returns whether the provided byte span contains any non-zero byte.
        /// </summary>
        private static bool AnyNonZero(ReadOnlySpan<byte> data)
        {
            foreach (var b in data) if (b != 0) return true;
            return false;
        }

        // -----------------------------
        // DTO builder + helpers
        // -----------------------------

        /// <summary>
        /// Builds a cross-process safe snapshot (<see cref="CellResultDTO"/>) of a single cell.
        /// </summary>
        /// <param name="t">The resolved table instance used to read the cell.</param>
        /// <param name="column">Zero-based column index.</param>
        /// <param name="row">Zero-based row index.</param>
        /// <param name="mode">
        /// Controls which payload representations are materialized:
        /// raw bytes (<see cref="CellPayloadModeDTO.RawOnly"/>), UTF-8 strings (<see cref="CellPayloadModeDTO.Utf8Only"/>),
        /// or both (<see cref="CellPayloadModeDTO.Both"/>).
        /// </param>
        /// <returns>
        /// A populated DTO snapshot. When the cell cannot be resolved (missing/unreadable),
        /// returns a DTO with <see cref="CellResultDTO.HasCell"/> set to <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This helper is intended for IPC boundaries: it converts the underlying mapped storage representation
        /// into a stable DTO that can be serialized without exposing internal types.
        /// </para>
        /// <para>
        /// <b>Value-only columns:</b> VALUE bytes are read directly from the fixed VALUE segment and
        /// <see cref="CellResultDTO.HasAnyValue"/> is computed by scanning for any non-zero byte.
        /// </para>
        /// <para>
        /// <b>Key/value columns:</b> KEY is read via <see cref="MetadataCell.TryGetKey(out ReadOnlySpan{byte})"/>; when present,
        /// VALUE is read via <see cref="MetadataCell.TryGetValue(ReadOnlySpan{byte}, out ReadOnlySpan{byte})"/> using that KEY.
        /// </para>
        /// <para>
        /// Copies (raw arrays and/or UTF-8 strings) are created only according to <paramref name="mode"/> to avoid
        /// unnecessary allocations during bulk reads.
        /// </para>
        /// <para>
        /// The <see cref="CellResultDTO.Preview"/> field is a best-effort, human-readable snippet:
        /// it prefers VALUE, falls back to KEY, and is <see langword="null"/> when neither is present.
        /// </para>
        /// </remarks>
        internal static CellResultDTO? BuildCellDto(IMetadataTable t, uint column, uint row, CellPayloadModeDTO mode)
        {
            var meta = t.Spec.Columns[(int)column];
            int keyCap = meta.Size.GetKeySize();
            int valCap = meta.Size.GetValueSize();
            bool isKeyValue = keyCap > 0;

            if (!t.TryGetCell(column, row, out var cell))
                return new CellResultDTO(
                    HasCell: false,
                    EntrySize: meta.Size,
                    KeyCapacity: keyCap,
                    ValueCapacity: valCap,
                    IsKeyValue: isKeyValue,
                    HasKey: false,
                    HasAnyValue: false,
                    KeyUtf8LengthHint: 0,
                    ValueUtf8LengthHint: 0,
                    Mode: mode,
                    KeyUtf8: null,
                    ValueUtf8: null,
                    KeyRaw: null,
                    ValueRaw: null,
                    Preview: null
                );

            var result = ReadCellUnsafe(valCap, isKeyValue, cell);

            var (keyLenHint, valLenHint) = GetLenghtHints(keyCap, valCap, result);

            var (keyRaw, valRaw, keyUtf8, valUtf8) = NormalizeMode(mode, result, keyLenHint, valLenHint);

            // preview prefers VALUE, fallback to KEY, else null
            string? preview = null;
            var prevSource = !result.Value.IsEmpty ? result.Value : result.HasKey ? result.Key : default;
            if (!prevSource.IsEmpty)
                preview = MakePreview(prevSource, 48);

            return new CellResultDTO(
                HasCell: true,
                EntrySize: meta.Size,
                KeyCapacity: keyCap,
                ValueCapacity: valCap,
                IsKeyValue: isKeyValue,
                HasKey: result.HasKey,
                HasAnyValue: result.HasAnyValue,
                KeyUtf8LengthHint: keyLenHint,
                ValueUtf8LengthHint: valLenHint,
                Mode: mode,
                KeyUtf8: keyUtf8,
                ValueUtf8: valUtf8,
                KeyRaw: keyRaw,
                ValueRaw: valRaw,
                Preview: preview
            );
        }

        /// <summary>
        /// Computes best-effort UTF-8 length hints for KEY and VALUE payloads.
        /// </summary>
        /// <param name="keyCap">Maximum KEY capacity in bytes for the column (0 for value-only columns).</param>
        /// <param name="valCap">Maximum VALUE capacity in bytes for the column.</param>
        /// <param name="result">The low-level read result returned by <see cref="ReadCellUnsafe(int, bool, MetadataCell)"/>.</param>
        /// <returns>
        /// A pair of length hints (<c>keyLenHint</c>, <c>valLenHint</c>) suitable for faster UTF-8 decoding attempts.
        /// Values are 0 when the corresponding payload is not present.
        /// </returns>
        /// <remarks>
        /// Hints are computed using <c>CStrLenHint</c> and are bounded by the segment capacities.
        /// They are only heuristics intended to reduce work when generating preview strings.
        /// </remarks>
        private static (int keyLenHint, int valLenHint) GetLenghtHints(int keyCap, int valCap, ReadCellUnsafeResult result)
        {
            int keyLenHint = result.HasKey && !result.Key.IsEmpty ? CStrLenHint(result.Key, keyCap) : 0;
            int valLenHint = result.HasAnyValue && !result.Value.IsEmpty ? CStrLenHint(result.Value, valCap) : 0;
            return (keyLenHint, valLenHint);
        }

        /// <summary>
        /// Materializes raw and/or UTF-8 payload copies from a low-level cell read according to a requested mode.
        /// </summary>
        /// <param name="mode">The payload selection mode.</param>
        /// <param name="result">The low-level read result containing span views of KEY/VALUE.</param>
        /// <param name="keyLenHint">Optional UTF-8 length hint for KEY (0 means unknown).</param>
        /// <param name="valLenHint">Optional UTF-8 length hint for VALUE (0 means unknown).</param>
        /// <returns>
        /// A tuple containing optional materialized payloads:
        /// <c>(keyRaw, valRaw, keyUtf8, valUtf8)</c>. Each element may be <see langword="null"/> depending on <paramref name="mode"/>
        /// and payload presence.
        /// </returns>
        /// <remarks>
        /// <para>
        /// When <paramref name="mode"/> includes raw output, spans are copied using <see cref="ReadOnlySpan{T}.ToArray"/>.
        /// </para>
        /// <para>
        /// When <paramref name="mode"/> includes UTF-8 output, decoding is attempted via <c>TryDecodePrintableUtf8</c>
        /// using the provided length hints.
        /// </para>
        /// </remarks>
        private static (byte[]? keyRaw, byte[]? valRaw, string? keyUtf8, string? valUtf8) NormalizeMode(
            CellPayloadModeDTO mode,
            ReadCellUnsafeResult result,
            int keyLenHint,
            int valLenHint)
        {
            byte[]? keyRaw = null;
            byte[]? valRaw = null;
            string? keyUtf8 = null;
            string? valUtf8 = null;

            if (mode is CellPayloadModeDTO.Both or CellPayloadModeDTO.RawOnly)
            {
                if (result.HasKey && !result.Key.IsEmpty) keyRaw = result.Key.ToArray();
                if (result.HasAnyValue && !result.Value.IsEmpty) valRaw = result.Value.ToArray();
            }

            if (mode is CellPayloadModeDTO.Both or CellPayloadModeDTO.Utf8Only)
            {
                if (result.HasKey && !result.Key.IsEmpty) keyUtf8 = TryDecodePrintableUtf8(result.Key, keyLenHint);
                if (result.HasAnyValue && !result.Value.IsEmpty) valUtf8 = TryDecodePrintableUtf8(result.Value, valLenHint);
            }

            return (keyRaw, valRaw, keyUtf8, valUtf8);
        }

        /// <summary>
        /// Reads KEY/VALUE data from a <see cref="MetadataCell"/> and returns span views without allocations.
        /// </summary>
        /// <param name="valCap">
        /// The fixed VALUE segment capacity in bytes for the target column (used for value-only columns).
        /// </param>
        /// <param name="isKeyValue">
        /// <see langword="true"/> for key/value columns; <see langword="false"/> for value-only columns.
        /// </param>
        /// <param name="cell">The cell to read.</param>
        /// <returns>
        /// A <see cref="ReadCellUnsafeResult"/> containing span views of KEY and VALUE (when present) and presence flags.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This is a hot-path helper that returns spans aliasing the underlying mapped storage. Do not store the returned spans
        /// beyond the immediate call site, and do not use them after the table/cell has been disposed or remapped.
        /// </para>
        /// <para>
        /// For value-only columns, VALUE is treated as a fixed blob starting at <see cref="MetadataCell.GetValuePointer"/>
        /// with length <paramref name="valCap"/>; presence is detected via <c>AnyNonZero</c>.
        /// </para>
        /// <para>
        /// For key/value columns, KEY is obtained via <see cref="MetadataCell.TryGetKey(out ReadOnlySpan{byte})"/> and, when present,
        /// VALUE is obtained via <see cref="MetadataCell.TryGetValue(ReadOnlySpan{byte}, out ReadOnlySpan{byte})"/>.
        /// </para>
        /// </remarks>
        private static unsafe ReadCellUnsafeResult ReadCellUnsafe(int valCap, bool isKeyValue, MetadataCell cell)
        {
            ReadOnlySpan<byte> key = default;
            ReadOnlySpan<byte> value = default;
            bool hasKey = false;
            bool hasAnyValue = false;

            if (!isKeyValue)
            {
                // value-only: VALUE is direct segment
                var raw = new ReadOnlySpan<byte>(cell.GetValuePointer(), valCap);
                hasAnyValue = AnyNonZero(raw);
                if (hasAnyValue) value = raw;
            }
            else
            {
                if (cell.TryGetKey(out ReadOnlySpan<byte> k) && k.Length != 0)
                {
                    hasKey = true;
                    key = k;

                    // classic: value obtained using key
                    if (cell.TryGetValue(k, out var v) && v.Length != 0)
                    {
                        value = v;
                        hasAnyValue = AnyNonZero(v); // optional signal
                    }
                }
            }

            return new ReadCellUnsafeResult(key, value, hasKey, hasAnyValue);
        }

        /// <summary>
        /// Computes a best-effort length hint up to the first <c>0</c> terminator, capped to <paramref name="cap"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CStrLenHint(ReadOnlySpan<byte> data, int cap)
        {
            int max = Math.Min(cap, data.Length);
            for (int i = 0; i < max; i++)
                if (data[i] == 0) return i;
            return max;
        }

        /// <summary>
        /// Truncates <paramref name="s"/> to <paramref name="maxChars"/> and appends an ellipsis when needed,
        /// preserving surrogate pairs at the cut boundary.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string EllipsisSafe(string s, int maxChars)
        {
            if (maxChars <= 0) return string.Empty;
            if (s.Length <= maxChars) return s;
            int cut = Math.Max(0, maxChars - 1);
            if (cut > 0 && char.IsHighSurrogate(s[cut - 1])) cut--;
            return s.AsSpan(0, cut).ToString() + "…";
        }

        /// <summary>
        /// Ensures the table can address <paramref name="row"/> in <paramref name="column"/> using best-effort growth.
        /// </summary>
        /// <param name="t">Resolved table.</param>
        /// <param name="column">Column index.</param>
        /// <param name="row">Target row index that must be addressable.</param>
        /// <param name="policy">
        /// Capacity behavior:
        /// <list type="bullet">
        ///   <item><description><see cref="CapacityPolicy.None"/>: no growth attempt; returns false on failure.</description></item>
        ///   <item><description><see cref="CapacityPolicy.TryGrow"/>: attempts growth; returns false if still failing.</description></item>
        ///   <item><description><see cref="CapacityPolicy.Throw"/>: throws when capacity cannot be ensured.</description></item>
        /// </list>
        /// </param>
        /// <returns><see langword="true"/> if capacity is ensured; otherwise <see langword="false"/>.</returns>
        internal static bool EnsureCapacityBestEffort(IMetadataTable t, uint column, uint row, CapacityPolicy policy)
        {
            // Needs capacity >= row+1
            try
            {
                // If store auto-grows, this is enough.
                _ = t.GetOrCreateCell(column, row);
                return true;
            }
            catch
            {
                if (policy == CapacityPolicy.None) return false;

                // best-effort try grow + retry once
                if (t.TryGrowColumnTo(column, row + 1, zeroInit: true))
                {
                    try { _ = t.GetOrCreateCell(column, row); return true; }
                    catch { /* fallthrough */ }
                }

                if (policy == CapacityPolicy.Throw)
                    throw;

                return false;
            }
        }

        /// <summary>
        /// Returns whether the specified index instance is a protected built-in index.
        /// </summary>
        /// <param name="idx">Index instance.</param>
        /// <returns><see langword="true"/> if the index is built-in; otherwise <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsBuiltIn(ITableIndex idx)
            => idx is Indexing.Internal.BuiltIn.ColumnKeyIndex
            || idx is Indexing.Internal.BuiltIn.GlobalKeyIndex;

        /// <summary>
        /// Produces a compact preview for diagnostics: printable UTF-8 when possible, otherwise hex, truncated to <paramref name="maxChars"/>.
        /// </summary>
        private static string MakePreview(ReadOnlySpan<byte> data, int maxChars)
        {
            var s = TryDecodePrintableUtf8(data, data.Length);
            if (s is not null) return EllipsisSafe(s, maxChars);

            // hex preview
            if (maxChars <= 0) return string.Empty;
            int maxBytes = Math.Max(0, (maxChars - 1) / 2);
            int bytes = Math.Min(maxBytes, data.Length);

            Span<char> chars = stackalloc char[bytes * 2 + (bytes < data.Length ? 1 : 0)];
            int ci = 0;

            static char Hex(byte x) => (char)(x < 10 ? '0' + x : 'A' + (x - 10));

            for (int i = 0; i < bytes; i++)
            {
                byte b = data[i];
                chars[ci++] = Hex((byte)(b >> 4));
                chars[ci++] = Hex((byte)(b & 0xF));
            }

            if (bytes < data.Length)
                chars[ci++] = '…';

            return new string(chars[..ci]);
        }

        /// <summary>
        /// Converts a runtime index instance into an <see cref="IndexInfoDTO"/> snapshot.
        /// </summary>
        /// <param name="idx">Index instance.</param>
        /// <returns>DTO describing the index.</returns>

        internal static IndexInfoDTO ToIndexInfoDTO(ITableIndex idx) => new(
            Name: idx.Name,
            Kind: idx switch
            {
                Indexing.Internal.BuiltIn.ColumnKeyIndex => IndexKindDTO.BuiltIn_ColumnKey,
                Indexing.Internal.BuiltIn.GlobalKeyIndex => IndexKindDTO.BuiltIn_GlobalKey,
                Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex => IndexKindDTO.BuiltIn_GlobalMultiTableKey,
                _ => IndexKindDTO.Unknown
            },
            IsRebuildable: idx is IRebuildableIndex,
            IsBuiltIn: IsBuiltIn(idx),
            Notes: null
        );

        /// <summary>
        /// Backend-specific factory that attempts to create a custom index instance from an <see cref="AddIndexRequestDTO"/>.
        /// </summary>
        /// <typeparam name="TIndex">
        /// The expected index interface/type to produce. This must match the caller’s target registry
        /// (e.g., an in-table index implementing <see cref="ITableIndex"/> or a cross-table index implementing
        /// the appropriate interface in addition to <see cref="ITableIndex"/>).
        /// </typeparam>
        /// <param name="request">
        /// Index creation request containing the kind discriminator, a JSON configuration payload
        /// (<see cref="AddIndexRequestDTO.IndexInputPayload"/>), and optionally a binary program payload
        /// (<see cref="AddIndexRequestDTO.ProgramBytes"/>) with an integrity hint (<see cref="AddIndexRequestDTO.ProgramHashSha256"/>).
        /// </param>
        /// <param name="created">
        /// When the method returns <see cref="IndexMutationStatusDTO.Ok"/>, receives the created index instance.
        /// Otherwise receives <see langword="default"/> (typically <see langword="null"/>).
        /// </param>
        /// <param name="notes">
        /// Receives diagnostic notes intended for callers (human-readable) describing why creation failed
        /// (unsupported kind, invalid payload schema, program verification failure, etc.).
        /// </param>
        /// <returns>
        /// A status describing the outcome:
        /// <list type="bullet">
        ///   <item><description><see cref="IndexMutationStatusDTO.Ok"/> when an index was created successfully.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.NotSupported"/> when the backend does not support the requested custom kind or payload.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.InvalidKind"/> when <paramref name="request"/> contains an unknown/invalid <see cref="IndexKindDTO"/>.</description></item>
        ///   <item><description><see cref="IndexMutationStatusDTO.Error"/> when creation fails unexpectedly.</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// <para>
        /// This factory is intentionally conservative. Custom index creation is considered an extension surface and should
        /// be gated behind a stable payload schema and a safe execution model (e.g., interpreted DSL/bytecode) to avoid
        /// loading or executing arbitrary .NET code across process boundaries.
        /// </para>
        /// <para>
        /// Typical implementations validate:
        /// </para>
        /// <list type="bullet">
        ///   <item><description><b>Kind</b>: only supported custom kinds are accepted.</description></item>
        ///   <item><description><b>Payload schema</b>: required fields exist and have correct types.</description></item>
        ///   <item><description><b>Program integrity</b>: if <see cref="AddIndexRequestDTO.ProgramBytes"/> is provided, optionally verify <see cref="AddIndexRequestDTO.ProgramHashSha256"/>.</description></item>
        ///   <item><description><b>Type compatibility</b>: the created instance must be assignable to <typeparamref name="TIndex"/>.</description></item>
        /// </list>
        /// </remarks>
        internal static IndexMutationStatusDTO TryCreateCustomIndex<TIndex>(
            AddIndexRequestDTO request,
            out TIndex? created,
            out string? notes)
            where TIndex : ITableIndex
        {
            created = default;

            if (request.Kind is not (IndexKindDTO.Custom_InTable or IndexKindDTO.Custom_CrossTable))
            {
                notes = $"Index kind '{request.Kind}' is not supported by this backend.";
                return IndexMutationStatusDTO.NotSupported;
            }

            notes = "Custom index creation is not supported in this backend version.";
            return IndexMutationStatusDTO.NotSupported;
        }

        /// <summary>
        /// Attempts to decode printable UTF-8 from the payload.
        /// </summary>
        /// <param name="data">Raw bytes to decode.</param>
        /// <param name="lenHint">Length hint to slice before decoding (commonly derived from a terminator scan).</param>
        /// <returns>
        /// The decoded string when valid printable UTF-8; otherwise <see langword="null"/> (binary/non-printable data).
        /// </returns>

        private static string? TryDecodePrintableUtf8(ReadOnlySpan<byte> data, int lenHint)
        {
            var slice = data[..Math.Clamp(lenHint, 0, data.Length)];
            var s = Encoding.UTF8.GetString(slice);
            if (s.Contains('\uFFFD')) return null;
            foreach (var ch in s)
                if (char.IsControl(ch) && ch != '\t' && ch != '\r' && ch != '\n')
                    return null;
            return s;
        }

        /// <summary>
        /// Writes <paramref name="src"/> into a fixed-size segment and zero-fills the remainder.
        /// </summary>
        /// <param name="dst">Destination pointer.</param>
        /// <param name="cap">Segment capacity in bytes.</param>
        /// <param name="src">Source bytes.</param>

        internal static unsafe void WriteFixed(byte* dst, int cap, byte[] src)
        {
            var span = new Span<byte>(dst, cap);
            span.Clear();
            int n = Math.Min(cap, src.Length);
            src.AsSpan(0, n).CopyTo(span);
        }

        /// <summary>
        /// Writes the KEY segment for key/value columns.
        /// </summary>
        /// <remarks>
        /// Raw bytes are preferred when <paramref name="mode"/> allows it; otherwise UTF-8 encoding is used.
        /// When room exists, a trailing <c>0</c> is written to preserve "C-string-like" semantics for textual keys.
        /// </remarks>

        internal static unsafe void WriteKeySegment(byte* keyPtr, int keyCap, byte[]? keyRaw, string? keyUtf8, CellPayloadModeDTO mode)
        {
            var seg = new Span<byte>(keyPtr, keyCap);
            seg.Clear();

            ReadOnlySpan<byte> payload = default;

            if ((mode == CellPayloadModeDTO.RawOnly || mode == CellPayloadModeDTO.Both) && keyRaw is { Length: > 0 })
                payload = keyRaw;
            else if ((mode == CellPayloadModeDTO.Utf8Only || mode == CellPayloadModeDTO.Both) && !string.IsNullOrEmpty(keyUtf8))
                payload = Encoding.UTF8.GetBytes(keyUtf8);

            if (payload.IsEmpty) return;

            int n = Math.Min(keyCap, payload.Length);
            payload[..n].CopyTo(seg);

            // C-string-like: ensure terminator when room exists
            if (n < keyCap) seg[n] = 0;
        }

        /// <summary>
        /// Writes the VALUE segment.
        /// </summary>
        /// <remarks>
        /// Raw bytes are preferred when <paramref name="mode"/> allows it; otherwise UTF-8 encoding is used.
        /// When room exists, a trailing <c>0</c> is written to preserve "C-string-like" semantics for textual values.
        /// </remarks>

        internal static unsafe void WriteValueSegment(byte* valuePtr, int valCap, byte[]? valueRaw, string? valueUtf8, CellPayloadModeDTO mode)
        {
            var seg = new Span<byte>(valuePtr, valCap);
            seg.Clear();

            ReadOnlySpan<byte> payload = default;

            if ((mode == CellPayloadModeDTO.RawOnly || mode == CellPayloadModeDTO.Both) && valueRaw is { Length: > 0 })
                payload = valueRaw;
            else if ((mode == CellPayloadModeDTO.Utf8Only || mode == CellPayloadModeDTO.Both) && !string.IsNullOrEmpty(valueUtf8))
                payload = Encoding.UTF8.GetBytes(valueUtf8);

            if (payload.IsEmpty) return;

            int n = Math.Min(valCap, payload.Length);
            payload[..n].CopyTo(seg);

            if (n < valCap) seg[n] = 0;
        }

        /// <summary>
        /// Clears <paramref name="bytes"/> bytes starting at <paramref name="ptr"/> by writing zeros.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void ZeroFill(byte* ptr, int bytes)
        {
            if (bytes <= 0) return;
            new Span<byte>(ptr, bytes).Clear();
        }

        /// <summary>
        /// Builds an <c>HRESULT</c> that encodes both the failing RPC operation and a coarse error category.
        /// </summary>
        /// <param name="op">The RPC operation identifier.</param>
        /// <param name="err">The coarse error classification.</param>
        /// <returns>
        /// A failing <c>HRESULT</c> with severity=1, facility=ITF (4), and a 16-bit code packing <paramref name="op"/> and <paramref name="err"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Encoding: Severity=1 (failure), Facility=ITF (4), Code = <c>(opLow8 &lt;&lt; 8) | err</c>.
        /// This yields a stable, compact signal across IPC boundaries without exposing backend exception types.
        /// </para>
        /// <para>
        /// Note: only the low 8 bits of <paramref name="op"/> are currently stored in the 16-bit code.
        /// If you need to preserve the full 16-bit operation id, update this encoding and keep compatibility in mind.
        /// </para>
        /// </remarks>
        internal static int MakeRpcHResult(RpcOp op, RpcErr err)
        {
            const int FACILITY_ITF = 4;
            int code16 = ((int)op & 0xFF) << 8 | (int)err & 0xFF;
            return unchecked(1 << 31 | FACILITY_ITF << 16 | code16 & 0xFFFF);
        }

        /// <summary>
        /// Maps an exception to a coarse <see cref="RpcErr"/> category suitable for <c>HRESULT</c> encoding.
        /// </summary>
        /// <param name="ex">The exception to classify.</param>
        /// <returns>A coarse classification for the exception.</returns>
        /// <remarks>
        /// <para>
        /// This is intentionally heuristic. Adjust the pattern matching to your real exception types/messages as the library evolves.
        /// </para>
        /// <para>
        /// Prefer mapping to stable library-defined exception types rather than message parsing when possible.
        /// </para>
        /// </remarks>
        internal static RpcErr ClassifyErr(Exception ex)
        {
            // Ajusta esto a tus excepciones reales si tienes tipos propios.
            return ex switch
            {
                ArgumentNullException or ArgumentException or FormatException => RpcErr.InvalidArg,
                KeyNotFoundException => RpcErr.NotFound,
                NotSupportedException => RpcErr.NotSupported,
                UnauthorizedAccessException => RpcErr.Protected,
                ObjectDisposedException => RpcErr.Disposed,

                // Tu caso real: "already registered"
                InvalidOperationException ioe when ioe.Message.Contains("already registered", StringComparison.OrdinalIgnoreCase)
                    => RpcErr.AlreadyExists,

                _ => RpcErr.Unexpected
            };
        }

        /// <summary>
        /// Executes an RPC operation and, on failure, stamps the thrown exception with an operation-aware <c>HRESULT</c>.
        /// </summary>
        /// <typeparam name="T">Return type of the operation.</typeparam>
        /// <param name="op">The operation identifier to encode into the <c>HRESULT</c>.</param>
        /// <param name="body">The operation body.</param>
        /// <returns>The value produced by <paramref name="body"/>.</returns>
        /// <exception cref="Exception">
        /// Re-throws the original exception after stamping <see cref="Exception.HResult"/>.
        /// </exception>
        /// <remarks>
        /// Use this for synchronous RPC-exposed methods to preserve the original stack trace while attaching
        /// a stable cross-process error code.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static T Rpc<T>(RpcOp op, Func<T> body, Action throwIfDisposed)
        {
            throwIfDisposed();
            try { return body(); }
            catch (Exception ex)
            {
                var err = ClassifyErr(ex);
                ex.HResult = MakeRpcHResult(op, err);
                throw; // NO "throw ex;"
            }
        }

        /// <summary>
        /// Executes a void RPC operation and, on failure, stamps the thrown exception with an operation-aware <c>HRESULT</c>.
        /// </summary>
        /// <param name="op">The operation identifier to encode into the <c>HRESULT</c>.</param>
        /// <param name="body">The operation body.</param>
        /// <remarks>
        /// Internally calls <see cref="Rpc{T}"/> to reuse error stamping logic.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void RpcVoid(RpcOp op, Action body, Action throwIfDisposed)
        {
            Rpc(op, () => { body(); return 0; }, throwIfDisposed);
        }

        /// <summary>
        /// Executes an asynchronous RPC operation and, on failure, stamps the thrown exception with an operation-aware <c>HRESULT</c>.
        /// </summary>
        /// <param name="op">The operation identifier to encode into the <c>HRESULT</c>.</param>
        /// <param name="body">The async operation body.</param>
        /// <returns>A task representing the operation.</returns>
        /// <exception cref="Exception">
        /// Re-throws the original exception after stamping <see cref="Exception.HResult"/>.
        /// </exception>
        /// <remarks>
        /// You must <c>await</c> inside the try/catch; returning the <see cref="Task"/> directly would move the faulting
        /// exception outside the wrapper and prevent stamping.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static async Task RpcAsync(RpcOp op, Func<Task> body, Action throwIfDisposed)
        {
            throwIfDisposed();
            try
            {
                await body().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var err = ClassifyErr(ex);
                ex.HResult = MakeRpcHResult(op, err);
                throw;
            }
        }

        /// <summary>
        /// Executes an asynchronous RPC operation returning a value and, on failure, stamps the thrown exception with an operation-aware <c>HRESULT</c>.
        /// </summary>
        /// <typeparam name="T">Return type of the operation.</typeparam>
        /// <param name="op">The operation identifier to encode into the <c>HRESULT</c>.</param>
        /// <param name="body">The async operation body.</param>
        /// <returns>A task producing the value returned by <paramref name="body"/>.</returns>
        /// <exception cref="Exception">
        /// Re-throws the original exception after stamping <see cref="Exception.HResult"/>.
        /// </exception>
        /// <remarks>
        /// You must <c>await</c> inside the try/catch; returning the <see cref="Task{TResult}"/> directly would move the faulting
        /// exception outside the wrapper and prevent stamping.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static async Task<T> RpcAsync<T>(RpcOp op, Func<Task<T>> body, Action throwIfDisposed)
        {
            throwIfDisposed();
            try
            {
                return await body().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var err = ClassifyErr(ex);
                ex.HResult = MakeRpcHResult(op, err);
                throw;
            }
        }

        /// <summary>
        /// Lightweight, stack-only result container produced by <c>ReadCellUnsafe</c>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a <see langword="ref struct"/> so it can safely carry <see cref="ReadOnlySpan{T}"/> views
        /// that may alias mapped/unmanaged storage without accidentally escaping to the heap.
        /// </para>
        /// <para>
        /// The spans are only valid as long as the underlying table/cell memory remains valid and unchanged.
        /// Do not store this instance, do not capture it in lambdas/async, and do not let it outlive the read scope.
        /// </para>
        /// <para>
        /// <see cref="HasKey"/> indicates that a non-empty KEY payload was found (key/value columns only).
        /// <see cref="HasAnyValue"/> is a presence signal for VALUE:
        /// for value-only columns it is computed by scanning the fixed VALUE segment for non-zero bytes;
        /// for key/value columns it is set when a non-empty VALUE payload is returned by the cell.
        /// </para>
        /// </remarks>
        private ref struct ReadCellUnsafeResult(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool hasKey, bool hasAnyValue)
        {
            /// <summary>
            /// Read-only view of the KEY payload when present; otherwise <see cref="ReadOnlySpan{T}.Empty"/>.
            /// </summary>
            public ReadOnlySpan<byte> Key = key;

            /// <summary>
            /// Read-only view of the VALUE payload when present; otherwise <see cref="ReadOnlySpan{T}.Empty"/>.
            /// </summary>
            public ReadOnlySpan<byte> Value = value;

            /// <summary>
            /// Gets whether a non-empty KEY payload was present for the cell.
            /// </summary>
            public bool HasKey = hasKey;

            /// <summary>
            /// Gets whether a VALUE payload is considered present for the cell.
            /// </summary>
            public bool HasAnyValue = hasAnyValue;
        }
    }
}