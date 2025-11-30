using System.Globalization;
using System.Runtime.CompilerServices;

namespace Extend0.Metadata
{
    /// <summary>
    /// Lightweight value type that identifies a cell in a metadata table by
    /// <see cref="Row"/> and <see cref="Column"/> coordinates.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="MetadataCellPointer"/> is designed to be compact, easily hashable and
    /// convenient to serialize/parse. It implements:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><see cref="IEquatable{T}"/> for fast equality comparison.</description></item>
    ///   <item><description><see cref="ISpanFormattable"/> for allocation-free formatting.</description></item>
    ///   <item><description><see cref="IComparable{T}"/> with ordering by <c>(Column, Row)</c>.</description></item>
    ///   <item><description><see cref="IParsable{T}"/> and <see cref="ISpanParsable{T}"/> for text parsing.</description></item>
    ///   <item><description><see cref="IUtf8SpanParsable{T}"/> for UTF-8 based parsing.</description></item>
    /// </list>
    /// <para>
    /// The default textual representation is <c>"row:column"</c> in decimal form.
    /// Alternative formats allow comma separators and hexadecimal representation.
    /// </para>
    /// </remarks>
    public readonly struct MetadataCellPointer(uint row, uint column) :
        IEquatable<MetadataCellPointer>,
        ISpanFormattable,
        IComparable<MetadataCellPointer>,
        IParsable<MetadataCellPointer>,
        ISpanParsable<MetadataCellPointer>,
        IUtf8SpanParsable<MetadataCellPointer>
    {
        /// <summary>
        /// Gets the zero-based row index of the cell.
        /// </summary>
        public readonly uint Row = row;

        /// <summary>
        /// Gets the zero-based column index of the cell.
        /// </summary>
        public readonly uint Column = column;

        /// <summary>
        /// Returns a string representation in the default <c>"row:column"</c> format.
        /// </summary>
        /// <returns>A string in the form <c>"row:column"</c> using invariant culture.</returns>
        public override string ToString() => ToString("G", CultureInfo.InvariantCulture);

        /// <summary>
        /// Formats this instance using the specified format string and format provider.
        /// </summary>
        /// <param name="format">
        /// Format specifier:
        /// <list type="bullet">
        ///   <item><description><c>"G"</c> (or empty): <c>"row:column"</c> in decimal.</description></item>
        ///   <item><description><c>"C"</c>: <c>"row,column"</c> in decimal.</description></item>
        ///   <item><description><c>"X"</c>: <c>"row:column"</c> in hexadecimal.</description></item>
        /// </list>
        /// Only the first character is considered; casing is ignored.
        /// </param>
        /// <param name="formatProvider">Optional format provider; typically ignored.</param>
        /// <returns>
        /// A formatted string representation of this pointer.
        /// </returns>
        public string ToString(string? format, IFormatProvider? formatProvider)
        {
            Span<char> buf = stackalloc char[32]; // enough for 10+1+10 (dec) or 8+1+8 (hex)
            if (TryFormat(buf, out int written, format, formatProvider))
                return new string(buf[..written]);

            // Fallback (should not normally happen)
            return $"{Row}:{Column}";
        }

        /// <summary>
        /// Tries to format this instance into the provided character span.
        /// </summary>
        /// <param name="destination">Target buffer for the formatted characters.</param>
        /// <param name="charsWritten">On success, receives the number of characters written.</param>
        /// <param name="format">
        /// Optional format specifier (see <see cref="ToString(string?, IFormatProvider?)"/> for supported values).
        /// </param>
        /// <param name="provider">Optional format provider.</param>
        /// <returns>
        /// <see langword="true"/> if the value was formatted successfully;
        /// otherwise <see langword="false"/> if the destination buffer was too small.
        /// </returns>
        public bool TryFormat(Span<char> destination, out int charsWritten,
                              ReadOnlySpan<char> format, IFormatProvider? provider)
        {
            char f = (format.Length == 0) ? 'G' : char.ToUpperInvariant(format[0]);
            char sep = f == 'C' ? ',' : ':';
            ReadOnlySpan<char> numFmt = f == 'X' ? "X" : [];

            // Capacity pre-check (worst case: decimal 10+1+10; hex 8+1+8)
            int worst = f == 'X' ? 17 : 21;
            if (destination.Length < worst)
            {
                charsWritten = 0;
                return false;
            }

            // row
            if (!Row.TryFormat(destination, out int w1, numFmt, provider)) { charsWritten = 0; return false; }
            if (w1 >= destination.Length) { charsWritten = 0; return false; }
            destination[w1] = sep;

            // col
            var tail = destination[(w1 + 1)..];
            if (!Column.TryFormat(tail, out int w2, numFmt, provider)) { charsWritten = 0; return false; }

            charsWritten = w1 + 1 + w2;
            return true;
        }

        /// <summary>
        /// Attempts to parse a pointer from a textual representation.
        /// </summary>
        /// <param name="s">
        /// Input span containing either <c>"row:col"</c> or <c>"row,col"</c>,
        /// in decimal or hexadecimal (prefix <c>0x</c> allowed for each part).
        /// </param>
        /// <param name="value">
        /// When this method returns <see langword="true"/>, contains the parsed value.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if parsing succeeded; otherwise <see langword="false"/>.
        /// </returns>
        public static bool TryParse(ReadOnlySpan<char> s, out MetadataCellPointer value)
            => TryParse(s, CultureInfo.InvariantCulture, out value);

        /// <summary>
        /// Attempts to parse a pointer from a textual representation using a specific format provider.
        /// </summary>
        /// <param name="s">
        /// Input span containing either <c>"row:col"</c> or <c>"row,col"</c>,
        /// in decimal or hexadecimal (prefix <c>0x</c> allowed for each part).
        /// </param>
        /// <param name="provider">
        /// Format provider used for numeric parsing. When <see langword="null"/>,
        /// <see cref="CultureInfo.InvariantCulture"/> is used.
        /// </param>
        /// <param name="value">
        /// When this method returns <see langword="true"/>, contains the parsed value.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if parsing succeeded; otherwise <see langword="false"/>.
        /// </returns>
        public static bool TryParse(ReadOnlySpan<char> s, IFormatProvider? provider, out MetadataCellPointer value)
        {
            var culture = provider as CultureInfo ?? CultureInfo.InvariantCulture;

            s = s.Trim();
            int i = s.IndexOfAny(':', ',');
            if (i <= 0 || i >= s.Length - 1) { value = default; return false; }

            var left = s[..i].Trim();
            var right = s[(i + 1)..].Trim();

            static bool ParseUInt(ReadOnlySpan<char> txt, CultureInfo culture, out uint n)
            {
                if (txt.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
                    return uint.TryParse(txt[2..], NumberStyles.HexNumber, culture, out n);

                return uint.TryParse(txt, NumberStyles.None, culture, out n);
            }

            if (!ParseUInt(left, culture, out var r) || !ParseUInt(right, culture, out var c))
            {
                value = default;
                return false;
            }

            value = new MetadataCellPointer(r, c);
            return true;
        }

        /// <summary>
        /// Parses a pointer from a string representation.
        /// </summary>
        /// <param name="s">
        /// Input string in the format <c>"row:col"</c> or <c>"row,col"</c>,
        /// in decimal or hexadecimal (prefix <c>0x</c> allowed for each part).
        /// </param>
        /// <returns>The parsed <see cref="MetadataCellPointer"/>.</returns>
        /// <exception cref="FormatException">If the input is not in a valid format.</exception>
        public static MetadataCellPointer Parse(string s)
            => TryParse(s.AsSpan(), CultureInfo.InvariantCulture, out var v)
               ? v
               : throw new FormatException("Expected 'row:col' or 'row,col'");

        // IParsable<T> / ISpanParsable<T>

        /// <inheritdoc />
        static MetadataCellPointer IParsable<MetadataCellPointer>.Parse(string s, IFormatProvider? provider)
            => TryParse(s.AsSpan(), provider, out var v)
               ? v
               : throw new FormatException("Expected 'row:col' or 'row,col'");

        /// <inheritdoc />
        static bool IParsable<MetadataCellPointer>.TryParse(
            string? s,
            IFormatProvider? provider,
            out MetadataCellPointer result)
        {
            if (s is null)
            {
                result = default;
                return false;
            }

            return TryParse(s.AsSpan(), provider, out result);
        }

        /// <inheritdoc />
        static MetadataCellPointer ISpanParsable<MetadataCellPointer>.Parse(
            ReadOnlySpan<char> s,
            IFormatProvider? provider)
            => TryParse(s, provider, out var v)
               ? v
               : throw new FormatException("Expected 'row:col' or 'row,col'");

        /// <inheritdoc />
        static bool ISpanParsable<MetadataCellPointer>.TryParse(
            ReadOnlySpan<char> s,
            IFormatProvider? provider,
            out MetadataCellPointer result)
            => TryParse(s, provider, out result);

        /// <summary>
        /// Determines whether this instance and another pointer represent the same cell.
        /// </summary>
        /// <param name="other">Pointer to compare with.</param>
        /// <returns>
        /// <see langword="true"/> if both have the same <see cref="Row"/> and <see cref="Column"/>;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(MetadataCellPointer other) => Row == other.Row && Column == other.Column;

        /// <inheritdoc/>
        public override bool Equals(object? obj) => obj is MetadataCellPointer m && Equals(m);

        /// <inheritdoc/>
        public override int GetHashCode() => HashCode.Combine(Row, Column);

        /// <summary>
        /// Equality operator; compares two pointers by row and column.
        /// </summary>
        public static bool operator ==(MetadataCellPointer left, MetadataCellPointer right) => left.Equals(right);

        /// <summary>
        /// Inequality operator; compares two pointers by row and column.
        /// </summary>
        public static bool operator !=(MetadataCellPointer left, MetadataCellPointer right) => !left.Equals(right);

        /// <summary>
        /// Implicit conversion to value tuple <c>(row, column)</c>.
        /// </summary>
        public static implicit operator (uint row, uint column)(MetadataCellPointer p) => (p.Row, p.Column);

        /// <summary>
        /// Implicit conversion from value tuple <c>(row, column)</c>.
        /// </summary>
        public static implicit operator MetadataCellPointer((uint row, uint column) t) => new(t.row, t.column);

        /// <summary>
        /// Deconstructs this pointer into its row and column components.
        /// </summary>
        /// <param name="row">Receives the row index.</param>
        /// <param name="column">Receives the column index.</param>
        public void Deconstruct(out uint row, out uint column) { row = Row; column = Column; }

        /// <summary>
        /// Packs this pointer into a single 64-bit unsigned integer.
        /// </summary>
        /// <returns>
        /// A <see cref="ulong"/> where the high 32 bits contain <see cref="Column"/>
        /// and the low 32 bits contain <see cref="Row"/>.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ToPacked() => ((ulong)Column << 32) | Row;

        /// <summary>
        /// Unpacks a pointer from a 64-bit value produced by <see cref="ToPacked"/>.
        /// </summary>
        /// <param name="packed">Packed representation of a pointer.</param>
        /// <returns>The unpacked <see cref="MetadataCellPointer"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MetadataCellPointer FromPacked(ulong packed)
            => new(row: (uint)packed, column: (uint)(packed >> 32));

        /// <summary>
        /// Explicit conversion to <see cref="ulong"/> using <see cref="ToPacked"/>.
        /// </summary>
        public static explicit operator ulong(MetadataCellPointer p) => p.ToPacked();

        /// <summary>
        /// Explicit conversion from <see cref="ulong"/> using <see cref="FromPacked"/>.
        /// </summary>
        public static explicit operator MetadataCellPointer(ulong packed) => FromPacked(packed);

        /// <summary>
        /// Compares this instance with another <see cref="MetadataCellPointer"/> to determine sort order.
        /// </summary>
        /// <param name="other">Pointer to compare with.</param>
        /// <returns>
        /// A signed integer that indicates whether this instance precedes, equals, or follows
        /// <paramref name="other"/>. The comparison is performed on the packed representation,
        /// effectively ordering by <c>(Column, Row)</c>.
        /// </returns>
        public int CompareTo(MetadataCellPointer other)
            => ToPacked().CompareTo(other.ToPacked());

        /// <summary>
        /// Formats this instance as UTF-8 into the provided buffer.
        /// </summary>
        /// <param name="utf8Destination">Destination span for the UTF-8 encoded bytes.</param>
        /// <param name="bytesWritten">On success, receives the number of bytes written.</param>
        /// <param name="format">
        /// Optional format specifier (same semantics as the character-based overload).
        /// </param>
        /// <param name="provider">Optional format provider.</param>
        /// <returns>
        /// <see langword="true"/> if the value was formatted successfully; otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// This implementation formats into a temporary <see cref="char"/> buffer and then
        /// transcodes to UTF-8 for simplicity.
        /// </remarks>
        public bool TryFormat(System.Span<byte> utf8Destination, out int bytesWritten, System.ReadOnlySpan<char> format, IFormatProvider? provider)
        {
            // Format to char and transcode (simple and fast enough).
            Span<char> tmp = stackalloc char[32];
            if (!TryFormat(tmp, out int w, format, provider))
            {
                bytesWritten = 0;
                return false;
            }
            return System.Text.Encoding.UTF8.TryGetBytes(tmp[..w], utf8Destination, out bytesWritten);
        }

        /// <summary>
        /// Tries to parse a pointer from a UTF-8 encoded representation.
        /// </summary>
        /// <param name="utf8Text">
        /// Input span containing UTF-8 text in the format <c>"row:col"</c> or <c>"row,col"</c>,
        /// in decimal or hexadecimal (prefix <c>0x</c> allowed for each part).
        /// </param>
        /// <param name="provider">Optional format provider (ignored).</param>
        /// <param name="value">
        /// When this method returns <see langword="true"/>, contains the parsed value.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if parsing succeeded; otherwise <see langword="false"/>.
        /// </returns>
        public static bool TryParse(ReadOnlySpan<byte> utf8Text, IFormatProvider? provider, out MetadataCellPointer value)
        {
            utf8Text = TrimAscii(utf8Text);
            int sep = utf8Text.IndexOf((byte)':');
            if (sep < 0) sep = utf8Text.IndexOf((byte)',');

            if (sep <= 0 || sep >= utf8Text.Length - 1)
            {
                value = default;
                return false;
            }

            var left = utf8Text[..sep];
            var right = utf8Text[(sep + 1)..];

            if (!TryParseUIntUtf8(left, out uint r) || !TryParseUIntUtf8(right, out uint c))
            {
                value = default;
                return false;
            }

            value = new MetadataCellPointer(r, c);
            return true;
        }

        /// <summary>
        /// Trims leading and trailing ASCII whitespace characters from the given span.
        /// </summary>
        /// <param name="s">
        /// The input span containing ASCII bytes. Characters with values less than or
        /// or equal to <c>0x20</c> at the start and end are treated as whitespace.
        /// </param>
        /// <returns>
        /// A slice of <paramref name="s"/> with leading and trailing ASCII whitespace removed.
        /// If the span consists only of whitespace, returns an empty span.
        /// </returns>
        static ReadOnlySpan<byte> TrimAscii(ReadOnlySpan<byte> s)
        {
            int i = 0, j = s.Length - 1;
            while (i <= j && s[i] <= 0x20) i++;
            while (j >= i && s[j] <= 0x20) j--;
            return s.Slice(i, j - i + 1);
        }

        /// <summary>
        /// Attempts to parse an unsigned 32-bit integer from a UTF-8 byte span.
        /// </summary>
        /// <param name="s">
        /// The UTF-8 encoded input span. Leading and trailing ASCII whitespace
        /// (bytes &lt;= <c>0x20</c>) are ignored.
        /// Supports decimal numbers (e.g. <c>"123"</c>) and hexadecimal numbers
        /// prefixed with <c>"0x"</c> or <c>"0X"</c> (e.g. <c>"0xFF"</c>).
        /// </param>
        /// <param name="n">
        /// When this method returns <see langword="true"/>, contains the parsed
        /// unsigned 32-bit integer value.
        /// When it returns <see langword="false"/>, the value is undefined.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the span contains a valid unsigned integer in
        /// decimal or hexadecimal form; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// Decimal parsing uses a checked context and will throw an <see cref="OverflowException"/>
        /// if the value does not fit in a <see cref="uint"/>.
        /// Hexadecimal parsing returns <see langword="false"/> for any non-hex character.
        /// </remarks>
        static bool TryParseUIntUtf8(ReadOnlySpan<byte> s, out uint n)
        {
            n = 0;
            s = TrimAscii(s);
            if (s.Length == 0) return false;

            // Hex with 0x/0X prefix
            if (s.Length >= 2 && s[0] == (byte)'0' && (s[1] == (byte)'x' || s[1] == (byte)'X'))
            {
                s = s[2..];
                if (s.Length == 0) return false;
                foreach (byte b in s)
                {
                    uint v = b switch
                    {
                        >= (byte)'0' and <= (byte)'9' => (uint)(b - (byte)'0'),
                        >= (byte)'a' and <= (byte)'f' => (uint)(b - (byte)'a' + 10),
                        >= (byte)'A' and <= (byte)'F' => (uint)(b - (byte)'A' + 10),
                        _ => 0xFFFF_FFFFu
                    };
                    if (v == 0xFFFF_FFFFu) return false;
                    n = (n << 4) | v;
                }
                return true;
            }

            // Decimal
            foreach (byte b in s)
            {
                if (b < (byte)'0' || b > (byte)'9') return false;
                n = checked(n * 10 + (uint)(b - (byte)'0'));
            }
            return true;
        }

        /// <summary>
        /// Parses a pointer from a UTF-8 encoded representation.
        /// </summary>
        /// <param name="utf8Text">
        /// Input UTF-8 text in the format <c>"row:col"</c> or <c>"row,col"</c>.
        /// </param>
        /// <param name="provider">Optional format provider (ignored).</param>
        /// <returns>The parsed <see cref="MetadataCellPointer"/>.</returns>
        /// <exception cref="FormatException">If the input is not in a valid format.</exception>
        public static MetadataCellPointer Parse(ReadOnlySpan<byte> utf8Text, IFormatProvider? provider)
        {
            if (TryParse(utf8Text, provider, out var v))
                return v;

            throw new FormatException("Expected 'row:col' or 'row,col' (UTF-8).");
        }
    }
}
