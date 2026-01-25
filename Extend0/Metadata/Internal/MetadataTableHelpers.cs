using System.Text;

namespace Extend0.Metadata.Internal
{
    internal static class MetadataTableHelpers
    {
        private static readonly UTF8Encoding s_Utf8Strict = new(false, true);

        /// <summary>
        /// Builds a horizontal border line for the textual table representation.
        /// </summary>
        /// <param name="colCount">Number of data columns in the table.</param>
        /// <param name="widths">
        /// Column widths array, where index 0 corresponds to the row index column and indices 1..colCount
        /// correspond to the data columns.
        /// </param>
        /// <returns>
        /// A string containing a border line composed of <c>'+'</c> and <c>'-'</c> characters,
        /// followed by a newline.
        /// </returns>
        internal static string Border(uint colCount, int[] widths)
        {
            var b = new StringBuilder();
            b.Append('+').Append(new string('-', widths[0] + 2));
            for (int c = 0; c < colCount; c++)
                b.Append('+').Append(new string('-', widths[c + 1] + 2));
            b.Append("+\n");
            return b.ToString();
        }

        /// <summary>
        /// Converts a sequence of bytes to their uppercase hexadecimal representation
        /// and writes the result into a caller-provided character span.
        /// </summary>
        /// <param name="v">The input bytes to convert.</param>
        /// <param name="outChars">
        /// Destination span that receives the hexadecimal characters. Its length must be
        /// at least <c>2 * v.Length</c>.
        /// </param>
        private static void FillHex(ReadOnlySpan<byte> v, Span<char> outChars)
        {
            static char Hex(byte x) => (char)(x < 10 ? '0' + x : 'A' + (x - 10));
            int ci = 0;
            foreach (var b in v)
            {
                outChars[ci++] = Hex((byte)(b >> 4));
                outChars[ci++] = Hex((byte)(b & 0xF));
            }
        }

        /// <summary>
        /// Renders VALUE bytes as an uppercase hexadecimal string, truncated with an ellipsis if needed.
        /// </summary>
        /// <param name="v">The raw VALUE bytes to render.</param>
        /// <param name="maxChars">
        /// Maximum number of characters in the output, including the ellipsis character if truncation occurs.
        /// </param>
        /// <returns>
        /// A hexadecimal representation of <paramref name="v"/>. If the full representation would exceed
        /// <paramref name="maxChars"/>, the output is truncated to fit and suffixed with <c>'…'</c>.
        /// </returns>
        private static string HexPreview(ReadOnlySpan<byte> v, int maxChars)
        {
            if (maxChars <= 0) return string.Empty;

            int fullChars = v.Length * 2;
            if (fullChars <= maxChars)
            {
                var chars = new char[fullChars];
                FillHex(v, chars);
                return new string(chars);
            }
            else
            {
                int maxBytes = Math.Max(0, (maxChars - 1) / 2); // leave 1 char for ellipsis
                var chars = new char[maxBytes * 2 + 1];
                FillHex(v[..maxBytes], chars);
                chars[maxBytes * 2] = '…';
                return new string(chars);
            }
        }

        /// <summary>
        /// Pads a string on the right with spaces so that its length is at least the specified width.
        /// </summary>
        /// <param name="s">The string to pad.</param>
        /// <param name="w">The desired minimum width.</param>
        /// <returns>
        /// The original string if its length is greater than or equal to <paramref name="w"/>;
        /// otherwise, the string padded with spaces on the right to reach the given width.
        /// </returns>
        internal static string Pad(string s, int w) => s.Length >= w ? s : s + new string(' ', w - s.Length);

        /// <summary>
        /// Produces a short textual preview for a VALUE payload given the maximum width.
        /// </summary>
        /// <param name="v">The raw VALUE bytes to render.</param>
        /// <param name="maxChars">Maximum number of characters to include in the preview.</param>
        /// <returns>
        /// An empty string when <paramref name="v"/> is empty; otherwise a UTF-8 or hexadecimal
        /// preview string truncated with an ellipsis if necessary.
        /// </returns>
        /// <remarks>
        /// Delegates to <see cref="Utf8Preview(ReadOnlySpan{byte}, int)"/> which prefers UTF-8 decoding
        /// when the bytes form a printable string, or to a hexadecimal preview otherwise.
        /// </remarks>
        internal static string Preview(ReadOnlySpan<byte> v, int maxChars) => v.Length == 0 ? "" : Utf8Preview(v, maxChars);

        /// <summary>
        /// Safely truncates a string to a maximum length and appends an ellipsis when needed.
        /// </summary>
        /// <param name="s">The input string to truncate.</param>
        /// <param name="maxChars">Maximum allowed number of characters in the result.</param>
        /// <returns>
        /// The original string if it fits within <paramref name="maxChars"/>;
        /// otherwise, a truncated version with a trailing <c>'…'</c>. Surrogate pairs at the cut
        /// boundary are preserved by backing off one character when necessary.
        /// </returns>
        private static string SafeEllipsis(string s, int maxChars)
        {
            if (maxChars <= 0) return string.Empty;
            if (s.Length <= maxChars) return s;
            int cut = Math.Max(0, maxChars - 1);
            if (cut > 0 && char.IsHighSurrogate(s[cut - 1])) cut--;
            return s.AsSpan(0, cut).ToString() + "…";
        }

        /// <summary>
        /// Tries to decode a UTF-8 byte span into a printable string.
        /// </summary>
        /// <param name="v">The UTF-8 encoded bytes to decode.</param>
        /// <param name="text">
        /// When this method returns <see langword="true"/>, contains the decoded string.
        /// When it returns <see langword="false"/>, the value is undefined.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the bytes decode cleanly to UTF-8 without replacement characters
        /// and do not contain control characters other than TAB, CR, or LF; otherwise <see langword="false"/>.
        /// </returns>
        private static bool TryDecodePrintableUtf8(ReadOnlySpan<byte> v, out string text)
        {
            try
            {
                text = s_Utf8Strict.GetString(v);
            }
            catch
            {
                text = string.Empty;
                return false;
            }

            if (text.Contains('\uFFFD')) return false;
            foreach (var ch in text)
                if (char.IsControl(ch) && ch != '\t' && ch != '\r' && ch != '\n')
                    return false;
            return true;
        }

        /// <summary>
        /// Attempts to render the given VALUE bytes as a printable UTF-8 string, falling back
        /// to a hexadecimal preview when decoding fails or produces non-printable characters.
        /// </summary>
        /// <param name="v">The raw VALUE bytes to render.</param>
        /// <param name="maxChars">Maximum number of characters allowed in the resulting string.</param>
        /// <returns>
        /// A UTF-8 decoded preview string (possibly truncated with an ellipsis) when the data is
        /// printable, or a hexadecimal preview produced by <see cref="HexPreview(ReadOnlySpan{byte}, int)"/>
        /// otherwise.
        /// </returns>
        private static string Utf8Preview(ReadOnlySpan<byte> v, int maxChars)
        {
            if (!TryDecodePrintableUtf8(v, out var s)) return HexPreview(v, maxChars);
            return SafeEllipsis(s, maxChars);
        }
    }
}