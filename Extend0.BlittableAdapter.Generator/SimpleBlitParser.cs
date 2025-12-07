using System.Text.RegularExpressions;

namespace Extend0.BlittableAdapter.Generator
{
    /// <summary>
    /// Very small, regex-based parser for the simplified JSON definition used by the
    /// blittable adapter generator.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is not a general-purpose JSON parser. It assumes a constrained schema, for example:
    /// </para>
    /// <code lang="json">
    /// {
    ///   "recordName": "UserRecord",
    ///   "adapterNamespace": "My.App.Adapter",
    ///   "columnName": "Users",
    ///   "fields": [
    ///     { "name": "Id", "kind": "u64", "maxBytes": 8 },
    ///     { "name": "Name", "kind": "utf8", "maxBytes": 128 }
    ///   ]
    /// }
    /// </code>
    /// <para>
    /// It extracts only the tokens it needs (<c>recordName</c>, <c>adapterNamespace</c>,
    /// <c>columnName</c> and the <c>fields</c> array) and ignores the rest.
    /// </para>
    /// </remarks>
    internal static class SimpleBlitParser
    {
        // Regex for top-level properties
        private static readonly Regex RecordNameRx =
            new("\"recordName\"\\s*:\\s*\"(?<v>[^\"]+)\"", RegexOptions.Compiled);

        private static readonly Regex AdapterNsRx =
            new("\"adapterNamespace\"\\s*:\\s*\"(?<v>[^\"]+)\"", RegexOptions.Compiled);

        private static readonly Regex ColumnNameRx =
            new("\"columnName\"\\s*:\\s*\"(?<v>[^\"]+)\"", RegexOptions.Compiled);

        // Regex to extract simple field objects from the "fields" array
        private static readonly Regex FieldObjectRx =
            new("\\{[^\\{\\}]*\\}", RegexOptions.Compiled);

        private static readonly Regex NameRx =
            new("\"name\"\\s*:\\s*\"(?<v>[^\"]+)\"", RegexOptions.Compiled);

        private static readonly Regex KindRx =
            new("\"kind\"\\s*:\\s*\"(?<v>[^\"]+)\"", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private static readonly Regex MaxBytesRx =
            new("\"maxBytes\"\\s*:\\s*(?<v>\\d+)", RegexOptions.Compiled);

        /// <summary>
        /// Tries to parse a simplified JSON definition into a <see cref="DefinitionModel"/>.
        /// </summary>
        /// <param name="json">The JSON text containing the generator definition.</param>
        /// <returns>
        /// A populated <see cref="DefinitionModel"/> when parsing succeeds, or <see langword="null"/>
        /// when the input is empty or does not contain a valid <c>recordName</c>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// If <c>adapterNamespace</c> is not present, it defaults to
        /// <c>"Extend0.Metadata.Generated"</c>. If <c>columnName</c> is not present,
        /// the <c>recordName</c> is reused as the column name.
        /// </para>
        /// </remarks>
        public static DefinitionModel? TryParse(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                return null;

            string recordName = GetSingle(RecordNameRx, json) ?? "";
            string adapterNs = GetSingle(AdapterNsRx, json) ?? "Extend0.Metadata.Generated";
            string columnName = GetSingle(ColumnNameRx, json) ?? recordName;

            if (recordName.Length == 0)
                return null; // invalid definition

            var fields = ParseFields(json);

            return new DefinitionModel
            {
                RecordName = recordName,
                AdapterNamespace = adapterNs,
                ColumnName = columnName,
                Fields = [.. fields]
            };
        }

        /// <summary>
        /// Runs a single-match regex against the provided text and returns the value of the named
        /// capture group <c>"v"</c>, or <see langword="null"/> if the pattern does not match.
        /// </summary>
        /// <param name="rx">Compiled regular expression with a <c>"v"</c> capture group.</param>
        /// <param name="text">Input text to search.</param>
        private static string? GetSingle(Regex rx, string text)
        {
            var m = rx.Match(text);
            return m.Success ? m.Groups["v"].Value : null;
        }

        /// <summary>
        /// Parses the <c>"fields"</c> array from the JSON definition into a list of
        /// <see cref="FieldModel"/> instances.
        /// </summary>
        /// <param name="json">The full JSON definition text.</param>
        /// <returns>
        /// A list of parsed <see cref="FieldModel"/> entries. The list is empty when no
        /// <c>"fields"</c> array is found or when it is malformed.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method expects a simple array of flat objects:
        /// <c>{ "name": "...", "kind": "...", "maxBytes": N }</c>.
        /// Unknown properties are ignored. The <c>kind</c> defaults to <c>"binary"</c> when missing
        /// and <c>maxBytes</c> is optional.
        /// </para>
        /// </remarks>
        private static List<FieldModel> ParseFields(string json)
        {
            ParseFieldCheckReturnValue checkReturnValue = ParseFieldCheck(json);
            if (!checkReturnValue.Completed) return [];

            string arraySlice = json.Substring(checkReturnValue.StartArray, checkReturnValue.Length + 1);
            return AddMatches(arraySlice);
        }

        /// <summary>
        /// Extracts all field objects from the JSON array slice and converts them
        /// into <see cref="FieldModel"/> instances.
        /// </summary>
        /// <param name="arraySlice">
        /// A string containing only the JSON array segment that holds the field definitions.
        /// </param>
        /// <returns>
        /// A list of <see cref="FieldModel"/> objects parsed from the array slice.
        /// </returns>
        private static List<FieldModel> AddMatches(string arraySlice)
        {
            List<FieldModel> result = [];
            foreach (Match objMatch in FieldObjectRx.Matches(arraySlice))
                AddMatch(result, objMatch);
            return result;
        }

        /// <summary>
        /// Locates the <c>"fields"</c> array within the JSON text and returns its
        /// start and end indices.
        /// </summary>
        /// <param name="json">The full JSON definition text.</param>
        /// <returns>
        /// A <see cref="ParseFieldCheckReturnValue"/> indicating whether the array
        /// was found (<see cref="ParseFieldCheckReturnValue.Completed"/>) and, if so,
        /// the start and end indices of the array brackets.
        /// </returns>
        /// <remarks>
        /// The method searches for the <c>"fields"</c> property name, then looks for
        /// the opening <c>'['</c> bracket and its matching closing <c>']'</c>.
        /// If any of these steps fail, the returned value has
        /// <see cref="ParseFieldCheckReturnValue.Completed"/> set to <c>false</c>.
        /// </remarks>
        private static ParseFieldCheckReturnValue ParseFieldCheck(string json)
        {
            int idx = json.IndexOf("\"fields\"", StringComparison.OrdinalIgnoreCase);
            if (idx < 0) return (false, 0, 0);

            var startArray = json.IndexOf('[', idx);
            if (startArray < 0) return (false, startArray, 0);

            var endArray = FindMatchingBracket(json, startArray, '[', ']');
            if (endArray < 0) return (false, startArray, endArray);

            return (true, startArray, endArray);
        }

        /// <summary>
        /// Parses a single JSON object match and, if valid, adds a corresponding
        /// <see cref="FieldModel"/> instance to the result list.
        /// </summary>
        /// <param name="result">
        /// The list that will receive the new <see cref="FieldModel"/> instance.
        /// </param>
        /// <param name="objMatch">
        /// A regular expression match representing a single JSON object within the fields array.
        /// </param>
        /// <remarks>
        /// <para>
        /// The method attempts to extract:
        /// </para>
        /// <list type="bullet">
        ///   <item><description><c>name</c> (required; entry is skipped if missing or empty)</description></item>
        ///   <item><description><c>kind</c> (optional; defaults to <c>"binary"</c>)</description></item>
        ///   <item><description><c>maxBytes</c> (optional; parsed as an <see cref="int"/> if present)</description></item>
        /// </list>
        /// Unknown properties are ignored.
        /// </remarks>
        private static void AddMatch(List<FieldModel> result, Match objMatch)
        {
            string obj = objMatch.Value;

            string? name = GetSingle(NameRx, obj);
            if (string.IsNullOrWhiteSpace(name)) return;

            string? kind = GetSingle(KindRx, obj) ?? "binary";
            int? maxBytes = null;

            var maxMatch = MaxBytesRx.Match(obj);
            if (maxMatch.Success && int.TryParse(maxMatch.Groups["v"].Value, out var parsed))
                maxBytes = parsed;

            result.Add(new FieldModel
            {
                Name = name!,
                Kind = kind!,
                MaxBytes = maxBytes
            });
        }

        /// <summary>
        /// Finds the index of the matching closing bracket for a given opening bracket in a string,
        /// taking into account nested pairs.
        /// </summary>
        /// <param name="text">The input text to scan.</param>
        /// <param name="start">
        /// Index of the opening bracket to start from. <paramref name="text"/>[<paramref name="start"/>]
        /// is expected to be <paramref name="open"/>.
        /// </param>
        /// <param name="open">Opening bracket character (e.g. <c>'['</c>).</param>
        /// <param name="close">Closing bracket character (e.g. <c>']'</c>).</param>
        /// <returns>
        /// The index of the matching closing bracket, or <c>-1</c> if no matching bracket is found.
        /// </returns>
        /// <remarks>
        /// This is a minimal depth counter for the very specific use of locating the end of the
        /// <c>"fields"</c> array; it does not handle strings or escaped characters.
        /// </remarks>
        private static int FindMatchingBracket(string text, int start, char open, char close)
        {
            int depth = 0;
            for (int i = start; i < text.Length; i++)
            {
                char c = text[i];
                if (c == open) depth++;
                else if (c == close)
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// Represents the result of checking a parsed field within an array segment.
        /// </summary>
        /// <param name="Completed">
        /// Indicates whether the parsing/check operation for the field has completed successfully.
        /// </param>
        /// <param name="StartArray">
        /// Zero-based index in the source array where the field segment begins (inclusive).
        /// </param>
        /// <param name="EndArray">
        /// Zero-based index in the source array where the field segment ends (exclusive).
        /// </param>
        private record struct ParseFieldCheckReturnValue(bool Completed, int StartArray, int EndArray)
            : IEquatable<ParseFieldCheckReturnValue>
        {
            /// <summary>
            /// Gets the length of the array segment covered by this result.
            /// </summary>
            /// <remarks>
            /// Computed as <c>EndArray - StartArray</c>.
            /// </remarks>
            public readonly int Length => EndArray - StartArray;

            /// <summary>
            /// Implicitly converts a <see cref="ParseFieldCheckReturnValue"/> instance to a tuple
            /// containing the completion flag and the array bounds.
            /// </summary>
            /// <param name="value">
            /// The <see cref="ParseFieldCheckReturnValue"/> to convert.
            /// </param>
            /// <returns>
            /// A tuple in the form <c>(completed, startArray, endArray)</c>.
            /// </returns>
            public static implicit operator (bool completed, int startArray, int endArray)(ParseFieldCheckReturnValue value)
                => (value.Completed, value.StartArray, value.EndArray);

            /// <summary>
            /// Implicitly converts a tuple containing a completion flag and array bounds
            /// into a new <see cref="ParseFieldCheckReturnValue"/> instance.
            /// </summary>
            /// <param name="value">
            /// The tuple in the form <c>(completed, startArray, endArray)</c> to convert.
            /// </param>
            /// <returns>
            /// A new <see cref="ParseFieldCheckReturnValue"/> instance populated from the tuple.
            /// </returns>
            public static implicit operator ParseFieldCheckReturnValue((bool completed, int startArray, int endArray) value)
                => new(value.completed, value.startArray, value.endArray);
        }

    }
}
