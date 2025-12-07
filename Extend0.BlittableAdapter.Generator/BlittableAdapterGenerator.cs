using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;
using System.Text;

namespace Extend0.BlittableAdapter.Generator
{
    /// <summary>
    /// Source generator that produces strongly-typed blittable structs from JSON definition files (<c>*.blit.json</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each <c>.blit.json</c> file is deserialized into a <see cref="DefinitionModel"/> and used to emit
    /// a single <c>unsafe struct</c> with sequential layout:
    /// </para>
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       A common header with <c>Guid Id</c>, <c>CreatedUtcTicks</c> and <c>OffsetMinutes</c>,
    ///       suitable for storage in a <see cref="Extend0.Metadata.MetadataTable"/> value cell.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       For each slot, a length prefix (<c>ushort XxxLen</c>) and an inline fixed buffer
    ///       (<c>fixed byte Xxx[MAX]</c>) for <c>utf8</c> / <c>binary</c> kinds.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       Span-based helpers <c>TrySetXxx</c> / <c>TryGetXxx</c>, plus string helpers for <c>utf8</c> fields.
    ///     </description>
    ///   </item>
    /// </list>
    /// <para>
    /// The generated struct is intended to be used with <c>TableSpec.Column&lt;T&gt;</c>,
    /// so that <c>ValueSize == sizeof(T)</c> for the owning column.
    /// </para>
    /// </remarks>
    [Generator]
    public sealed class BlittableAdapterGenerator : IIncrementalGenerator
    {
        /// <summary>
        /// Configures the incremental generator pipeline.
        /// </summary>
        /// <param name="context">
        /// The initialization context used to register inputs and outputs for this generator.
        /// </param>
        /// <remarks>
        /// This method:
        /// <list type="number">
        ///   <item>
        ///     <description>
        ///       Filters <see cref="AdditionalText"/> inputs to those whose path ends with <c>.blit.json</c>.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       Deserializes each JSON file into a <see cref="DefinitionModel"/> using <see cref="SimpleBlitParser"/>.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       For each valid model, generates a blittable struct source file via <see cref="GenerateAdapter(DefinitionModel,string)"/>.
        ///     </description>
        ///   </item>
        /// </list>
        /// Invalid or unparsable JSON files are ignored silently by this generator.
        /// </remarks>
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            // Generator assembly version (e.g. 1.0.0.0)
            var generatorVersion =
                typeof(BlittableAdapterGenerator).Assembly
                    .GetName()
                    .Version?
                    .ToString() ?? "0.0.0.0";

            var jsonFiles = context.AdditionalTextsProvider
                .Where(a => a.Path.EndsWith(".blit.json", StringComparison.OrdinalIgnoreCase));

            var models = jsonFiles.Select((text, ct) =>
            {
                var sourceText = text.GetText(ct);
                if (sourceText is null)
                    return default;

                var json = sourceText.ToString();
                return SimpleBlitParser.TryParse(json);
            }).Where(m => m is not null)!;

            context.RegisterSourceOutput(models, (spc, model) =>
            {
                var m = model!;
                var hintName = $"{m.RecordName}.g.cs";
                var source = GenerateAdapter(m, generatorVersion);
                spc.AddSource(hintName, SourceText.From(source, Encoding.UTF8));
            });
        }

        /// <summary>
        /// Generates the C# source code for a blittable struct from the given definition.
        /// </summary>
        /// <param name="def">
        /// The definition model describing the struct to generate, including namespace, record
        /// name and slot metadata.
        /// </param>
        /// <param name="generatorVersion">Generator assembly version string.</param>
        /// <returns>
        /// A C# source file containing an <c>unsafe struct</c> with inline buffers and helper
        /// methods for each configured slot.
        /// </returns>
        /// <remarks>
        /// The generated struct:
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///       Declares a fixed header with <c>Id</c>, <c>CreatedUtcTicks</c> and <c>OffsetMinutes</c>.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       Emits <c>ushort XxxLen</c> and <c>fixed byte Xxx[MAX]</c> for each <c>utf8</c> or <c>binary</c> slot,
        ///       where <c>MAX</c> comes from <c>slot.MaxBytes</c> (or a safe default if omitted).
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       Provides <c>TrySetXxx</c> / <c>TryGetXxx</c> methods over <c>ReadOnlySpan&lt;byte&gt;</c>, and
        ///       <c>TrySetXxxString</c> / <c>TryGetXxxString</c> helpers for <c>utf8</c> slots.
        ///     </description>
        ///   </item>
        /// </list>
        /// </remarks>
        private static string GenerateAdapter(DefinitionModel def, string generatorVersion)
        {
            var sb = new StringBuilder();

            AppendFilePreamble(sb, def, generatorVersion);

            var fields = def.Fields ?? [];

            AppendLengthPrefixes(sb, fields);
            AppendInlineBuffers(sb, fields);
            AppendDateTimeHelpers(sb, def.RecordName);
            AppendFieldHelpers(sb, fields);

            sb.AppendLine("    }");
            sb.AppendLine("}");

            return sb.ToString();
        }

        // ─────────────────────────────────────────────────────────────
        // Generation helpers (reduced verbosity, same logic)
        // ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Appends the file header, using directives and struct header (including the common metadata header fields).
        /// </summary>
        /// <param name="sb">The target <see cref="StringBuilder"/>.</param>
        /// <param name="def">The definition model describing the adapter namespace and record name.</param>
        /// <param name="generatorVersion">Generator assembly version string.</param>
        private static void AppendFilePreamble(StringBuilder sb, DefinitionModel def, string generatorVersion)
        {
            sb.AppendLine($$"""
// <auto-generated by Extend0.BlittableAdapter.Generator v{{generatorVersion}}/>
#nullable enable
using System;
using System.Text;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace {{def.AdapterNamespace}}
{
    [SkipLocalsInit]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public unsafe struct {{def.RecordName}}
    {
        // ---- Common header ----
        public Guid Id;
        public long CreatedUtcTicks;
        public int OffsetMinutes;

""");
        }

        /// <summary>
        /// Determines whether a field should be treated as part of the common header
        /// and therefore not emitted as a separate slot.
        /// </summary>
        /// <param name="field">The field model to inspect.</param>
        /// <param name="kindLower">The normalized (lowercase) kind of the field.</param>
        /// <returns><c>true</c> if the field belongs to the header; otherwise <c>false</c>.</returns>
        private static bool IsHeaderField(FieldModel field, string kindLower)
        {
            var name = field.Name;

            if (string.Equals(name, "Id", StringComparison.OrdinalIgnoreCase) &&
                kindLower == "guid")
                return true;

            if (string.Equals(name, "CreatedAt", StringComparison.OrdinalIgnoreCase) &&
                kindLower == "datetimeoffset")
                return true;

            return false;
        }

        /// <summary>
        /// Emits explicit <c>ushort XxxLen</c> length-prefix fields for UTF-8 and binary slots.
        /// </summary>
        /// <param name="sb">The target <see cref="StringBuilder"/>.</param>
        /// <param name="fields">The set of field definitions from the model.</param>
        private static void AppendLengthPrefixes(StringBuilder sb, FieldModel[] fields)
        {
            if (fields.Length == 0)
                return;

            sb.AppendLine("        // ---- Explicit length prefixes for variable fields ----");

            foreach (var field in fields)
            {
                var name = field.Name;
                var kindLower = (field.Kind ?? "").Trim().ToLowerInvariant();

                if (IsHeaderField(field, kindLower))
                    continue;

                if (kindLower is "utf8" or "binary")
                {
                    sb.AppendLine($$"""
        public ushort {{name}}Len;
""");
                }
            }

            sb.AppendLine();
        }

        /// <summary>
        /// Emits inline fixed-size buffers for UTF-8 and binary slots, together with their <c>MAX</c> constants.
        /// </summary>
        /// <param name="sb">The target <see cref="StringBuilder"/>.</param>
        /// <param name="fields">The set of field definitions from the model.</param>
        private static void AppendInlineBuffers(StringBuilder sb, FieldModel[] fields)
        {
            if (fields.Length == 0)
                return;

            sb.AppendLine("        // ---- Inline UTF-8 / binary buffers (fixed capacity) ----");

            foreach (var field in fields)
            {
                var name = field.Name;
                var kindLower = (field.Kind ?? "").Trim().ToLowerInvariant();

                if (IsHeaderField(field, kindLower))
                    continue;

                if (kindLower is not ("utf8" or "binary"))
                    continue;

                var constName = name.ToUpperInvariant() + "_MAX";
                var max = field.MaxBytes.GetValueOrDefault(256);

                sb.AppendLine($$"""
        public const int {{constName}} = {{max}};
        public fixed byte {{name}}[{{constName}}];

""");
            }
        }

        /// <summary>
        /// Emits helpers that operate on the common <c>DateTimeOffset</c>-based header
        /// (<c>Id</c>, <c>CreatedUtcTicks</c>, <c>OffsetMinutes</c>).
        /// </summary>
        /// <param name="sb">The target <see cref="StringBuilder"/>.</param>
        /// <param name="recordName">The generated struct name.</param>
        private static void AppendDateTimeHelpers(StringBuilder sb, string recordName)
        {
            sb.AppendLine($$"""
        // ---- Helpers: DateTimeOffset header ----
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static {{recordName}} Create(Guid id, DateTimeOffset created)
        {
            return new {{recordName}}
            {
                Id              = id,
                CreatedUtcTicks = created.UtcTicks,
                OffsetMinutes   = (int)created.Offset.TotalMinutes
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly DateTimeOffset GetCreatedAt()
            => new DateTimeOffset(CreatedUtcTicks, TimeSpan.FromMinutes(OffsetMinutes));

""");
        }

        /// <summary>
        /// Emits per-field helper methods (<c>TrySetXxx</c> / <c>TryGetXxx</c> and friends)
        /// for each slot defined in the model.
        /// </summary>
        /// <param name="sb">The target <see cref="StringBuilder"/>.</param>
        /// <param name="fields">The set of field definitions from the model.</param>
        private static void AppendFieldHelpers(StringBuilder sb, FieldModel[] fields)
        {
            if (fields.Length == 0)
                return;

            foreach (var field in fields)
            {
                var name = field.Name;
                var kindLower = (field.Kind ?? "").Trim().ToLowerInvariant();

                if (IsHeaderField(field, kindLower))
                    continue;

                if (kindLower is "utf8" or "binary")
                {
                    AppendUtf8OrBinaryField(sb, field, kindLower);
                }
                else
                {
                    sb.AppendLine($$"""
        // ---- {{name}} ({{field.Kind}}) not yet implemented as primitive ----

""");
                }
            }
        }

        /// <summary>
        /// Emits helper methods for a UTF-8 or binary field:
        /// <c>TrySetXxx</c> / <c>TryGetXxx</c> over spans, and for UTF-8 fields,
        /// string-based helpers as well.
        /// </summary>
        /// <param name="sb">The target <see cref="StringBuilder"/>.</param>
        /// <param name="field">The field definition.</param>
        /// <param name="kindLower">The normalized (lowercase) kind of the field.</param>
        private static void AppendUtf8OrBinaryField(StringBuilder sb, FieldModel field, string kindLower)
        {
            var name = field.Name;
            var constName = name.ToUpperInvariant() + "_MAX";

            sb.AppendLine($$"""
        // ---- {{name}} ({{field.Kind}}) ----
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySet{{name}}(ReadOnlySpan<byte> value)
        {
            if (value.Length > {{constName}}) return false;
            fixed (byte* p = {{name}})
            {
                var dst = new Span<byte>(p, {{constName}});
                dst.Clear();
                value.CopyTo(dst);
            }
            {{name}}Len = (ushort)value.Length;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet{{name}}(out ReadOnlySpan<byte> value)
        {
            if ({{name}}Len == 0)
            {
                value = default;
                return false;
            }
            fixed (byte* p = {{name}})
            {
                value = new ReadOnlySpan<byte>(p, {{name}}Len);
                return true;
            }
        }

""");

            if (kindLower == "utf8")
            {
                sb.AppendLine($$"""
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySet{{name}}String(string? value)
        {
            if (value is null)
            {
                {{name}}Len = 0;
                return true;
            }
            var bytes = Encoding.UTF8.GetBytes(value);
            return TrySet{{name}}(bytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet{{name}}String(out string? value)
        {
            if (!TryGet{{name}}(out var span))
            {
                value = null;
                return false;
            }
            value = Encoding.UTF8.GetString(span);
            return true;
        }

""");
            }
        }
    }
}
