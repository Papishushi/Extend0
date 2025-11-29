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
    /// a single <c>unsafe struct</c> with explicit layout:
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
        ///       Deserializes each JSON file into a <see cref="DefinitionModel"/> using <see cref="JsonSerializer"/>.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       For each valid model, generates a blittable struct source file via <see cref="GenerateAdapter(DefinitionModel)"/>.
        ///     </description>
        ///   </item>
        /// </list>
        /// Invalid or unparsable JSON files are ignored silently by this generator.
        /// </remarks>
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
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
                var source = GenerateAdapter(m);
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
        private static string GenerateAdapter(DefinitionModel def)
        {
            var sb = new StringBuilder();

            sb.AppendLine("// <auto-generated />");
            sb.AppendLine("#nullable enable");
            sb.AppendLine("using System;");
            sb.AppendLine("using System.Text;");
            sb.AppendLine("using System.Text.Json;");
            sb.AppendLine("using System.Runtime.CompilerServices;");
            sb.AppendLine("using System.Runtime.InteropServices;");
            sb.AppendLine();
            sb.Append("namespace ").Append(def.AdapterNamespace).AppendLine();
            sb.AppendLine("{");

            sb.Append("    [SkipLocalsInit]").AppendLine();
            sb.Append("    [StructLayout(LayoutKind.Sequential, Pack = 1)]").AppendLine();
            sb.Append("    public unsafe struct ").Append(def.RecordName).AppendLine();
            sb.AppendLine("    {");

            // ---- Common header ----
            sb.AppendLine("        // ---- Common header ----");
            sb.AppendLine("        public Guid Id;");
            sb.AppendLine("        public long CreatedUtcTicks;");
            sb.AppendLine("        public int OffsetMinutes;");
            sb.AppendLine();

            var fields = def.Fields ?? [];

            // ---- Length prefixes for variable fields ----
            if (fields.Length > 0)
            {
                sb.AppendLine("        // ---- Explicit length prefixes for variable fields ----");
                foreach (var field in fields)
                {
                    var name = field.Name;
                    var kind = (field.Kind ?? "").Trim().ToLowerInvariant();

                    // Id / CreatedAt van en el header: se ignoran aquí
                    if (string.Equals(name, "Id", StringComparison.OrdinalIgnoreCase) &&
                        kind == "guid")
                        continue;

                    if (string.Equals(name, "CreatedAt", StringComparison.OrdinalIgnoreCase) &&
                        kind == "datetimeoffset")
                        continue;

                    if (kind is "utf8" or "binary")
                    {
                        sb.Append("        public ushort ")
                          .Append(name)
                          .AppendLine("Len;");
                    }
                }
                sb.AppendLine();
            }

            // ---- Inline buffers ----
            if (fields.Length > 0)
            {
                sb.AppendLine("        // ---- Inline UTF-8 / binary buffers (fixed capacity) ----");
                foreach (var field in fields)
                {
                    var name = field.Name;
                    var kind = (field.Kind ?? "").Trim().ToLowerInvariant();

                    // header fields -> skip
                    if (string.Equals(name, "Id", StringComparison.OrdinalIgnoreCase) &&
                        kind == "guid")
                        continue;

                    if (string.Equals(name, "CreatedAt", StringComparison.OrdinalIgnoreCase) &&
                        kind == "datetimeoffset")
                        continue;

                    if (kind is not ("utf8" or "binary"))
                        continue;

                    var constName = name.ToUpperInvariant() + "_MAX";
                    var max = field.MaxBytes.GetValueOrDefault(256);

                    sb.Append("        public const int ")
                      .Append(constName)
                      .Append(" = ")
                      .Append(max)
                      .AppendLine(";");

                    sb.Append("        public fixed byte ")
                      .Append(name)
                      .Append('[')
                      .Append(constName)
                      .AppendLine("];");
                    sb.AppendLine();
                }
            }

            // ---- Helpers: DateTimeOffset header ----
            sb.AppendLine("        // ---- Helpers: DateTimeOffset header ----");
            sb.AppendLine("        [MethodImpl(MethodImplOptions.AggressiveInlining)]");
            sb.Append("        public static ")
              .Append(def.RecordName)
              .AppendLine(" Create(Guid id, DateTimeOffset created)");
            sb.AppendLine("        {");
            sb.Append("            return new ")
              .Append(def.RecordName)
              .AppendLine();
            sb.AppendLine("            {");
            sb.AppendLine("                Id = id,");
            sb.AppendLine("                CreatedUtcTicks = created.UtcTicks,");
            sb.AppendLine("                OffsetMinutes   = (int)created.Offset.TotalMinutes");
            sb.AppendLine("            };");
            sb.AppendLine("        }");
            sb.AppendLine();

            sb.AppendLine("        [MethodImpl(MethodImplOptions.AggressiveInlining)]");
            sb.AppendLine("        public readonly DateTimeOffset GetCreatedAt()");
            sb.AppendLine("            => new DateTimeOffset(CreatedUtcTicks, TimeSpan.FromMinutes(OffsetMinutes));");
            sb.AppendLine();

            // ---- Per-field helpers ----
            if (fields.Length > 0)
            {
                foreach (var field in fields)
                {
                    var name = field.Name;
                    var kind = (field.Kind ?? "").Trim().ToLowerInvariant();

                    // header fields -> sin helpers de momento
                    if (string.Equals(name, "Id", StringComparison.OrdinalIgnoreCase) &&
                        kind == "guid")
                        continue;

                    if (string.Equals(name, "CreatedAt", StringComparison.OrdinalIgnoreCase) &&
                        kind == "datetimeoffset")
                        continue;

                    if (kind is "utf8" or "binary")
                    {
                        var constName = name.ToUpperInvariant() + "_MAX";

                        sb.AppendLine($"        // ---- {name} ({field.Kind}) ----");

                        // TrySetXxx(ReadOnlySpan<byte>)
                        sb.AppendLine("        [MethodImpl(MethodImplOptions.AggressiveInlining)]");
                        sb.Append("        public bool TrySet")
                          .Append(name)
                          .AppendLine("(ReadOnlySpan<byte> value)");
                        sb.AppendLine("        {");
                        sb.Append("            if (value.Length > ")
                          .Append(constName)
                          .AppendLine(") return false;");
                        sb.Append("            fixed (byte* p = ")
                          .Append(name)
                          .AppendLine(")");
                        sb.AppendLine("            {");
                        sb.Append("                var dst = new Span<byte>(p, ")
                          .Append(constName)
                          .AppendLine(");");
                        sb.AppendLine("                dst.Clear();");
                        sb.AppendLine("                value.CopyTo(dst);");
                        sb.AppendLine("            }");
                        sb.Append("            ")
                          .Append(name)
                          .AppendLine("Len = (ushort)value.Length;");
                        sb.AppendLine("            return true;");
                        sb.AppendLine("        }");
                        sb.AppendLine();

                        // TryGetXxx(out ReadOnlySpan<byte>)
                        sb.AppendLine("        [MethodImpl(MethodImplOptions.AggressiveInlining)]");
                        sb.Append("        public bool TryGet")
                          .Append(name)
                          .AppendLine("(out ReadOnlySpan<byte> value)");
                        sb.AppendLine("        {");
                        sb.Append("            if (")
                          .Append(name)
                          .AppendLine("Len == 0)");
                        sb.AppendLine("            {");
                        sb.AppendLine("                value = default;");
                        sb.AppendLine("                return false;");
                        sb.AppendLine("            }");
                        sb.Append("            fixed (byte* p = ")
                          .Append(name)
                          .AppendLine(")");
                        sb.AppendLine("            {");
                        sb.Append("                value = new ReadOnlySpan<byte>(p, ")
                          .Append(name)
                          .AppendLine("Len);");
                        sb.AppendLine("                return true;");
                        sb.AppendLine("            }");
                        sb.AppendLine("        }");
                        sb.AppendLine();

                        if (kind == "utf8")
                        {
                            // TrySetXxxString(string?)
                            sb.AppendLine("        [MethodImpl(MethodImplOptions.AggressiveInlining)]");
                            sb.Append("        public bool TrySet")
                              .Append(name)
                              .AppendLine("String(string? value)");
                            sb.AppendLine("        {");
                            sb.AppendLine("            if (value is null)");
                            sb.AppendLine("            {");
                            sb.Append("                ")
                              .Append(name)
                              .AppendLine("Len = 0;");
                            sb.AppendLine("                return true;");
                            sb.AppendLine("            }");
                            sb.AppendLine("            var bytes = System.Text.Encoding.UTF8.GetBytes(value);");
                            sb.AppendLine("            return TrySet" + name + "(bytes);");
                            sb.AppendLine("        }");
                            sb.AppendLine();

                            // TryGetXxxString(out string?)
                            sb.AppendLine("        [MethodImpl(MethodImplOptions.AggressiveInlining)]");
                            sb.Append("        public bool TryGet")
                              .Append(name)
                              .AppendLine("String(out string? value)");
                            sb.AppendLine("        {");
                            sb.AppendLine("            if (!TryGet" + name + "(out var span))");
                            sb.AppendLine("            {");
                            sb.AppendLine("                value = null;");
                            sb.AppendLine("                return false;");
                            sb.AppendLine("            }");
                            sb.AppendLine("            value = System.Text.Encoding.UTF8.GetString(span);");
                            sb.AppendLine("            return true;");
                            sb.AppendLine("        }");
                            sb.AppendLine();
                        }
                    }
                    else
                    {
                        // Aquí más adelante puedes mapear "Int32", "Int64", etc. a campos primitivos.
                        sb.AppendLine($"        // ---- {name} ({field.Kind}) not yet implemented as primitive ----");
                        sb.AppendLine();
                    }
                }
            }

            sb.AppendLine("    }");
            sb.AppendLine("}");

            return sb.ToString();
        }

    }
}
