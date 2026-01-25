using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Text;

namespace Extend0.MetadataEntry.Generator
{
    /// <summary>
    /// Roslyn incremental generator that discovers <c>[GenerateMetadataEntry]</c> assembly
    /// attributes and emits fixed-size metadata entry structs plus a typed <c>MetadataCell</c> wrapper.
    /// </summary>
    [Generator]
    public sealed class MetadataEntryGenerator : IIncrementalGenerator
    {
        /// <summary>
        /// Fully-qualified metadata name of the <c>GenerateMetadataEntryAttribute</c> used
        /// to drive this generator.
        /// </summary>
        private const string AttrFullName = "Extend0.Metadata.CodeGen.GenerateMetadataEntryAttribute";

        /// <summary>
        /// Configures the incremental source generation pipeline:
        /// injects the attribute definition, collects all<c>[GenerateMetadataEntry]</c> usages,
        /// and produces the enum, entry structs and <c>MetadataCell</c> wrapper.
        /// </summary>
        /// <param name="context">
        /// The incremental generator initialization context used to register inputs and outputs.
        /// </param>
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            // Generator assembly version (e.g. 1.0.0.0)
            var generatorVersion =
                typeof(MetadataEntryGenerator).Assembly
                    .GetName()
                    .Version?
                    .ToString() ?? "0.0.0.0";

            // Inject the attribute definition into the compilation
            context.RegisterPostInitializationOutput(ctx =>
            {
                ctx.AddSource(
                    "GenerateMetadataEntryAttribute.g.cs",
                    SourceText.From(GetAttributeSource(generatorVersion), Encoding.UTF8));
            });

            // Capture [assembly: GenerateMetadataEntry(key, value)] usages
            IncrementalValueProvider<System.Collections.Immutable.ImmutableArray<KeyValuePair<int, int>>> attrPairs =
                CaptureGenerateMetadataEntryAttributes(context);

            context.RegisterSourceOutput(
                attrPairs,
                (spc, entries) => RegisterSourceOutputAction(spc, entries, generatorVersion));
        }

        /// <summary>
        /// Handles the final source emission step for the generator:
        /// given the collected (keySize, valueSize) pairs, emits the
        /// <c>MetadataEntrySize</c> enum + extensions, the concrete
        /// <c>MetadataEntry&lt;KxV&gt;</c> structs, and the <c>MetadataCell</c> wrapper.
        /// </summary>
        /// <param name="spc">The source production context provided by Roslyn.</param>
        /// <param name="entries">
        /// The collected list of (keySize, valueSize) pairs obtained from
        /// <c>[GenerateMetadataEntry]</c> attributes.
        /// </param>
        /// <param name="generatorVersion">
        /// Version string of the generator assembly used to annotate generated files.
        /// </param>
        private static void RegisterSourceOutputAction(
            SourceProductionContext spc,
            System.Collections.Immutable.ImmutableArray<KeyValuePair<int, int>> entries,
            string generatorVersion)
        {
            var uniq = entries.Distinct().OrderBy(x => x.Key).ThenBy(x => x.Value).ToArray();
            if (uniq.Length == 0) return;

            // 0) Emit the enum and extension methods based on detected variants
            var enumAndExt = EmitEnumAndExtensions(uniq, generatorVersion);
            spc.AddSource("MetadataEntrySize.g.cs", SourceText.From(enumAndExt, Encoding.UTF8));

            // 1) Emit each MetadataEntry{K}x{V} struct
            foreach (var pair in uniq)
            {
                var key = pair.Key;
                var value = pair.Value;
                var code = EmitVariant(key, value, generatorVersion);
                spc.AddSource($"MetadataEntry_{key}x{value}.g.cs", SourceText.From(code, Encoding.UTF8));
            }

            // 2) Emit MetadataCell with a switch covering only generated variants
            var cell = EmitMetadataCell(uniq, generatorVersion);
            spc.AddSource("MetadataCell.g.cs", SourceText.From(cell, Encoding.UTF8));
        }

        /// <summary>
        /// Configures the incremental pipeline that discovers all
        /// <c>[GenerateMetadataEntry]</c> attribute usages at the assembly level
        /// and collects their (keySize, valueSize) arguments.
        /// </summary>
        /// <param name="context">The incremental generator initialization context.</param>
        /// <returns>
        /// An <see cref="IncrementalValueProvider{T}"/> that yields an immutable array of
        /// (keySize, valueSize) pairs to be consumed at source output time.
        /// </returns>
        private static IncrementalValueProvider<System.Collections.Immutable.ImmutableArray<KeyValuePair<int, int>>> CaptureGenerateMetadataEntryAttributes(
            IncrementalGeneratorInitializationContext context)
        {
            var attrPairs = context.SyntaxProvider
                .CreateSyntaxProvider(
                    predicate: static (node, _) => node is AttributeSyntax,
                    transform: CaptureGenerateMetadataEntryAttributesTransform)
                .Where(static x => x.HasValue)
                .Select(static (x, _) => x!.Value)
                .Collect();

            return attrPairs;
        }

        /// <summary>
        /// Per-attribute transform used by the syntax provider to extract
        /// the constant (keySize, valueSize) arguments from
        /// <c>[GenerateMetadataEntry]</c> attribute applications.
        /// </summary>
        /// <param name="ctx">
        /// The generator syntax context for the current <see cref="AttributeSyntax"/> node.
        /// </param>
        /// <param name="_">
        /// Cancellation token (currently unused).
        /// </param>
        /// <returns>
        /// A tuple <c>(keySize, valueSize)</c> when the attribute is a valid
        /// <c>GenerateMetadataEntryAttribute</c> with constant arguments; otherwise <see langword="null"/>.
        /// </returns>
        private static KeyValuePair<int, int>? CaptureGenerateMetadataEntryAttributesTransform(GeneratorSyntaxContext ctx, CancellationToken _)
        {
            var attr = (AttributeSyntax)ctx.Node;

            // We only care about the GenerateMetadataEntryAttribute constructor
            if (!SkipNonRelevantSymbols(ctx, attr)) return null;

            var args = attr.ArgumentList?.Arguments;
            if (args is null || args.Value.Count < 2)
                return null;

            var keyConst = ctx.SemanticModel.GetConstantValue(args.Value[0].Expression);
            var valConst = ctx.SemanticModel.GetConstantValue(args.Value[1].Expression);
            if (!keyConst.HasValue || !valConst.HasValue)
                return null;

            return new((int)keyConst.Value!, (int)valConst.Value!);
        }

        /// <summary>
        /// Filters out attribute nodes whose symbol does not correspond to
        /// <c>GenerateMetadataEntryAttribute</c>.
        /// </summary>
        /// <param name="ctx">
        /// The generator syntax context providing the semantic model.
        /// </param>
        /// <param name="attr">
        /// The <see cref="AttributeSyntax"/> node being inspected.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the attribute resolves to the
        /// <c>GenerateMetadataEntryAttribute</c> constructor; otherwise <see langword="false"/>.
        /// </returns>
        private static bool SkipNonRelevantSymbols(GeneratorSyntaxContext ctx, AttributeSyntax attr)
        {
            var sym = ctx.SemanticModel.GetSymbolInfo(attr).Symbol as IMethodSymbol;
            return sym?.ContainingType?.ToDisplayString() == AttrFullName;
        }

        // ─────────────────────────────────────────────────────────────
        // VARIANT STRUCT
        // ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Generates the source code for a single fixed-size
        /// <c>MetadataEntry&lt;key x value&gt;</c> struct variant.
        /// </summary>
        /// <param name="key">Maximum UTF-8 byte capacity of the key segment.</param>
        /// <param name="val">Maximum UTF-8 byte capacity of the value segment.</param>
        /// <param name="generatorVersion">Generator assembly version string.</param>
        private static string EmitVariant(int key, int val, string generatorVersion)
        {
            var name = $"MetadataEntry{key}x{val}";

            return $$"""
// <auto-generated by Extend0.MetadataEntry.Generator v{{generatorVersion}}/>
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using Extend0.Metadata.Contract;

namespace Extend0.Metadata.CodeGen
{
    /// <summary>
    /// A fixed-size metadata entry storing a UTF-8 key/value pair inline in unmanaged memory.
    /// </summary>
    [SkipLocalsInit]
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = KEY_SIZE + VALUE_SIZE)]
    public unsafe struct {{name}} : IMetadataEntry
    {
        /// <summary>
        /// Maximum number of bytes reserved for the UTF-8 key segment.
        /// </summary>
        public const int KEY_SIZE   = {{key}};

        /// <summary>
        /// Maximum number of bytes reserved for the UTF-8 value segment.
        /// </summary>
        public const int VALUE_SIZE = {{val}};

        /// <summary>
        /// Maximum key capacity in bytes.
        /// </summary>
        public static int KeyCapacity   => KEY_SIZE;

        /// <summary>
        /// Maximum value capacity in bytes.
        /// </summary>
        public static int ValueCapacity => VALUE_SIZE;

        /// <summary>
        /// Inline UTF-8 key buffer. Zero-terminated if shorter than <see cref="KEY_SIZE"/>.
        /// </summary>
        public fixed byte Key[KEY_SIZE];

        /// <summary>
        /// Inline UTF-8 value buffer. Zero-terminated if shorter than <see cref="VALUE_SIZE"/>.
        /// </summary>
        public fixed byte Value[VALUE_SIZE];

        /// <summary>
        /// Initializes a new entry with all bytes set to zero.
        /// </summary>
        public {{name}}()
        {
            Unsafe.InitBlock(ref Key[0],   0, KEY_SIZE);
            Unsafe.InitBlock(ref Value[0], 0, VALUE_SIZE);
        }

        /// <summary>
        /// Attempts to set the key from a UTF-8 byte span.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 encoded key bytes.</param>
        /// <returns><c>true</c> if the key fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetKey(ReadOnlySpan<byte> keyUtf8)
        {
            if (keyUtf8.Length >= KEY_SIZE) return false;
            fixed (byte* pKey = Key)
            {
                var dst = new Span<byte>(pKey, KEY_SIZE);
                dst.Clear();
                keyUtf8.CopyTo(dst);
                return true;
            }
        }

        /// <summary>
        /// Attempts to set the value from a UTF-8 byte span.
        /// </summary>
        /// <param name="valueUtf8">UTF-8 encoded value bytes.</param>
        /// <returns><c>true</c> if the value fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValue(ReadOnlySpan<byte> valueUtf8)
        {
            if (valueUtf8.Length >= VALUE_SIZE) return false;
            fixed (byte* pVal = Value)
            {
                var dst = new Span<byte>(pVal, VALUE_SIZE);
                dst.Clear();
                valueUtf8.CopyTo(dst);
                return true;
            }
        }

        /// <summary>
        /// Compares the key stored at <paramref name="entry"/> with the provided UTF-8 bytes.
        /// </summary>
        /// <param name="entry">Pointer to a <see cref="{{name}}"/> instance.</param>
        /// <param name="keyUtf8">UTF-8 encoded key bytes.</param>
        /// <returns><c>true</c> if keys are equal; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool KeyEquals(void* entry, ReadOnlySpan<byte> keyUtf8)
        {
            if (keyUtf8.Length >= KEY_SIZE) return false;
            byte* pKey = (byte*)entry;
            var stored = new ReadOnlySpan<byte>(pKey, KEY_SIZE);

            int storedLen = stored.IndexOf((byte)0);
            if (storedLen < 0) storedLen = KEY_SIZE;

            return storedLen == keyUtf8.Length
                && stored[..storedLen].SequenceEqual(keyUtf8);
        }

        /// <summary>
        /// Attempts to set the key from a managed <see cref="string"/> encoded as UTF-8.
        /// </summary>
        /// <param name="key">Managed key string.</param>
        /// <returns><c>true</c> if the key fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetKey(string key)
        {
            int n = Encoding.UTF8.GetByteCount(key);
            if (n >= KEY_SIZE) return false;
            Span<byte> tmp = n <= 256 ? stackalloc byte[n] : new byte[n];
            Encoding.UTF8.GetBytes(key, tmp);
            return TrySetKey(tmp);
        }

        /// <summary>
        /// Attempts to set the value from a managed <see cref="string"/> encoded as UTF-8.
        /// </summary>
        /// <param name="str">Managed value string.</param>
        /// <returns><c>true</c> if the value fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValue(string str)
        {
            int n = Encoding.UTF8.GetByteCount(str);
            if (n >= VALUE_SIZE) return false;
            Span<byte> tmp = n <= 512 ? stackalloc byte[n] : new byte[n];
            Encoding.UTF8.GetBytes(str, tmp);
            return TrySetValue(tmp);
        }

        /// <summary>
        /// Compares the key stored at <paramref name="entry"/> with the provided managed string.
        /// </summary>
        /// <param name="entry">Pointer to a <see cref="{{name}}"/> instance.</param>
        /// <param name="key">Managed key string.</param>
        /// <returns><c>true</c> if keys are equal; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool KeyEquals(void* entry, string key)
        {
            int n = Encoding.UTF8.GetByteCount(key);
            if (n >= KEY_SIZE) return false;
            Span<byte> tmp = n <= 256 ? stackalloc byte[n] : new byte[n];
            Encoding.UTF8.GetBytes(key, tmp);
            return KeyEquals(entry, (ReadOnlySpan<byte>)tmp);
        }

        /// <summary>
        /// Gets a pointer to the start of the value segment for an entry.
        /// </summary>
        /// <param name="entry">Pointer to a <see cref="{{name}}"/> instance.</param>
        /// <returns>Pointer to the first value byte.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte* GetValuePointer(void* entry)
            => (byte*)entry + KEY_SIZE;
    }
}
""";
        }

        // ─────────────────────────────────────────────────────────────
        // METADATACELL
        // ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Generates the source code for the <c>MetadataCell</c> wrapper.
        /// </summary>
        /// <param name="uniq">Set of unique (keySize, valueSize) pairs backing the supported variants.</param>
        /// <param name="generatorVersion">Generator assembly version string.</param>
        private static string EmitMetadataCell(KeyValuePair<int, int>[] uniq, string generatorVersion)
        {
            var arms = BuildMetadataCellSwitchArms(uniq);

            return $$"""
// <auto-generated by Extend0.MetadataEntry.Generator v{{generatorVersion}}/>
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

#nullable enable

namespace Extend0.Metadata.CodeGen
{
    /// <summary>
    /// Typed wrapper around a single fixed-size metadata entry allocated in unmanaged memory.
    /// </summary>
    public readonly unsafe partial struct MetadataCell : IDisposable
    {
        private readonly MetadataEntrySize _size;
        private readonly void* _entry;
        private readonly bool _owns;

        private static int   KeyCap(MetadataEntrySize s)   => s.GetKeySize();
        private static int   ValueCap(MetadataEntrySize s) => s.GetValueSize();
        private static byte* ValuePtr(void* entry, MetadataEntrySize s) => (byte*)entry + KeyCap(s);

        /// <summary>
        /// Creates a new <see cref="MetadataCell"/> over an existing unmanaged pointer.
        /// </summary>
        /// <param name="size">Entry size descriptor.</param>
        /// <param name="ptr">Pointer to the entry memory.</param>
        /// <param name="owns">
        /// If <c>true</c>, this cell will free the underlying memory on <see cref="Dispose"/>.
        /// </param>
        private MetadataCell(MetadataEntrySize size, void* ptr, bool owns)
        {
            _size  = size;
            _entry = ptr;
            _owns  = owns;
        }

        /// <summary>
        /// Allocates a new unmanaged entry for the given size variant and wraps it in a <see cref="MetadataCell"/>.
        /// </summary>
        /// <param name="size">Entry size variant to allocate.</param>
        public MetadataCell(MetadataEntrySize size)
        {
            _size = size;
            _owns = true;
            _entry = size switch
            {
{{arms}}
                _ => throw new ArgumentOutOfRangeException(nameof(size), size, "Unsupported MetadataEntrySize"),
            };
        }

        /// <summary>
        /// Attempts to set the key from a UTF-8 byte span.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 encoded key bytes.</param>
        /// <returns><c>true</c> if the key fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetKey(ReadOnlySpan<byte> keyUtf8)
        {
            int cap = KeyCap(_size);
            if (keyUtf8.Length >= cap) return false;
            var dst = new Span<byte>(_entry, cap);
            dst.Clear();
            keyUtf8.CopyTo(dst);
            return true;
        }

        /// <summary>
        /// Attempts to set the value from a UTF-8 byte span.
        /// </summary>
        /// <param name="valueUtf8">UTF-8 encoded value bytes.</param>
        /// <returns><c>true</c> if the value fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValue(ReadOnlySpan<byte> valueUtf8)
        {
            int cap = ValueCap(_size);
            if (valueUtf8.Length >= cap) return false;
            var dst = new Span<byte>(ValuePtr(_entry, _size), cap);
            dst.Clear();
            valueUtf8.CopyTo(dst);
            return true;
        }

        /// <summary>
        /// Attempts to set the key from a managed <see cref="string"/> encoded as UTF-8.
        /// </summary>
        /// <param name="key">Managed key string.</param>
        /// <returns><c>true</c> if the key fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetKey(string key)
        {
            int n = Encoding.UTF8.GetByteCount(key);
            if (n >= KeyCap(_size)) return false;
            Span<byte> tmp = n <= 256 ? stackalloc byte[n] : new byte[n];
            Encoding.UTF8.GetBytes(key, tmp);
            return TrySetKey(tmp);
        }

        /// <summary>
        /// Attempts to set the value from a managed <see cref="string"/> encoded as UTF-8.
        /// </summary>
        /// <param name="value">Managed value string.</param>
        /// <returns><c>true</c> if the value fits; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValue(string value)
        {
            int n = Encoding.UTF8.GetByteCount(value);
            if (n >= ValueCap(_size)) return false;
            Span<byte> tmp = n <= 512 ? stackalloc byte[n] : new byte[n];
            Encoding.UTF8.GetBytes(value, tmp);
            return TrySetValue(tmp);
        }

        /// <summary>
        /// Attempts to retrieve the stored key as a UTF-8 byte span.
        /// </summary>
        /// <param name="keyUtf8">
        /// When this method returns <see langword="true"/>, contains a slice of the key slot
        /// representing the stored key (excluding the terminating <c>NUL</c> byte). When this
        /// method returns <see langword="false"/>, the value is <see langword="default"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the cell contains a valid, non-empty <c>NUL</c>-terminated key;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Keys are stored in a fixed-size key slot and are expected to be <c>NUL</c>-terminated.
        /// The key length is determined by the first <c>0</c> byte.
        /// </para>
        /// <para>
        /// If the key slot does not contain any <c>NUL</c> terminator (<c>IndexOf(0) == -1</c>),
        /// the slot is treated as uninitialized/invalid and the method returns <see langword="false"/>.
        /// This prevents interpreting random memory as a full-length key when the underlying storage
        /// is not guaranteed to be zero-initialized.
        /// </para>
        /// <para>
        /// Empty keys (<c>IndexOf(0) == 0</c>) are considered not present and also return
        /// <see langword="false"/>.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetKeyUtf8(out ReadOnlySpan<byte> keyUtf8)
        {
            int cap = KeyCap(_size);
            var stored = new ReadOnlySpan<byte>(_entry, cap);
        
            int len = stored.IndexOf((byte)0);
            if (len <= 0) // 0 => empty, -1 => invalid/uninitialized
            {
                keyUtf8 = default;
                return false;
            }
        
            keyUtf8 = stored[..len];
            return true;
        }

        /// <summary>
        /// Attempts to retrieve the stored key as a managed <see cref="string"/> (UTF-8).
        /// </summary>
        /// <param name="key">
        /// When this method returns <see langword="true"/>, receives the decoded key string.
        /// When this method returns <see langword="false"/>, the value is <see langword="null"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the entry contains a valid, non-empty <c>NUL</c>-terminated key; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This overload delegates to <see cref="TryGetKeyUtf8(out ReadOnlySpan{byte})"/> and decodes the returned UTF-8
        /// bytes into a managed string.
        /// </para>
        /// <para>
        /// If the underlying bytes are not valid UTF-8, the decoder will replace invalid sequences according to the
        /// runtime's UTF-8 decoding behavior.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetKeyUtf8(out string? key)
        {
            if (TryGetKeyUtf8(out ReadOnlySpan<byte> k))
            {
                key = Encoding.UTF8.GetString(k);
                return true;
            }
            key = null;
            return false;
        }
        
        /// <summary>
        /// Attempts to retrieve the stored key as a fixed-size raw byte span (binary-safe).
        /// </summary>
        /// <param name="keyRaw">
        /// When this method returns <see langword="true"/>, receives a span over the entire key slot.
        /// The span length is exactly <see cref="KeySize"/> bytes and it may contain zeros.
        /// When this method returns <see langword="false"/>, the value is <see langword="default"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if this entry has a key segment (<see cref="KeySize"/> &gt; 0); otherwise,
        /// <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method does not validate key presence or terminators. It simply exposes the raw fixed-size key slot.
        /// </para>
        /// <para>
        /// The returned span is a view over unmanaged/mapped memory. It is only valid while the underlying storage remains
        /// alive and unmoved (e.g., until the owning store is disposed or remapped).
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetKeyRaw(out ReadOnlySpan<byte> keyRaw)
        {
            int cap = KeyCap(_size);
            if (cap == 0)
            {
                keyRaw = default;
                return false;
            }
        
            keyRaw = new ReadOnlySpan<byte>(_entry, cap);
            return true;
        }
        
        /// <summary>
        /// Returns <see langword="true"/> if the key slot exists and contains at least one non-zero byte (binary-safe).
        /// </summary>
        /// <returns>
        /// <see langword="true"/> if the entry has a key segment and it is not all zeros; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This is a binary-safe presence check suitable for fixed-size binary keys (for example, GUID keys stored as 16 bytes).
        /// </para>
        /// <para>
        /// For text keys, prefer <see cref="TryGetKeyUtf8(out ReadOnlySpan{byte})"/> / <see cref="KeyEquals(ReadOnlySpan{byte})"/>
        /// which validate a non-empty <c>NUL</c>-terminated UTF-8 key.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasKeyRaw()
        {
            if (!TryGetKeyRaw(out var raw))
                return false;
        
            for (int i = 0; i < raw.Length; i++)
                if (raw[i] != 0)
                    return true;
        
            return false;
        }
        
        /// <summary>
        /// Compares the entry's raw fixed-size key slot against the provided raw key bytes (binary-safe).
        /// </summary>
        /// <param name="keyRaw">Raw key bytes to compare with the stored key slot.</param>
        /// <returns>
        /// <see langword="true"/> if this entry has a key segment and the stored key slot matches <paramref name="keyRaw"/>
        /// byte-for-byte; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This comparison requires <paramref name="keyRaw"/> to be exactly <see cref="KeySize"/> bytes long.
        /// If the length differs, the method returns <see langword="false"/>.
        /// </para>
        /// <para>
        /// This method performs a full-slot comparison and does not interpret terminators. It is intended for fixed-size
        /// binary keys.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool KeyEqualsRaw(ReadOnlySpan<byte> keyRaw)
        {
            if (!TryGetKeyRaw(out var stored))
                return false;
        
            if (keyRaw.Length != stored.Length)
                return false;
        
            return stored.SequenceEqual(keyRaw);
        }

        /// <summary>
        /// Attempts to retrieve the stored value as UTF-8 bytes for the specified UTF-8 key.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 encoded key bytes to match against the stored key.</param>
        /// <param name="valueUtf8">
        /// When this method returns <see langword="true"/>, receives a slice of the value slot interpreted as UTF-8 bytes.
        /// The returned span is trimmed at the first <c>NUL</c> byte, if present; otherwise the full value slot is returned.
        /// When this method returns <see langword="false"/>, the value is <see langword="default"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the stored key is present and matches <paramref name="keyUtf8"/>; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method first validates and compares the stored key (it must be non-empty and <c>NUL</c>-terminated) and only
        /// then exposes the value. No additional "value exists" check is performed: the value slot is fixed-size.
        /// </para>
        /// <para>
        /// If the underlying storage is not zero-initialized, an entry that was never written may contain arbitrary bytes.
        /// In that scenario, the returned data may be meaningless unless an explicit occupancy marker is used.
        /// </para>
        /// <para>
        /// The returned span is a view over unmanaged/mapped memory. It is only valid while the underlying storage remains
        /// alive and unmoved (e.g., until the owning store is disposed or remapped).
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValueUtf8(ReadOnlySpan<byte> keyUtf8, out ReadOnlySpan<byte> valueUtf8)
        {
            if (!KeyEquals(keyUtf8))
            {
                valueUtf8 = default;
                return false;
            }
            int cap = ValueCap(_size);
            var stored = new ReadOnlySpan<byte>(ValuePtr(_entry, _size), cap);
            int len = stored.IndexOf((byte)0);
            valueUtf8 = len < 0 ? stored : stored[..len];
            return true;
        }
        
        /// <summary>
        /// Attempts to retrieve the stored value as a managed <see cref="string"/> for the specified key.
        /// </summary>
        /// <param name="key">Managed key string to match against the stored key.</param>
        /// <param name="value">
        /// When this method returns <see langword="true"/>, receives the decoded value string (UTF-8).
        /// When this method returns <see langword="false"/>, the value is <see langword="null"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the stored key is present and matches <paramref name="key"/>; otherwise,
        /// <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This overload encodes <paramref name="key"/> as UTF-8 and delegates to
        /// <see cref="TryGetValueUtf8(ReadOnlySpan{byte}, out ReadOnlySpan{byte})"/>.
        /// </para>
        /// <para>
        /// The value is decoded from UTF-8. If the underlying bytes are not valid UTF-8, the decoder will replace invalid
        /// sequences according to the runtime's UTF-8 decoding behavior.
        /// </para>
        /// </remarks>
        public bool TryGetValueUtf8(string key, out string? value)
        {
            int n = Encoding.UTF8.GetByteCount(key);
            Span<byte> tmp = n <= 256 ? stackalloc byte[n] : new byte[n];
            Encoding.UTF8.GetBytes(key, tmp);
            if (TryGetValueUtf8(tmp, out var v))
            {
                value = Encoding.UTF8.GetString(v);
                return true;
            }
            value = null;
            return false;
        }

        /// <summary>
        /// Returns the raw fixed-size value slot as a binary-safe byte span.
        /// </summary>
        /// <returns>
        /// A span over the entire value slot. Its length is exactly <see cref="ValueSize"/> bytes and it may contain zeros.
        /// If <see cref="ValueSize"/> is 0, returns an empty span.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method does not interpret the value (no UTF-8 trimming, no terminator search) and does not check the key.
        /// It simply exposes the underlying value bytes for the entry.
        /// </para>
        /// <para>
        /// If the underlying storage is not zero-initialized, the returned span may contain arbitrary bytes for entries
        /// that were never written. This API does not provide an occupancy signal.
        /// </para>
        /// <para>
        /// The returned span is a view over unmanaged/mapped memory. It is only valid while the underlying
        /// storage remains alive and unmoved (e.g., until the owning store is disposed or remapped).
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> GetValueRaw()
        {
            int cap = ValueCap(_size);
            return new ReadOnlySpan<byte>(ValuePtr(_entry, _size), cap);
        }
        
        /// <summary>
        /// Returns <see langword="true"/> if the raw value slot contains any non-zero byte.
        /// </summary>
        /// <returns>
        /// <see langword="true"/> if at least one byte in the value slot is non-zero; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This is a binary-safe heuristic that can be used as a "likely written" signal when the underlying storage is
        /// known to be zero-initialized for unwritten entries (for example, when the backing file was created with
        /// zeroed capacity or when growth uses <c>zeroInit</c>).
        /// </para>
        /// <para>
        /// If the underlying storage is not zero-initialized, unwritten entries may contain arbitrary bytes and this method
        /// may return <see langword="true"/> even though no value was ever written. In that scenario, this method must not be
        /// used to determine occupancy.
        /// </para>
        /// <para>
        /// Note: a legitimately stored value may also be all zeros (for example, an integer <c>0</c>), in which case this
        /// method returns <see langword="false"/> even though the value may be meaningful.
        /// </para>
        /// <para>
        /// If you need a definitive "written vs never written" signal regardless of initialization policy, store an explicit
        /// occupancy marker (for example, a per-entry flag byte or a bitmap per column).
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasAnyValueRaw()
        {
            var v = GetValueRaw();
            for (int i = 0; i < v.Length; i++)
                if (v[i] != 0) return true;
            return false;
        }

        /// <summary>
        /// Attempts to retrieve the raw fixed-size value slot (binary-safe).
        /// </summary>
        /// <param name="valueRaw">
        /// When this method returns <see langword="true"/>, receives a span over the entire value slot.
        /// The span length is exactly <see cref="ValueSize"/> bytes and may contain zeros.
        /// When this method returns <see langword="false"/>, the value is <see langword="default"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if this entry has a value segment (<see cref="ValueSize"/> &gt; 0);
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method does not interpret the value (no UTF-8 trimming, no terminator search) and does not
        /// check the key. It simply exposes the raw value bytes for the entry.
        /// </para>
        /// <para>
        /// If the underlying storage is not zero-initialized, the returned span may contain arbitrary bytes for entries
        /// that were never written. This API does not provide an occupancy signal.
        /// </para>
        /// <para>
        /// The returned span is a view over unmanaged/mapped memory. It is only valid while the underlying
        /// storage remains alive and unmoved (e.g., until the owning store is disposed or remapped).
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValueRaw(out ReadOnlySpan<byte> valueRaw)
        {
            int cap = ValueCap(_size);
            if (cap == 0) { valueRaw = default; return false; }
            valueRaw = new ReadOnlySpan<byte>(ValuePtr(_entry, _size), cap);
            return true;
        }

        /// <summary>
        /// Compares the cell's stored key against the provided UTF-8 key bytes.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 encoded key bytes to compare with the stored key.</param>
        /// <returns>
        /// <see langword="true"/> if the cell contains a valid, non-empty key and it matches
        /// <paramref name="keyUtf8"/> byte-for-byte; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Keys in a <see cref="MetadataCell"/> are stored in a fixed-size key slot and are
        /// expected to be <c>NUL</c>-terminated. A key is considered valid only when the slot
        /// contains a <c>0</c> byte terminator at some position &gt; 0.
        /// </para>
        /// <para>
        /// If the key slot contains no <c>NUL</c> terminator (<c>IndexOf(0) == -1</c>), this method
        /// treats the slot as uninitialized/invalid and returns <see langword="false"/>. This is
        /// critical for scenarios where the underlying storage may not be zero-initialized.
        /// </para>
        /// <para>
        /// This method rejects empty keys (<c>len == 0</c>) and keys that do not fit the slot
        /// (<paramref name="keyUtf8"/> length must be strictly less than the slot capacity).
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool KeyEquals(ReadOnlySpan<byte> keyUtf8)
        {
            int cap = KeyCap(_size);
            if (keyUtf8.Length >= cap) return false;
        
            var stored = new ReadOnlySpan<byte>(_entry, cap);
            int len = stored.IndexOf((byte)0);
            if (len < 0) return false; // no NUL => invalid/uninitialized
            if (len == 0) return false; // empty key
        
            return len == keyUtf8.Length && stored[..len].SequenceEqual(keyUtf8);
        }

        /// <summary>
        /// Returns a pointer to the start of the value segment.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetValuePointer() => ValuePtr(_entry, _size);

        /// <summary>
        /// Wraps an existing unmanaged pointer in a <see cref="MetadataCell"/>.
        /// </summary>
        /// <param name="size">Entry size variant.</param>
        /// <param name="ptr">Pointer to the entry memory.</param>
        /// <param name="owns">
        /// If <c>true</c>, the cell will free the underlying memory on <see cref="Dispose"/>.
        /// </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MetadataCell FromPointer(MetadataEntrySize size, void* ptr, bool owns)
            => new(size, ptr, owns);

        /// <summary>
        /// Maximum key capacity in bytes for the underlying entry.
        /// </summary>
        public int KeySize   => KeyCap(_size);

        /// <summary>
        /// Maximum value capacity in bytes for the underlying entry.
        /// </summary>
        public int ValueSize => ValueCap(_size);

        /// <summary>
        /// Releases the unmanaged memory associated with this cell if it owns it.
        /// </summary>
        public void Dispose()
        {
            if (_owns && _entry != null)
            {
                NativeMemory.Free(_entry);
            }
        }
    }
}
""";
        }

        private static string BuildMetadataCellSwitchArms(KeyValuePair<int, int>[] uniq)
        {
            var sb = new StringBuilder();
            foreach (var pair in uniq)
            {
                var k = pair.Key;
                var v = pair.Value;
                sb.AppendLine(
                    $"                MetadataEntrySize.Entry{k}x{v} => NativeMemory.AllocZeroed(1, (nuint)sizeof(MetadataEntry{k}x{v})),");

            }
            return sb.ToString().TrimEnd('\r', '\n');
        }

        // ─────────────────────────────────────────────────────────────
        // ATTRIBUTE SOURCE
        // ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Builds the embedded source for the <c>[GenerateMetadataEntry]</c> attribute that consumers
        /// apply at the assembly level to declare supported key/value size pairs.
        /// </summary>
        /// <param name="generatorVersion">Generator assembly version string.</param>
        private static string GetAttributeSource(string generatorVersion) => $$"""
// <auto-generated by Extend0.MetadataEntry.Generator v{{generatorVersion}}/>
using System;

namespace Extend0.Metadata.CodeGen
{
    /// <summary>
    /// Declares a fixed-size MetadataEntry struct variant, e.g. [assembly: GenerateMetadataEntry(64, 256)].
    /// </summary>
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
    internal sealed class GenerateMetadataEntryAttribute : Attribute
    {
        /// <summary>
        /// Maximum UTF-8 byte capacity for the key segment.
        /// </summary>
        public int KeySize { get; }

        /// <summary>
        /// Maximum UTF-8 byte capacity for the value segment.
        /// </summary>
        public int ValueSize { get; }

        /// <summary>
        /// Creates a new <see cref="GenerateMetadataEntryAttribute"/> with the given capacities.
        /// </summary>
        /// <param name="keySize">Maximum key capacity in bytes.</param>
        /// <param name="valueSize">Maximum value capacity in bytes.</param>
        public GenerateMetadataEntryAttribute(int keySize, int valueSize)
        {
            if (keySize <= 0) throw new ArgumentOutOfRangeException(nameof(keySize));
            if (valueSize <= 0) throw new ArgumentOutOfRangeException(nameof(valueSize));
            KeySize = keySize;
            ValueSize = valueSize;
        }
    }
}
""";

        // ─────────────────────────────────────────────────────────────
        // ENUM + EXTENSIONS
        // ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Generates the <c>MetadataEntrySize</c> enum and its extension methods
        /// based on the discovered (keySize, valueSize) combinations.
        /// </summary>
        /// <param name="uniq">
        /// Set of unique (keySize, valueSize) pairs collected from attribute usages.
        /// </param>
        /// <param name="generatorVersion">Generator assembly version string.</param>
        /// <returns>
        /// C# source code that defines the enum and its helpers.
        /// </returns>
        private static string EmitEnumAndExtensions(KeyValuePair<int, int>[] uniq, string generatorVersion)
        {
            var enumMembers = BuildEnumMembers(uniq);
            var packed = uniq.Select(p => (p.Key << 16) | p.Value).OrderBy(x => x).ToArray();
            var packedLiterals = string.Join(", ", packed.Select(x => $"0x{x:X8}"));

            var enumSource = $$"""
// <auto-generated by Extend0.MetadataEntry.Generator v{{generatorVersion}}/>
using System;
using System.Linq;

namespace Extend0.Metadata.CodeGen
{
    /// <summary>
    /// Describes supported MetadataEntry variants. The value is encoded as (KeySize << 16) | ValueSize.
    /// </summary>
    public enum MetadataEntrySize : int
    {
{{enumMembers}}
    }
}
""";

            var extSource = $$"""
// <auto-generated by Extend0.MetadataEntry.Generator v{{generatorVersion}}/>

namespace Extend0.Metadata.CodeGen
{
    /// <summary>
    /// Extension methods for <see cref="MetadataEntrySize"/> to pack/unpack sizes and validate variants.
    /// </summary>
    public static class MetadataEntrySizeExtensions
    {
        // Sorted array of supported packed variants (packed = (key << 16) | value)
        private static readonly int[] ValidPacked = new int[] { {{packedLiterals}} };

        /// <summary>
        /// Returns the key and value sizes unpacked from the enum.
        /// </summary>
        public static (int KeySize, int ValueSize) Decompose(this MetadataEntrySize kind)
            => (((int)kind >> 16) & 0xFFFF, (int)kind & 0xFFFF);

        /// <summary>
        /// Returns the key size (in bytes) for this variant.
        /// </summary>
        public static int GetKeySize(this MetadataEntrySize kind)   => ((int)kind >> 16) & 0xFFFF;

        /// <summary>
        /// Returns the value size (in bytes) for this variant.
        /// </summary>
        public static int GetValueSize(this MetadataEntrySize kind) => (int)kind        & 0xFFFF;

        /// <summary>
        /// Returns the fully-qualified type name of the generated struct for this variant.
        /// </summary>
        public static string GetGeneratedTypeName(this MetadataEntrySize kind)
        {
            var (k, v) = kind.Decompose();
            return $"Extend0.Metadata.CodeGen.MetadataEntry{k}x{v}";
        }

        /// <summary>
        /// Packs the enum value to an <see cref="int"/> without additional validation.
        /// </summary>
        public static int ToPacked(this MetadataEntrySize kind) => (int)kind;

        // ---------------- PACK/UNPACK helpers ----------------

        /// <summary>
        /// Packs (keySize, valueSize) into a <see cref="MetadataEntrySize"/> value without validation.
        /// </summary>
        public static MetadataEntrySize PackUnchecked(int keySize, int valueSize)
            => (MetadataEntrySize)(((keySize & 0xFFFF) << 16) | (valueSize & 0xFFFF));

        /// <summary>
        /// Attempts to pack (keySize, valueSize) into a <see cref="MetadataEntrySize"/> value
        /// only if the pair is supported.
        /// </summary>
        /// <param name="keySize">Key size in bytes.</param>
        /// <param name="valueSize">Value size in bytes.</param>
        /// <param name="kind">Resulting enum value if supported.</param>
        /// <returns><c>true</c> if the pair is supported; otherwise <c>false</c>.</returns>
        public static bool TryPack(int keySize, int valueSize, out MetadataEntrySize kind)
        {
            int packed = ((keySize & 0xFFFF) << 16) | (valueSize & 0xFFFF);
            bool ok = Array.BinarySearch(ValidPacked, packed) >= 0;
            kind = ok ? (MetadataEntrySize)packed : default;
            return ok;
        }

        /// <summary>
        /// Converts a packed <see cref="int"/> to a <see cref="MetadataEntrySize"/> without validation.
        /// </summary>
        public static MetadataEntrySize FromPackedUnchecked(int packed)
            => (MetadataEntrySize)packed;

        /// <summary>
        /// Attempts to convert a packed <see cref="int"/> to a <see cref="MetadataEntrySize"/>
        /// only if it matches a supported variant.
        /// </summary>
        /// <param name="packed">Packed size value.</param>
        /// <param name="kind">Resulting enum value if supported.</param>
        /// <returns><c>true</c> if the packed value corresponds to a supported variant; otherwise <c>false</c>.</returns>
        public static bool TryFromPacked(int packed, out MetadataEntrySize kind)
        {
            bool ok = Array.BinarySearch(ValidPacked, packed) >= 0;
            kind = ok ? (MetadataEntrySize)packed : default;
            return ok;
        }

        /// <summary>
        /// Returns <c>true</c> if the given (keySize, valueSize) pair is supported by the generated variants.
        /// </summary>
        public static bool IsSupported(int keySize, int valueSize)
        {
            int packed = ((keySize & 0xFFFF) << 16) | (valueSize & 0xFFFF);
            return Array.BinarySearch(ValidPacked, packed) >= 0;
        }
    }
}
""";

            return enumSource + extSource + "\n";
        }

        private static string BuildEnumMembers(KeyValuePair<int, int>[] uniq)
        {
            var sb = new StringBuilder();
            foreach (var pair in uniq)
            {
                var k = pair.Key;
                var v = pair.Value;
                var packed = (k << 16) | v;
                sb.AppendLine($"        Entry{k}x{v} = 0x{packed:X8},");
            }
            return sb.ToString().TrimEnd('\r', '\n');
        }
    }
}