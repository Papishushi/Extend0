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
        /// injects the attribute definition, collects all attribute usages, and
        /// produces the enum, entry structs and <c>MetadataCell</c> wrapper.
        /// </summary>
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
            var attrPairs = context.SyntaxProvider
                .CreateSyntaxProvider(
                    predicate: static (node, _) => node is AttributeSyntax,
                    transform: static (ctx, _) =>
                    {
                        var attr = (AttributeSyntax)ctx.Node;

                        // We only care about the GenerateMetadataEntryAttribute constructor
                        var sym = ctx.SemanticModel.GetSymbolInfo(attr).Symbol as IMethodSymbol;
                        if (sym?.ContainingType?.ToDisplayString() != AttrFullName)
                            return ((int key, int val)?)null;

                        var args = attr.ArgumentList?.Arguments;
                        if (args is null || args.Value.Count < 2)
                            return null;

                        var keyConst = ctx.SemanticModel.GetConstantValue(args.Value[0].Expression);
                        var valConst = ctx.SemanticModel.GetConstantValue(args.Value[1].Expression);
                        if (!keyConst.HasValue || !valConst.HasValue)
                            return null;

                        return ((int)keyConst.Value!, (int)valConst.Value!);
                    })
                .Where(static x => x.HasValue)
                .Select(static (x, _) => x!.Value)
                .Collect();

            context.RegisterSourceOutput(attrPairs, (spc, entries) =>
            {
                var uniq = entries.Distinct().OrderBy(x => x.key).ThenBy(x => x.val).ToArray();
                if (uniq.Length == 0) return;

                // 0) Emit the enum and extension methods based on detected variants
                var enumAndExt = EmitEnumAndExtensions(uniq, generatorVersion);
                spc.AddSource("MetadataEntrySize.g.cs", SourceText.From(enumAndExt, Encoding.UTF8));

                // 1) Emit each MetadataEntry{K}x{V} struct
                foreach (var (key, value) in uniq)
                {
                    var code = EmitVariant(key, value, generatorVersion);
                    spc.AddSource($"MetadataEntry_{key}x{value}.g.cs", SourceText.From(code, Encoding.UTF8));
                }

                // 2) Emit MetadataCell with a switch covering only generated variants
                var cell = EmitMetadataCell(uniq, generatorVersion);
                spc.AddSource("MetadataCell.g.cs", SourceText.From(cell, Encoding.UTF8));
            });
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
        private static string EmitMetadataCell((int key, int val)[] uniq, string generatorVersion)
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
        /// Attempts to get the stored key as a UTF-8 byte span.
        /// </summary>
        /// <param name="keyUtf8">Receives the key span if present.</param>
        /// <returns><c>true</c> if a non-empty key is stored; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetKey(out ReadOnlySpan<byte> keyUtf8)
        {
            int cap = KeyCap(_size);
            var stored = new ReadOnlySpan<byte>(_entry, cap);
            int len = stored.IndexOf((byte)0);
            if (len == 0)
            {
                keyUtf8 = default;
                return false;
            }
            keyUtf8 = len < 0 ? stored : stored[..len];
            return true;
        }

        /// <summary>
        /// Attempts to get the stored key as a managed <see cref="string"/>.
        /// </summary>
        /// <param name="key">Receives the key string if present.</param>
        /// <returns><c>true</c> if a non-empty key is stored; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetKey(out string? key)
        {
            if (TryGetKey(out ReadOnlySpan<byte> k))
            {
                key = Encoding.UTF8.GetString(k);
                return true;
            }
            key = null;
            return false;
        }

        /// <summary>
        /// Attempts to read the value associated with the given key bytes.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 encoded key bytes to match.</param>
        /// <param name="valueUtf8">Receives the value bytes if the key matches.</param>
        /// <returns><c>true</c> if the key matches and a value exists; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(ReadOnlySpan<byte> keyUtf8, out ReadOnlySpan<byte> valueUtf8)
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
        /// Attempts to read the value associated with the given managed key string.
        /// </summary>
        /// <param name="key">Managed key string.</param>
        /// <param name="value">Receives the value string if the key matches.</param>
        /// <returns><c>true</c> if the key matches and a value exists; otherwise <c>false</c>.</returns>
        public bool TryGetValue(string key, out string? value)
        {
            int n = Encoding.UTF8.GetByteCount(key);
            Span<byte> tmp = n <= 256 ? stackalloc byte[n] : new byte[n];
            Encoding.UTF8.GetBytes(key, tmp);
            if (TryGetValue(tmp, out var v))
            {
                value = Encoding.UTF8.GetString(v);
                return true;
            }
            value = null;
            return false;
        }

        /// <summary>
        /// Compares the stored key with the provided UTF-8 bytes.
        /// </summary>
        /// <param name="keyUtf8">UTF-8 encoded key bytes.</param>
        /// <returns><c>true</c> if keys are equal; otherwise <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool KeyEquals(ReadOnlySpan<byte> keyUtf8)
        {
            int cap = KeyCap(_size);
            if (keyUtf8.Length >= cap) return false;
            var stored = new ReadOnlySpan<byte>(_entry, cap);
            int len = stored.IndexOf((byte)0);
            if (len < 0) len = cap;
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

        private static string BuildMetadataCellSwitchArms((int key, int val)[] uniq)
        {
            var sb = new StringBuilder();
            foreach (var (k, v) in uniq)
            {
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
        private static string EmitEnumAndExtensions((int key, int val)[] uniq, string generatorVersion)
        {
            var enumMembers = BuildEnumMembers(uniq);
            var packed = uniq.Select(p => (p.key << 16) | p.val).OrderBy(x => x).ToArray();
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

        private static string BuildEnumMembers((int key, int val)[] uniq)
        {
            var sb = new StringBuilder();
            foreach (var (k, v) in uniq)
            {
                var packed = (k << 16) | v;
                sb.AppendLine($"        Entry{k}x{v} = 0x{packed:X8},");
            }
            return sb.ToString().TrimEnd('\r', '\n');
        }
    }
}