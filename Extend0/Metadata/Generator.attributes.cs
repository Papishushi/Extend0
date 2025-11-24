/// <summary>
/// Declares the "catalog" of metadata entry shapes that the code generator must emit.
/// </summary>
/// <remarks>
/// <para>
/// This file contains a set of <c>[assembly: GenerateMetadataEntry(keyBytes, valueBytes)]</c>
/// attributes that drive the <c>Extend0.MetadataEntry.Generator</c> source generator.
/// Each attribute instructs the generator to produce a blittable
/// <c>MetadataEntry{Key}x{Value}</c> struct for a specific fixed key/value layout.
/// </para>
/// <para>
/// By pre-declaring the most common key/value size combinations (small tags, standard
/// keys, long keys, and “chubby” blobs), we avoid:
/// </para>
/// <list type="bullet">
///   <item><description>Allocations and per-entry shape metadata at runtime.</description></item>
///   <item><description>Having to generate or reflect layouts dynamically.</description></item>
///   <item><description>Inconsistent binary formats across processes or versions.</description></item>
/// </list>
/// <para>
/// In other words: this file defines the fixed binary shapes that <see cref="MetadataTable"/>,
/// <c>MappedStore</c> and related components can rely on when packing keys and values into
/// memory-mapped columns.
/// </para>
/// </remarks>

// Very small keys (tags, small IDs) — small/medium/large values
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(16, 64)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(16, 128)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(16, 256)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(16, 512)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(16, 768)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(16, 1024)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(16, 1536)]

// Short keys — values from 64 up to ~2 KB
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 64)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 128)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 256)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 512)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 768)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 1024)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 1536)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(32, 2048)]

// Standard keys (64 bytes) — sweet spot for “chubby” values
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 64)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 128)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 256)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 512)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 768)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 1024)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 1536)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 2048)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 3072)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 4096)]

// Long keys (namespaces, paths, etc.) — medium/large values
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 128)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 256)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 512)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 768)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 1024)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 1536)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 2048)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(128, 3072)]

// “Chubby” entries for short-to-medium blobs
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(256, 256)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(256, 512)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(256, 768)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(256, 1024)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(256, 1536)]
[assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(256, 2048)]
