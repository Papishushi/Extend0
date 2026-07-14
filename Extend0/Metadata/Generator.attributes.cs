// Declares the "catalog" of metadata entry shapes that the code generator must emit.
//
// This file contains a set of [assembly: GenerateMetadataEntry(keyBytes, valueBytes)]
// attributes that drive the Extend0.MetadataEntry.Generator source generator.
// Each attribute instructs the generator to produce a blittable
// MetadataEntry{Key}x{Value} struct for a specific fixed key/value layout.
//
// By pre-declaring the most common key/value size combinations, we avoid allocations,
// runtime layout reflection, and inconsistent binary formats across processes or versions.

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
