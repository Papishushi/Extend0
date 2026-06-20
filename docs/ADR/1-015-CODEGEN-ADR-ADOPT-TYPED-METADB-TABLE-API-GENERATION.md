# ADR 1-015: Adopt Typed MetaDB Table API Generation

## Status

Accepted

## Date

2026-06-20

## Context

MetaDB tables are governed at runtime by `TableSpec`: table name, map path, column names, key/value byte sizes, read-only flags, capacity, storage options, and schema version metadata.

That runtime contract is intentionally layout-oriented. A value segment of 16 bytes might represent a `Guid`, two integers, a custom blittable struct, or opaque bytes. The persisted `TableSpec` should not become coupled to one CLR consumer type system.

At the same time, consumers need safer APIs than repeated numeric column indexes and manual `MetadataCell` pointer/span manipulation. Platform examples such as `ClusterNodes` naturally want code like `ClusterNodesTable.NodeId.Set(row, id)` and `ClusterNodesTable.Services.TryAdd(...)`, while still producing normal MetaDB table specs.

## Decision

Extend0 adopts generated typed MetaDB table APIs as part of the major `1` code generation pipeline.

Typed table wrappers are generated from opt-in additional files ending in `.typed.tablespec.json`. These descriptors enrich the normal table layout with code-generation-only metadata such as wrapper namespace, wrapper name, column property names, CLR value types, UTF-8 string intent, and reference-vector intent.

Consumer projects must include these descriptors as Roslyn `AdditionalFiles`; the generator does not scan arbitrary project files or runtime directories.

The generated wrapper must:

- expose the underlying `IMetadataTable` as `Inner`
- expose stable column index constants
- expose strongly typed column helpers such as `MetadataValueColumn<T>`, `MetadataUtf8Column`, `MetadataRefsColumn`, or `MetadataRawColumn`
- generate `CreateSpec(string mapPath)` so the typed API can recreate the runtime `TableSpec`
- generate `Register(...)` and `RegisterAndCreate(...)` helpers for `IMetaDBManagerCommon` and `IMetaDBManager`
- validate the runtime `TableSpec` when wrapping an existing table

The runtime `TableSpec` remains the persistence and storage contract. The typed descriptor is a generator contract, not a replacement for `TableSpec`.

## Descriptor Convention

Typed table descriptors use JSON with this expected shape:

```json
{
  "name": "ClusterNodes",
  "typedApiNamespace": "Extend0.Metadata.Typed.Generated",
  "typedApiName": "ClusterNodesTable",
  "schemaVersion": 1,
  "columns": [
    {
      "name": "node_id",
      "propertyName": "NodeId",
      "valueType": "System.Guid",
      "keyBytes": 0,
      "valueBytes": 16,
      "initialCapacity": 256
    }
  ]
}
```

Columns may use:

- `valueType` for unmanaged value columns
- `kind: "utf8"` for fixed-size UTF-8 value columns
- `kind: "refs"` for MetaDB reference-vector columns
- no type/kind for raw fixed-size byte columns

`keyBytes`, `valueBytes`, and `initialCapacity` should be explicit for durable descriptors. The generator may infer common unmanaged value sizes and reference-vector value sizes, but explicit layout remains preferred.

## Consistency Rules

- Generated wrappers must not hide layout mismatches; construction must fail if the table name, schema version, column names, or key/value sizes do not match.
- The typed descriptor must evolve with the `TableSpec` it recreates.
- Runtime storage semantics remain byte-layout semantics. A typed wrapper gives consumer ergonomics and validation, not a separate storage model.
- Custom or uncommon entry sizes still require the runtime/generator support needed by MetaDB storage; typed table generation does not by itself generate every metadata-entry shape.
- Reference-vector columns should use `MetadataRefsColumn` and the `MetadataTableRefVec` layout rather than ad-hoc byte parsing.

## Relationship To Existing ADRs

This ADR extends ADR 1-008 by adding typed MetaDB table wrappers to the schema-driven generation pipeline.

It complements ADR 1-011 by keeping `TableSpec` as the versioned runtime schema while allowing a richer generator-facing descriptor to produce safer consumer APIs.

## Consequences

- Consumers can use generated table-specific APIs without hand-maintaining column indexes.
- Demo and platform schemas can become reusable public contracts instead of copied snippets.
- The generator contract introduces one more artifact that must be kept aligned with MetaDB schema changes.
- Future CLI validation can inspect typed descriptors in addition to raw `TableSpec` files.
