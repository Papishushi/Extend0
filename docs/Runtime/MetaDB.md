# MetaDB Runtime Model

## Purpose

Describe the current operational semantics of `MetaDB` in Extend0 major `1`.

## Audience

This page is for contributors working on structured metadata storage, schema handling, references, or MetaDB access surfaces.

## Core Runtime Concepts

- `MetaDB system`
- `manager`
- `table`
- `schema`
- `column`
- `row`
- `cell`
- `reference`
- `index`
- `storage model`
- `access surface`

## Runtime Reading

`MetaDB` is the structured metadata system of Extend0.

- the system owns the table-oriented metadata model
- the manager owns registration, materialization, maintenance, and operational access
- the access surface owns the stable consumer entry point

## Current Public Access Truth

The current public access model has two explicit entry surfaces:

- `MetaDB.CreateManager(...)` for same-process/local access through `IMetaDBManager`
- `MetaDB.CreateSingleton(...)` for owner/client shared access through `MetaDBManagerSingleton`

The main RPC-safe cross-process contract remains `IMetaDBManagerRPCCompatible`.

That means:

- the system now has a public local access surface and a public singleton access surface
- the manager concept is still central to the domain
- but the internal concrete `MetaDBManager` should not be documented as the public entry point unless the public API changes

## Current Cleanup Targets

- keep README and examples aligned with the real public access surface
- preserve the conceptual distinction between the `MetaDB` system and the manager that operates it
- keep demo tables out of the core domain vocabulary unless they become accepted architecture

## Schema Evolution

`TableSpec` is the schema contract for a MetaDB table.

Under major `1`:

- every persisted `TableSpec` should declare `schemaVersion`
- legacy specs without `schemaVersion` are interpreted as effective version `1`
- schema compatibility should be checked before changing storage shape
- migration planning describes whether a change is metadata-only, a storage rewrite, a data transform, or unsupported
- snapshots are table-level filesystem artifacts that capture the normalized spec and materialized runtime files when present
- restore is explicit about the target path and relocates the restored `TableSpec.MapPath`

Schema migration planning is intentionally conservative. It may say that a change is known and planned without claiming that Extend0 can apply the change automatically.

## Governing ADRs

- [ADR 1-005](../ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)
- [ADR 1-007](../ADR/1-007-METADB-ADR-DEFINE-TABLE-SCHEMA-STORAGE-AND-INDEXING-PIPELINE.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
- [ADR 1-011](../ADR/1-011-METADB-ADR-ADOPT-SCHEMA-VERSIONING-MIGRATIONS-AND-SNAPSHOTS.md)
