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

## Governing ADRs

- [ADR 1-005](../ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)
- [ADR 1-007](../ADR/1-007-METADB-ADR-DEFINE-TABLE-SCHEMA-STORAGE-AND-INDEXING-PIPELINE.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
