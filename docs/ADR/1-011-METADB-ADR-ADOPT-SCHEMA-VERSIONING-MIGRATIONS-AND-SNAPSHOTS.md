# ADR 1-011: Adopt Schema Versioning, Migrations, and Snapshots for MetaDB

## Status

Accepted

## Date

2026-06-09

## Context

MetaDB tables are already governed by `TableSpec`, storage layout, column shape, references, and indexes.

As MetaDB becomes a platform-core subsystem, table schemas need an explicit evolution model. Without one, contributors can change column layouts, storage choices, or public examples without knowing whether existing runtime storage can still be opened safely.

Extend0 also needs a conservative backup story before more ambitious features such as semantic storage, coordination workflows, ontology-backed interoperability, or UByteC integration rely on MetaDB state.

## Decision

MetaDB adopts `TableSpec` schema versioning, compatibility validation, migration planning, and table-level snapshots as major `1` runtime contracts.

Under major `1`:

- `TableSpec` has a semantic `SchemaVersion`.
- persisted legacy specs that omit `SchemaVersion` are interpreted as effective version `1`.
- newly persisted specs should write schema version `1` or higher explicitly.
- schema compatibility is evaluated before a migration or storage rewrite is attempted.
- migration planning may classify changes as metadata-only, storage rewrite, data transform, or unsupported.
- snapshots capture a normalized `TableSpec` plus materialized runtime files when they exist.
- restore is path-explicit and relocates the restored `TableSpec.MapPath` to the requested target.

## Compatibility Rules

- decreasing schema version is incompatible.
- changing structural schema content without a version bump is incompatible by default.
- adding columns requires a migration plan.
- removing columns is incompatible until an explicit data transform policy exists.
- shrinking key/value byte shapes or initial capacity is incompatible by default.
- growing key/value byte shapes or initial capacity requires a storage rewrite.
- renaming columns requires a data transform because existing row keys may encode column-name-derived keys.
- changing storage layout or chunk size requires a storage rewrite.
- changing `MapPath` alone is relocation metadata, not a schema incompatibility.

## Migration Rules

Migration planning is not the same as executing a migration.

The initial major `1` migration contract is intentionally plan-first:

- safe metadata-only changes can be represented explicitly.
- storage rewrites can be identified before implementation.
- data transforms must be visible before any automated path is allowed.
- unsupported changes must remain blocked until a later ADR defines semantics.

Future executable migrations must preserve this distinction and should refuse to run when the plan contains unsupported or manual data-transform steps.

## Snapshot Rules

MetaDB table snapshots are table-level filesystem artifacts.

For single-file tables, a snapshot may contain:

- normalized `tablespec.json`
- `snapshot.json`
- the backing map file when materialized

For chunked tables, a snapshot may contain:

- normalized `tablespec.json`
- `snapshot.json`
- `manifest.json` when materialized
- `chunks/` files when materialized

Restore must:

- load `snapshot.json`
- validate the captured `TableSpec`
- rewrite `MapPath` to the requested restore target
- restore the runtime files that were captured
- write a fresh sidecar/spec at the restored location

## Consequences

- MetaDB schema evolution becomes explicit instead of implicit.
- Tooling can validate compatibility without opening a live manager.
- Future CLI commands can report schema diffs, migration plans, and snapshot status.
- Runtime code can use the same compatibility model from same-process and singleton/RPC access paths.
- Destructive migration behavior is deferred until a later ADR defines executable data-transform rules.

## Non-Goals

- This ADR does not define a general data transformation engine.
- This ADR does not promise that every migration can be applied automatically.
- This ADR does not replace storage validation; it complements it with schema-level compatibility.
- This ADR does not make snapshots a cross-table transactional backup system.

## Governing Baseline

This ADR is governed by ADR 1-000 and extends the MetaDB architecture defined by ADR 1-005 and ADR 1-007.
