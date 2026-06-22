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

## Storage Protection

MetaDB major `1` consumes Lifecycle assurance policies when they are declared by `TableSpec`. MetaDB does not own the generic assurance vocabulary, but it does enforce those policies before opening table state.

Physical file/storage validation remains MetaDB-owned: headers, chunks, sidecars, table specs, capacity, snapshots, and restore all stay in the MetaDB runtime model.

MetaDB uses evidence-based storage protection rather than a built-in encrypted hot store.

`TableSpec.Protection` can require a minimum `StorageProtectionLevel` before the table backing path is materialized.

Current levels are:

- `None`
- `DeclaredEncrypted`
- `ProviderAttestedEncrypted`
- `PlatformVerifiedEncrypted`
- `Extend0ManagedProtectedMount`

When protection is required, MetaDB verifies the table `MapPath` before opening or creating `MappedStore` or `SegmentedMappedStore`. If evidence is missing, insufficient, or the path is outside the protected mount root, materialization fails closed.

The portable evidence artifact is `.extend0-protection.json`. It records provider id, protection id, protection level, mount root, provider version, and creation metadata. It must not contain secrets or key material.

This model preserves the mmap/chunked hot path. External encrypted volumes, platform verifiers, or future optional providers can supply stronger evidence without changing table storage semantics.

## Storage Continuity

Lifecycle ownership movement does not, by itself, prove that a new MetaDB owner can see the old owner's table contents.

MetaDB major `1` models this with `TableSpec.Continuity` and evidence-based storage continuity.

Current levels are:

- `None`
- `LocalOnly`
- `RestorableSnapshot`
- `SharedBackingStore`
- `SymmetricReplication`

For durable state movement, the table needs `SharedBackingStore` or `SymmetricReplication` evidence. `RestorableSnapshot` is useful for explicit recovery, but it does not make a takeover transparent.

When continuity is required, MetaDB verifies the table `MapPath` before opening or creating mapped storage. If evidence is missing, insufficient, or the path is outside the declared continuity root, materialization fails closed.

The portable evidence artifact is `.extend0-continuity.json`. It records provider id, continuity id, continuity level, continuity root, optional topology id, provider version, and creation metadata. It must not contain secrets or replication credentials.

Use `extend0 metadb validate --ownership-transfer` to inspect owner movement without requiring state continuity. Use `extend0 metadb validate --state-continuity` when the new owner must see the same table contents.

## Hardware Attestation

Storage protection and continuity do not prove that the execution environment opening the store is trusted.

MetaDB major `1` consumes trusted-execution evidence through `TableSpec.Attestation` and `HardwareAttestationPolicy`.

Current attestation technologies are:

- `IntelSgx`
- `IntelTdx`
- `AmdSevSnp`
- `ArmTrustZone`
- `ArmCcaRealm`
- `TpmSealed`
- `CustomHardwareAttested`

Current attestation levels are:

- `None`
- `Declared`
- `ProviderAttested`
- `PlatformVerified`
- `RemoteAttested`

When attestation is required, MetaDB verifies the table `MapPath` before opening or creating mapped storage. If evidence is missing, insufficient, or the path is outside the declared attested root, materialization fails closed.

The portable evidence artifact is `.extend0-attestation.json`. It records provider id, attestation id, technology, level, root path, optional measurement, optional policy id, optional report format/digest, provider version, and creation metadata. It must not contain raw quotes, credentials, keys, tokens, or secrets.

Use `extend0 metadb validate --attestation` to check whether a table has trusted-execution evidence. High-assurance deployments should prefer `RemoteAttested` evidence with measurement or policy-id matching.

## Governing ADRs

- [ADR 1-005](../ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)
- [ADR 1-007](../ADR/1-007-METADB-ADR-DEFINE-TABLE-SCHEMA-STORAGE-AND-INDEXING-PIPELINE.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
- [ADR 1-011](../ADR/1-011-METADB-ADR-ADOPT-SCHEMA-VERSIONING-MIGRATIONS-AND-SNAPSHOTS.md)
- [ADR 1-019](../ADR/1-019-METADB-ADR-ADOPT-EVIDENCE-BASED-STORAGE-PROTECTION.md)
- [ADR 1-020](../ADR/1-020-METADB-ADR-ADOPT-STORAGE-CONTINUITY-FOR-OWNERSHIP-MOVEMENT.md)
- [ADR 1-021](../ADR/1-021-METADB-ADR-ADOPT-HARDWARE-ATTESTATION-EVIDENCE-FOR-STORAGE-ACCESS.md)
- [ADR 1-022](../ADR/1-022-LIFECYCLE-ADR-PROMOTE-ASSURANCE-POLICIES-TO-LIFECYCLE.md)
