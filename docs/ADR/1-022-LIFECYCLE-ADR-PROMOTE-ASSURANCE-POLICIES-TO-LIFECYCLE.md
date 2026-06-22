# ADR 1-022: Promote Assurance Policies to Lifecycle

## Status

Accepted

## Date

2026-06-21

## Context

ADR 1-019, ADR 1-020, and ADR 1-021 introduced evidence-based storage protection, storage continuity, and hardware attestation from the immediate MetaDB use case.

That was useful because MetaDB is the first subsystem that needs to fail closed before opening protected state. But the concepts are not MetaDB-only:

- a Lifecycle-owned service may need protected storage even when it is not a MetaDB service
- ownership movement and durable state continuity apply to any stateful owner, not just a table manager
- hardware attestation is a trust property of the execution environment that opens service state
- diagnostics should expose these guarantees as platform assurance, not as a private MetaDB storage detail

At the same time, physical file/storage validation is still MetaDB-specific. Headers, chunks, sidecars, table specs, physical capacity, snapshots, and restore are part of the MetaDB storage model and should not be moved into Lifecycle.

## Decision

Extend0 promotes the generic assurance model to `Lifecycle`.

Under major `1`:

- `Extend0.Lifecycle.Assurance` is the canonical namespace for storage protection, storage continuity, protected storage handles, and hardware attestation evidence.
- `StorageProtectionPolicy`, `StorageContinuityPolicy`, and `HardwareAttestationPolicy` are cross-service Lifecycle assurance policies.
- `StorageProtectionVerifier`, `StorageContinuityVerifier`, and `HardwareAttestationVerifier` are cross-service evidence verifiers.
- `TableSpec.Protection`, `TableSpec.Continuity`, and `TableSpec.Attestation` remain valid MetaDB schema fields because MetaDB can consume Lifecycle assurance policies before materializing table storage.
- MetaDB remains responsible for physical storage validation, including headers, chunks, sidecars, table specs, capacity, snapshots, and restore.
- `extend0 lifecycle assurance storage diagnose` is the Lifecycle-facing command surface for protected-storage assurance diagnostics.
- `extend0 storage diagnose` remains available as a compatibility alias.
- `extend0 metadb validate` remains the MetaDB-facing command that combines physical table validation with any assurance policies declared by a `TableSpec`.

## Boundary Rules

Lifecycle Assurance owns:

- policy and evidence vocabulary for cross-service guarantees
- protected-storage provider handles and manifests
- state-continuity evidence for owner movement when durable state matters
- hardware-attestation evidence for trusted execution
- CLI surfaces that diagnose those guarantees independent of a concrete service implementation

MetaDB owns:

- `TableSpec` schema serialization and compatibility rules
- mapped and chunked table storage layout
- runtime physical storage validation
- table sidecars and manifests that describe table shape
- table snapshots and restores
- enforcement of assurance policies when those policies are declared on a `TableSpec`

## Consistency Rules

- Docs must not describe storage protection, continuity, or hardware attestation as MetaDB-only concepts.
- Docs must not move MetaDB physical storage validation into Lifecycle.
- A service may use Lifecycle assurance without using MetaDB.
- MetaDB may consume Lifecycle assurance policies when it opens protected table state.
- Future assurance protocol changes require Lifecycle ADRs.
- Future MetaDB storage format changes require MetaDB ADRs.

## Relationship To Existing ADRs

This ADR refines ADR 1-019, ADR 1-020, and ADR 1-021 by moving their generic policy/evidence model from a MetaDB-owned namespace to Lifecycle Assurance.

It does not supersede their runtime fail-closed requirements for MetaDB tables.

It extends ADR 1-004 and ADR 1-016 by making assurance part of the Lifecycle ownership and service-trust model.

It preserves ADR 1-005, ADR 1-007, and ADR 1-011 as the governing decisions for MetaDB schema, physical storage, snapshots, and restore.

## Consequences

- Assurance can apply to any Extend0 service, not only MetaDB.
- MetaDB keeps the responsibilities that are genuinely table/storage-specific.
- CLI users get a Lifecycle-facing assurance path while existing storage and MetaDB validation commands continue to work.
- The ontology can model protection, continuity, and attestation as Lifecycle assurance concepts without losing the MetaDB enforcement relationship.
