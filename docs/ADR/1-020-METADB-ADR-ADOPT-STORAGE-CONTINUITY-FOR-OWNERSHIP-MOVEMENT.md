# ADR 1-020: Adopt Storage Continuity for Ownership Movement

## Status

Accepted

## Date

2026-06-21

## Context

Lifecycle can coordinate unique service ownership and allow a new process or node to become the active owner when an old owner disappears or is replaced.

For MetaDB, ownership continuity and state continuity are not the same promise.

A new MetaDB owner may be able to recreate the same service topology, contract identity, transport endpoint, schemas, and table registrations while still lacking the old owner's backing bytes. This happens when the old owner used storage that is local to a machine, local to a user profile, protected by filesystem permissions unavailable to the new owner, or mounted only in the old execution environment.

In that situation the service is not a real replica. It may look alive, expose the same API surface, and rebuild the same logical topology, but its table contents are missing or stale. Operationally, that is a lobotomized takeover rather than transparent ownership movement.

There is also a valid opposite case: some distributed services are ephemeral, stateless, cache-like, or able to reconstruct state from another authority. For those services, ownership movement may be valid without shared storage or symmetric replication.

MetaDB already has snapshots and restore, and ADR 1-019 adds evidence-based storage protection. Neither of those decisions proves that a future owner will see the same state automatically.

## Decision

Extend0 adopts evidence-based storage continuity for MetaDB ownership movement.

Under major `1`:

- Lifecycle ownership movement only proves that a service owner can be resolved or replaced.
- MetaDB state continuity requires separate storage evidence.
- `TableSpec` may declare a `StorageContinuityPolicy`.
- `StorageContinuityPolicy` defines the minimum continuity level required before a table path may be materialized.
- `StorageContinuityVerifier` evaluates path evidence and returns `StorageContinuityEvidence`.
- Tables that require storage continuity fail closed during materialization when evidence is missing or insufficient.
- `extend0 metadb validate --ownership-transfer` reports ownership-movement continuity diagnostics without requiring durable state continuity by default.
- `extend0 metadb validate --state-continuity` requires durable storage continuity suitable for transparent state movement.
- The default continuity level required by `--state-continuity` is `SharedBackingStore`.

## Continuity Levels

The major `1` levels are:

- `None`: no storage continuity evidence is required or observed.
- `LocalOnly`: storage is tied to the current node, user, mount, or filesystem view and is not safe for transparent owner movement.
- `RestorableSnapshot`: contents can be moved by explicit snapshot/restore, but takeover is not transparent.
- `SharedBackingStore`: the same backing bytes are reachable by every eligible owner through shared storage.
- `SymmetricReplication`: contents are duplicated across eligible stores with provider-defined consistency semantics.

For transparent durable state movement, `SharedBackingStore` or `SymmetricReplication` is required.

`RestorableSnapshot` is useful for disaster recovery or migration, but it is not a live ownership-transfer guarantee.

## Manifest Contract

The portable manifest filename is:

`.extend0-continuity.json`

The manifest records:

- provider id
- provider version when available
- continuity id
- continuity level
- root path
- topology id when available
- creation time
- evidence source

Secrets, credentials, mount tokens, raw certificates, and replication keys must not be written to the manifest or diagnostics output.

## Runtime Semantics

When `TableSpec.Continuity.RequiredLevel` is not `None`, MetaDB must verify the table `MapPath` before creating the backing store.

If verification returns `FailClosed`, materialization must stop before opening or creating mapped storage.

Path verification must confirm that the table path is inside the declared continuity root. A path outside that root is a hard failure.

## CLI Semantics

`extend0 metadb validate --ownership-transfer` answers what continuity evidence is visible during an owner-movement diagnostic. It does not require durable state continuity by default because ownership transfer may be ephemeral.

`extend0 metadb validate --state-continuity` answers whether a table is safe to use when the new owner must see the same durable table contents.

The command should report:

- required continuity level
- observed continuity level
- final decision
- provider id
- continuity id
- topology id when available
- continuity root
- path containment result
- findings

If no continuity manifest is found and state continuity is requested, validation must fail closed.

## Non-Goals

This ADR does not introduce:

- a distributed MetaDB replication engine
- automatic synchronization between independent local stores
- quorum, consensus, or conflict resolution
- transparent snapshot shipping
- a shared filesystem implementation
- a guarantee that all external shared-storage providers are safe

Those features require future ADRs and implementation work.

## Consistency Rules

- Docs and diagnostics must not imply that Lifecycle ownership transfer alone preserves MetaDB contents.
- A MetaDB owner that restarts on another node without shared or replicated backing storage must be treated as a different state holder, even if it exposes the same topology.
- A service may declare or document ephemeral ownership transfer when state continuity is intentionally not required.
- Storage protection and storage continuity are separate controls. Encrypted local storage can be secure but not transferable.
- Snapshot/restore may support relocation, but it must be described as explicit recovery/migration unless a future replication protocol makes it transparent.
- Future Lifecycle or MetaDB APIs that advertise transferable ownership must expose or require continuity policy/evidence when durable state matters.

## Relationship To Existing ADRs

This ADR is refined by ADR 1-022, which promotes the generic continuity policy, evidence, and verifier model to Lifecycle Assurance while preserving MetaDB's fail-closed table enforcement when durable table state matters.

This ADR extends ADR 1-004 and ADR 1-016 by clarifying that service ownership and lease observation do not imply state continuity.

It extends ADR 1-005 and ADR 1-007 by adding continuity policy to the MetaDB table/storage model.

It complements ADR 1-011 because snapshots and restores are explicit state movement tools, not transparent live ownership transfer.

It complements ADR 1-019 because storage protection and storage continuity are independent policy axes.

## Consequences

- Extend0 can now distinguish "a new owner exists" from "the new owner sees the same MetaDB state."
- MetaDB can fail closed for tables that require owner-movement continuity but only have local storage.
- The CLI can distinguish ephemeral owner movement from durable state-continuity requirements before a deployment relies on it.
- Future shared-storage or replication providers have a stable evidence contract to satisfy.
