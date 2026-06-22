# ADR 1-016: Adopt Observable Lease Snapshot

## Status

Accepted

## Date

2026-06-21

## Context

Lifecycle ownership is currently enforced by the cross-process singleton orchestrator through an OS mutex and exposed to consumers through a service proxy.

Before this ADR, diagnostics could observe that an owner responded, that the handshake passed, and that heartbeat was alive, but the runtime contract did not expose a structured lease snapshot. That left operators and future tooling with only inferred ownership state.

Extend0 now needs a stable way to describe the active owner without binding the domain model to named pipes, TCP, Unix domain sockets, or any single future coordination backend.

## Decision

Extend0 adopts an observable `Lease` snapshot as part of the `ICrossProcessService` diagnostic contract.

Under major `1`:

- `ICrossProcessService.GetLeaseAsync()` returns the current lease snapshot for the hosted service.
- `Lease` is a serializable DTO suitable for JSON-RPC transport.
- `Lease` describes ownership identity, contract identity, process identity, fingerprint, acquisition time, observation time, endpoint, transport kind, coordination kind, coordination scope, exclusivity, activity, and optional expiration.
- Built-in cross-process owners configure their lease from the orchestrator-owned coordination state.
- Current OS-mutex-backed ownership reports `CoordinationKind = "OSMutex"`, exclusive ownership, and no expiration.
- Direct in-process services that do not pass through the cross-process orchestrator may report an in-process diagnostic lease.
- `extend0 lifecycle diagnose` should call `GetLeaseAsync()` after owner service info and before heartbeat/connectivity checks.
- If a custom or older owner cannot expose a lease snapshot, diagnostics may fall back to `ImpliedByOwnerObservation`.

## Lease Semantics

A lease is an observable ownership snapshot, not necessarily a durable row, persisted record, or time-limited grant.

`ExpiresUtc` is optional because some coordination backends, including the current OS mutex model, do not expose TTL semantics. Future backends may use expiring leases, renewable leases, MetaDB-backed records, or distributed coordination records without changing the basic diagnostic shape.

`OwnershipName` identifies the coordination identity being protected. It must not be assumed to be the same value as the transport endpoint.

`EndpointName`, `EndpointServerName`, and `TransportKind` describe how the owner is currently reachable. They are access-surface details, not the ownership primitive itself.

## Consistency Rules

- Lease snapshots must remain transport-neutral.
- Lease snapshots must not expose live OS handles, mutexes, sockets, streams, cancellation tokens, or other non-serializable runtime resources.
- A service info fingerprint and lease fingerprint should normally match for the same owner observation.
- A heartbeat fingerprint and lease fingerprint should normally match for the same owner observation.
- Any future persistent lease table, MetaDB-backed coordination record, or distributed lease protocol requires its own ADR.

## Relationship To Existing ADRs

This ADR extends ADR 1-004 by making ownership diagnostics explicit.

It extends ADR 1-006 by exposing a structured snapshot of the cross-process owner branch.

It extends ADR 1-014 by replacing purely inferred lease reporting with an owner-provided lease snapshot while preserving fallback behavior for incompatible owners.

## Consequences

- Lifecycle diagnostics can distinguish owner reachability, heartbeat liveness, and ownership lease state.
- Tooling can reason about owner identity without parsing endpoint names.
- Future transports and coordination backends have a stable place to report their ownership model.
- The `ICrossProcessService` contract grows, so test harnesses and custom service implementations must implement `GetLeaseAsync()`.
