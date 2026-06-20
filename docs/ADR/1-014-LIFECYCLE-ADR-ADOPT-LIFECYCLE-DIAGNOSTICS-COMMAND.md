# ADR 1-014: Adopt Lifecycle Diagnostics Command

## Status

Accepted

## Date

2026-06-20

## Context

Lifecycle now has multiple real built-in transports and a shared protocol descriptor/handshake model.

`extend0 lifecycle probe` is useful for resolving transport, protocol, endpoint, and optional connectivity, but it intentionally remains a lightweight preflight command. Operators and contributors also need a deeper diagnostic command that can answer operational questions:

- is there a compatible owner?
- which endpoint and transport are being used?
- did the handshake fail?
- who is the owner process?
- is heartbeat alive?
- is lease state observable?

## Decision

Extend0 adopts `extend0 lifecycle diagnose` as the owner/heartbeat-focused Lifecycle diagnostic command.

Under major `1`, `lifecycle diagnose`:

- resolves the same lifecycle identity, transport, endpoint, and protocol inputs as `lifecycle probe`
- attempts a real client connection by default
- validates the transport handshake through the selected built-in client transport
- reports whether a compatible owner is reachable
- calls `GetServiceInfoAsync` to report owner identity and endpoint details
- calls `PingAsync` to report heartbeat liveness, timestamp, uptime, and fingerprint
- calls `CanConnectAsync` to include the owner-reported endpoint connectivity probe result
- emits human output and JSON output

`diagnostics` may be accepted as an alias, but `diagnose` is the canonical command name.

## Lease Reporting

Lifecycle currently enforces ownership through owner coordination and OS-level primitives, but it does not expose a standalone lease record through `ICrossProcessService`.

Therefore, `lifecycle diagnose` must not invent lease state.

It reports:

- `NotExposed` when no compatible owner is observed or lease state cannot be inferred
- `ImpliedByOwnerObservation` when a compatible owner responds and heartbeat can be queried

A future explicit lease/ownership record may extend this output, but that requires a compatible runtime contract change.

## Relationship to `lifecycle probe`

`lifecycle probe` remains the lightweight command for non-mutating resolution and optional handshake-only connectivity.

`lifecycle diagnose` is the deeper operational command. It connects by default and asks the owner for service info and heartbeat state.

## Consequences

- Lifecycle has a first-class troubleshooting command for owner, endpoint, transport, handshake, and heartbeat issues.
- Handshake failures become visible as their own diagnostic state instead of being collapsed into generic connectivity failure.
- The current absence of an exposed lease record is explicit and machine-readable.
- Future Lifecycle runtime changes can add richer lease/heartbeat data without changing the basic command boundary.

## Non-Goals

- This ADR does not introduce a persistent lease table.
- This ADR does not start or repair owner processes.
- This ADR does not add destructive runtime management actions.
- This ADR does not replace application-level health checks.

## Governing Baseline

This ADR is governed by ADR 1-000 and extends the CLI and Lifecycle contracts defined by ADR 1-004, ADR 1-006, ADR 1-010, ADR 1-012, and ADR 1-013.
