# Major 1 Direction

## Purpose

Capture the current execution direction for Extend0 major `1` in one place.

## Audience

This page is for maintainers and contributors planning the next `1-2` milestones.

## Direction

Extend0 major `1` should currently optimize for platform-core consolidation, not feature sprawl.

The next milestones should stabilize four assets together:

- `Lifecycle` as the service identity, unique-access, and transport-coordination system
- `MetaDB` as the structured metadata and coordination-ready state system
- ontology as the semantic model of the platform
- ADRs and docs as the public contract for how the platform is described

## What To Prioritize Now

- Align `README`, docs, ADRs, and ontology around the same platform reading.
- Tighten the actual contracts and vocabulary of `Lifecycle` and `MetaDB`.
- Keep the TBox constrained to stable concepts justified by code or accepted ADR direction.
- Track architecture-versus-implementation gaps explicitly instead of smoothing them over in docs.

## Deferred But Prepared

The following directions are in-bounds for major `1`, but should not dominate the immediate milestone:

- deeper `UByteC` integration
- a native ontology subsystem inside Extend0
- MetaDB-backed semantic or coordination workflows
- additional transports beyond the current `NamedPipe` and `TcpSocket` built-ins
- cross-service ontology-backed interoperability

## Governing ADR

- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
