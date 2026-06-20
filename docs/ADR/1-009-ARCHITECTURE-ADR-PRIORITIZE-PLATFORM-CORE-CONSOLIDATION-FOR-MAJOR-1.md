# ADR 1-009: Prioritize Platform-Core Consolidation for Major 1

## Status

Accepted

## Date

2026-05-24

## Context

ADR 1-000 and ADR 1-003 establish the major `1` reading of Extend0 as a platform of cooperating systems. The repository, however, still contains visible mismatches between that architectural story and the public-facing implementation story:

- `README.md` has historically described Extend0 too narrowly
- `Lifecycle` is architecturally transport-oriented, but endpoint discovery and coordination policy still need hardening beyond the default transport
- `MetaDB` is conceptually first-class, but past examples have documented the wrong public entry surface
- ontology is intended to mirror the platform truth, but it must stay stricter than demos when deciding what belongs in the core model

Without an explicit execution direction, the next milestone could drift into more features or experiments before the platform core becomes internally coherent.

## Decision

For the next `1-2` milestones, Extend0 major `1` prioritizes platform-core consolidation over feature sprawl.

The primary consolidation targets are:

- `Lifecycle`
- `MetaDB`
- ontology
- ADRs and docs

This means the immediate goal is to make Extend0 legible, internally consistent, and structurally ready for future ecosystem work rather than to expand the platform surface quickly.

## Priority Areas

### 1. Platform Narrative Alignment

- align `README`, docs, ADRs, and ontology around the same platform story
- stop describing Extend0 primarily as a small utility library
- make the system boundaries of `Lifecycle`, `MetaDB`, code generation, and ontology explicit

### 2. Lifecycle Hardening

- normalize the language of service identity, access surface, ownership, roles, and resolution modes
- preserve transport abstraction as a real architectural axis
- track and plan cleanup of transport-specific wording and endpoint-discovery gaps without pretending the work is already done

### 3. MetaDB Hardening

- standardize the conceptual vocabulary of system, manager, table, schema, column, row, cell, reference, index, and storage model
- keep public documentation aligned with the actual public access surface
- treat onboarding and generator-consumer packaging issues as part of platform hardening, not as unrelated polish

### 4. Ontology Discipline

- keep the TBox focused on stable platform concepts
- move scenario-specific and demo-specific material out of the core model
- use ontology review as a gate for major naming or boundary changes

## Deferred But Prepared Directions

The following directions remain valid for major `1`, but they are not the immediate implementation center of this phase:

- deeper `UByteC` integration
- a native ontology subsystem inside Extend0
- MetaDB-backed semantic or coordination workflows
- additional transports beyond the current `NamedPipe`, `UnixDomainSocket`, and `TcpSocket` built-ins
- cross-service ontology-backed interoperability

## Tracked Inconsistencies

The current phase explicitly tracks these mismatches as architecture work:

- `README` versus the platform story accepted in ADRs
- transport abstraction versus remaining transport-specific implementation language and endpoint-discovery gaps
- MetaDB public API truth versus older documentation examples
- demo-driven concepts versus ontology core concepts

## Consequences

- the next milestone should be expressible as “harden Lifecycle and MetaDB around accepted architecture”
- public documentation should become more honest about current truths and current gaps
- ontology work should favor conceptual cleanup and executable invariants over speculative growth
- future ecosystem expansion should build on a clarified platform core instead of substituting for it

## Relationship to Other ADRs

- ADR 1-000 defines the major `1` governance and platform baseline
- ADR 1-003 defines the system-of-systems reading
- ADR 1-004 and ADR 1-005 define the current first-class runtime systems
- ADR 1-012 defines the built-in Lifecycle transport plugin model and TCP socket transport
- ADR 1-013 adds Unix domain sockets as a local built-in transport under the same protocol descriptor model
- this ADR defines the current execution direction for how those decisions should be implemented and documented
