# System Boundaries

## Purpose

Define the top-level boundaries that keep Extend0 legible as a platform rather than a bag of classes.

## Audience

This page is for contributors deciding where a concept, fix, or new capability belongs.

## Boundary Summary

- `Lifecycle` owns service identity, ownership, uniqueness, singleton resolution, and transport-mediated access.
- `MetaDB` owns structured metadata state, tables, schemas, rows, cells, references, indexes, and storage-backed management.
- ontology owns the canonical platform vocabulary and the semantic relationship model between systems.
- code generation owns derived artifacts produced from declarative or schema-driven definitions.

## Boundary Notes

- `Lifecycle` may later use MetaDB as a coordination backing store, but that does not make MetaDB a lifecycle subsystem.
- `MetaDB` may later participate in ontology-aware integrations, but that does not make ontology an implementation detail of MetaDB.
- code generation should be described through the structures it derives, not as an isolated tool island.
- demo artifacts are not domain concepts unless an ADR or stable code contract promotes them into architecture.

## Current Known Gaps

- the architecture treats transport as abstract, while the runtime still centers on named pipes
- the README and public examples must stay aligned with the actual public MetaDB access surface
- the ontology should remain stricter than demos when deciding what is core domain

## Governing ADRs

- [ADR 1-003](../ADR/1-003-ARCHITECTURE-ADR-MODEL-EXTEND0-AS-A-PLATFORM-OF-COOPERATING-SYSTEMS.md)
- [ADR 1-004](../ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md)
- [ADR 1-005](../ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)
