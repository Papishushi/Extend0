# ADR 1-003: Model Extend0 as a Platform of Cooperating Systems

## Status

Accepted

## Date

2026-05-24

## Context

Per ADR 1-000, Extend0 major `1` is not well described as a flat utility library. The codebase already contains several substantial systems with their own responsibilities, contracts, and extension pressures:

- `Lifecycle`
- `MetaDB`
- code generation
- ontology and future semantic integration

Without an explicit architectural reading, those systems are easy to misread as unrelated implementation islands. That makes documentation weaker, ontology design noisier, and future integration work harder to evaluate.

## Decision

Extend0 is modeled under major `1` as a platform of cooperating systems.

The following top-level systems are recognized as architecturally meaningful:

- `Lifecycle` for identity, ownership, uniqueness, and transport-mediated service access
- `MetaDB` for structured metadata state, schema-defined tables, references, and indexes
- code generation for schema-driven derived artifacts
- ontology for domain meaning and future semantic interoperability

This ADR defines a systems view, not a claim that every system is equally mature in implementation.

## System Boundaries

- A system may have multiple namespaces, contracts, and internal helpers.
- A subsystem is not required to expose its entire internal implementation as ontology concepts.
- Cross-system integration is expected, but each system should retain a clear conceptual boundary.

## Architectural Rules

- New major capabilities should be evaluated first as additions to an existing system or as a new system.
- Documentation and ontology should describe Extend0 primarily through systems and their relationships.
- Implementation detail should not be allowed to flatten the platform model back into a bag of classes.

## Consequences

- The ontology can model Extend0 using stable system concepts.
- Later ADRs can speak precisely about system responsibilities and pipelines.
- Contributors should think in terms of system boundaries, not only namespaces or files.
