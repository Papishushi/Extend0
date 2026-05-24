# ADR 1-002: Adopt Ontology as Domain Source of Truth

## Status

Accepted

## Date

2026-05-24

## Context

Per ADR 1-000, Extend0 major `1` needs a durable governance model for domain meaning that survives refactors, implementation drift, and cross-language tooling.

The repository is introducing an ontology as a code-adjacent source of truth so that humans and AIs can answer domain questions from explicit semantic artifacts before exploring implementation details. To be trustworthy, that ontology needs canonical paths, namespaces, validation artifacts, query tooling, and consistency obligations that evolve together.

Extend0 is especially exposed to terminology drift because it combines several technical areas in one codebase:

- lifecycle and singleton orchestration
- cross-process RPC and transport concepts
- metadata storage and indexing
- source generation and schema-driven modeling

Those concepts are meaningful above the code level. If they are described only through implementation names, then future refactors, alternative language tooling, or AI-assisted development will tend to fragment the domain model.

## Decision

Extend0 adopts `ontology/` as the repository location for its domain source of truth under repository major `1`.

The ontology is authoritative for Extend0 domain terminology and relationships, but it must remain consistent with code and documentation rather than drifting into an isolated model.

The following decisions are normative:

- the canonical TBox path is `ontology/tbox/extend0.owl`
- the TBox namespace is `https://extend0.se777en.fyi/ns#`
- the ABox namespace convention is `https://extend0.se777en.fyi/abox#`
- AIs and humans should consult the ontology before deep code exploration when the primary question is domain meaning, classification, or relationships

The ontology is "code-adjacent" rather than "code-independent" in a strict sense:

- it is allowed to abstract and stabilize terminology
- it is not allowed to drift away from real Extend0 behavior and concepts
- it should clarify the domain before code exploration, not replace implementation truth for runtime behavior

## Scope

This ADR governs:

- the canonical ontology location and namespace conventions
- the core artifact families that make the ontology operational
- the role of ontology in domain understanding and repository governance

This ADR does not yet define:

- the full content of the initial TBox
- a complete diagnostics/remediation platform
- public hosted ontology publication infrastructure
- automatic ontology/code synchronization mechanisms

## Ontology Conventions

### Canonical Artifact Layout

The ontology system under major `1` uses these conventions:

- TBox in OWL 2 RDF/XML
- ABox examples and fixtures in Turtle
- SHACL schema in `ontology/abox/abox-schema.ttl`
- truth-question harness in `ontology/tests/`
- query tooling in `ontology/skills/ontology-query/`
- diagnostics scaffold in `ontology/diagnostics/`
- IRI naming rules governed by `ontology/abox/IRI-CONVENTIONS.md`

The artifact families are not optional add-ons. Together they define what "ontology as source of truth" means in practice:

- TBox defines classes, relationships, and controlled semantics
- ABox fixtures show instance-level truth
- SHACL closes operational constraints not captured by OWL alone
- truth-question harnesses protect semantic invariants
- query tooling makes the ontology usable by humans and AIs
- diagnostics scaffolding prepares future operational validation and repair workflows

### Governance Rules

- The ontology should model real Extend0 domain concepts, not disconnected abstractions.
- New concepts that materially affect domain meaning should not appear only in code without corresponding ontology consideration.
- Tooling, fixtures, and validation artifacts are part of the ontology system and must be treated as one governed unit.

Additional governance rules:

- ontology terminology should be stable enough to outlive individual class or method renames in code
- ontology changes should prefer explicit semantic clarity over mirroring incidental implementation structures
- if a concept is too implementation-specific to deserve ontological representation, it should remain in code/docs rather than be forced into the ontology

## Consistency Model

Consistency between ontology and code means:

- domain concepts should map to real Extend0 capabilities or structures
- ontology relationships should not imply behavior that the codebase does not support
- code should not silently introduce major new domain concepts without ontology review
- documentation should use ontology-aligned terminology when explaining domain structure

## Consistency Obligations

- Ontology changes that alter semantics must be reflected in code-facing docs and vice versa.
- Future ontology protocol changes require ADRs.
- Fixtures, SHACL, queries, and truth-question harnesses must evolve together rather than independently.
- The ontology is authoritative for domain meaning, but implementation truth still matters for behavior, supported APIs, and release state.

## Usage Expectations

The expected workflow for future contributors and AI agents is:

- consult ontology first when the task is primarily about meaning, classification, or relationships
- consult code next when the task is about implementation behavior, APIs, or runtime mechanics
- update ontology when a real semantic change is introduced
- update docs when that semantic change affects explanation, onboarding, or supported behavior

## Change Triggers

A new ADR should be considered when changes would:

- move the canonical TBox path
- change the base namespace strategy
- redefine the role of ontology relative to code or docs
- introduce a new ontology protocol, repair model, or authoritative artifact family
- change the rules for IRI governance or cross-artifact consistency

Routine additions of classes, individuals, fixtures, or truth questions under the existing model do not require a new ADR unless they change the governance model itself.

## Consequences

- Extend0 gains a semantic layer that can be queried, validated, and used as a pre-code source of understanding.
- Humans and AI tools get a stable, explicit place to resolve domain terminology and relationships.
- Ontology maintenance becomes a governed activity instead of an optional side artifact.
- Future ontology evolution must remain compatible with ADR 1-000 or explicitly supersede the relevant governance rules.

In practice, this also means:

- ontology work is now part of architectural maintenance, not just documentation
- later tooling work in `ontology/` should be judged against this ADR's governance rules
- semantic drift between ontology, docs, and code is now a repository-quality issue rather than a cosmetic issue
