# Ontology

## Purpose

Describe how ontology currently participates in the Extend0 architecture.

## Audience

This page is for contributors evolving the TBox, ABox conventions, docs vocabulary, or truth-question harnesses.

## Current Role

Ontology is a major `1` architecture artifact used to:

- stabilize domain language above code symbols
- expose inconsistencies between docs, code, and architecture
- define what counts as a core concept versus a demo concept

## Core Concepts Kept Central

- `Lifecycle`
- `MetaDB`
- `Transport`
- `ServiceIdentity`
- `AccessSurface`
- `OwnershipClaim`
- `Lease`
- `HeartbeatSignal`

## Discipline Rules

- keep the TBox limited to stable platform concepts
- move scenario-specific or demo-specific material into ABox examples and tests
- treat ontology review as a gate for major naming or boundary changes in docs and architecture

## Operational Tooling

The ontology foundation now includes two lightweight operational surfaces:

- `ontology/skills/ontology-query/query.py` for local JSON-based ontology queries
- `ontology/diagnostics/abox-doctor.py` for minimal diagnostics and fix-document projection

Canonical commands:

```bash
python ontology/skills/ontology-query/query.py classes
python ontology/skills/ontology-query/query.py class LifecycleSystem
python ontology/skills/ontology-query/query.py find transport
python ontology/skills/ontology-query/query.py sparql "SELECT ?c WHERE { ?c rdf:type owl:Class . }"
python ontology/diagnostics/abox-doctor.py
python ontology/diagnostics/abox-doctor.py --emit-fix-doc
```

## Current Limits

- the query tool supports a deliberately small SPARQL-like subset
- the doctor is read-only and does not mutate ontology files
- full RDF, SHACL, and repair automation are deferred until a later phase

## Governing ADRs

- [ADR 1-002](../ADR/1-002-EXTEND0-ADR-ADOPT-ONTOLOGY-AS-DOMAIN-SOURCE-OF-TRUTH.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
