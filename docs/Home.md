# Extend0 Docs

## Purpose

This wiki is the durable documentation entry point for Extend0 major `1`. It explains the platform through stable systems and architecture contracts rather than through ad hoc implementation fragments.

## Audience

This page is for:

- contributors who need the current architectural direction quickly
- consumers who need to understand what Extend0 is before using it
- AI agents that should anchor on docs and ADRs before inferring meaning from code alone

## Child Document Map

- [Requirements](Requirements.md): major `1` direction, scope, and system boundaries
- [Runtime](Runtime.md): runtime behavior and the current operational model of `Lifecycle` and `MetaDB`
- [Extensions](Extensions.md): code generation, ontology, and other extensibility-oriented surfaces
- [Deployment](Deployment.md): packaging and consumption guidance for the current phase
- [Architecture Decision Records](ADR.md): the governing architecture contract of the repository

## Current Reading Order

For a contributor trying to understand the repository today, the recommended read is:

1. [README](../README.md)
2. [Requirements](Requirements.md)
3. [Runtime](Runtime.md)
4. [ADR 1-000](ADR/1-000-EXTEND0-ADR-DEFINE-EXTEND0-MAJOR-VERSION-1.md)
5. [ADR 1-003](ADR/1-003-ARCHITECTURE-ADR-MODEL-EXTEND0-AS-A-PLATFORM-OF-COOPERATING-SYSTEMS.md)
6. [ADR 1-004](ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md)
7. [ADR 1-005](ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)
8. [ADR 1-010](ADR/1-010-ARCHITECTURE-ADR-ADOPT-CLI-AS-PLATFORM-DIAGNOSTIC-SURFACE.md)

## What Extend0 Is Right Now

Extend0 major `1` is currently being consolidated as a platform of cooperating systems with four architectural centers and one diagnostic surface:

- `Lifecycle`
- `MetaDB`
- ontology
- code generation
- CLI diagnostics

The repository is deliberately treating those areas as one coherent platform story instead of as unrelated libraries.
