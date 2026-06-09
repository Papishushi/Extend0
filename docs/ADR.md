# Architecture Decision Records

This page indexes the accepted Architecture Decision Records (ADRs) that govern Extend0.

## Purpose

ADRs capture durable architectural, editorial, and governance decisions that should remain valid longer than any single implementation detail or temporary task note.

Use an ADR when a change affects repository-wide conventions, documentation policy, ontology governance, public architectural direction, subsystem boundaries, or any rule that future contributors must continue to follow.

## Naming Convention

ADR files live under `docs/ADR/` and use this filename format:

`1-{NNN}-{SUBSYSTEM OR ARCHITECTURE}-ADR-{UPPERCASE-HYPHENATED-TITLE}.md`

Rules:

- `1` is the current repository governance major established by ADR 1-000.
- `{NNN}` is a zero-padded sequence number.
- `{SUBSYSTEM OR ARCHITECTURE}` identifies the primary subsystem or architectural scope of the ADR.
- `000` is reserved for baseline or governance-defining decisions.
- `{UPPERCASE-HYPHENATED-TITLE}` must be stable, descriptive, and safe for Git-based navigation.

## Status Model

The ADRs created in this phase are normative immediately on creation and use status `Accepted`.

Future ADRs may supersede earlier decisions, but they must do so explicitly and explain the compatibility impact.

## Index

| ADR | Title | Status | Summary |
| --- | --- | --- | --- |
| [1-000](ADR/1-000-EXTEND0-ADR-DEFINE-EXTEND0-MAJOR-VERSION-1.md) | Define Extend0 Major Version 1 | Accepted | Establishes repository governance major `1`, the platform interpretation of Extend0, and the UByteC ecosystem context. |
| [1-001](ADR/1-001-EXTEND0-ADR-ADOPT-CODE-AS-WIKI-DOCUMENTATION.md) | Adopt Code-as-Wiki Documentation | Accepted | Defines the GitHub-first documentation model, wiki structure, and editorial conventions for `docs/`. |
| [1-002](ADR/1-002-EXTEND0-ADR-ADOPT-ONTOLOGY-AS-DOMAIN-SOURCE-OF-TRUTH.md) | Adopt Ontology as Domain Source of Truth | Accepted | Defines the ontology governance model, canonical paths, namespaces, and consistency obligations for `ontology/`. |
| [1-003](ADR/1-003-ARCHITECTURE-ADR-MODEL-EXTEND0-AS-A-PLATFORM-OF-COOPERATING-SYSTEMS.md) | Model Extend0 as a Platform of Cooperating Systems | Accepted | Defines the top-level architectural reading of Extend0 as cooperating systems rather than a flat utility library. |
| [1-004](ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md) | Adopt Lifecycle as Service Identity and Unique Access System | Accepted | Defines Lifecycle as the system for ownership, uniqueness, access surfaces, and transport-mediated service access. |
| [1-005](ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md) | Adopt MetaDB as Structured Metadata System | Accepted | Defines MetaDB as the structured metadata system of Extend0 and fixes its core conceptual boundaries. |
| [1-006](ADR/1-006-LIFECYCLE-ADR-DEFINE-SINGLETON-SERVICE-RESOLUTION-PIPELINE.md) | Define Singleton Service Resolution Pipeline | Accepted | Defines the conceptual pipeline that resolves singleton-backed services in in-process and cross-process modes. |
| [1-007](ADR/1-007-METADB-ADR-DEFINE-TABLE-SCHEMA-STORAGE-AND-INDEXING-PIPELINE.md) | Define Table Schema Storage and Indexing Pipeline | Accepted | Defines the conceptual pipeline by which MetaDB turns schemas into materialized tables, storage, references, and indexes. |
| [1-008](ADR/1-008-CODEGEN-ADR-ADOPT-SCHEMA-DRIVEN-GENERATION-PIPELINE.md) | Adopt Schema-Driven Generation Pipeline | Accepted | Defines the generation pipeline used to derive metadata and blittable code artifacts from declarative inputs. |
| [1-009](ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md) | Prioritize Platform-Core Consolidation for Major 1 | Accepted | Sets the current execution direction: align docs, ontology, Lifecycle, and MetaDB before expanding the platform surface. |
| [1-010](ADR/1-010-ARCHITECTURE-ADR-ADOPT-CLI-AS-PLATFORM-DIAGNOSTIC-SURFACE.md) | Adopt CLI as Platform Diagnostic Surface | Accepted | Defines `Extend0.Cli` as the major `1` diagnostic surface and fixes command grouping, exit codes, JSON output, and dotnet tool packaging rules. |

## Usage Notes

- Consumer-facing guidance belongs in the relevant docs landing pages and their child documents.
- Governing architectural or editorial rules belong in ADRs.
- If a change would alter the meaning of an existing ADR, create a new ADR instead of silently editing history.
