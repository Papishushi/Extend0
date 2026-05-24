# ADR 1-008: Adopt Schema-Driven Generation Pipeline

## Status

Accepted

## Date

2026-05-24

## Context

Extend0 already contains generation components that derive code artifacts from declarative definitions:

- metadata entry size declarations
- blittable definitions

That work is not accidental build machinery. It is part of the architectural strategy to derive stable low-level artifacts from higher-level schema or definition inputs.

## Decision

Extend0 adopts a schema-driven generation pipeline as a first-class platform pipeline.

The conceptual stages are:

1. declare a generator input or definition
2. interpret that definition according to a generator contract
3. derive stable generated artifacts
4. feed those artifacts back into the wider platform as usable shapes or code

## Current Pipeline Families

- metadata entry generation from declared key and value size combinations
- blittable generation from external definitions

## Architectural Rules

- generation inputs are part of the platform model, not just private build implementation
- generated artifacts should reflect stable domain or structural needs
- future generators should be evaluated as part of the same generation pipeline family rather than as isolated tools

## Consequences

- code generation gains a stable architectural place in Extend0
- docs and ontology can describe generation as a pipeline from definition to artifact
- future UByteC or ontology-related generation work can extend this pipeline coherently
