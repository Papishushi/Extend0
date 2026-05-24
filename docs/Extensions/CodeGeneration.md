# Code Generation

## Purpose

Describe the current role of source generation in Extend0 major `1`.

## Audience

This page is for contributors working on generated metadata-entry or blittable artifacts.

## Current Role

Code generation in Extend0 is currently treated as schema-driven artifact generation:

- metadata-entry generation creates fixed-layout metadata shapes
- blittable adapter generation creates payload-friendly structs from declarative definitions

## Current Constraints

- generators should stay aligned with the domain vocabulary used by MetaDB and ontology
- packaging and consumer onboarding still need hardening, so project-reference workflows are the safest recommendation in the current phase

## Governing ADR

- [ADR 1-008](../ADR/1-008-CODEGEN-ADR-ADOPT-SCHEMA-DRIVEN-GENERATION-PIPELINE.md)
