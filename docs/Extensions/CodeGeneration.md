# Code Generation

## Purpose

Describe the current role of source generation in Extend0 major `1`.

## Audience

This page is for contributors working on generated metadata-entry or blittable artifacts.

## Current Role

Code generation in Extend0 is currently treated as schema-driven artifact generation:

- metadata-entry generation creates fixed-layout metadata shapes
- blittable adapter generation creates payload-friendly structs from declarative definitions

## Metadata Entry Shape Contract

`Extend0/Metadata/Generator.attributes.cs` declares the built-in metadata-entry catalog shipped by Extend0. That catalog is not the complete universe of possible shapes: consumers may declare additional `[GenerateMetadataEntry(keyBytes, valueBytes)]` attributes in their own assemblies when they need stable blittable layouts that are not bundled by default.

The runtime contract is layout-oriented. MetaDB table schemas describe key/value byte capacities, and generated entry structs are convenient typed shapes for those capacities. Future docs and APIs should avoid implying that only the built-in entry classes can be stored by MetaDB.

## Current Constraints

- generators should stay aligned with the domain vocabulary used by MetaDB and ontology
- packaging and consumer onboarding still need hardening, so project-reference workflows are the safest recommendation in the current phase

## Governing ADR

- [ADR 1-008](../ADR/1-008-CODEGEN-ADR-ADOPT-SCHEMA-DRIVEN-GENERATION-PIPELINE.md)
