# ADR 1-007: Define Table Schema Storage and Indexing Pipeline

## Status

Accepted

## Date

2026-05-24

## Context

MetaDB already follows a repeatable conceptual flow from schema definition to operational table behavior. That flow spans:

- `TableSpec`
- column configuration
- table materialization
- store backing
- references
- index registration and rebuild

Without making that pipeline explicit, the system is easy to misread as only an API surface.

## Decision

Extend0 defines the MetaDB table pipeline as the core pipeline by which structured metadata becomes operational state.

The conceptual stages are:

1. define a table schema
2. define its columns and payload shapes
3. materialize or open the metadata table
4. bind the table to a storage model
5. read and write rows and cells through the schema-defined structure
6. maintain references between metadata locations
7. register, rebuild, and consult indexes

## Pipeline Semantics

- schema comes before operational table behavior
- columns define the structural roles available to cells
- storage supports the table, but does not replace schema as the primary concept
- indexes accelerate discovery but do not redefine the underlying truth of the table
- references are domain relationships expressed through addressable metadata locations

## Consequences

- MetaDB documentation can explain the system from schema to runtime instead of from method list to method list.
- The ontology can model MetaDB around stable structural stages.
- Future work such as coordination backing or ontology persistence can describe where it enters this pipeline.
