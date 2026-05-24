# ADR 1-005: Adopt MetaDB as Structured Metadata System

## Status

Accepted

## Date

2026-05-24

## Context

The `Extend0.Metadata` codebase already forms a coherent system around:

- schema-defined tables
- rows, columns, and cells
- references between metadata locations
- per-table and cross-table indexes
- storage-backed materialization
- operational management through `MetaDBManager`

The ontology and docs need a stable reading of what MetaDB is so that future features do not blur system boundaries.

## Decision

Extend0 adopts `MetaDB` as its structured metadata system.

Under major `1`, MetaDB is defined by these core concepts:

- metadata table
- table schema
- column
- row
- cell
- data shape
- reference
- index
- storage model
- manager
- access surface

## Architectural Reading

- `MetaDBSystem` is the system.
- `MetaDBManager` is the operational manager of that system.
- `MetaDB` is not merely a persistence adapter; it is a structured metadata model with operational semantics.
- `MetaDB` may later serve as a coordination store for other systems, but that possibility does not redefine its primary role.

## Boundary Rules

- A concrete table used by a demo is not automatically a core domain concept.
- Storage layout details may support ontology traceability, but they are not the center of MetaDB semantics.
- Public domain language should prefer concepts such as schema, table, column, reference, and index over incidental helper names.

## Consequences

- MetaDB becomes a first-class system in docs and ontology.
- Future integrations should describe whether they use MetaDB as structured state, coordination backing, or both.
- The distinction between MetaDB itself and any particular demo table becomes explicit.
