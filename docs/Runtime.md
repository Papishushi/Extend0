# Runtime

## Purpose

This section explains how the current Extend0 runtime behaves at the system level.

## Audience

This page is for:

- contributors implementing or reviewing runtime behavior
- consumers who need the operational model of `Lifecycle` and `MetaDB`
- ontology work that must stay anchored to real runtime semantics

## Child Document Map

- [Lifecycle Runtime Model](Runtime/Lifecycle.md): service identity, singleton resolution, roles, scopes, and transport
- [MetaDB Runtime Model](Runtime/MetaDB.md): tables, schemas, manager responsibilities, and access surfaces
- [CLI Runtime and Diagnostic Surface](Runtime/CLI.md): command-line diagnostics, validation contracts, output rules, and packaging role

## Runtime Focus

The current runtime focus is not adding more subsystems. It is clarifying the real semantics of the two core runtime systems already present:

- `Lifecycle`
- `MetaDB`
- `Extend0.Cli`

That includes documenting where the architecture is broader than the current implementation, especially around transport pluggability and coordination backing.
