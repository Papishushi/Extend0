# Extensions

## Purpose

This section documents the architectural surfaces that extend or derive from the core runtime systems.

## Audience

This page is for contributors working on generators, ontology, or future integration-oriented capabilities.

## Child Document Map

- [Code Generation](Extensions/CodeGeneration.md): metadata-entry and blittable generation in the current platform model
- [Ontology](Extensions/Ontology.md): the current role of the TBox, ABox conventions, and truth-question discipline

## Extension Reading

In major `1`, Extend0 should treat extensions as architecture-adjacent surfaces that support the platform story:

- code generation derives stable artifacts from declarative definitions
- ontology stabilizes domain meaning above implementation names

Neither should be documented as isolated tooling if it changes how the platform is understood.
