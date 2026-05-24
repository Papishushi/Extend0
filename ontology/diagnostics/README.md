# Ontology Diagnostics

This folder contains a minimal executable diagnostics scaffold for the Extend0 ontology phase.

## Current Scope

The scaffold can:

- load the current TBox and one or more ABox files
- run a narrow diagnostic pass over ontology foundation expectations
- emit a structured result document
- emit a schema-shaped fix document skeleton

The scaffold does not yet:

- repair ontology files automatically
- run full SHACL validation
- run a full RDF stack with external dependencies

## Entry Point

```bash
python ontology/diagnostics/abox-doctor.py
python ontology/diagnostics/abox-doctor.py --abox ontology/abox/example-abox.ttl --emit-fix-doc
```
