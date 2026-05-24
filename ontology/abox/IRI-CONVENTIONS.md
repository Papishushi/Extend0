# IRI Conventions

## Purpose

This document defines the current naming rules for Extend0 ontology instance IRIs in the `abox` layer.

## Canonical Bases

- TBox namespace: `https://extend0.se777en.fyi/ns#`
- ABox namespace: `https://extend0.se777en.fyi/abox#`

## Naming Rules

- Use lowercase kebab-case for local ABox names.
- Keep names stable and semantic, not file-oriented.
- Prefer role or concept names over implementation names.
- Do not encode temporary demo context into canonical instance names.

## Examples

- `extend0-platform`
- `lifecycle-system`
- `metadb-system`
- `metadb-access`
- `settings-table`
- `settings-schema`

## Anti-Patterns

- `clusternodes-demo-row-17`
- `test123`
- `new-manager-final`
- `heartbeatrecord`

## Slug Fixture

Shared slug expectations are captured in [`slug-fixture.json`](slug-fixture.json).
