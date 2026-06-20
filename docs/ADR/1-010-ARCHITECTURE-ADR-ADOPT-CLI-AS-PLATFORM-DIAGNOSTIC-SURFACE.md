# ADR 1-010: Adopt CLI as Platform Diagnostic Surface

## Status

Accepted

## Date

2026-06-09

## Context

Extend0 major `1` is being consolidated as a platform of cooperating systems. ADR 1-009 sets the current direction as platform-core consolidation: make `Lifecycle`, `MetaDB`, ontology, ADRs, and docs legible and internally consistent before expanding into larger ecosystem features.

The repository now has an `Extend0.Cli` project with commands that inspect and validate the active platform contract:

- `extend0 doctor`
- `extend0 lifecycle probe`
- `extend0 lifecycle diagnose`
- `extend0 metadb inspect`
- `extend0 metadb validate`
- `extend0 ontology inspect`
- `extend0 ontology validate`

Those commands are no longer incidental development helpers. They expose operational truth about the repository, runtime access surfaces, metadata storage, and ontology contract.

Without an explicit ADR, the CLI could drift into ad hoc scripts, unstable output, or feature-specific tooling that does not preserve the major `1` platform story.

## Decision

Extend0 adopts `Extend0.Cli` as the platform diagnostic surface for major `1`.

The CLI is a first-class architectural surface for:

- repository contract checks
- lifecycle transport, endpoint, protocol, owner, connectivity, and heartbeat diagnostics
- MetaDB schema, sidecar, storage, and runtime-storage validation
- ontology structure and semantic guardrail validation
- human and automation-friendly diagnostics

The CLI should be distributed as a dotnet tool with command name `extend0`, while still remaining runnable from source through `dotnet run --project Extend0.Cli -- ...`.

## CLI Contract Rules

The CLI contract follows these rules under major `1`:

- Commands should remain grouped by platform subsystem or architecture surface.
- Human output should be readable and explain what was checked.
- `--json` output should be suitable for automation and CI.
- Exit code `0` means the requested diagnostic completed without errors.
- Exit code `1` means the requested diagnostic completed and found a broken contract or failed probe.
- Exit code `2` means invalid CLI usage or unknown command/options.
- Validation commands should prefer non-invasive reads and avoid mutating runtime state.
- Connectivity probes may attempt real transport connections only when explicitly requested with options such as `--connect`.
- Command names should prefer durable domain vocabulary over implementation details.

## Current Command Surfaces

### `doctor`

`doctor` checks repository-level contracts such as solution presence, docs/ADR presence, ontology baseline files, test harness presence, target framework alignment, and selected ontology invariants.

### `lifecycle probe`

`lifecycle probe` resolves lifecycle identity, transport kind, endpoint name, protocol descriptor, and optional client connectivity.

By default, it is non-mutating and does not acquire ownership or start an owner host.

### `lifecycle diagnose`

`lifecycle diagnose` connects to a resolved Lifecycle endpoint and reports owner observation, handshake status, service info, heartbeat liveness, owner-reported connectivity, and lease exposure status.

It is diagnostic-only: it does not start an owner and does not acquire ownership.

### `metadb inspect`

`metadb inspect` reads a `TableSpec` from a direct spec file, a map-path sidecar, a sibling generated spec, or a chunked table directory and reports schema and storage shape.

### `metadb validate`

`metadb validate` checks the `TableSpec` and, when storage is materialized, validates single-file headers or chunked manifest/chunk consistency.

### `ontology inspect`

`ontology inspect` reports TBox/ABox file presence and structural TBox counts.

### `ontology validate`

`ontology validate` checks ontology namespaces, expected scaffolds, core relationships, and required platform concepts.

## Packaging Rules

`Extend0.Cli` should be packable as a dotnet tool package.

Packaging rules:

- the package id is `Extend0.Cli`
- the tool command name is `extend0`
- package metadata should identify the repository, license, icon, README, and major `1` diagnostic purpose
- source execution through `dotnet run --project Extend0.Cli -- ...` remains supported for contributors
- documentation must not imply that the CLI replaces library package consumption

The CLI tool is a diagnostic and operational companion to Extend0, not the primary library API.

## Consequences

- New diagnostic commands should be added under `Extend0.Cli` unless there is a strong reason to keep them external.
- Docs and README must describe CLI commands as an actual supported surface rather than future ideas.
- Major CLI contract changes should be recorded in ADRs when they affect naming, output semantics, exit codes, or packaging.
- Validation command output becomes part of how Extend0 exposes architectural consistency to humans, tests, and automation.

## Non-Goals

This ADR does not require:

- a full operational management CLI
- destructive repair commands
- a long-lived daemon
- a complete SPARQL query interface
- replacing source-level APIs with CLI-only workflows

## Relationship to Other ADRs

- ADR 1-000 defines the major `1` governance baseline.
- ADR 1-003 defines Extend0 as a platform of cooperating systems.
- ADR 1-004 and ADR 1-006 govern lifecycle concepts surfaced by `lifecycle probe` and `lifecycle diagnose`.
- ADR 1-014 defines the owner/heartbeat-focused Lifecycle diagnostics command.
- ADR 1-005 and ADR 1-007 govern MetaDB concepts surfaced by `metadb inspect` and `metadb validate`.
- ADR 1-002 governs ontology artifacts checked by `ontology inspect` and `ontology validate`.
- ADR 1-009 establishes platform-core consolidation as the current execution direction.
