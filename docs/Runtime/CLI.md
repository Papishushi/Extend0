# CLI Runtime and Diagnostic Surface

## Purpose

Describe the current `Extend0.Cli` surface and the runtime contracts it exposes for Extend0 major `1`.

## Audience

This page is for:

- contributors adding or reviewing diagnostic commands
- consumers who need to inspect repository, lifecycle, MetaDB, or ontology state
- automation and CI workflows that need stable command results
- AI agents that should query platform truth before inferring it from implementation details

## Runtime Role

`Extend0.Cli` is the platform diagnostic surface for Extend0 major `1`.

It does not replace the library API. It provides a command-line way to inspect and validate the contracts that make the platform legible:

- repository shape and docs/ADR/ontology presence
- lifecycle transport, endpoint, protocol, and connectivity behavior
- MetaDB `TableSpec`, sidecar, storage, and runtime materialization health
- ontology structure, namespaces, scaffolds, and core semantic relationships

## Command Map

### `extend0 doctor`

Checks repository-level expectations:

- solution and core project presence
- CLI project presence and dotnet tool packaging contract
- README and docs/ADR presence
- ADR 1-010 and CLI runtime documentation presence
- major `1` ADR baseline
- ontology baseline files
- test harness presence
- target framework alignment
- selected ontology invariants

Use `--repo <path>` to validate a repository root other than the current working directory.

### `extend0 lifecycle probe`

Resolves lifecycle access details:

- logical service identity
- transport kind
- endpoint name
- server name
- protocol id and version
- built-in client transport availability

By default, the probe is non-mutating: it does not acquire ownership and does not start an owner host.

Use `--connect` only when an owner is expected to be reachable and a real client connection plus handshake should be tested.

Built-in lifecycle probe transports currently include:

- `NamedPipe`, with endpoint derived from service identity when omitted
- `TcpSocket`, with explicit `--endpoint <host:port>` required

### `extend0 metadb inspect`

Reads a `TableSpec` and prints schema/storage shape.

Accepted inputs:

- direct `tablespec.json`
- `*.tablespec.json`
- a map file with a sidecar
- a map file with a single sibling `*.tablespec.json`
- a chunked table directory containing `tablespec.json`

### `extend0 metadb validate`

Validates a `TableSpec` and, when present, the materialized runtime storage.

Static checks include:

- duplicate column names
- column key/value/entry sizes
- value-only columns
- chunk size fit
- storage layout
- sidecar conventions
- estimated logical and physical bytes

Runtime checks include:

- single-file header magic, version, column count, descriptors, capacity, and required bytes
- chunked manifest version, chunk size, column count, column shapes, required chunks, chunk sizes, missing chunks, and orphan chunks

### `extend0 metadb schema`

Compares two `TableSpec` inputs and prints:

- source and target schema versions
- compatibility level
- compatibility findings
- migration plan steps
- whether the plan can be applied automatically
- whether a manual data transform is required

The command exits with `1` only when the schema comparison is incompatible. A plan that requires migration but is structurally valid exits with `0`.

### `extend0 metadb snapshot`

Creates a table-level snapshot from a `TableSpec` input.

The snapshot captures:

- normalized `tablespec.json`
- `snapshot.json`
- single-file map storage when materialized
- chunked `manifest.json` and `chunks/` files when materialized

Use `--out <snapshot-dir>` to choose the destination. Use `--overwrite` only when replacing known snapshot files in an existing snapshot directory.

### `extend0 metadb restore`

Restores a table-level snapshot to an explicit target path.

For single-file snapshots, `--map-path <path>` is the restored map file. For chunked snapshots, `--map-path <path>` is the restored table directory.

Restore relocates the captured `TableSpec.MapPath` to the requested target and writes the correct restored spec/sidecar for the layout.

### `extend0 ontology inspect`

Reports ontology file presence and structural TBox metadata:

- namespace
- XML base
- version
- class count
- object/datatype/annotation property counts
- individual count
- sample classes

### `extend0 ontology validate`

Validates ontology contract guardrails:

- expected namespace and XML base
- TBox version presence
- required TBox class and object property presence
- required platform concepts
- `governsAccessTo -> AccessSurface`
- ABox SHACL schema presence and relevant constraint
- example ABox, query tool, IRI conventions, tests, and diagnostics scaffolds

## Output Contract

Human output should explain what was checked and summarize info, warnings, and errors.

JSON output is enabled with `--json` where supported and should be preferred by automation.

Exit codes:

- `0`: command completed without errors
- `1`: command completed and found broken contracts or a failed probe
- `2`: invalid command usage, unknown command, or invalid option

## Packaging

The CLI is runnable from source:

```bash
dotnet run --project Extend0.Cli -- doctor
```

It is also packable as a dotnet tool using command name `extend0`.

See [Packaging and Consumption](../Deployment/Packaging.md) for current packaging guidance.

## Current Non-Goals

The CLI is not currently:

- a destructive repair tool
- a long-running daemon
- a replacement for `Lifecycle` or `MetaDB` APIs
- an executable schema migration runner
- a complete ontology query runtime

## Governing ADRs

- [ADR 1-010](../ADR/1-010-ARCHITECTURE-ADR-ADOPT-CLI-AS-PLATFORM-DIAGNOSTIC-SURFACE.md)
- [ADR 1-011](../ADR/1-011-METADB-ADR-ADOPT-SCHEMA-VERSIONING-MIGRATIONS-AND-SNAPSHOTS.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
- [ADR 1-004](../ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md)
- [ADR 1-005](../ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)
- [ADR 1-002](../ADR/1-002-EXTEND0-ADR-ADOPT-ONTOLOGY-AS-DOMAIN-SOURCE-OF-TRUTH.md)
