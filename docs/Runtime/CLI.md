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
- lifecycle transport, endpoint, protocol, owner, connectivity, and heartbeat behavior
- MetaDB `TableSpec`, sidecar, storage, and runtime materialization health
- Lifecycle assurance evidence for storage protection, state continuity, hardware attestation, and fail-closed decisions
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
- contract scope
- transport kind
- endpoint name
- server name
- protocol id and version
- authentication mode
- built-in client transport availability

By default, the probe is non-mutating: it does not acquire ownership and does not start an owner host.

Use `--connect` only when an owner is expected to be reachable and a real client connection plus handshake should be tested.

Use `--auth shared-secret-hmac --secret <value>` when the owner requires shared-secret HMAC authentication. The secret is used for the handshake proof and is not printed in human or JSON output.

Use `--contract metadb` to resolve the same contract-scoped endpoint used by `MetaDBManagerSingleton`. Contract-scoped endpoints include the service contract and Extend0 build fingerprint, so a CLI built from one Extend0 assembly will not silently attach to an owner from another build unless an explicit `--endpoint` is supplied.

Built-in lifecycle probe transports currently include:

- `NamedPipe`, with endpoint derived from service identity when omitted
- `UnixDomainSocket`, with local socket path derived from service identity when omitted
- `TcpSocket`, with explicit `--endpoint <host:port>` required
- `TlsTcpSocket`, with explicit `--endpoint <host:port>` required and TLS server certificate validation

### `extend0 lifecycle diagnose`

Connects to a Lifecycle owner and reports operational state:

- logical service identity
- contract scope
- transport kind and endpoint
- protocol id and version
- authentication mode
- whether a compatible owner is reachable
- handshake status and handshake failure details
- owner service info, including machine, process, implementation, fingerprint, and reported endpoint
- heartbeat status, timestamp, uptime, fingerprint, and observed age
- owner-reported endpoint connectivity probe result
- lease status and, when exposed by the owner, the current lease snapshot

`diagnose` attempts a real connection by default. It does not start an owner process and does not acquire ownership.

When the owner requires shared-secret HMAC authentication, pass `--auth shared-secret-hmac --secret <value>`. Diagnostics reports the authentication mode but never prints the secret.

When using `TlsTcpSocket`, pass `--tls-target-host <name>` if the certificate identity should be validated against a name different from the endpoint host.

Use `--contract metadb` when diagnosing the MetaDB owner. If a named-pipe owner candidate exists for the same contract and logical name but a different build fingerprint, diagnostics report it as a version-mismatch candidate instead of leaving only a timeout.

Lease reporting is an owner-provided diagnostic snapshot. Current built-in Lifecycle owners expose the active ownership name, coordination kind, scope, transport endpoint, fingerprint, acquisition time, observed time, exclusivity, and optional expiration. Built-in OS-mutex ownership is exclusive and non-expiring, so `ExpiresUtc` is normally empty. If an older or custom owner does not expose a lease snapshot, diagnostics falls back to `ImpliedByOwnerObservation` instead of inventing lease state.

### `extend0 lifecycle assurance storage diagnose`

Evaluates Lifecycle assurance evidence for protected storage paths.

This is the Lifecycle-facing command surface for cross-service storage protection checks. It reports the same evidence as the compatibility `extend0 storage diagnose` command:

- required protection level
- observed protection level
- final decision
- evidence source
- provider id and version when available
- protection id
- mount root
- whether the path is inside the mount
- verification findings

Use this command when the question is "can this service safely use this protected path?" rather than "is this MetaDB table physically valid?"

### `extend0 metadb inspect`

Reads a `TableSpec` and prints schema/storage shape.

Accepted inputs:

- direct `tablespec.json`
- `*.tablespec.json`
- a direct TableSpec file with a custom extension
- a map path whose sibling TableSpec was produced by `TableSpec.SaveToDirectory(...)`, including custom extensions
- a map path with a single sibling file that deserializes as a valid `TableSpec`
- a chunked table directory containing `tablespec.json`

### `extend0 metadb validate`

Validates a `TableSpec` and, when present, the materialized runtime storage.

Static checks include:

- duplicate column names
- column key/value/entry sizes
- value-only columns
- chunk size fit
- storage layout
- resolved TableSpec path conventions, including custom extensions
- estimated logical and physical bytes

Runtime checks include:

- single-file header magic, version, column count, descriptors, capacity, and required bytes
- chunked manifest version, chunk size, column count, column shapes, required chunks, chunk sizes, missing chunks, and orphan chunks

Security checks include:

- `TableSpec.Protection` policy when declared
- explicit `--security` diagnostics when requested
- `--require-protection <level>` policy override
- provider id and protection id matching when supplied
- protected mount root containment
- final `Pass`, `Warning`, or `FailClosed` storage protection decision

Use `--protection-manifest <path>` to pass an explicit `.extend0-protection.json` manifest. Otherwise, validation searches for the nearest manifest above the table path.

Ownership-transfer checks include:

- `TableSpec.Continuity` policy when declared
- explicit `--ownership-transfer` diagnostics when requested
- explicit `--state-continuity` checks when durable state continuity is required
- `--require-continuity <level>` policy override
- provider id and continuity id matching when supplied
- continuity root containment
- final `Pass`, `Warning`, or `FailClosed` storage continuity decision

Use `--ownership-transfer` to inspect owner movement without assuming durable table contents must move with the owner. This is valid for ephemeral/stateless services or services that can reconstruct state externally.

Use `--state-continuity` to require `SharedBackingStore` by default. This distinguishes a new owner that can expose the same topology from a new owner that can actually see the same table contents. Use `--require-continuity <level>` to choose a different minimum level, and `--continuity-manifest <path>` to pass an explicit `.extend0-continuity.json` manifest.

Hardware-attestation checks include:

- `TableSpec.Attestation` policy when declared
- explicit `--attestation` diagnostics when requested
- `--require-attestation <level>` policy override
- `--attestation-technology <kind>` matching when supplied
- provider id, attestation id, measurement, and policy id matching when supplied
- attested root containment
- final `Pass`, `Warning`, or `FailClosed` hardware-attestation decision

Use `--attestation` to require provider-attested execution by default. Use `--require-attestation remote-attested` with `--measurement` or `--attestation-policy-id` for high-assurance deployments. Use `--attestation-manifest <path>` to pass an explicit `.extend0-attestation.json` manifest.

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

### `extend0 storage diagnose`

Evaluates protected storage evidence for any file or directory path.

This command remains available as a compatibility alias for `extend0 lifecycle assurance storage diagnose`.

The command reports:

- required protection level
- observed protection level
- final decision
- evidence source
- provider id and version when available
- protection id
- mount root
- whether the path is inside the mount
- verification findings

Use `--require <level>` to fail closed when evidence is below the requested level. Supported aliases include `none`, `declared`, `provider-attested`, `platform-verified`, and `managed`.

Use `--provider <id>` and `--protection-id <id>` when a specific provider or protected mount identity is required.

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
- [ADR 1-013](../ADR/1-013-LIFECYCLE-ADR-ADOPT-UNIX-DOMAIN-SOCKET-BUILT-IN-TRANSPORT.md)
- [ADR 1-014](../ADR/1-014-LIFECYCLE-ADR-ADOPT-LIFECYCLE-DIAGNOSTICS-COMMAND.md)
- [ADR 1-016](../ADR/1-016-LIFECYCLE-ADR-ADOPT-OBSERVABLE-LEASE-SNAPSHOT.md)
- [ADR 1-017](../ADR/1-017-LIFECYCLE-ADR-ADOPT-RPC-AUTHENTICATION-MODEL.md)
- [ADR 1-018](../ADR/1-018-LIFECYCLE-ADR-ADOPT-TLS-TCP-SOCKET-BUILT-IN-TRANSPORT.md)
- [ADR 1-019](../ADR/1-019-METADB-ADR-ADOPT-EVIDENCE-BASED-STORAGE-PROTECTION.md)
- [ADR 1-020](../ADR/1-020-METADB-ADR-ADOPT-STORAGE-CONTINUITY-FOR-OWNERSHIP-MOVEMENT.md)
- [ADR 1-021](../ADR/1-021-METADB-ADR-ADOPT-HARDWARE-ATTESTATION-EVIDENCE-FOR-STORAGE-ACCESS.md)
- [ADR 1-022](../ADR/1-022-LIFECYCLE-ADR-PROMOTE-ASSURANCE-POLICIES-TO-LIFECYCLE.md)
- [ADR 1-005](../ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)
- [ADR 1-002](../ADR/1-002-EXTEND0-ADR-ADOPT-ONTOLOGY-AS-DOMAIN-SOURCE-OF-TRUTH.md)
