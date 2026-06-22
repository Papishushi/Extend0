# ADR 1-019: Adopt Evidence-Based Storage Protection

## Status

Accepted

## Date

2026-06-21

## Context

MetaDB is optimized around predictable fixed layouts, memory-mapped files, chunked mapped storage, blittable values, and low-overhead access paths.

Adding a first-party encrypted hot storage backend would make encryption easy to select, but it would also move encryption and decryption into MetaDB's runtime read/write path. That would weaken the core mmap-first design by requiring plaintext caches, block encryption metadata, nonce/tag management, flush semantics, and additional concurrency/crash-recovery rules.

At the same time, Extend0 needs a security posture that does not simply trust a user-provided path. If a table requires protected storage, Extend0 should be able to fail closed when evidence is missing or insufficient.

## Decision

Extend0 adopts evidence-based storage protection for MetaDB.

Under major `1`:

- MetaDB hot storage remains `InMemoryStore`, `MappedStore`, and `SegmentedMappedStore`.
- Extend0 does not add an encrypted hot storage backend as the default protection strategy.
- `TableSpec` may declare a `StorageProtectionPolicy`.
- `StorageProtectionPolicy` defines the minimum evidence level required before a table path may be used.
- `StorageProtectionVerifier` evaluates path evidence and returns `StorageProtectionEvidence`.
- Protected tables fail closed during materialization when the required evidence is missing or insufficient.
- `extend0 lifecycle assurance storage diagnose` reports the evidence, observed level, decision, provider ids, mount root, path containment, and findings.
- `extend0 storage diagnose` remains available as a compatibility alias.
- `extend0 metadb validate` runs storage protection diagnostics when a `TableSpec` requires protection or when `--security` / `--require-protection` is supplied.

## Protection Levels

The major `1` levels are:

- `None`: no protection evidence is required or observed.
- `DeclaredEncrypted`: protection is declared by configuration or manifest.
- `ProviderAttestedEncrypted`: a storage provider or provider-owned manifest attests protected storage.
- `PlatformVerifiedEncrypted`: a provider reports platform/OS verification.
- `Extend0ManagedProtectedMount`: an Extend0-approved provider lifecycle manages the protected mount.

The core verifier is evidence-based. It does not claim to cryptographically prove external volume encryption unless a provider or platform verifier supplies that evidence under the policy.

## Manifest Contract

The portable manifest filename is:

`.extend0-protection.json`

The manifest records:

- provider id
- provider version when available
- protection id
- protection level
- mount root
- creation time
- evidence source

Secrets, passphrases, key material, raw certificates, and token values must not be written to the manifest or diagnostics output.

## Runtime Semantics

When `TableSpec.Protection.RequiredLevel` is not `None`, MetaDB must verify the table `MapPath` before creating the backing store.

If verification returns `FailClosed`, materialization must stop before opening or creating mapped storage.

Path verification must confirm that the table path is inside the protected mount root. A path outside the mount root is a hard failure.

## CLI Semantics

`extend0 lifecycle assurance storage diagnose` is the Lifecycle-facing storage-protection diagnostic command.

`extend0 storage diagnose` remains available as a compatibility alias.

`extend0 metadb validate` includes storage-protection findings when security checks are requested or when the spec requires them.

Human and JSON output must expose:

- required protection level
- observed protection level
- final decision
- provider id
- protection id
- mount root
- path containment result
- findings

## Non-Goals

This ADR does not introduce:

- a built-in encrypted mmap store
- a portable OS volume mounting abstraction
- VeraCrypt, BitLocker, LUKS, FileVault, rclone, FUSE, Dokan, or WinFsp integration in core
- runtime encryption/decryption of every cell, page, chunk, or column slab
- authorization rules for RPC or filesystem access

External protected-storage providers may be added in optional packages later.

## Consistency Rules

- Hot MetaDB storage should preserve the mmap/chunked performance model unless a future ADR explicitly supersedes this decision.
- Protected storage policy must fail closed when a required evidence level is not met.
- Provider-specific integration must remain outside core unless it is fully portable and does not introduce platform-specific runtime dependencies.
- Snapshot/export encryption may be introduced separately because it does not sit on the hot storage path.
- Diagnostics must distinguish declared, provider-attested, platform-verified, and Extend0-managed evidence instead of collapsing them into a misleading boolean.

## Relationship To Existing ADRs

This ADR is refined by ADR 1-022, which promotes the generic protection policy, evidence, verifier, and provider-handle model to Lifecycle Assurance while preserving MetaDB's fail-closed table enforcement.

This ADR extends ADR 1-005 and ADR 1-007 by adding storage protection policy to the MetaDB table/storage model.

It extends ADR 1-010 by adding a storage diagnostic CLI surface.

It complements ADR 1-011 because snapshots and restores may later add artifact encryption without changing the hot storage policy.

It complements ADR 1-017 and ADR 1-018 because IPC authentication/transport security and storage-at-rest protection are separate controls.

## Consequences

- Extend0 can enforce protected storage requirements without adding slow encrypted hot storage by default.
- Users get clear diagnostics showing what evidence exists and whether it satisfies policy.
- Provider-specific mounting and platform verification remain possible through optional packages.
- The first implementation relies on manifests and handles as evidence; stronger providers can improve evidence without changing `TableSpec` semantics.
