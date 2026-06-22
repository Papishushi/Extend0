# ADR 1-021: Adopt Hardware Attestation Evidence for Storage Access

## Status

Accepted

## Date

2026-06-21

## Context

ADR 1-019 defines storage protection: whether the backing path is protected or encrypted.

ADR 1-020 defines storage continuity: whether a future owner can see the same state through shared or replicated backing storage.

There is a third security question: whether the execution environment that opens those bytes is trusted.

Technologies such as Intel SGX, Intel TDX, AMD SEV-SNP, Arm TrustZone, Arm CCA realms, TPM-sealed workflows, or custom hardware-backed providers can produce evidence that code is running in a measured or isolated environment. That evidence is useful for high-assurance MetaDB deployments, but it is not the same thing as storage encryption or storage continuity.

Extend0 core should not embed a platform-specific attestation stack. Hardware attestation protocols are vendor-specific, deployment-specific, and often require external quote verification, policy services, firmware/platform trust roots, and provider operational knowledge.

## Decision

Extend0 adopts evidence-based hardware attestation for MetaDB storage access.

Under major `1`:

- `TableSpec` may declare a `HardwareAttestationPolicy`.
- `HardwareAttestationPolicy` defines the minimum attestation evidence required before a table path may be used.
- `HardwareAttestationVerifier` evaluates path-scoped evidence and returns `HardwareAttestationEvidence`.
- Tables that require hardware attestation fail closed during materialization when evidence is missing or insufficient.
- `extend0 metadb validate --attestation` reports hardware attestation evidence, observed level, technology, provider identity, measurement, policy id, and final decision.
- Extend0 core validates manifests and policy matching; it does not verify raw SGX quotes, TDX reports, SEV-SNP reports, TrustZone secure-world state, CCA realm reports, or TPM quote chains by itself.

## Attestation Technologies

Major `1` models these technology identifiers:

- `IntelSgx`
- `IntelTdx`
- `AmdSevSnp`
- `ArmTrustZone`
- `ArmCcaRealm`
- `TpmSealed`
- `CustomHardwareAttested`

The list is not a promise that Extend0 core can create or verify those reports natively. It is a vocabulary for provider evidence and policy matching.

## Attestation Levels

Major `1` models these evidence levels:

- `None`: no hardware attestation evidence is required or observed.
- `Declared`: attestation is declared by configuration or manifest.
- `ProviderAttested`: a provider attests that the storage access environment is hardware-backed or TEE-backed.
- `PlatformVerified`: a provider reports local platform verification.
- `RemoteAttested`: remote attestation has been verified against an expected measurement or policy.

For high-assurance deployments, `RemoteAttested` with an expected measurement or policy id is preferred.

## Manifest Contract

The portable manifest filename is:

`.extend0-attestation.json`

The manifest records:

- provider id
- provider version when available
- attestation id
- attestation technology
- attestation level
- root path controlled by the attested environment
- measurement when available
- policy id when available
- report format when available
- report digest when available
- creation time
- evidence source

Raw quotes, certificates, private keys, symmetric keys, tokens, passwords, and long-lived secrets must not be written to the manifest or diagnostics output.

## Runtime Semantics

When `TableSpec.Attestation.RequiredLevel` is not `None`, MetaDB must verify the table `MapPath` before creating the backing store.

If verification returns `FailClosed`, materialization must stop before opening or creating mapped storage.

Path verification must confirm that the table path is inside the declared attested root. A path outside that root is a hard failure.

## CLI Semantics

`extend0 metadb validate --attestation` runs hardware-attestation diagnostics.

The command should report:

- required attestation level
- required attestation technology
- observed attestation level
- observed attestation technology
- final decision
- provider id
- attestation id
- measurement
- policy id
- report format and digest when available
- attested root containment result
- findings

`--require-attestation remote-attested` requires evidence at or above `RemoteAttested`.

`--attestation-technology intel-sgx`, `intel-tdx`, `amd-sev-snp`, `arm-trustzone`, `arm-cca`, `tpm-sealed`, or `custom` can constrain the accepted technology.

## Non-Goals

This ADR does not introduce:

- native SGX quote generation or verification
- native TDX report verification
- native SEV-SNP report verification
- native TrustZone secure monitor integration
- native Arm CCA realm verification
- native TPM quote-chain verification
- a confidential-computing scheduler
- a guarantee that hardware attestation alone protects MetaDB data

Provider packages may implement deeper platform-specific verification later.

## Consistency Rules

- Hardware attestation must remain separate from storage protection and storage continuity.
- Docs must not imply that encrypted storage proves trusted execution.
- Docs must not imply that hardware attestation proves shared or replicated state.
- Remote-attested deployments should prefer explicit measurement or policy-id matching.
- Core diagnostics must distinguish declared, provider-attested, platform-verified, and remote-attested evidence.

## Relationship To Existing ADRs

This ADR is refined by ADR 1-022, which promotes the generic hardware-attestation policy, evidence, and verifier model to Lifecycle Assurance while preserving MetaDB's fail-closed table enforcement for trusted storage access.

This ADR extends ADR 1-005 and ADR 1-007 by adding hardware-attestation policy to the MetaDB table/storage model.

It complements ADR 1-019 because storage-at-rest protection and trusted execution are separate controls.

It complements ADR 1-020 because owner movement and state continuity are separate from execution trust.

It complements ADR 1-017 and ADR 1-018 because RPC authentication/transport security and hardware-attested storage access are separate controls.

## Consequences

- Extend0 can express high-assurance storage access requirements without hard-coding vendor-specific attestation stacks into core.
- Providers can supply SGX, TDX, SEV-SNP, TrustZone, CCA, TPM, or custom evidence through a stable manifest contract.
- MetaDB can fail closed when a table requires trusted execution but only untrusted or undeclared execution evidence is present.
- The first implementation is evidence-based; stronger provider packages can improve evidence quality without changing `TableSpec` semantics.
