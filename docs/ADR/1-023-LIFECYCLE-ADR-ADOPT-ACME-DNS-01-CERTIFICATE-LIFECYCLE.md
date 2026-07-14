# ADR 1-023: Adopt ACME DNS-01 Certificate Lifecycle

## Status

Accepted

## Date

2026-06-22

## Context

ADR 1-018 added `TlsTcpSocket` as the built-in encrypted TCP transport for Lifecycle services. That transport requires server certificates, and optionally client certificates for mTLS, but it intentionally did not define how certificates are issued, renewed, stored, or protected.

Extend0 now needs a certificate lifecycle that can support public-domain TLS without assuming HTTP reachability, reverse proxy ownership, or a specific DNS provider. DNS-01 is the correct first ACME challenge type because it proves control over the authoritative DNS zone and can issue certificates for hosts that are not directly reachable over HTTP. It also supports wildcard certificates.

Certificate lifecycle state is sensitive. ACME account keys identify the ACME account, certificate private keys identify the issued TLS endpoint, and order URLs can continue an issuance flow. Treating that state as ordinary diagnostic JSON would create a footgun for users and downstream services.

## Decision

Extend0 adopts ACME DNS-01 as the first built-in certificate lifecycle for Lifecycle TLS deployments.

Under major `1`:

- `Extend0.Lifecycle.Certificates` is the canonical namespace for certificate lifecycle primitives.
- `Dns01Challenge` defines DNS-01 TXT proof calculation.
- `IDns01RecordProvisioner` defines the DNS record provisioning boundary.
- `ManualDns01RecordProvisioner` is the first supported provisioner and does not call any DNS provider API.
- `AcmeDns01Client` owns the minimal ACME flow for DNS-01 issuance.
- `AcmeDns01OrderState` is the persisted local state for ACME account, order, authorization, and certificate key material.
- `AcmeDns01StateFile` owns protected state serialization.
- `extend0 lifecycle certificate dns-01` is the CLI surface for DNS-01 proof generation and ACME DNS-01 order lifecycle.

The supported ACME DNS-01 lifecycle is:

1. `order`: create or reuse ACME account material, create an ACME order, fetch DNS-01 authorizations, and print TXT records.
2. User or future DNS provider plugin publishes the TXT records.
3. `validate`: ask the ACME server to validate the DNS-01 challenges and refresh order status.
4. `status`: refresh order and authorization state without mutating challenge validation.
5. `finalize`: create a CSR, finalize the order, download the certificate chain, and write certificate output files.

## State Protection

ACME DNS-01 state must be treated as secret material.

The state may contain:

- ACME account private key material
- certificate private key material
- order and authorization URLs
- DNS-01 challenge metadata
- certificate issuance state

Extend0 supports `--protect-state passphrase` as the first portable state protection mode.

Under major `1`:

- passphrase-protected state uses PBKDF2-HMAC-SHA256 for key derivation
- passphrase-protected state uses AES-256-GCM for authenticated encryption
- protected state files must fail closed when opened without a passphrase
- protected state files must fail closed when opened with the wrong passphrase
- CLI users should prefer `--state-passphrase-env <name>` over `--state-passphrase <value>` to avoid shell history leakage
- unprotected state remains supported for compatibility and explicit local experiments, but it must be treated as insecure for real certificate material

## Certificate Output

`finalize` writes certificate material for use by Lifecycle TLS deployments.

Under major `1`:

- certificate chain output is written as PEM
- certificate private key output is written as PEM
- PFX output is optional and requires an explicit password
- output private keys must be treated as sensitive even when the ACME state file is protected
- Extend0 does not yet provide portable OS-level ACL hardening for output directories
- users remain responsible for filesystem permissions, secret distribution, and deployment-specific key custody

## DNS Provider Boundary

Manual DNS publication is the first supported provisioning model.

This is intentional:

- DNS provider APIs are provider-specific
- provider credentials are sensitive and must not be improvised
- some deployments intentionally require manual approval before publishing validation records
- manual publication works for any authoritative DNS provider

Future provider plugins may implement the `IDns01RecordProvisioner` boundary for Cloudflare, Route53, Azure DNS, custom internal DNS, or other providers.

Provider plugins must not change the DNS-01 proof calculation rules. They only publish, observe, and optionally clean up DNS records.

## Security Semantics

DNS-01 proves operational control over the DNS zone for a domain. It does not prove legal ownership, organizational identity, service authorization, or user identity.

ACME-issued certificates provide TLS server identity for hostnames included in the certificate. They do not authorize RPC methods, grant client access, or replace Lifecycle RPC authentication.

TLS certificate validation belongs to transport security and is governed by ADR 1-018. RPC authentication belongs to the Lifecycle protocol layer and is governed by ADR 1-017. ACME DNS-01 certificate lifecycle supplies certificate material for TLS; it does not replace either transport validation or RPC authentication.

## CLI Contract

The CLI certificate surface is:

- `extend0 lifecycle certificate dns-01 --domain <domain> --token <token> --key-authorization <value>`
- `extend0 lifecycle certificate dns-01 --domain <domain> --token <token> --account-thumbprint <value>`
- `extend0 lifecycle certificate dns-01 order --domain <domain> --email <email> --accept-terms --state <path>`
- `extend0 lifecycle certificate dns-01 validate --state <path>`
- `extend0 lifecycle certificate dns-01 status --state <path>`
- `extend0 lifecycle certificate dns-01 finalize --state <path> --out <directory>`

The direct `--token` forms prepare DNS-01 proof material from an existing challenge.

The `order`, `validate`, `status`, and `finalize` forms operate on ACME DNS-01 order state.

CLI output may show DNS TXT names and TXT values because those records must be published. CLI output must not print ACME account private keys, certificate private keys, passphrases, or raw protected state payloads.

## Consistency Rules

- Certificate lifecycle belongs to Lifecycle, not MetaDB.
- ACME DNS-01 state protection must remain explicit and visible in CLI output.
- State protection and certificate output protection are separate concerns.
- `--protect-state passphrase` protects the ACME state file, not the final PEM/PFX output unless future work explicitly adds output protection.
- The default ACME environment should prefer staging unless production is explicitly requested.
- DNS-01 proof calculation must remain independent of DNS provider integration.
- Provider-specific DNS automation must not bypass the same TXT proof model used by the manual provisioner.
- Certificate lifecycle docs must warn that DNS-01 proves DNS control, not legal domain ownership or RPC authorization.

## Deferred Work

Future ADRs or implementation plans may add:

- automated DNS provider plugins
- certificate renewal scheduling
- secure output directory policies
- OS-specific ACL hardening
- PFX-only or hardware-backed key output modes
- integration commands that validate PEM/PFX files for `TlsTcpSocket`
- integration commands that start owners directly from issued certificate material
- revocation support
- account key rotation
- provider credential secret-store integration

## Relationship To Existing ADRs

This ADR extends ADR 1-018 by defining how Lifecycle can obtain certificate material for `TlsTcpSocket`.

It complements ADR 1-017 because ACME certificates and TLS server identity do not replace Lifecycle RPC authentication.

It complements ADR 1-014 because diagnostics may later inspect certificate, hostname, and TLS readiness as part of Lifecycle owner observation.

It complements ADR 1-022 because protected ACME state is another Lifecycle assurance concern, but certificate lifecycle has its own certificate-specific state, output, and DNS provider boundaries.

## Consequences

- Extend0 can obtain public ACME certificates without requiring HTTP challenge reachability.
- Wildcard and private-host deployment patterns become feasible through DNS-01.
- Users can keep ACME state encrypted with a portable passphrase-based mechanism.
- Manual DNS publication is usable immediately while provider automation remains cleanly deferred.
- Private key output remains a security-sensitive deployment responsibility until future output protection work is accepted.
