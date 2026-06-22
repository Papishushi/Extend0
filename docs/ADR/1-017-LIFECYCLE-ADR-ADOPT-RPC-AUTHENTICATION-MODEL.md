# ADR 1-017: Adopt RPC Authentication Model

## Status

Accepted

## Date

2026-06-21

## Context

Lifecycle cross-process services can expose RPC access over multiple transports. The existing handshake verifies protocol compatibility, transport kind, protocol id, protocol version, and build fingerprint, but compatibility is not trust.

Without a real authentication boundary, any process that can reach an endpoint may attempt RPC calls. That is acceptable only for explicitly local, compatibility-mode deployments protected by operating-system endpoint permissions.

OWASP API guidance treats broken authentication and broken authorization as separate risks. Extend0 therefore needs an authentication model before it expands further into TCP, cross-host, BFF, or cross-service deployment patterns.

## Decision

Extend0 adopts transport-neutral Lifecycle RPC authentication.

Under major `1`:

- `AuthenticationMode` defines the authentication mechanism advertised by the Lifecycle handshake.
- `CrossProcessAuthenticationOptions` carries authentication configuration through singleton options, orchestrator, transport factory contexts, built-in client transports, and owner hosts.
- Built-in transports support `AuthenticationMode.None` for compatibility, `AuthenticationMode.SharedSecretHmac` for shared-secret challenge-response authentication, and `AuthenticationMode.SignedChallenge` for asymmetric challenge-response authentication.
- `SharedSecretHmac` uses a server nonce in the `HELLO` line and requires the client to return an HMAC proof before any JSON-RPC method can be invoked.
- `SignedChallenge` uses a server nonce in the `HELLO` line and requires the client to return a public-key signature proof before any JSON-RPC method can be invoked.
- The shared secret is never sent on the wire by the built-in handshake and must not be logged or serialized in CLI reports.
- `extend0 lifecycle probe` and `extend0 lifecycle diagnose` may authenticate with `--auth shared-secret-hmac --secret <value>`.
- CLI key-loading for `SignedChallenge` is deferred; library consumers can configure signed-challenge authentication directly with `CrossProcessAuthenticationOptions`.
- `None` remains the default for backwards compatibility, but it must be understood as unauthenticated compatibility mode, not as a secure network posture.

## Security Semantics

Authentication answers "who or what can prove access to this endpoint?"

It does not replace authorization. A future ADR must define RPC authorization policies for method-level and resource-level access control before Extend0 treats remote administrative surfaces as safe for multi-tenant or hostile environments.

`SharedSecretHmac` proves knowledge of a shared secret and prevents the secret from being transmitted directly. It does not encrypt traffic. Sensitive network deployments should prefer `TlsTcpSocket`, a custom transport with equivalent protection, or a trusted authenticated boundary.

`SignedChallenge` proves possession of a private key without sending that key to the owner. The built-in helper currently supports ECDSA/SHA-256 and also allows custom signer/verifier delegates for other algorithms. It authenticates the peer at the Extend0 protocol layer but, like HMAC, does not encrypt traffic.

TLS and mTLS are transport-security concerns governed by ADR 1-018. They may be combined with protocol authentication, but they are not represented as `AuthenticationMode` values.

## Supported Modes

- `None`: compatibility mode; endpoint reachability is not proof of trust.
- `SharedSecretHmac`: built-in challenge-response using a shared secret and server nonce.
- `SignedChallenge`: built-in asymmetric challenge-response using a server nonce, key id, algorithm label, and signature.
- `OsIdentity`: reserved for OS-backed peer identity and endpoint ACL integration.
- `Custom`: reserved for custom transports that enforce their own authentication contract.

## Consistency Rules

- Protocol compatibility handshake and authentication must remain distinct concepts.
- A client must not invoke RPC methods until the required authentication proof has been accepted.
- Built-in transports must fail closed when the owner requires an unsupported or mismatched authentication mode.
- Authentication configuration must flow through custom transport factory contexts so non-built-in transports can honor the same platform contract.
- Diagnostic output may report authentication mode, but must never print shared secrets or raw credential material.

## Relationship To Existing ADRs

This ADR extends ADR 1-004 by adding trust establishment to Lifecycle service access.

It extends ADR 1-012 and ADR 1-013 by applying the same authentication contract to `NamedPipe`, `TcpSocket`, and `UnixDomainSocket`. ADR 1-018 extends the protected transport model with `TlsTcpSocket`.

It extends ADR 1-014 by making `lifecycle diagnose` capable of authenticated owner observation.

It complements ADR 1-016 because leases identify ownership state but do not authenticate peers.

## Consequences

- Local compatibility scenarios continue to work without authentication configuration.
- Secure deployments can require a shared-secret proof before accepting RPC calls.
- Deployments that should avoid shared secrets can require signed-challenge authentication and validate clients by public key.
- TCP remains unsuitable for sensitive network use without additional transport security such as TLS/mTLS.
- Future work can add authorization, OS identity, CLI key loading, and richer key rotation/discovery without changing the high-level Lifecycle security model.
