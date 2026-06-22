# ADR 1-018: Adopt TLS TCP Socket Built-In Transport

## Status

Accepted

## Date

2026-06-21

## Context

ADR 1-012 added raw `TcpSocket` as a built-in transport for Lifecycle RPC. Raw TCP is useful for local/private experiments and custom secured deployments, but it does not provide confidentiality, integrity, or certificate-based peer trust by itself.

ADR 1-017 added Lifecycle RPC authentication, including shared-secret HMAC. That authenticates knowledge of a secret at the RPC handshake layer, but it does not encrypt traffic.

Extend0 needs a built-in network-capable transport that can protect the channel before the JSON-RPC NDJSON protocol handshake begins.

## Decision

Extend0 adopts `TransportKind.TlsTcpSocket` as a built-in Lifecycle transport.

Under major `1`:

- `TlsTcpSocket` uses TCP connectivity plus `SslStream`.
- TLS negotiation completes before the Extend0 `HELLO` protocol handshake is sent.
- Server-side hosts require `CrossProcessTlsOptions` with a server certificate.
- Client-side transports validate the server certificate through normal platform validation unless a caller provides an explicit validation callback.
- `CrossProcessTlsOptions` can carry client certificates and server-side `RequireClientCertificate` for mutual TLS.
- `TlsTcpSocket` uses the same `extend0-jsonrpc-ndjson` protocol id and version as other built-ins, scoped to `TransportKind.TlsTcpSocket`.
- Shared-secret HMAC authentication remains available as an optional RPC-layer proof inside the TLS channel.

## Security Semantics

TLS provides transport confidentiality, integrity, and server authentication.

Mutual TLS can provide client authentication when the owner requires and validates a client certificate.

TLS does not replace method-level or resource-level authorization. Future authorization policy work remains required before exposed service surfaces should be considered safe for multi-tenant or hostile environments.

Raw `TcpSocket` remains available, but sensitive network deployments should prefer `TlsTcpSocket` or a custom transport with equivalent protection.

## Consistency Rules

- `TcpSocket` and `TlsTcpSocket` must remain distinct transport kinds.
- `TlsTcpSocket` endpoints use the same explicit `host:port` endpoint convention as `TcpSocket`.
- Server certificates and private keys must not be generated implicitly by the runtime.
- CLI diagnostics may use platform certificate validation and target-host configuration, but must not add insecure trust-bypass defaults.
- TLS configuration must flow through transport factory contexts so custom lifecycle setup can reason about it consistently.

## Relationship To Existing ADRs

This ADR extends ADR 1-012 by adding a secured TCP built-in beside raw `TcpSocket`.

It extends ADR 1-017 by providing a transport-security layer that complements RPC authentication.

It complements ADR 1-014 because `lifecycle probe` and `lifecycle diagnose` can resolve and connect with `TlsTcpSocket`.

## Consequences

- Lifecycle now has a built-in encrypted network-capable transport.
- Owners can require mTLS by setting `RequireClientCertificate`.
- Consumers can validate self-signed, private CA, or public CA certificates through `CrossProcessTlsOptions`.
- Production network exposure still requires careful certificate lifecycle, authorization policy, logging, and deployment hardening.
