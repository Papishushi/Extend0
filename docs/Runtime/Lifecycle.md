# Lifecycle Runtime Model

## Purpose

Describe the current operational semantics of `Lifecycle` in Extend0 major `1`.

## Audience

This page is for contributors working on singleton behavior, ownership, transport, or cross-process service access.

## Core Runtime Concepts

- `service identity`
- `access surface`
- `owner role`
- `client role`
- `in-process resolution`
- `cross-process resolution`
- `transport`
- `authentication mode`
- `heartbeat`
- `transport factory`
- `assurance policy`
- `assurance evidence`

## Resolution Model

`Lifecycle` uses a stable access surface while allowing different resolution modes underneath:

- in `SingletonMode.InProcess`, the service resolves directly to the live instance in the current process
- in `SingletonMode.CrossProcess`, the owner process resolves directly while client processes resolve through a proxy
- the consumer-facing contract remains stable across those cases

## Current Implementation Truth

- `NamedPipe`, `UnixDomainSocket`, `TcpSocket`, and `TlsTcpSocket` are the current built-in transports used by the cross-process runtime
- `UnixDomainSocket` is local-only and derives a deterministic socket path from service identity unless an explicit path is supplied
- `TcpSocket` requires an explicit `host:port` endpoint; Lifecycle does not infer TCP ports from service identity
- `TlsTcpSocket` also requires an explicit `host:port` endpoint and negotiates TLS before the Extend0 protocol handshake
- all built-ins use the `extend0-jsonrpc-ndjson` protocol descriptor and shared handshake validation
- built-in transports support unauthenticated compatibility mode, shared-secret HMAC challenge-response authentication, and signed-challenge authentication
- shared-secret HMAC and signed challenge authenticate a peer before RPC dispatch, but they do not encrypt traffic; sensitive network deployments should prefer `TlsTcpSocket` or a custom transport with equivalent protection
- TLS and mTLS are transport-security concerns configured on TLS-capable transports; they are separate from protocol authentication modes
- `extend0 lifecycle diagnose` is the current CLI surface for observing owner process details, handshake status, heartbeat liveness, and lease exposure status
- `extend0 lifecycle assurance storage diagnose` is the Lifecycle-facing CLI surface for storage protection assurance evidence
- `Extend0.Lifecycle.Assurance` owns cross-service protection, continuity, and attestation policy/evidence models
- the runtime now supports custom client and owner-side server transport factories for callers that provide compatible endpoint semantics and protocol descriptors
- the current public contract still contains pipe-centric naming in places, and that is a known cleanup target rather than accepted conceptual truth

## Current Cleanup Targets

- remaining pipe-centric compatibility surface around `ServiceInfo.PipeName`
- future transport additions should continue through `CrossProcessTransportFactory` and protocol descriptors
- TCP endpoint discovery still needs an explicit coordination story before automatic endpoint assignment is safe
- RPC authorization is still a separate follow-up; authentication proves peer access, but method/resource permission checks are not yet policy-driven
- Unix domain socket stale path cleanup policy may need hardening for crash-recovery scenarios
- broader Lifecycle assurance diagnostics for continuity and attestation are still follow-up work; MetaDB validation currently consumes those policies when declared on a `TableSpec`

## Governing ADRs

- [ADR 1-004](../ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md)
- [ADR 1-006](../ADR/1-006-LIFECYCLE-ADR-DEFINE-SINGLETON-SERVICE-RESOLUTION-PIPELINE.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
- [ADR 1-012](../ADR/1-012-LIFECYCLE-ADR-ADOPT-BUILT-IN-TRANSPORT-PLUGIN-MODEL-AND-TCP-SOCKET.md)
- [ADR 1-013](../ADR/1-013-LIFECYCLE-ADR-ADOPT-UNIX-DOMAIN-SOCKET-BUILT-IN-TRANSPORT.md)
- [ADR 1-014](../ADR/1-014-LIFECYCLE-ADR-ADOPT-LIFECYCLE-DIAGNOSTICS-COMMAND.md)
- [ADR 1-016](../ADR/1-016-LIFECYCLE-ADR-ADOPT-OBSERVABLE-LEASE-SNAPSHOT.md)
- [ADR 1-017](../ADR/1-017-LIFECYCLE-ADR-ADOPT-RPC-AUTHENTICATION-MODEL.md)
- [ADR 1-018](../ADR/1-018-LIFECYCLE-ADR-ADOPT-TLS-TCP-SOCKET-BUILT-IN-TRANSPORT.md)
- [ADR 1-022](../ADR/1-022-LIFECYCLE-ADR-PROMOTE-ASSURANCE-POLICIES-TO-LIFECYCLE.md)
