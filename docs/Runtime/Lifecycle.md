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
- `heartbeat`
- `transport factory`

## Resolution Model

`Lifecycle` uses a stable access surface while allowing different resolution modes underneath:

- in `SingletonMode.InProcess`, the service resolves directly to the live instance in the current process
- in `SingletonMode.CrossProcess`, the owner process resolves directly while client processes resolve through a proxy
- the consumer-facing contract remains stable across those cases

## Current Implementation Truth

- `NamedPipe`, `UnixDomainSocket`, and `TcpSocket` are the current built-in transports used by the cross-process runtime
- `UnixDomainSocket` is local-only and derives a deterministic socket path from service identity unless an explicit path is supplied
- `TcpSocket` requires an explicit `host:port` endpoint; Lifecycle does not infer TCP ports from service identity
- all built-ins use the `extend0-jsonrpc-ndjson` protocol descriptor and shared handshake validation
- the runtime now supports custom client and owner-side server transport factories for callers that provide compatible endpoint semantics and protocol descriptors
- the current public contract still contains pipe-centric naming in places, and that is a known cleanup target rather than accepted conceptual truth

## Current Cleanup Targets

- remaining pipe-centric compatibility surface around `ServiceInfo.PipeName`
- future transport additions should continue through `CrossProcessTransportFactory` and protocol descriptors
- TCP endpoint discovery still needs an explicit coordination story before automatic endpoint assignment is safe
- Unix domain socket stale path cleanup policy may need hardening for crash-recovery scenarios

## Governing ADRs

- [ADR 1-004](../ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md)
- [ADR 1-006](../ADR/1-006-LIFECYCLE-ADR-DEFINE-SINGLETON-SERVICE-RESOLUTION-PIPELINE.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
- [ADR 1-012](../ADR/1-012-LIFECYCLE-ADR-ADOPT-BUILT-IN-TRANSPORT-PLUGIN-MODEL-AND-TCP-SOCKET.md)
- [ADR 1-013](../ADR/1-013-LIFECYCLE-ADR-ADOPT-UNIX-DOMAIN-SOCKET-BUILT-IN-TRANSPORT.md)
