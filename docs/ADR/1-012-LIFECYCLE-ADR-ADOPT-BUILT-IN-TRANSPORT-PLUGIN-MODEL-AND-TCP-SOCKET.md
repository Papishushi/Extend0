# ADR 1-012: Adopt Built-In Transport Plugin Model and TCP Socket Transport

## Status

Accepted

## Date

2026-06-20

## Context

Lifecycle already treats transport as an architectural axis rather than as an implementation detail of named pipes.

Before this decision, the runtime had a transport factory and custom injection points, but `NamedPipe` was the only built-in transport that could be created by the factory. That made the model conceptually broader than the implementation.

## Decision

Extend0 adopts a built-in transport plugin model for Lifecycle cross-process access.

Under major `1`:

- `NamedPipe` remains a built-in transport.
- `TcpSocket` is added as a second built-in transport.
- both built-in transports use the same JSON-RPC-over-NDJSON wire protocol id: `extend0-jsonrpc-ndjson`.
- both built-in transports use the shared `CrossProcessHandshake` protocol descriptor validation.
- transport selection is resolved through `CrossProcessTransportFactory`.
- custom transports remain supported through client and server host factories.

## TCP Endpoint Rules

`TcpSocket` requires an explicit endpoint.

The endpoint should use `host:port` form, for example:

- `127.0.0.1:43001`
- `localhost:43001`
- `tcp://127.0.0.1:43001`

Lifecycle must not invent a default TCP port from service identity. Automatic port assignment would make non-owner clients unable to discover the owner reliably without a coordination backend.

## Protocol Rules

The transport is responsible for carrying bytes.

The wire protocol is described separately by `CrossProcessProtocolDescriptor`.

For built-in `NamedPipe` and `TcpSocket`, the current descriptor is:

- protocol id: `extend0-jsonrpc-ndjson`
- protocol version: `1`

The handshake must identify:

- runtime fingerprint
- transport kind
- protocol id
- protocol version

Clients must reject handshakes whose transport or protocol descriptor does not match their expected descriptor.

## Consequences

- Lifecycle now has more than one real built-in transport.
- Tests can verify that transport abstraction is not just a named-pipe wrapper.
- TCP can support local and network-reachable scenarios when callers provide a stable endpoint.
- Future transports should integrate through the same factory/context/descriptor model.
- A future coordination backend may be needed before TCP endpoints can be discovered automatically.

## Non-Goals

- This ADR does not define TLS, authentication, authorization, or encryption for TCP.
- This ADR does not define automatic port discovery.
- This ADR does not make TCP the default transport.
- This ADR does not remove the compatibility `PipeName` field from `ServiceInfo`.

## Governing Baseline

This ADR is governed by ADR 1-000 and extends the Lifecycle architecture defined by ADR 1-004 and ADR 1-006.
