# ADR 1-013: Adopt Unix Domain Socket Built-In Transport

## Status

Accepted

## Date

2026-06-20

## Context

ADR 1-012 established the Lifecycle built-in transport plugin model and added `TcpSocket` beside `NamedPipe`.

That model should not remain biased toward either named pipes or TCP. Extend0 also needs a local socket transport that is path-based, service-identity-derived when possible, and suitable for platforms that support Unix domain sockets.

## Decision

Extend0 adds `UnixDomainSocket` as a built-in Lifecycle transport.

Under major `1`:

- `NamedPipe` remains the default built-in transport.
- `TcpSocket` remains the built-in network-capable socket transport that requires an explicit `host:port` endpoint.
- `UnixDomainSocket` becomes a local built-in transport that uses a socket path endpoint.
- `UnixDomainSocket` uses the same JSON-RPC-over-NDJSON protocol id as the other built-ins: `extend0-jsonrpc-ndjson`.
- `UnixDomainSocket` uses the shared `CrossProcessHandshake` descriptor validation.
- `UnixDomainSocket` is selected and instantiated through `CrossProcessTransportFactory`.

## Endpoint Rules

When an explicit endpoint is supplied, `UnixDomainSocket` treats it as a socket path.

When no explicit endpoint is supplied, Lifecycle derives a deterministic path from service identity under the OS temp directory.

This is intentionally different from `TcpSocket`: Unix domain sockets are local and path-addressed, so a deterministic identity-derived endpoint can be shared by owner and client processes without allocating or discovering a port.

## Protocol Rules

The transport carries bytes only.

The wire protocol remains described by `CrossProcessProtocolDescriptor`.

For built-in `UnixDomainSocket`, the descriptor is:

- transport kind: `UnixDomainSocket`
- protocol id: `extend0-jsonrpc-ndjson`
- protocol version: `1`

Clients must reject handshakes whose declared transport kind or protocol descriptor does not match the expected descriptor.

## Consequences

- Lifecycle now has three real built-in transports: `NamedPipe`, `UnixDomainSocket`, and `TcpSocket`.
- Local socket scenarios can avoid named-pipe-specific semantics while still using the same Lifecycle access model.
- The transport abstraction is validated by another carrier that is neither named-pipe-specific nor TCP-port-based.
- Future local IPC work should distinguish endpoint path lifecycle and stale socket cleanup from wire-protocol compatibility.

## Non-Goals

- This ADR does not make `UnixDomainSocket` the default transport.
- This ADR does not define remote Unix socket forwarding.
- This ADR does not define authentication, authorization, or encryption for Unix domain sockets.
- This ADR does not define automatic stale socket recovery after process crashes.
- This ADR does not remove the compatibility `PipeName` field from `ServiceInfo`.

## Governing Baseline

This ADR is governed by ADR 1-000 and extends the Lifecycle architecture defined by ADR 1-004, ADR 1-006, and ADR 1-012.
