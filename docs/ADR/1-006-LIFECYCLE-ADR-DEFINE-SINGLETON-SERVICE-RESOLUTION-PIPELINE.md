# ADR 1-006: Define Singleton Service Resolution Pipeline

## Status

Accepted

## Date

2026-05-24

## Context

The `CrossProcessSingleton<TService>` implementation already encodes a meaningful resolution flow:

- identify the service
- determine mode
- determine ownership
- resolve direct instance or proxy
- expose a stable `Service` surface to consumers

This is a domain pipeline as much as an implementation pipeline, and it should be documented as such.

## Decision

Extend0 defines the singleton service resolution pipeline as a Lifecycle pipeline.

The conceptual stages are:

1. establish the service identity
2. determine the singleton mode
3. determine whether the current participant is owner or client
4. resolve transport and endpoint requirements when coordination crosses process boundaries
5. resolve the consumer-facing service as either a direct instance or a proxy
6. maintain liveness and upgrade or reconnect behavior as needed

## Resolution Rules

- In `InProcess` mode, the service resolves as a direct instance.
- In `CrossProcess` mode, the owner resolves the service as a direct instance.
- In `CrossProcess` mode, a non-owner resolves the service as a proxy.
- Consumers should reason about one access surface and one service identity even when the underlying resolution differs.

## Architectural Constraints

- Transport choice is an implementation of the pipeline, not the definition of the pipeline.
- Ownership is part of the domain semantics, not merely mutex state.
- Upgrade and reconnection behavior belong to the same resolution pipeline because they preserve service identity continuity.

## Consequences

- The ontology can describe singleton resolution independently from named pipes.
- Lifecycle docs can explain owner and client behavior without overfocusing on individual classes.
- Alternative transports remain compatible with the same conceptual pipeline.
