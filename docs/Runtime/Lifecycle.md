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

## Resolution Model

`Lifecycle` uses a stable access surface while allowing different resolution modes underneath:

- in `SingletonMode.InProcess`, the service resolves directly to the live instance in the current process
- in `SingletonMode.CrossProcess`, the owner process resolves directly while client processes resolve through a proxy
- the consumer-facing contract remains stable across those cases

## Current Implementation Truth

- named pipes are the current built-in transport used by the cross-process runtime
- the architecture already treats transport as an abstraction, but the runtime is not yet fully pluggable end-to-end
- the current public contract still contains pipe-centric naming in places, and that is a known cleanup target rather than accepted conceptual truth

## Current Cleanup Targets

- `CrossProcessOrchestator` naming typo
- pipe-centric wording in `ICrossProcessService` and `ServiceInfo`
- architecture/runtime gap between abstract transport and named-pipe-centered orchestration

## Governing ADRs

- [ADR 1-004](../ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md)
- [ADR 1-006](../ADR/1-006-LIFECYCLE-ADR-DEFINE-SINGLETON-SERVICE-RESOLUTION-PIPELINE.md)
- [ADR 1-009](../ADR/1-009-ARCHITECTURE-ADR-PRIORITIZE-PLATFORM-CORE-CONSOLIDATION-FOR-MAJOR-1.md)
