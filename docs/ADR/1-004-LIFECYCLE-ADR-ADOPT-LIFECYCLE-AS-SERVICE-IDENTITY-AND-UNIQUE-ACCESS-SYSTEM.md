# ADR 1-004: Adopt Lifecycle as Service Identity and Unique Access System

## Status

Accepted

## Date

2026-05-24

## Context

The `Extend0.Lifecycle` code is more than singleton utilities. It already contains concepts for:

- in-process uniqueness
- cross-process ownership
- transport-mediated service access
- remote service diagnostics and liveness

Those concerns are cohesive enough to be treated as a system, not only as helpers around `Singleton`.

## Decision

Extend0 adopts `Lifecycle` as the system responsible for service identity, unique access, ownership, and coordinated access resolution.

Within major `1`, the core domain concepts of `Lifecycle` are:

- service identity
- access surface
- ownership
- owner role
- client role
- execution scope
- transport
- liveness signal

## Responsibilities

`Lifecycle` is responsible for:

- ensuring unique access within an execution scope
- resolving whether a participant is owner or client
- exposing a stable service access surface
- coordinating transport-backed access when uniqueness crosses process boundaries
- providing minimal liveness and service identity diagnostics

`Lifecycle` is not, by itself, the owner of structured metadata storage. That remains the responsibility of `MetaDB`.

## Scope Model

The current conceptual scopes are:

- process scope
- machine scope
- network scope

The implementation may support these unevenly, but the architecture recognizes all three as valid scopes for the system.

## Consequences

- Singleton behavior should be documented as part of Lifecycle, not as a disconnected pattern.
- Transport abstractions belong conceptually to Lifecycle.
- Future coordination backends, including MetaDB-backed ones, are in-bounds for Lifecycle evolution.
