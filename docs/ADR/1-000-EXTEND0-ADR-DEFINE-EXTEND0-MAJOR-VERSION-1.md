# ADR 1-000: Define Extend0 Major Version 1

## Status

Accepted

## Date

2026-05-24

## Context

Extend0 is introducing a repository-level documentation and ontology governance system. Those systems need a stable baseline so that filenames, numbering, future policy changes, and cross-document references remain coherent over time.

The repository also needs a way to distinguish durable governance eras from package implementation details. Without that distinction, future contributors could misread documentation numbering as a package version promise or change conventions without a clear supersession path.

At this stage, the repository does not yet have a prior ADR lineage that defines how architectural governance evolves. That makes the first governance decision unusually important: it needs to establish both the current baseline and the mechanism for changing the baseline later without ambiguity.

This ADR therefore serves two roles:

- it defines the active governance major for the repository
- it defines how later ADRs relate to that governance major

Extend0 also needs a clearer statement of product intent. The repository is no longer well described as only a small utility library or a grab bag of unrelated components. The current direction is that Extend0 major `1` should be understood as the first explicit platform era for a set of cooperating systems:

- lifecycle and unique-access coordination
- metadata storage and structured domain state through MetaDB
- ontology-backed domain modeling and semantic interoperability
- code generation and future language/runtime integration

That direction matters because the ontology and documentation systems are not neutral additions. They are part of an attempt to make Extend0 legible as a platform, not just as an implementation.

There is also a broader ecosystem context. Extend0 is being developed in proximity to the UByteC ecosystem, and MetaDB itself originates from metadata-system needs around UByteC AST modeling. That history should be made explicit at the governance level so that future contributors understand why metadata, ontology, language/runtime concerns, and cross-service integration are all appearing in the same architectural conversation.

At the same time, the repository must not pretend that every strategic direction is already implemented. The governance baseline needs to distinguish:

- what major `1` means conceptually and architecturally
- what is already implemented in code
- what remains a declared direction for future ADRs

## Decision

Extend0 is governed under repository major baseline `1`.

Under this baseline:

- all initial ADRs for this initiative use the prefix `1-*`
- documentation governance introduced by ADR 1-001 is normative under major `1`
- ontology governance introduced by ADR 1-002 is normative under major `1`
- incompatible future changes to docs or ontology governance must be introduced by a new ADR that explicitly supersedes this baseline or a later ADR derived from it

Under repository major `1`, Extend0 is also defined as a platform-oriented architectural era rather than as a purely package-oriented era.

That means the major `1` baseline includes these platform-level statements:

- Extend0 is treated as a composition of cooperating systems rather than a single isolated subsystem.
- `Lifecycle` and `MetaDB` are first-class systems within that composition.
- ontology is expected to become part of the architectural core of the platform rather than an external documentation convenience.
- Extend0 is understood as developing inside, and potentially helping structure, the wider UByteC ecosystem.

Within that ecosystem framing, the following strategic directions are explicitly recognized under major `1`:

- MetaDB has historical roots in the metadata needs of UByteC, especially around structured metadata and AST-adjacent modeling.
- Extend0 may later grow a native ontology subsystem that makes ontology integration easier across the platform.
- Extend0 may later support ontology-aware cross-service integration with persistent backing through MetaDB.
- UByteC may later become a first-class language or runtime integration target for Extend0.

These are governance-recognized directions, not immediate implementation guarantees. They are allowed and supported by the major `1` baseline, but they still require later ADRs and code changes before they become operational commitments.

This major baseline is a repository and architecture governance marker. It does not, by itself, change package versioning behavior, release semantics, or binary compatibility guarantees.

Repository major `1` is the governance umbrella under which the initial documentation and ontology conventions are introduced. It is intentionally broader than a single subsystem, but intentionally narrower than product marketing or package-release semantics.

## Scope

This ADR governs:

- ADR numbering for the current repository governance era
- the baseline under which documentation and ontology governance decisions are adopted
- the meaning of "major `1`" when used in ADR, docs-governance, and ontology-governance contexts
- the platform-level interpretation of Extend0 as a composition of cooperating systems
- the recognition of UByteC ecosystem alignment as an architectural context for major `1`

This ADR does not govern:

- NuGet package semantic versioning
- assembly version increments
- binary compatibility guarantees for code artifacts
- release cadence or packaging policy
- automatic adoption of any future subsystem, language runtime, or ontology-integration mechanism without further ADRs

## Platform Interpretation of Major 1

Under major `1`, Extend0 should be interpreted as a platform with an evolving but coherent systems model.

The current platform reading is:

- `Lifecycle` governs unique access, ownership, singleton coordination, and transport-mediated access.
- `MetaDB` governs structured metadata storage, schema-defined tables, references, and coordination-ready state.
- ontology governance defines how domain meaning is stabilized above implementation names.
- code generation supports declarative or schema-driven expansion of the system.

This interpretation allows the repository to evolve toward stronger integration between these systems without requiring that all integrations already exist in the current codebase.

## Ecosystem Alignment

Major `1` explicitly records that Extend0 is not architecturally isolated from UByteC.

The governance interpretation is:

- Extend0 may act as infrastructure for UByteC-related systems.
- UByteC-related metadata needs are part of the historical context for MetaDB.
- future runtime, language, ontology, or service-integration work may legitimately target ecosystem coherence rather than only local repository concerns.

This alignment should not be read as a claim that Extend0 and UByteC have already been fully unified. It is instead a declaration that such alignment is architecturally meaningful and in-bounds for major `1`.

## Strategic Directions Recognized but Not Yet Standardized

The following directions are recognized by this ADR as compatible with major `1`, but none are standardized solely by mentioning them here:

- integrating UByteC more directly into Extend0
- making ontology a more operational subsystem of Extend0
- supporting ontology-aware cross-service integration patterns
- using MetaDB as backing state for semantic or coordination workflows
- treating Extend0 as a home for ecosystem-level infrastructure rather than only repository-local helpers

Each of these directions still requires later ADRs if it changes public contracts, system boundaries, governance rules, or canonical subsystem roles.

## Consequences

- Extend0 now has a durable governance anchor for ADR numbering and future documentation policy.
- Future contributors can read `1-*` ADRs as belonging to the same repository governance era.
- Documentation and ontology conventions can evolve, but only through explicit architectural decisions.
- Package versioning remains a separate concern unless a later ADR binds the two systems together.
- Extend0 major `1` now has an explicit platform interpretation rather than only a numbering interpretation.
- UByteC ecosystem alignment is recognized as part of repository architecture context rather than left implicit.

Additional consequences:

- all foundational governance ADRs can now reference a single baseline instead of redefining their own numbering meaning
- future repository-wide convention changes become easier to review because they must state whether they are compatible with major `1`
- future contributors should not infer that a documentation major change automatically implies a code or package major change
- future contributors should distinguish clearly between current implementation truth and major `1` strategic direction
- ontology and platform work can now be justified as part of the major `1` baseline instead of appearing as unrelated initiatives

## What Counts as a Governance-Major Change

A future move from repository major `1` to repository major `2` should only happen if Extend0 changes one or more of the following in a materially incompatible way:

- ADR numbering rules or interpretation
- the governing structure of repository-wide documentation
- the canonical role of ontology in the repository
- the meaning of compatibility for repository-level governance artifacts
- the platform-level interpretation of Extend0 as a system-of-systems
- the declared architectural relationship between Extend0 and the UByteC ecosystem

The following are not, by themselves, governance-major changes:

- adding new ADRs within the current numbering scheme
- adding new docs sections compatible with the current documentation model
- extending ontology content without changing its governance model
- changing implementation details in code while preserving the established governance rules
- adding or removing implementation experiments that do not alter the major `1` governance interpretation
- exploring UByteC integration directions without yet changing the governance model

## Versioning Rules

- ADR filenames for the current governance era use `1-{NNN}-{SUBSYSTEM OR ARCHITECTURE}-ADR-{TITLE}.md`.
- Sequence `000` is reserved for baseline or governance-defining decisions.
- Later ADRs in the same governance era continue with `1-001`, `1-002`, and so on.
- A future governance era must use a new leading major and be established by an explicit ADR.
- Governance-major changes should only be used for incompatible changes to repository-wide rules, conventions, or architectural governance.

Interpretation rules:

- the leading `1` is a governance-major marker, not a package version alias
- the sequence number is chronological within the governance major
- title slugs should remain stable once published unless a correction is unavoidable

## Responsibilities

Contributors creating later ADRs under major `1` must:

- preserve the meaning of repository major `1`
- reference this ADR when introducing repository-wide conventions that rely on the governance baseline
- state explicitly when a new ADR is compatible with the major `1` baseline
- avoid overloading package version language into ADR numbering unless a later ADR formalizes that relationship
- distinguish between platform vision, architectural intent, and implementation status
- avoid presenting a strategic direction recognized here as a standardized subsystem unless a later ADR defines it

Contributors working on future UByteC or ontology integration should:

- treat this ADR as permission to explore those directions within major `1`
- create follow-up ADRs when such exploration hardens into canonical architecture or public contracts
- keep the terminology of docs, ontology, and code aligned as those directions mature

## Supersession Rules

- No document may silently redefine the meaning of repository major `1`.
- A future move to repository major `2` requires a new ADR that explicitly supersedes this one.
- Any ADR that changes numbering, governance, or compatibility rules established here must reference this ADR directly and describe the transition impact.
- Until such an ADR exists, ADR 1-001 and ADR 1-002 remain normative under this baseline.

Superseding ADRs should explain:

- why major `1` is no longer sufficient
- what remains compatible from the prior baseline
- whether old ADR filenames remain part of the historical record unchanged
- how readers should interpret mixed-major ADR histories in the repository
- whether the relationship between Extend0 and the UByteC ecosystem has changed materially
