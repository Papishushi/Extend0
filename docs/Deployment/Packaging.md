# Packaging and Consumption

## Purpose

State the current recommended way to consume Extend0 while the major `1` platform contract is being consolidated.

## Audience

This page is for consumers and maintainers responsible for project references, package distribution, or release guidance.

## Recommended Path Right Now

Use direct project references when:

- you are developing inside the same solution
- you need the source generators in lockstep with your code
- you want the most reliable path while generator packaging is still being tightened

## Distribution Notes

- NuGet remains an intended distribution surface
- repository releases remain useful for pinned binary consumption
- generator packaging should be described conservatively until the consumer experience is fully hardened

## Documentation Rule

Do not describe a packaging path as the primary recommendation if it is known to be weaker than direct source consumption for the current phase.
