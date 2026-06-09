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
- `Extend0.Cli` is distributed as a dotnet tool package with command name `extend0`

## CLI Tool Packaging

`Extend0.Cli` is the platform diagnostic companion for Extend0. It can be used from source during development:

```bash
dotnet run --project Extend0.Cli -- doctor
```

It can also be packed as a dotnet tool:

```bash
dotnet pack Extend0.Cli/Extend0.Cli.csproj -c Release
```

Install from a local package output when testing a release candidate:

```bash
dotnet tool install --global Extend0.Cli --add-source Extend0.Cli/bin/Release
extend0 --help
```

If the tool is already installed locally or globally, update it from the same source:

```bash
dotnet tool update --global Extend0.Cli --add-source Extend0.Cli/bin/Release
```

The CLI package is for diagnostics and validation. It does not replace the main `Extend0` library package or direct project references for development.

## Documentation Rule

Do not describe a packaging path as the primary recommendation if it is known to be weaker than direct source consumption for the current phase.
