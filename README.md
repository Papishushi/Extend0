# Extend0 [![Codacy Badge](https://app.codacy.com/project/badge/Grade/8cf185b2f8204e80bb3419cbceb7362a)](https://app.codacy.com/gh/Papishushi/Extend0/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

Extend0 major `1` is being shaped as the infrastructure core of a wider ecosystem, not as a flat utility package. Its current center of gravity is the consolidation of four cooperating architectural assets:

- `Lifecycle` for service identity, unique access, ownership, and transport-mediated resolution
- `MetaDB` for structured metadata state, schemas, references, indexes, and coordination-ready storage
- ontology for domain meaning and cross-system consistency
- code generation for schema-driven derived artifacts

The immediate priority is platform-core consolidation: make the repository legible, internally consistent, and ready for the next layer of ecosystem work without pretending that every strategic direction is already implemented.

## Major 1 Direction

Extend0 major `1` should currently be read through these system boundaries:

- `Lifecycle` governs who owns a service, how a singleton resolves in-process or cross-process, and how a consumer reaches a stable access surface.
- `MetaDB` governs structured tables, schemas, rows, cells, references, indexes, and storage-backed operational state.
- ontology governs the canonical vocabulary used to describe the platform above implementation detail.
- code generation derives metadata-entry and blittable artifacts from declarative inputs.

This direction is aligned with the wider `UByteC` ecosystem, but Extend0 is not yet claiming a completed platform unification, a full ontology runtime, or a finished language integration story.

## Current Implementation Truth

The architecture is ahead of the implementation in a few visible places, and this repository now treats those gaps explicitly instead of hiding them:

- `Lifecycle` is architected around transport abstraction, but the current built-in cross-process transport is named pipes.
- `MetaDB` is a first-class system, but the public access story is centered on `MetaDBManagerSingleton` and RPC-safe contracts rather than direct public construction of the internal `MetaDBManager`.
- ontology is already part of the architecture contract, but the operational ontology subsystem is still in its foundation phase.
- `UByteC` is a strategic ecosystem direction, not yet the implementation center of the next milestone.

## Installation

Project references are currently the most reliable way to consume Extend0 while the major `1` platform story is being hardened:

```bash
dotnet add <YourProject>.csproj reference Extend0/Extend0.csproj
dotnet add <YourProject>.csproj reference Extend0.MetadataEntry.Generator/Extend0.MetadataEntry.Generator.csproj
dotnet add <YourProject>.csproj reference Extend0.BlittableAdapter.Generator/Extend0.BlittableAdapter.Generator.csproj
```

This is the recommended path when you want to:

- develop directly against Extend0
- debug or modify internals
- keep source generators in lockstep with your code
- avoid drift while packaging and generator-consumer workflows are still being tightened

### NuGet and prebuilt binaries

The main package is available through NuGet and repository releases remain useful for pinned binaries, but the safest onboarding path for source-generator-heavy work in the current phase is still direct project reference.

## CLI

`Extend0.Cli` is the command-line entry point for repository and platform diagnostics. It currently provides repo diagnostics, lifecycle probing, MetaDB inspection/validation, and ontology inspection/validation commands that are useful for humans and automation.

Run from source during development:

```bash
dotnet run --project Extend0.Cli -- doctor
dotnet run --project Extend0.Cli -- doctor --json
dotnet run --project Extend0.Cli -- doctor --repo <path-to-extend0>
dotnet run --project Extend0.Cli -- lifecycle probe
dotnet run --project Extend0.Cli -- lifecycle probe --name Extend0.Metadata.MetaDB --transport NamedPipe
dotnet run --project Extend0.Cli -- lifecycle probe --name Extend0.Metadata.MetaDB --json
dotnet run --project Extend0.Cli -- metadb inspect <path-to-table-or-spec>
dotnet run --project Extend0.Cli -- metadb inspect <path-to-table-or-spec> --json
dotnet run --project Extend0.Cli -- metadb validate <path-to-table-or-spec>
dotnet run --project Extend0.Cli -- metadb validate <path-to-table-or-spec> --json
dotnet run --project Extend0.Cli -- metadb schema <source-table-or-spec> <target-table-or-spec>
dotnet run --project Extend0.Cli -- metadb snapshot <path-to-table-or-spec> --out <snapshot-dir>
dotnet run --project Extend0.Cli -- metadb restore <snapshot-dir> --map-path <restore-map-or-table-dir>
dotnet run --project Extend0.Cli -- ontology inspect
dotnet run --project Extend0.Cli -- ontology inspect --json
dotnet run --project Extend0.Cli -- ontology validate
dotnet run --project Extend0.Cli -- ontology validate --json
```

Use `lifecycle probe --connect` when an owner process is expected to be reachable and you want the probe to test the client transport and handshake.

Pack and install as a dotnet tool when testing the distributable command:

```bash
dotnet pack Extend0.Cli/Extend0.Cli.csproj -c Release
dotnet tool install --global Extend0.Cli --add-source Extend0.Cli/bin/Release
extend0 --help
```

If the tool is already installed, use `dotnet tool update --global Extend0.Cli --add-source Extend0.Cli/bin/Release`.

The CLI is intentionally repo-first in this phase. Future commands should hang from the same entry point, for example richer transport diagnostics, executable MetaDB migration runners, and SPARQL-backed `ontology query`.

### Target framework

The library currently targets:

- `net10.0`

Current runtime logging dependencies are:

- `Microsoft.Extensions.Logging`
- `Microsoft.Extensions.Logging.Abstractions`
- `Microsoft.Extensions.Logging.Console`

## Lifecycle

`Lifecycle` is the system that gives Extend0 a stable service identity and unique-access story.

Its current public concepts are:

- `CrossProcessSingleton<TService>`
- `CrossProcessServiceBase<TService>`
- `ICrossProcessService`
- `SingletonMode`
- `CrossProcessSingletonOptions`

The important semantic rule is that consumers use a stable access surface while the resolution changes depending on mode and ownership:

- `SingletonMode.InProcess` resolves directly to the live instance inside the current process.
- `SingletonMode.CrossProcess` resolves directly for the owner process and through a proxy for client processes.
- the same access surface is preserved across those cases even though the resolution mode changes

Named pipes are the current built-in transport used by the cross-process runtime, but the architecture treats transport as an abstraction rather than as the permanent center of the system.

### Lifecycle quickstart

```csharp
using Extend0.Lifecycle.CrossProcess;
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Information));
using var singleton = new ClockSingleton(loggerFactory);

Console.WriteLine(ClockSingleton.IsOwner ? "Owner" : "Client");
Console.WriteLine(await ClockSingleton.Service.PingAsync());
Console.WriteLine(await ClockSingleton.Service.NowIsoAsync());

public interface IClock : ICrossProcessService
{
    Task<string> NowIsoAsync();
}

public sealed class Clock : CrossProcessServiceBase<IClock>, IClock
{
    protected override string? PipeName => "Extend0.Clock";
    public Task<string> NowIsoAsync() => Task.FromResult(DateTimeOffset.UtcNow.ToString("O"));
}

public sealed class ClockSingleton(ILoggerFactory loggerFactory) : CrossProcessSingleton<IClock>(
    () => new Clock(),
    new()
    {
        Mode = SingletonMode.CrossProcess,
        CrossProcessName = "Extend0.Clock",
        CrossProcessServer = ".",
        CrossProcessConnectTimeoutMs = 5000,
        Overwrite = true,
        Logger = loggerFactory.CreateLogger<CrossProcessSingletonOptions>()
    },
    loggerFactory)
{
}
```

## MetaDB

`MetaDB` is the structured metadata system of Extend0. It is not just a persistence helper: it is the table-oriented state model used to define stable schemas, rows, cells, references, and indexes.

Its current conceptual vocabulary is:

- system
- manager
- table
- schema
- column
- row
- cell
- reference
- index
- storage model
- access surface

The current public access story now has two explicit entry surfaces:

- `MetaDB.CreateManager(...)` for same-process/local usage through `IMetaDBManager`
- `MetaDB.CreateSingleton(...)` for shared owner/client usage through `MetaDBManagerSingleton` and `IMetaDBManagerRPCCompatible`

### MetaDB quickstart: same-process

```csharp
using Extend0.Metadata;
using Extend0.Metadata.Schema;

using var manager = MetaDB.CreateManager();

var spec = new TableSpec(
    Name: "Settings",
    MapPath: "./data/settings.meta",
    Columns: new[]
    {
        TableSpec.Helpers.Column("Entries", capacity: 512, keyBytes: 64, valueBytes: 512),
    });

var tableId = manager.RegisterTable(spec, createNow: true);
var table = manager.GetOrCreate(tableId);

Console.WriteLine(tableId);
Console.WriteLine(string.Join(", ", table.GetColumnNames()));
```

### MetaDB quickstart: shared singleton

```csharp
using Extend0.Metadata;
using Extend0.Metadata.CrossProcess;
using Extend0.Metadata.Schema;

using var metaDb = MetaDB.CreateSingleton();

var spec = new TableSpec(
    Name: "Settings",
    MapPath: "./data/settings.meta",
    Columns: new[]
    {
        TableSpec.Helpers.Column("Entries", capacity: 512, keyBytes: 64, valueBytes: 512),
    });

var tableId = MetaDBManagerSingleton.Service.RegisterTable(spec, createNow: true);
var columnNames = MetaDBManagerSingleton.Service.GetColumnNames(tableId);
var preview = MetaDBManagerSingleton.Service.PreviewTable(tableId);

Console.WriteLine(string.Join(", ", columnNames));
Console.WriteLine(preview);
```

These examples are intentionally aligned with the public access surfaces instead of documenting direct construction of the internal `MetaDBManager`.

## Ontology

The ontology under `ontology/` is now part of the architecture contract of Extend0 major `1`.

Its role in the current phase is:

- define stable domain concepts above implementation names
- expose conceptual inconsistencies between code, docs, and architecture
- keep demo-specific material out of the core TBox unless it becomes accepted architecture
- prepare later ontology-aware and cross-service integration work without forcing it early

The current TBox keeps these concepts central:

- `Lifecycle`
- `MetaDB`
- `Transport`
- `ServiceIdentity`
- `AccessSurface`
- `OwnershipClaim`
- `Lease`
- `HeartbeatSignal`

The current ontology tooling is intentionally lightweight but already usable:

```bash
python ontology/skills/ontology-query/query.py classes
python ontology/skills/ontology-query/query.py class MetaDBSystem
python ontology/skills/ontology-query/query.py sparql "SELECT ?c WHERE { ?c rdf:type owl:Class . }"
python ontology/diagnostics/abox-doctor.py
```

The query tool is designed for quick human or AI inspection of the TBox and example ABox, while `abox-doctor.py` provides a minimal diagnostic and fix-document scaffold for the current phase.

## Code Generation

Extend0 currently includes two source-generation surfaces:

- `Extend0.MetadataEntry.Generator` for fixed-layout metadata entry shapes
- `Extend0.BlittableAdapter.Generator` for blittable payload adapters

In major `1`, these are treated as schema-driven artifact generation, not as isolated tooling.
The metadata-entry catalog shipped in `Extend0/Metadata/Generator.attributes.cs` provides built-in shapes, but consumers can declare additional `[GenerateMetadataEntry(keyBytes, valueBytes)]` entries in their own assemblies when they need custom blittable layouts.

## Documentation and ADRs

The architectural contract for major `1` lives in:

- [`docs/Home.md`](docs/Home.md)
- [`docs/ADR.md`](docs/ADR.md)
- [`docs/ADR/1-000-EXTEND0-ADR-DEFINE-EXTEND0-MAJOR-VERSION-1.md`](docs/ADR/1-000-EXTEND0-ADR-DEFINE-EXTEND0-MAJOR-VERSION-1.md)
- [`docs/ADR/1-003-ARCHITECTURE-ADR-MODEL-EXTEND0-AS-A-PLATFORM-OF-COOPERATING-SYSTEMS.md`](docs/ADR/1-003-ARCHITECTURE-ADR-MODEL-EXTEND0-AS-A-PLATFORM-OF-COOPERATING-SYSTEMS.md)
- [`docs/ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md`](docs/ADR/1-004-LIFECYCLE-ADR-ADOPT-LIFECYCLE-AS-SERVICE-IDENTITY-AND-UNIQUE-ACCESS-SYSTEM.md)
- [`docs/ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md`](docs/ADR/1-005-METADB-ADR-ADOPT-METADB-AS-STRUCTURED-METADATA-SYSTEM.md)

If you want the shortest possible read of the current direction, start with `README`, then `ADR 1-000`, `ADR 1-003`, `ADR 1-004`, and `ADR 1-005`.
