# Extend0

Extend0 is a small .NET utility library that provides three main building blocks:

- **Lifecycle primitives** for in-process and cross-process singletons, backed by named pipes and a lightweight RPC proxy.
- **Task utilities** that make it safer to run fire-and-forget asynchronous work.
- **Metadata storage and generators** for packing fixed-layout key/value pairs and blittable payloads into mapped tables without allocations.

The library is designed for services that need a single owner across multiple processes while still allowing simple in-process use when IPC is unnecessary, and for tools that need predictable, allocation-free metadata persistence.

## Installation

Add project references from your solution. For example, from the repository root:

```bash
dotnet add <YourProject>.csproj reference Extend0/Extend0.csproj
# Optional source generators
dotnet add <YourProject>.csproj reference Extend0.MetadataEntry.Generator/Extend0.MetadataEntry.Generator.csproj
dotnet add <YourProject>.csproj reference Extend0.BlittableAdapter.Generator/Extend0.BlittableAdapter.Generator.csproj
```

The package targets `net9.0` and depends on `Microsoft.Extensions.Logging.Abstractions` for optional logging. But an update to `net10.0` is planned soon.

## Cross-process singletons

The `Extend0.Lifecycle.CrossProcess` namespace exposes the components needed to host a single service instance across processes:

- `CrossProcessSingleton<TService>` wires up ownership, IPC hosting, and the static `Service` accessor.
- `CrossProcessServiceBase<TService>` provides diagnostics helpers (`PingAsync`, `GetServiceInfoAsync`, `CanConnectAsync`) and hosting utilities for named-pipe servers.
- `CrossProcessSingletonOptions` controls whether you run in-process or cross-process, which pipe name to use, and how aggressively to overwrite existing owners.

### Quickstart

The snippet below shows a simple clock service that runs as a cross-process singleton. The first process to start becomes the owner (hosting the named-pipe server); subsequent processes transparently act as clients through the generated proxy.

```csharp
using Extend0.Lifecycle.CrossProcess;
using Microsoft.Extensions.Logging;

var closing = false;
using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Debug)
        .AddProvider(new SomeLoggerProvider...());
});

var loggerInstance = loggerFactory.CreateLogger<Clock>();
var _instance = new ClockSingleton(loggerInstance);

Console.CancelKeyPress += (_, __) => closing = true;

while (!closing)
{
    Console.Clear();
    try
    {
        Console.WriteLine(ClockSingleton.IsOwner
            ? $"[Owner {ClockSingleton.Service.ContractName}]"
            : $"[Client {ClockSingleton.Service.ContractName}]");

        if (!ClockSingleton.IsOwner)
        {
            Console.WriteLine(await ClockSingleton.Service.PingAsync());
            //Console.WriteLine(await ClockSingleton.Service.GetServiceInfoAsync());
            Console.WriteLine(await ClockSingleton.Service.NowIsoAsync());
        }
    }
    catch (RemoteInvocationException rEx) when (rEx.HResult == 426)
    {
        // Swallow upgrade-in-progress errors.
    }

    await Task.Delay(1000);
}

public interface IClock : ICrossProcessService
{
    Task<string> NowIsoAsync();
}

public sealed class Clock : CrossProcessServiceBase<IClock>, IClock
{
    protected override string? PipeName => "Extend0.Clock";
    public Task<string> NowIsoAsync() => Task.FromResult(DateTimeOffset.UtcNow.ToString("O"));
}

public sealed class ClockSingleton(ILogger logger) : CrossProcessSingleton<IClock>(
    () => new Clock(),
    new()
    {
        Mode = SingletonMode.CrossProcess,
        CrossProcessName = "Extend0.Clock",
        CrossProcessServer = ".",
        CrossProcessConnectTimeoutMs = 5000,
        Overwrite = true,
        Logger = logger
    })
{
    static ClockSingleton()
    {
        RpcDispatchProxy<IClock>.UpgradeHandler = static async ex =>
        {
            var loggerFactory = new LoggerFactory();
            var logger = loggerFactory.CreateLogger<Clock>();

            try
            {
                // Re-create singleton when the owner restarts.
                _ = new ClockSingleton(logger);

                Console.WriteLine("[Upgrade] Recreated ClockSingleton. IsOwner = {0}", IsOwner);

                await Task.Yield();
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("[Upgrade] Failed to recreate ClockSingleton: {0}", e);
                return false;
            }
        };
    }
}
```

Key behaviors to remember:

- Constructing `ClockSingleton` initializes the static `ClockSingleton.Service` property. Clients and owners use the same API.
- `SingletonMode.CrossProcess` enforces a single owner across processes; switch to `SingletonMode.InProcess` to bypass IPC for tests.
- `CrossProcessSingletonOptions.Overwrite` controls whether a new instance replaces an existing owner (useful for upgrades or crash recovery).
- `CrossProcessServiceBase` implements the contract helpers (`PingAsync`, `GetServiceInfoAsync`, `CanConnectAsync`) so your service only needs to provide domain methods.

## Metadata storage and generators

Extend0’s metadata layer provides fixed-size, allocation-free key/value storage backed by memory-mapped files. Two source generators help you define the binary shapes that live in these tables:

- **`Extend0.MetadataEntry.Generator`** reads `[assembly: GenerateMetadataEntry(keyBytes, valueBytes)]` attributes and emits blittable `MetadataEntry{Key}x{Value}` structs plus a typed `MetadataCell` wrapper. The repository declares a catalog of common shapes in [`Metadata/Generator.attributes.cs`](Extend0/Metadata/Generator.attributes.cs), covering small tag-style keys up to larger “chubby” entries.
- **`Extend0.BlittableAdapter.Generator`** consumes `*.blit.json` files and generates blittable structs with inline UTF-8/binary buffers. These adapters are intended for use as typed value columns inside metadata tables.

### Quickstart: defining and using a table

1. Declare one or more entry shapes (or use the defaults in `Generator.attributes.cs`):

    ```csharp
    [assembly: Extend0.Metadata.CodeGen.GenerateMetadataEntry(64, 512)]
    ```

2. Describe a table layout using `TableSpec`, mixing entry cells and blittable payloads:

    ```csharp
    using Extend0.Metadata;
    using Extend0.Metadata.Schema;

    var spec = new TableSpec(
        Name: "Settings",
        MapPath: "./data/settings.meta",
        Columns: new[]
        {
            // Key/value entry column with 64-byte keys and 512-byte values
            TableSpec.Column<MetadataEntry64x512>("Entries", capacity: 512),

            // Blittable payload column generated from a .blit.json file
            TableSpec.Column<MyBlittablePayload>("Payload", capacity: 512)
        });
    spec.SaveToFile("./data/settings.spec.json");
    ```

3. Register and use the table through `MetaDBManager`:

    ```csharp
    using Extend0.Metadata;

    var manager = new MetaDBManager(logger: null);
    var tableId = manager.RegisterTable(spec, createNow: true);

    if (manager.TryGetCreated(spec.Name, out var table) &&
        table.TryGetCell("Entries", row: 0, out var cell))
    {
        // `cell` is a view over the fixed-size buffer described by the generated entry type.
    }
    ```

This workflow keeps your on-disk layout deterministic while letting Roslyn generate the unsafe structs needed to interact with the metadata store safely.

## MetaDB – Performance notes

This section documents the current micro-benchmarks for `MetaDBManager` and its columnar storage engine.  
All numbers below come from `MetaDBManagerBench` (BenchmarkDotNet) and are meant both as a sanity check and as a rough “performance contract” for future work.
In benchmarks with large, sequential column operations, MetaDBManager becomes memory-bandwidth bound and behaves similarly to a linear memcpy (O(n)), saturating L1, L2 and L3 caches. So throughput is effectively limited by your hardware.

---

### Benchmark setup

```text
BenchmarkDotNet v0.15.4  
OS      : Windows 11 (24H2)
CPU     : AMD Ryzen 7 4800H (8C / 16T @ 2.90 GHz)
Runtime : .NET 9.0.8 (RyuJIT x64-v3)

Job config:
- Runtime=.NET 9.0
- LaunchCount   = 1
- WarmupCount   = 1
- IterationCount= 5
````

Common parameters across most benchmarks:

* `Cols`           : 7
* `RowsPerCol`     : 24 or 2048 (small vs large tables)
* `KeySize`        : 16 bytes
* `ValueSize`      : 64 or 256 bytes
* `Ops`            : 10,000 logical operations per benchmark
* `ChildPoolSize`  : 16
* `RefsPerBatch`   : 1, 4 or 16 (for ref-related tests)

---

### Benchmark groups

The suite is organized into high-level categories:

1. **Copy**

   * `Copy_A_to_B_Column0_AllRows`
   * `Copy_InPlace_TableA_Col0_to_Col1_AllRows`
   * Measures raw column copy throughput (inter-table and in-place).

2. **Fill**

   * `Fill_Column0_TableA`
   * `Fill_Column0_TableB`
   * `Fill_Typed_Small16`
   * `Fill_Typed_Exact`
   * `Fill_Raw_Writer`
   * Measures different fill strategies (generic vs strongly-typed vs raw writer).

4. **Refs**

   * `EnsureRefVec_*`
   * `LinkRef_*`
   * `EnumerateRefs_CountSum`
   * Focused on **reference vectors**, link operations and bulk linking from pools.

5. **Register**

   * `RegisterTable_Lazy_NoPersist`
   * `RegisterTable_Eager_DisposeAndDelete`
   * Exercises table registration / lifecycle paths, with and without persistence.

---

### Copy performance (columnar moves)

Representative results (KeySize=16, ValueSize=64):

| Scenario                             | Rows/Col | ValueSize |    Mean (ns) | GB/s (rd+wr) |
| ------------------------------------ | -------- | --------- | -----------: | -----------: |
| `Copy_A_to_B_Column0_AllRows`        | 24       | 64        |     ~104–107 |       ~28–30 |
| `Copy_InPlace_TableA_Col0_to_Col1_*` | 24       | 64        |     ~104–107 |       ~28–29 |
| `Copy_A_to_B_Column0_AllRows`        | 2048     | 64        | ~3,709–3,727 |       ~70–71 |
| `Copy_InPlace_TableA_Col0_to_Col1_*` | 2048     | 64        | ~3,709–3,739 |       ~70–71 |

Key takeaways:

* For **small tables** (24 rows) the copy kernels run around **100 ns** per op.
* For **large tables** (2048 rows) we hit around **35 GB/s write** and **~70 GB/s aggregate read+write**.
* In-place copies (`Col0 → Col1` in the same table) are within a few percent of inter-table copies, which is good: the in-place path does not introduce hidden overhead.

---

### Fill performance

Representative results for `RowsPerCol=24`, `KeySize=16`:

| Method                | ValueSize | Mean (ns) | Relative to `Fill_Column0_TableA` |
| --------------------- | --------- | --------: | --------------------------------: |
| `Fill_Column0_TableA` | 64        |  ~439–457 |                             1.00× |
| `Fill_Column0_TableB` | 64        |  ~420–450 |                       ~0.95–1.03× |
| `Fill_Typed_Small16`  | 64        |  ~195–202 |                       ~0.44–0.46× |
| `Fill_Raw_Writer`     | 64        |  ~410–417 |                       ~0.93–0.95× |
| `Fill_Typed_Exact`    | 64        |  ~272–281 |                       ~0.62–0.65× |

For `ValueSize=256` the patterns are similar:

* `Fill_Typed_Small16` stays around **~200 ns**, outperforming the generic column-fill.
* `Fill_Typed_Exact` is a bit slower for large values but still competitive vs the baseline column fill.
* Raw writer sits close to the column fill baseline (it’s more of a “control” path here).

High-level conclusions:

* **Strongly-typed fills (`Fill_Typed_*`) are consistently the fastest way** to push data into the engine, especially for smaller value sizes.
* The generic column fill (`Fill_Column0_TableA/B`) is competitive, but you pay around **2×** versus the specialized typed path on small structs.

---

### Ref vectors & linking

The ref-related benchmarks exercise the machinery that manages **reference vectors** and **bulk linking**.

Representative small-table numbers (`RowsPerCol=24`, `ValueSize=64`):

| Method                              |   Mean (ns) | Notes                                       |
| ----------------------------------- | ----------: | ------------------------------------------- |
| `EnsureRefVec_Cold_InitOnce`        |     ~2,8 µs | First allocation / cold path                |
| `EnsureRefVec_Idempotent_Only`      | ~2,7–2,8 µs | “Already initialized” path (idempotent)     |
| `EnsureRefVec_And_LinkRef_FromPool` | ~3,8–4,2 µs | Ensure + link in one go                     |
| `LinkRef_Bulk_PerRow`               |     ~2,7 ns | Only the linking work (ref pool pre-primed) |

On large tables (`RowsPerCol=2048`):

* Ensure+link runs around **~360–550 µs**, depending on `ValueSize` and batch params.
* Bulk linking alone (`LinkRef_Bulk_PerRow`) stays noticeably faster, roughly **0.7–0.8×** of the ensure+link combo.

Design-wise:

* Ensuring the ref vector has a **one-time cost** (cold path) that is acceptable given its rare usage.
* The **idempotent** path is close in cost to pure linking, which means we can call `EnsureRefVec` defensively without paying a big penalty when it is already initialized.

---

### Table registration

The `Register` benchmarks focus on **table lifecycle**:

* `RegisterTable_Lazy_NoPersist`

  * Around **1.9–2.0 µs**.
  * Allocations ≈ **4.7–4.8 KB**.
  * This is the fast path for ephemeral tables.

* `RegisterTable_Eager_DisposeAndDelete`

  * Around **2.2 ms** and above.
  * Allocations ≈ **11 KB** and visible `Gen0` activity.
  * This path simulates a heavier “register + persist + dispose” lifecycle and is not expected to run frequently in hot loops.

In other words:

* **Lazy registration** is cheap enough to use at runtime for dynamic metadata.
* **Eager + persistent** registration is more expensive by design, and should be used for long-lived / persisted tables (startup, migrations, etc.).

---

### Summary

* Column copies achieve **tens of GB/s** of effective throughput with both inter-table and in-place paths.
* Typed fill paths (`Fill_Typed_*`) are the **preferred API for hot paths**, halving the cost versus generic column fills in typical configurations.
* Integrity and enumeration benchmarks give us a baseline for **correctness checks** and highlight where allocations occur.
* Ref-vector initialization and linking are within a small constant factor of raw linking, so the API can stay **ergonomic and defensive** without wrecking performance.
* Table registration is split into **fast, lazy registration** and **heavier eager/persistent registration**, which matches the expected usage patterns.

These numbers are my current “performance budget”. Any future changes to `MetaDBManager` should be validated against this suite to avoid silent regressions.

## Task utilities

`TaskExtensions.Forget` lets you safely execute background tasks while:

- Observing and logging exceptions (optional `ILogger`).
- Running custom callbacks on failure or completion.
- Recording execution time when `measureDuration` is enabled.

Example:

```csharp
SomeAsyncOperation()
    .Forget(_logger,
            onExceptionMessage: "Background operation failed",
            onExceptionAction: ex => Telemetry.TrackException(ex),
            finallyAction: () => _metrics.Increment("background.done"),
            measureDuration: true);
```

This pattern keeps fire-and-forget work from surfacing unobserved exceptions and provides consistent diagnostics hooks.

## Building and testing

From the repository root:

```bash
dotnet build
```

The library contains no unit tests by default; add your own in consumer solutions as needed.
