# Extend0

Extend0 is a small .NET utility library that provides two main building blocks:

- **Lifecycle primitives** for in-process and cross-process singletons, backed by named pipes and a lightweight RPC proxy.
- **Task utilities** that make it safer to run fire-and-forget asynchronous work.

The library is designed for services that need a single owner across multiple processes while still allowing simple in-process use when IPC is unnecessary.

## Installation

Add a reference to the project from your solution. For example, from the repository root:

```bash
dotnet add <YourProject>.csproj reference Extend0/Extend0.csproj
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
