using Extend0.Cli;

return await Extend0Cli.RunAsync(
    args,
    Console.Out,
    Console.Error,
    Directory.GetCurrentDirectory(),
    CancellationToken.None).ConfigureAwait(false);
