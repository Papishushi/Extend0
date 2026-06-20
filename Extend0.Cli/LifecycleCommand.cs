namespace Extend0.Cli;

public static class LifecycleCommand
{
    public static Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentException.ThrowIfNullOrWhiteSpace(workingDirectory);

        if (args.Length == 0 || IsHelp(args[0]))
        {
            WriteHelp(output);
            return Task.FromResult(0);
        }

        var command = args[0];
        if (string.Equals(command, "probe", StringComparison.OrdinalIgnoreCase))
            return LifecycleProbeCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);
        if (string.Equals(command, "diagnose", StringComparison.OrdinalIgnoreCase)
            || string.Equals(command, "diagnostics", StringComparison.OrdinalIgnoreCase))
        {
            return LifecycleDiagnoseCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);
        }

        error.WriteLine($"Unknown lifecycle command '{command}'.");
        error.WriteLine();
        WriteHelp(error);
        return Task.FromResult(2);
    }

    private static bool IsHelp(string arg) =>
        string.Equals(arg, "-h", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "--help", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "help", StringComparison.OrdinalIgnoreCase);

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 lifecycle probe [--name <identity>] [--transport <kind>] [--endpoint <name>] [--connect] [--json]");
        writer.WriteLine("  extend0 lifecycle diagnose [--name <identity>] [--transport <kind>] [--endpoint <name>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Commands:");
        writer.WriteLine("  probe       Resolve lifecycle transport/protocol/endpoint details and optionally test connectivity.");
        writer.WriteLine("  diagnose    Connect to an owner, validate handshake, and report service info, lease status, and heartbeat.");
    }
}
