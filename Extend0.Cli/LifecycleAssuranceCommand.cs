namespace Extend0.Cli;

public static class LifecycleAssuranceCommand
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
        if (string.Equals(command, "storage", StringComparison.OrdinalIgnoreCase))
            return RunStorageAsync(args[1..], output, error, workingDirectory, cancellationToken);

        error.WriteLine($"Unknown lifecycle assurance command '{command}'.");
        error.WriteLine();
        WriteHelp(error);
        return Task.FromResult(2);
    }

    private static Task<int> RunStorageAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        if (args.Length == 0 || IsHelp(args[0]))
        {
            WriteHelp(output);
            return Task.FromResult(0);
        }

        if (string.Equals(args[0], "diagnose", StringComparison.OrdinalIgnoreCase))
            return StorageDiagnoseCommand.RunLifecycleAssuranceAsync(args[1..], output, error, workingDirectory, cancellationToken);

        error.WriteLine($"Unknown lifecycle assurance storage command '{args[0]}'.");
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
        writer.WriteLine("  extend0 lifecycle assurance storage diagnose <path> [--require <level>] [--provider <id>] [--protection-id <id>] [--manifest <path>] [--json]");
        writer.WriteLine();
        writer.WriteLine("Commands:");
        writer.WriteLine("  storage diagnose   Verify lifecycle assurance evidence for protected storage paths.");
        writer.WriteLine();
        writer.WriteLine("Notes:");
        writer.WriteLine("  MetaDB owns physical file/storage validation; Lifecycle assurance owns cross-service guarantees such as protection, continuity, and attestation.");
        writer.WriteLine("  The legacy 'extend0 storage diagnose' command remains available as a compatibility alias.");
    }
}
