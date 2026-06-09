namespace Extend0.Cli;

public static class MetaDbCommand
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
        if (string.Equals(command, "inspect", StringComparison.OrdinalIgnoreCase))
            return MetaDbInspectCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);

        error.WriteLine($"Unknown metadb command '{command}'.");
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
        writer.WriteLine("  extend0 metadb inspect <path> [--json]");
        writer.WriteLine();
        writer.WriteLine("Commands:");
        writer.WriteLine("  inspect    Read a TableSpec from a spec file, map path sidecar, or chunked table directory.");
    }
}
