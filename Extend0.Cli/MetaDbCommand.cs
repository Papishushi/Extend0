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
        if (string.Equals(command, "validate", StringComparison.OrdinalIgnoreCase))
            return MetaDbValidateCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);
        if (string.Equals(command, "schema", StringComparison.OrdinalIgnoreCase))
            return MetaDbSchemaCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);
        if (string.Equals(command, "snapshot", StringComparison.OrdinalIgnoreCase))
            return MetaDbSnapshotCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);
        if (string.Equals(command, "restore", StringComparison.OrdinalIgnoreCase))
            return MetaDbRestoreCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);

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
        writer.WriteLine("  extend0 metadb validate <path> [--security] [--require-protection <level>] [--json]");
        writer.WriteLine("  extend0 metadb schema <source> <target> [--json]");
        writer.WriteLine("  extend0 metadb snapshot <path> --out <snapshot-dir> [--label <text>] [--overwrite] [--json]");
        writer.WriteLine("  extend0 metadb restore <snapshot-dir> --map-path <path> [--overwrite] [--json]");
        writer.WriteLine();
        writer.WriteLine("Commands:");
        writer.WriteLine("  inspect    Read a TableSpec from a spec file, map path resolved by TableSpec conventions, or chunked table directory.");
        writer.WriteLine("  validate   Validate TableSpec shape, layout guardrails, resolved spec path, and optional storage protection evidence.");
        writer.WriteLine("  schema     Compare two TableSpecs and print compatibility plus migration plan.");
        writer.WriteLine("  snapshot   Capture a TableSpec and materialized runtime storage files.");
        writer.WriteLine("  restore    Restore a snapshot to an explicit map path or chunked table directory.");
    }
}
