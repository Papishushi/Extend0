namespace Extend0.Cli;

public static class Extend0Cli
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
        if (string.Equals(command, "doctor", StringComparison.OrdinalIgnoreCase))
            return DoctorCommand.RunAsync(args[1..], output, error, workingDirectory, cancellationToken);

        error.WriteLine($"Unknown command '{command}'.");
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
        writer.WriteLine("Extend0 CLI");
        writer.WriteLine();
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 doctor [--repo <path>] [--json]");
        writer.WriteLine("  extend0 --help");
        writer.WriteLine();
        writer.WriteLine("Commands:");
        writer.WriteLine("  doctor    Inspect the repository contract for docs, ontology, tests, and core project alignment.");
    }
}
