namespace Extend0.Cli;

internal sealed record MetaDbSnapshotOptions(
    string? InputPath,
    string? OutputDirectory,
    string? Label,
    bool Overwrite,
    bool Json,
    bool ShowHelp)
{
    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out MetaDbSnapshotOptions options,
        out string error)
    {
        string? inputPath = null;
        string? outputDirectory = null;
        string? label = null;
        var overwrite = false;
        var json = false;
        var showHelp = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "-h":
                case "--help":
                    showHelp = true;
                    break;

                case "--json":
                    json = true;
                    break;

                case "--overwrite":
                    overwrite = true;
                    break;

                case "--out":
                    if (!TryReadValue(args, ref i, "--out", out outputDirectory, out error))
                    {
                        options = new MetaDbSnapshotOptions(inputPath, outputDirectory, label, overwrite, json, showHelp);
                        return false;
                    }

                    outputDirectory = ResolvePath(outputDirectory, workingDirectory);
                    break;

                case "--label":
                    if (!TryReadValue(args, ref i, "--label", out label, out error))
                    {
                        options = new MetaDbSnapshotOptions(inputPath, outputDirectory, label, overwrite, json, showHelp);
                        return false;
                    }

                    break;

                default:
                    if (arg.StartsWith("-", StringComparison.Ordinal))
                    {
                        options = new MetaDbSnapshotOptions(inputPath, outputDirectory, label, overwrite, json, showHelp);
                        error = $"Unknown metadb snapshot option '{arg}'.";
                        return false;
                    }

                    if (inputPath is not null)
                    {
                        options = new MetaDbSnapshotOptions(inputPath, outputDirectory, label, overwrite, json, showHelp);
                        error = "metadb snapshot accepts exactly one path argument.";
                        return false;
                    }

                    inputPath = ResolvePath(arg, workingDirectory);
                    break;
            }
        }

        options = new MetaDbSnapshotOptions(inputPath, outputDirectory, label, overwrite, json, showHelp);
        if (!showHelp && string.IsNullOrWhiteSpace(inputPath))
        {
            error = "metadb snapshot requires a path argument.";
            return false;
        }

        if (!showHelp && string.IsNullOrWhiteSpace(outputDirectory))
        {
            error = "metadb snapshot requires --out <snapshot-dir>.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    private static bool TryReadValue(string[] args, ref int index, string option, out string value, out string error)
    {
        if (index + 1 >= args.Length || args[index + 1].StartsWith("-", StringComparison.Ordinal))
        {
            value = string.Empty;
            error = $"{option} requires a value.";
            return false;
        }

        value = args[++index];
        error = string.Empty;
        return true;
    }

    private static string ResolvePath(string path, string workingDirectory) =>
        Path.IsPathRooted(path) ? path : Path.Combine(workingDirectory, path);
}
