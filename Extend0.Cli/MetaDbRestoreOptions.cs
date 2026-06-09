namespace Extend0.Cli;

internal sealed record MetaDbRestoreOptions(
    string? SnapshotDirectory,
    string? RestoreMapPath,
    bool Overwrite,
    bool Json,
    bool ShowHelp)
{
    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out MetaDbRestoreOptions options,
        out string error)
    {
        string? snapshotDirectory = null;
        string? restoreMapPath = null;
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

                case "--map-path":
                    if (!TryReadValue(args, ref i, "--map-path", out restoreMapPath, out error))
                    {
                        options = new MetaDbRestoreOptions(snapshotDirectory, restoreMapPath, overwrite, json, showHelp);
                        return false;
                    }

                    restoreMapPath = ResolvePath(restoreMapPath, workingDirectory);
                    break;

                default:
                    if (arg.StartsWith("-", StringComparison.Ordinal))
                    {
                        options = new MetaDbRestoreOptions(snapshotDirectory, restoreMapPath, overwrite, json, showHelp);
                        error = $"Unknown metadb restore option '{arg}'.";
                        return false;
                    }

                    if (snapshotDirectory is not null)
                    {
                        options = new MetaDbRestoreOptions(snapshotDirectory, restoreMapPath, overwrite, json, showHelp);
                        error = "metadb restore accepts exactly one snapshot directory argument.";
                        return false;
                    }

                    snapshotDirectory = ResolvePath(arg, workingDirectory);
                    break;
            }
        }

        options = new MetaDbRestoreOptions(snapshotDirectory, restoreMapPath, overwrite, json, showHelp);
        if (!showHelp && string.IsNullOrWhiteSpace(snapshotDirectory))
        {
            error = "metadb restore requires a snapshot directory argument.";
            return false;
        }

        if (!showHelp && string.IsNullOrWhiteSpace(restoreMapPath))
        {
            error = "metadb restore requires --map-path <path>.";
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
