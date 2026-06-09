namespace Extend0.Cli;

internal sealed record MetaDbSchemaOptions(string? SourcePath, string? TargetPath, bool Json, bool ShowHelp)
{
    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out MetaDbSchemaOptions options,
        out string error)
    {
        string? sourcePath = null;
        string? targetPath = null;
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

                default:
                    if (arg.StartsWith("-", StringComparison.Ordinal))
                    {
                        options = new MetaDbSchemaOptions(sourcePath, targetPath, json, showHelp);
                        error = $"Unknown metadb schema option '{arg}'.";
                        return false;
                    }

                    var resolved = ResolvePath(arg, workingDirectory);
                    if (sourcePath is null)
                        sourcePath = resolved;
                    else if (targetPath is null)
                        targetPath = resolved;
                    else
                    {
                        options = new MetaDbSchemaOptions(sourcePath, targetPath, json, showHelp);
                        error = "metadb schema accepts exactly two path arguments.";
                        return false;
                    }

                    break;
            }
        }

        options = new MetaDbSchemaOptions(sourcePath, targetPath, json, showHelp);
        if (!showHelp && (string.IsNullOrWhiteSpace(sourcePath) || string.IsNullOrWhiteSpace(targetPath)))
        {
            error = "metadb schema requires source and target path arguments.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    private static string ResolvePath(string path, string workingDirectory) =>
        Path.IsPathRooted(path) ? path : Path.Combine(workingDirectory, path);
}
