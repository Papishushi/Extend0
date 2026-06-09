namespace Extend0.Cli;

internal sealed record MetaDbValidateOptions(string? InputPath, bool Json, bool ShowHelp)
{
    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out MetaDbValidateOptions options,
        out string error)
    {
        string? inputPath = null;
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
                        options = new MetaDbValidateOptions(inputPath, json, showHelp);
                        error = $"Unknown metadb validate option '{arg}'.";
                        return false;
                    }

                    if (inputPath is not null)
                    {
                        options = new MetaDbValidateOptions(inputPath, json, showHelp);
                        error = "metadb validate accepts exactly one path argument.";
                        return false;
                    }

                    inputPath = Path.IsPathRooted(arg)
                        ? arg
                        : Path.Combine(workingDirectory, arg);
                    break;
            }
        }

        options = new MetaDbValidateOptions(inputPath, json, showHelp);
        if (!showHelp && string.IsNullOrWhiteSpace(inputPath))
        {
            error = "metadb validate requires a path argument.";
            return false;
        }

        error = string.Empty;
        return true;
    }
}
