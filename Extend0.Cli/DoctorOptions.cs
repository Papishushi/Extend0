namespace Extend0.Cli;

internal sealed record DoctorOptions(string RepositoryRoot, bool Json, bool ShowHelp)
{
    public static bool TryParse(
        string[] args,
        string workingDirectory,
        out DoctorOptions options,
        out string error)
    {
        var repositoryRoot = workingDirectory;
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

                case "--repo":
                    if (i + 1 >= args.Length)
                    {
                        options = new DoctorOptions(repositoryRoot, json, showHelp);
                        error = "--repo requires a path value.";
                        return false;
                    }

                    repositoryRoot = args[++i];
                    break;

                default:
                    options = new DoctorOptions(repositoryRoot, json, showHelp);
                    error = $"Unknown doctor option '{arg}'.";
                    return false;
            }
        }

        options = new DoctorOptions(repositoryRoot, json, showHelp);
        error = string.Empty;
        return true;
    }
}
