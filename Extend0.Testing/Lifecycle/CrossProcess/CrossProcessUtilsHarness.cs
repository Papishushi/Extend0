using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Testing.Lifecycle.CrossProcess;

public static class CrossProcessUtilsHarness
{
    public static string CurrentFingerprint => CrossProcessUtils.CurrentFingerprint;

    public static string BuildNameFor<T>(string? name) => CrossProcessUtils.BuildNameFor<T>(name);

    public static string BuildPipeName(string baseName, string? prefix = "CPS.") =>
        CrossProcessUtils.BuildPipeName(baseName, prefix);
}
