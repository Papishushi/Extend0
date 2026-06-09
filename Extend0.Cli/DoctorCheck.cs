namespace Extend0.Cli;

public sealed record DoctorCheck(string Id, DoctorStatus Status, string Message)
{
    public static DoctorCheck Pass(string id, string message) =>
        new(id, DoctorStatus.Pass, message);
}
