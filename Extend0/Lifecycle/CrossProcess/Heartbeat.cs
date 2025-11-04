namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Minimal heartbeat payload for liveness checks.
    /// </summary>
    public sealed record Heartbeat(
        DateTimeOffset UtcTime,
        long UptimeSeconds,
        string Fingerprint
    );
}
