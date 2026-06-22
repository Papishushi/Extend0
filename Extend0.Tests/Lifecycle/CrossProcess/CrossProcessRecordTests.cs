using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessRecordTests
{
    [Fact]
    public void ServiceInfo_Heartbeat_AndLease_UseRecordValueSemantics()
    {
        var started = DateTimeOffset.UtcNow;
        var info = new ServiceInfo("IContract", "Impl", "1.0.0", "fingerprint", "machine", 42, "proc", started, "pipe", "endpoint", "server", TransportKind.NamedPipe);
        var sameInfo = info with { };
        var heartbeat = new Heartbeat(started, 12, "fingerprint");
        var sameHeartbeat = heartbeat with { };
        var lease = new Lease(
            "lease",
            "IContract",
            "ownership",
            "fingerprint",
            "machine",
            42,
            "proc",
            started,
            started.AddSeconds(1),
            null,
            "endpoint",
            "server",
            TransportKind.NamedPipe,
            "OSMutex",
            "ownership",
            "Global",
            true,
            true);
        var sameLease = lease with { };

        Assert.Equal(info, sameInfo);
        Assert.Equal(heartbeat, sameHeartbeat);
        Assert.Equal(lease, sameLease);
        Assert.Equal(TransportKind.NamedPipe, info.TransportKind);
        Assert.Equal("fingerprint", heartbeat.Fingerprint);
        Assert.Equal("ownership", lease.OwnershipName);
    }
}
