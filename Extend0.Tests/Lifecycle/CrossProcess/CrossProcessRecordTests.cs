using Extend0.Lifecycle.CrossProcess;

namespace Extend0.Tests.Lifecycle.CrossProcess;

public sealed class CrossProcessRecordTests
{
    [Fact]
    public void ServiceInfo_AndHeartbeat_UseRecordValueSemantics()
    {
        var started = DateTimeOffset.UtcNow;
        var info = new ServiceInfo("IContract", "Impl", "1.0.0", "fingerprint", "machine", 42, "proc", started, "pipe", "endpoint", "server", TransportKind.NamedPipe);
        var sameInfo = info with { };
        var heartbeat = new Heartbeat(started, 12, "fingerprint");
        var sameHeartbeat = heartbeat with { };

        Assert.Equal(info, sameInfo);
        Assert.Equal(heartbeat, sameHeartbeat);
        Assert.Equal(TransportKind.NamedPipe, info.TransportKind);
        Assert.Equal("fingerprint", heartbeat.Fingerprint);
    }
}
